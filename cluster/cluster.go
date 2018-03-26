/*
 * Copyright 2018 Marco Helmich
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cluster

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/mhelmich/carbon-copy-go/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// The cluster construct consists of a few components taking over different tasks.
// There is serf for cluster membership and metadata gossip.
// There is raft for leader election and coordination tasks that need consensus. It basically implements a consistent data store.
//
// This introduces a little weird interdependency between the cluster metadata and
// and the consensus store. Metadata about who the raft leader is is distributed by
// serf. The raft leader broadcasts its status via serf. That means serf needs to listen
// for raft leader changes. At the same time raft needs to know where new nodes are located.
// This way it will be able to always form a big enough (but not too big) voter base for consensus.
//
// Membership captures the state of the serf cluster and makes the state available to the node.
// The raft cluster however needs to know who else is available to ensure a quorum.
// Serf in turn needs to be updated with all status changes to the cluster structure (such as raft leadership changes or additions and removals).
//
// That means when a cluster starts, the components are started in the following order:
// * start serf
// *** join the current cluster or form a new one
// *** if we join an existing cluster, process all events
// *** find all the metadata required for operation
// ***** that would mostly be the raft roles in the cluster (leader, voters, nonvoters, none)
// * start raft
// *** the raft leader will decide whether any new node should be promoted to voter or nonvoter (based on the number of nodes already fulfilling these roles)
// * start raft service
//
// Cluster metadata is a flat map with a bunch of keys.
// This metadata is kept track of and socialized by serf.
// The map includes all information necessary to manage the cluster,
// the membership to clusters and specific roles and tasks that need to be
// fulfilled within the cluster.
// serf_addr: <hostname>:<port> - the address on which serf for this node operates
// raft_addr: <hostname>:<port> - the address on which raft for this node operates
// raft_service_addr: <hostname>:<port> - the address on which the raft service for this node operates
// raft_role: leader, voter, nonvoter, none - the role a particular node has in the raft cluster
// grid_addr: <hostname>:<port> - the addres on which the grid messages are being exchanged
//
// TODO:
// * How to allocate a cluster-unique, short (int32) node id? -> raft - maybe even in its seperate command implementation
// * How to pass on changes in cluster membership?            -> forwarding serf events
//
// I could toy around with this one day
// "github.com/araddon/qlbridge"
// "github.com/couchbase/blance"
// "github.com/couchbase/moss"

const (
	nameSeparator             = "/"
	consensusNamespaceName    = "carbon-copy"
	consensusNodesRaftCluster = consensusNamespaceName + nameSeparator + "raftCluster" + nameSeparator
	consensusNodesRootName    = consensusNamespaceName + nameSeparator + "nodes" + nameSeparator
)

type kvStr struct {
	key   string
	value string
}

type kvBytes struct {
	key   []byte
	value []byte
}

func createNewCluster(config ClusterConfig) (*clusterImpl, error) {
	cs, err := createNewConsensusStore(config)
	if err != nil {
		return nil, err
	}

	m, err := createNewMembership(config)
	if err != nil {
		return nil, err
	}

	// create the service providing non-consensus nodes with values
	raftServer, err := createRaftService(config, cs)
	if err != nil {
		return nil, err
	}

	proxy, err := newConsensusStoreProxy(config, cs, m.raftLeaderAddrChan)
	if err != nil {
		return nil, err
	}

	ci := &clusterImpl{
		membership:          m,
		consensusStore:      cs,
		consensusStoreProxy: proxy,
		raftService:         raftServer,
		config:              config,
		logger:              config.logger,
	}

	go ci.leaderWatch()
	return ci, nil
}

func createRaftService(config ClusterConfig, consensusStore *consensusStoreImpl) (*raftServiceImpl, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", config.hostname, config.RaftServicePort))
	if err != nil {
		return nil, err
	}

	grpcServer := grpc.NewServer()

	raftServer := &raftServiceImpl{
		grpcServer:          grpcServer,
		localConsensusStore: consensusStore,
		logger:              config.logger,
	}

	pb.RegisterRaftServiceServer(grpcServer, raftServer)
	go grpcServer.Serve(lis)
	return raftServer, nil
}

type clusterImpl struct {
	consensusStore      *consensusStoreImpl
	consensusStoreProxy *consensusStoreProxy
	membership          *membership
	raftService         *raftServiceImpl
	logger              *log.Entry
	config              ClusterConfig
}

func (ci *clusterImpl) leaderWatch() {
	for { // ever...
		isLeader := <-ci.consensusStore.raftNotifyCh
		if isLeader {
			// ci.logger.Info("I'm the leader ... wheeee!")
			go ci.leaderLoop()
		} else {
			ci.logger.Info("I'm not the leader ... cruel world!")
		}
	}
}

func (ci *clusterImpl) leaderLoop() {
	// initial house keeping work for the raft leader
	// 1. get the current cluster state according to raft
	// 2. reconcile serf and raft state
	// 3. put yourself into the driver seat as leader

	rvsProto, _ := ci.newLeaderHouseKeeping()

	for { // ever...
		if ci.consensusStore.isRaftLeader() {
			//
			// wait until the cluster topology changes
			// then go through the same loop again
			// or after every so and so seconds for good measure
			//
			select {
			case newMemberId := <-ci.membership.memberJoined:
				// if we see a new node come up, add it to the raft cluster if necessary
				rvsProto = ci.addNewMemberToRaftCluster(newMemberId, rvsProto)
				ci.setRaftClusterState(rvsProto)

			case memberId := <-ci.membership.memberLeft:
				delete(rvsProto.Voters, memberId)
				delete(rvsProto.Nonvoters, memberId)
				delete(rvsProto.AllNodes, memberId)

				rvsProto = ci.ensureConsensusStoreVoters(rvsProto)
				ci.setRaftClusterState(rvsProto)
			}
		} else {
			ci.membership.unmarkLeader()
			ci.logger.Info("I'm not the leader anymore ... Goodbye cruel world!")
			return
		}
	}
}

func (ci *clusterImpl) newLeaderHouseKeeping() (*pb.RaftVoterState, error) {
	if !ci.consensusStore.isRaftLeader() {
		return &pb.RaftVoterState{}, nil
	}

	// update my serf status
	// a bit hacky but gets the job done
	err := errors.New("No real error")
	for err != nil {
		// update my own state in serf
		err = ci.membership.markLeader()
		if err == nil {
			ci.logger.Infof("Updated serf status to reflect me being raft leader.")
		} else {
			ci.logger.Infof("Updating serf failed: %s", err.Error())
		}
	}

	// get the current cluster state
	rvsProto, err := ci.getRaftClusterState()
	if err == nil {
		// update me as leader
		myMemberId := ci.membership.myMemberId()
		// set me as leader
		rvsProto.Voters[myMemberId] = true
		// get my node info proto going
		info, _ := ci.membership.getMemberById(myMemberId)
		nodeInfoProto, _ := ci.convertNodeInfoFromSerfToRaft(info)
		// add myself to the cluster
		rvsProto.Voters[myMemberId] = true
		rvsProto.AllNodes[myMemberId] = nodeInfoProto
		// yes, I'm paranoid like this
		delete(rvsProto.Nonvoters, myMemberId)
		// rebalance the cluster
		rvsProto = ci.ensureConsensusStoreVoters(rvsProto)
		// set in consensus store
		ci.setRaftClusterState(rvsProto)
	} else {
		log.Warnf("Can't get %s from consensus store: %s", consensusNodesRaftCluster, err.Error())
	}

	return rvsProto, nil
}

func (ci *clusterImpl) getRaftClusterState() (*pb.RaftVoterState, error) {
	bites, err := ci.consensusStore.get(consensusNodesRaftCluster)
	if err != nil {
		return nil, err
	}

	if bites == nil {
		rvsProto := &pb.RaftVoterState{
			Voters:    make(map[string]bool),
			Nonvoters: make(map[string]bool),
			AllNodes:  make(map[string]*pb.NodeInfo),
		}
		return rvsProto, nil
	} else {
		rvsProto := &pb.RaftVoterState{}
		err = proto.Unmarshal(bites, rvsProto)
		return rvsProto, err
	}
}

func (ci *clusterImpl) setRaftClusterState(rvsProto *pb.RaftVoterState) error {
	bites, err := proto.Marshal(rvsProto)
	if err != nil {
		return err
	}

	return ci.consensusStore.set(consensusNodesRaftCluster, bites)
}

func (ci *clusterImpl) ensureConsensusStoreVoters(rvsProto *pb.RaftVoterState) *pb.RaftVoterState {
	// make sure we have enough voters in our raft cluster
	numVoters := len(rvsProto.Voters)
	numVotersIWant := ci.config.NumRaftVoters - numVoters

	//
	// add new voters to raft cluster
	//
	if numVotersIWant > 0 {
		// recruit more nodes to be voters
		for memberId := range rvsProto.Nonvoters {
			nodeInfo, ok := rvsProto.AllNodes[memberId]
			if ok {
				raftAddr := fmt.Sprintf("%s:%d", nodeInfo.Host, nodeInfo.RaftPort)
				err := ci.consensusStore.addVoter(memberId, raftAddr)
				if err == nil {
					rvsProto.Voters[memberId] = rvsProto.Nonvoters[memberId]
					delete(rvsProto.Nonvoters, memberId)
					numVotersIWant--
				}

				if numVotersIWant <= 0 {
					return rvsProto
				}
			}
		}
	}

	return rvsProto
}

func (ci *clusterImpl) addNewMemberToRaftCluster(newMemberId string, rvsProto *pb.RaftVoterState) *pb.RaftVoterState {
	_, isVoter := rvsProto.Voters[newMemberId]
	_, isNonvoter := rvsProto.Nonvoters[newMemberId]
	if isVoter || isNonvoter {
		return rvsProto
	}

	numVoters := len(rvsProto.Voters)
	numVotersIWant := ci.config.NumRaftVoters - numVoters

	info, _ := ci.membership.getMemberById(newMemberId)
	nodeInfoProto, _ := ci.convertNodeInfoFromSerfToRaft(info)
	nodeInfoProto = ci.findShortNodeId(newMemberId, nodeInfoProto, rvsProto)
	raftAddr := fmt.Sprintf("%s:%d", nodeInfoProto.Host, nodeInfoProto.RaftPort)

	//
	// add new voters to raft cluster
	//
	if numVotersIWant > 0 {
		err := ci.consensusStore.addVoter(newMemberId, raftAddr)
		if err == nil {
			rvsProto.Voters[newMemberId] = true
			rvsProto.AllNodes[newMemberId] = nodeInfoProto
			delete(rvsProto.Nonvoters, newMemberId)
		}
	} else {
		err := ci.consensusStore.addNonvoter(newMemberId, raftAddr)
		if err == nil {
			rvsProto.Nonvoters[newMemberId] = true
			rvsProto.AllNodes[newMemberId] = nodeInfoProto
			delete(rvsProto.Voters, newMemberId)
		}
	}

	return rvsProto
}

func (ci *clusterImpl) findShortNodeId(memberId string, nodeInfo *pb.NodeInfo, rvsProto *pb.RaftVoterState) *pb.NodeInfo {
	// collect all node infos
	a := make([]*pb.NodeInfo, len(rvsProto.AllNodes))
	idx := 0
	for _, info := range rvsProto.AllNodes {
		a[idx] = info
		idx++
	}

	// sort the node infos by short id
	sort.Sort(byShortNodeId(a))

	// count the sorted node infos up until you find a gap
	newShortNodeId := -1
	for idx = 0; idx < len(a); idx++ {
		if a[idx].ShortNodeId != int32(idx+1) {
			newShortNodeId = idx + 1
			break
		}
	}

	// or take the next node id available
	if newShortNodeId == -1 {
		newShortNodeId = len(a) + 2
	}

	nodeInfo.ShortNodeId = int32(newShortNodeId)
	return nodeInfo
}

func (ci *clusterImpl) convertNodeInfoFromSerfToRaft(serfInfo map[string]string) (*pb.NodeInfo, error) {
	host := serfInfo[serfMDKeyHost]

	serfPort, err := strconv.Atoi(serfInfo[serfMDKeySerfPort])
	if err != nil {
		return nil, err
	}

	raftPort, err := strconv.Atoi(serfInfo[serfMDKeyRaftPort])
	if err != nil {
		return nil, err
	}

	raftServicePort, err := strconv.Atoi(serfInfo[serfMDKeyRaftServicePort])
	if err != nil {
		return nil, err
	}

	gridPort, err := strconv.Atoi(serfInfo[serfMDKeyGridPort])
	if err != nil {
		return nil, err
	}

	return &pb.NodeInfo{
		Host:            host,
		SerfPort:        int32(serfPort),
		RaftPort:        int32(raftPort),
		ValueServerPort: int32(raftServicePort),
		GridPort:        int32(gridPort),
	}, nil
}

func (ci *clusterImpl) GetMyNodeId() int {
	// if ci.myNodeId == -1 {
	// 	ci.myNodeId = <-ci.myNodeIdCh
	// }
	// return ci.myNodeId
	return -1
}

func (ci *clusterImpl) GetNodeConnectionInfoUpdates() (<-chan []*NodeConnectionInfo, error) {
	// kvBytesChan, err := ci.consensus.watchKeyPrefix(context.Background(), consensusNodesRootName)
	// if err != nil {
	// 	return nil, err
	// }

	// nodeConnInfoChan := make(chan []*NodeConnectionInfo)
	// go func() {
	// 	for kvBatchBytes := range kvBytesChan {
	// 		if len(kvBatchBytes) <= 0 {
	// 			close(nodeConnInfoChan)
	// 			return
	// 		}

	// 		nodeInfos := make([]*NodeConnectionInfo, len(kvBatchBytes))
	// 		for idx, kvBytes := range kvBatchBytes {
	// 			nodeInfoProto := &pb.NodeInfo{}
	// 			err = proto.Unmarshal(kvBytes.value, nodeInfoProto)
	// 			if err == nil {
	// 				nodeInfos[idx] = &NodeConnectionInfo{
	// 					nodeId:      int(nodeInfoProto.NodeId),
	// 					nodeAddress: nodeInfoProto.Host,
	// 				}
	// 			}
	// 		}

	// 		nodeConnInfoChan <- nodeInfos
	// 	}
	// }()

	// return nodeConnInfoChan, nil
	return nil, nil
}

func (ci *clusterImpl) printClusterState() {
	state, _ := ci.getRaftClusterState()

	ci.logger.Info("CLUSTER STATE:")
	ci.logger.Infof("Voters [%d]:", len(state.Voters))
	for id := range state.Voters {
		ci.logger.Infof("%s", id)
	}

	ci.logger.Infof("Nonvoters [%d]:", len(state.Nonvoters))
	for id := range state.Nonvoters {
		ci.logger.Infof("%s", id)
	}

	ci.logger.Infof("AllNodes [%d]:", len(state.AllNodes))
	for _, info := range state.AllNodes {
		ci.logger.Infof("%s", info.String())
	}
}

func (ci *clusterImpl) Close() {
	ci.membership.close()
	time.Sleep(1 * time.Second)
	ci.raftService.close()
	ci.consensusStore.close()
	ci.consensusStoreProxy.close()
	time.Sleep(1 * time.Second)
}

// boiler plate code to implement go sort interface
type byShortNodeId []*pb.NodeInfo

func (a byShortNodeId) Len() int {
	return len(a)
}
func (a byShortNodeId) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a byShortNodeId) Less(i, j int) bool {
	return a[i].ShortNodeId < a[j].ShortNodeId
}
