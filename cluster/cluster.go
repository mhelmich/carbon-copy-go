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
	"github.com/golang/protobuf/proto"
	"github.com/mhelmich/carbon-copy-go/pb"
	log "github.com/sirupsen/logrus"
	"strconv"
	"time"
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

type clusterImpl struct {
	consensusStore *consensusStoreImpl
	membership     *membership
	logger         *log.Entry
	config         ClusterConfig
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

	ci := &clusterImpl{
		membership:     m,
		consensusStore: cs,
		config:         config,
		logger:         config.logger,
	}

	go ci.leaderWatch()
	return ci, nil
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
			ci.logger.Info("I'm not the leader anymore ... Goodbye cruel world!")
			return
		}
	}
}

func (ci *clusterImpl) newLeaderHouseKeeping() (*pb.RaftVoterState, error) {
	if ci.consensusStore.isRaftLeader() {
		// update my serf status
		// a bit hacky but gets the job done
		err := errors.New("No real error")
		for err != nil {
			// update my own state in serf
			newTags := make(map[string]string)
			newTags[serfMDKeyRaftRole] = raftRoleLeader
			err = ci.membership.updateRaftTag(newTags)
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
			myNodeId := ci.membership.myNodeId()
			// set me as leader
			rvsProto.Voters[myNodeId] = true
			// get my node info proto going
			info, _ := ci.membership.getNodeById(myNodeId)
			nodeInfoProto, _ := ci.convertNodeIdSerfToRaft(info)
			// add myself to the cluster
			rvsProto.AllNodes[myNodeId] = nodeInfoProto
			// yes, I'm paranoid like this
			delete(rvsProto.Nonvoters, myNodeId)
			// rebalance the cluster
			rvsProto = ci.ensureConsensusStoreVoters(rvsProto)
			// set in consensus store
			ci.setRaftClusterState(rvsProto)
		} else {
			log.Warnf("Can't get %s from consensus store: %s", consensusNodesRaftCluster, err.Error())
		}

		return rvsProto, nil
	} else {
		return &pb.RaftVoterState{}, nil
	}
}

func (ci *clusterImpl) getRaftClusterState() (*pb.RaftVoterState, error) {
	bites, err := ci.consensusStore.Get(consensusNodesRaftCluster)
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

	return ci.consensusStore.Set(consensusNodesRaftCluster, bites)
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
		for memberId, _ := range rvsProto.Nonvoters {
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

	info, _ := ci.membership.getNodeById(newMemberId)
	nodeInfoProto, _ := ci.convertNodeIdSerfToRaft(info)
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

func (ci *clusterImpl) convertNodeIdSerfToRaft(serfInfo map[string]string) (*pb.NodeInfo, error) {
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
	for id, _ := range state.Voters {
		ci.logger.Infof("%s", id)
	}

	ci.logger.Infof("Nonvoters [%d]:", len(state.Nonvoters))
	for id, _ := range state.Nonvoters {
		ci.logger.Infof("%s", id)
	}

	ci.logger.Infof("AllNodes [%d]:", len(state.AllNodes))
	for _, info := range state.AllNodes {
		ci.logger.Infof("%s", info.String())
	}

	cf := ci.consensusStore.raft.GetConfiguration()
	ci.logger.Infof("raft status [%d] [%d]:", cf.Index(), len(cf.Configuration().Servers))
	for _, s := range cf.Configuration().Servers {
		ci.logger.Infof("%v", s)
	}
}

func (ci *clusterImpl) Close() {
	ci.membership.close()
	time.Sleep(1 * time.Second)
	ci.consensusStore.Close()
	time.Sleep(1 * time.Second)
}
