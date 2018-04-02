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
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/mhelmich/carbon-copy-go/pb"
	"github.com/oklog/ulid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// The cluster construct consists of a few components taking over different tasks.
// There is serf for cluster membership and metadata gossip.
// There is raft for leader election and coordination tasks that need consensus. It basically implements a consistent data store.
//
// This introduces a little weird interdependency between the cluster metadata and
// and the consensus store. Metadata about who the raft leader is distributed by
// serf. The raft leader broadcasts its status via serf. That means serf needs to listen
// for raft leader changes. At the same time raft needs to know where new nodes are located.
// This way it will be able to always form a big enough (but not too big) voter base for consensus.
//
// Membership captures the state of the serf cluster and makes the state available to the node.
// The raft cluster however needs to know who else is available to ensure a quorum.
// Serf in turn needs to be updated with all status changes to the cluster structure (such as raft leadership changes or additions and removals).
//
// That means when a cluster starts, the components are started in the following order:
// * start raft
// *** the raft leader will decide whether any new node should be promoted to voter or nonvoter (based on the number of nodes already fulfilling these roles)
// * start serf
// *** join the current cluster or form a new one
// *** if we join an existing cluster, process all events
// *** find all the metadata required for operation
// ***** that would mostly be the raft roles in the cluster (leader, voters, nonvoters, none)
// * start raft service
//
// Cluster metadata is a flat map with a bunch of keys.
// This metadata is kept track of and socialized by serf.
// The map includes all information necessary to manage the cluster,
// the membership to clusters and specific roles and tasks that need to be
// fulfilled within the cluster.
// host: the advertized hostname of this node
// serf_port: the port on which serf for this node operates
// raft_port: the port on which raft for this node operates
// raft_service_port: the port on which the raft service for this node operates
// raft_role: leader, voter, nonvoter - the role a particular node has in the raft cluster
// grid_port: the port on which the grid messages are being exchanged
//
// I could toy around with this one day
// "github.com/araddon/qlbridge"
// "github.com/couchbase/blance"
// "github.com/couchbase/moss"

const (
	nameSeparator            = "/"
	consensusNamespaceName   = "carbon-grid"
	consensusMembersRootName = consensusNamespaceName + nameSeparator + "members" + nameSeparator
	consensusLeaderName      = consensusNamespaceName + nameSeparator + "consensus_leader" + nameSeparator
	consensusVotersName      = consensusNamespaceName + nameSeparator + "consensus_voters" + nameSeparator
	consensusNonVotersName   = consensusNamespaceName + nameSeparator + "consensus_nonvoters" + nameSeparator
)

func defaultClusterConfig(config ClusterConfig) ClusterConfig {
	host, err := os.Hostname()
	if err != nil {
		log.Errorf("Can't get hostname: %s", err.Error())
	}

	if config.hostname == "" {
		config.hostname = host
	}

	if config.longMemberId == "" {
		config.longMemberId = ulid.MustNew(ulid.Now(), rand.Reader).String()
	}

	if config.raftNotifyCh == nil {
		config.raftNotifyCh = make(chan bool, 8)
	}

	if config.logger == nil {
		config.logger = log.WithFields(log.Fields{
			"host":         host,
			"component":    "cluster",
			"longMemberId": config.longMemberId,
		})
	}

	return config
}

func createNewCluster(config ClusterConfig) (*clusterImpl, error) {
	config = defaultClusterConfig(config)
	// the consensus store comes first
	cs, err := createNewConsensusStore(config)
	if err != nil {
		return nil, err
	}

	// this channel is used only once at startup
	// the method GetMyShortMemberId() will block on it
	// until the underlying watcher fires
	shortMemberIdChan := make(chan int)
	cs.addWatcher(consensusMembersRootName+config.longMemberId, func(key string, value []byte) {
		if key == consensusMembersRootName+config.longMemberId {
			mi := &pb.MemberInfo{}
			errUnMarshall := proto.Unmarshal(value, mi)
			if errUnMarshall != nil {
				config.logger.Errorf("Can't acquire short member id: %s", errUnMarshall.Error())
			}

			shortMemberId := int(mi.ShortMemberId)
			if shortMemberId > 0 {
				shortMemberIdChan <- int(mi.ShortMemberId)
				close(shortMemberIdChan)
			}
		}
	})

	// then we need membership to announce our presence to others (or not)
	m, err := createNewMembership(config)
	if err != nil {
		return nil, err
	}

	// create the service providing non-consensus nodes with values
	raftServer, err := createRaftService(config, cs)
	if err != nil {
		return nil, err
	}

	// now we create the cluster object
	// the reason is that the consensusStoreProxy needs to know where the raft leader lives
	// this information though needs to be parsed out of the serf event stream in our membership object
	// therefore I need to create the cluster object now, start the processor loop
	// and have things go their merry way
	ci := &clusterImpl{
		membership:         m,
		consensusStore:     cs,
		raftService:        raftServer,
		config:             config,
		logger:             config.logger,
		shortMemberId:      -1,
		gridMemberInfoChan: make(chan *GridMemberConnectionEvent),
	}
	go ci.eventProcessorLoop()

	// at last create the proxy
	proxy, err := newConsensusStoreProxy(config, cs, m.raftLeaderServiceAddrChan)
	if err != nil {
		return nil, err
	}

	// don't forget to set the proxy
	// I know this is a little brittle and I don't like partial objects myself
	// but a shortcut is a shortcut :)
	ci.consensusStoreProxy = proxy

	// wait for the short member id to become available
	// after it becomes available (via consensus watcher), set the short id locally,
	// remove the watcher (to not run that forever), update the new id in membership
	select {
	case newShortMemberId := <-shortMemberIdChan:
		if newShortMemberId > 0 {
			ci.shortMemberId = newShortMemberId
			ci.consensusStore.removeWatcher(consensusMembersRootName + ci.config.longMemberId)
			ci.membership.updateMemberTag(serfMDKeyShortMemberId, strconv.Itoa(ci.shortMemberId))
		}
	case <-time.After(3 * time.Second):
		return nil, fmt.Errorf("Getting short member id timed out!")
	}

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
		logger: config.logger.WithFields(log.Fields{
			"sub_component": "raft_service",
		}),
	}

	pb.RegisterRaftServiceServer(grpcServer, raftServer)
	// fire up the server
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
	shortMemberId       int
	gridMemberInfoChan  chan *GridMemberConnectionEvent
}

// this function keeps all notification channels empty and clean
func (ci *clusterImpl) eventProcessorLoop() {
	for { // ever...
		// big select over all channels I need to coordinate
		select {
		// this channel fires in case raft leadership changes
		// it can be true if the local node becomes the new leader
		// and false if some other member becomes the new leader
		case isLeader := <-ci.consensusStore.raftLeaderChangeNotifyCh:
			if isLeader {
				// initial house keeping work for the raft leader
				// 1. get the current cluster state according to raft
				// 2. reconcile serf and raft state
				//    * take the serf state as I see it
				//    * drop it into raft and keep collecting the state from serf events
				// 3. put yourself into the driver seat as leader
				if err := ci.newLeaderHouseKeeping(); err != nil {
					ci.logger.Errorf("Can't do my housekeeping: %s", err.Error())
				}
			} else {
				// just unmark myself from being the leader
				// this might be unnecessary but I'm paranoid like that
				if err := ci.membership.unmarkLeader(); err != nil {
					ci.logger.Errorf("Can't unmark myself as leader: %s", err.Error())
				}
			}
		case memberJoined := <-ci.membership.memberJoinedChan:
			if memberJoined == "" {
				// the channel was closed, we're done
				ci.logger.Warn("Member joined channel is closed stopping event processor loop")
				return
			}

			// if I'm the leader, I need to do all sorts of housekeeping
			// These things include:
			// 1. add the new member to the list of all members
			// 2. add the new member to the raft cluster (voter or nonvoter)
			// 3. add the new member to the voter or nonvoter list
			// 4. find a short member id for the new members
			// if I'm NOT the leader, I just discard the message out of the channel
			if ci.consensusStore.isRaftLeader() {
				// if we see a new node come up, add it to the raft cluster if necessary
				err := ci.addNewMemberToRaftCluster(memberJoined)
				if err != nil {
					ci.logger.Errorf("Can't rebalance cluster: %s", err.Error())
				}
			}

		case memberUpdated := <-ci.membership.memberUpdatedChan:
			if memberUpdated == "" {
				// the channel was closed, we're done
				ci.logger.Warn("Member updated channel is closed stopping event processor loop")
				close(ci.gridMemberInfoChan)
				return
			}

			tags, ok := ci.membership.getMemberById(memberUpdated)
			if ok {
				shortMid, shortMidOk := tags[serfMDKeyShortMemberId]
				host, hostOk := tags[serfMDKeyHost]
				gridPort, gridPortOk := tags[serfMDKeyGridPort]
				if shortMidOk && hostOk && gridPortOk {
					id, err := strconv.Atoi(shortMid)
					if err != nil {
						ci.logger.Errorf("Can't convert short id [%s] to int: %s", shortMid, err.Error())
					} else {
						ci.gridMemberInfoChan <- &GridMemberConnectionEvent{
							Type:              MemberJoined,
							ShortMemberId:     id,
							MemberGridAddress: host + ":" + gridPort,
						}
					}
				}
			}

		case memberLeft := <-ci.membership.memberLeftChan:
			if memberLeft == "" {
				// the channel was closed, we're done
				ci.logger.Warn("Member left channel is closed stopping event processor loop")
				close(ci.gridMemberInfoChan)
				return
			}

			// if I'm the leader, I need to do all sorts of housekeeping
			// the tasks I need to do include:
			// 1. delete the old member out of all maps
			// 2. ensure that the raft cluster is still operational (and if necessary add new members to the voter pool)
			// if I'm NOT the leader, I just discard the message out of the channel
			if ci.consensusStore.isRaftLeader() {
				ci.consensusStore.delete(consensusMembersRootName + memberLeft)
				ci.consensusStore.delete(consensusVotersName + memberLeft)
				ci.consensusStore.delete(consensusNonVotersName + memberLeft)

				err := ci.ensureConsensusStoreVoters()
				if err != nil {
					ci.logger.Errorf("Can't rebalance cluster: %s", err.Error())
				}
			}

			mi, err := ci.getMemberInfoFromConsensusStore(memberLeft)
			if err != nil {
				ci.logger.Errorf("Can't get consensus info for node [%s]: %s", memberLeft, err.Error())
			} else {
				// drop the member leave into the channel
				ci.gridMemberInfoChan <- &GridMemberConnectionEvent{
					Type:          MemberLeft,
					ShortMemberId: int(mi.ShortMemberId),
				}
			}
		}
	}
}

/////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////
///////////////////////////////////////////
//         EXECUTED AS RAFT LEADER

func (ci *clusterImpl) newLeaderHouseKeeping() error {
	if !ci.consensusStore.isRaftLeader() {
		return fmt.Errorf("Not leader!!")
	}

	// update my serf status
	// a bit hacky but gets the job done
	err := fmt.Errorf("No real error")
	for err != nil {
		// update my own state in serf
		if err = ci.membership.markLeader(); err == nil {
			ci.logger.Infof("Updated serf status to reflect me being raft leader.")
		} else {
			ci.logger.Infof("Updating serf failed: %s", err.Error())
			time.Sleep(2 * time.Second)
		}
	}

	myMemberId := ci.membership.myLongMemberId()
	// get my member info out of the membership store
	info, ok := ci.membership.getMemberById(myMemberId)
	if !ok {
		return fmt.Errorf("Can't find my own [%s] metadata?", myMemberId)
	}

	// convert to proto
	memberInfoProto, err := ci.convertNodeInfoFromSerfToRaft(myMemberId, info)
	if err != nil {
		return err
	}

	if memberInfoProto.ShortMemberId <= 0 {
		// get me a short member id in addition to the long member id
		memberInfoProto, err = ci.findShortMemberId(myMemberId, memberInfoProto)
		if err != nil {
			return err
		}
	}

	// get the previous value in case one of the updates fails
	undoMemberInfo, err := ci.consensusStore.get(consensusMembersRootName + myMemberId)
	if err != nil {
		return err
	}

	// override my member info in consensus
	err = ci.setMemberInfoInConsensusStore(myMemberId, memberInfoProto)
	if err != nil {
		return err
	}

	// update me as leader in consensus
	_, err = ci.consensusStore.set(consensusLeaderName, []byte(myMemberId))
	if err != nil {
		// undo the previous change as well
		ci.consensusStore.set(consensusMembersRootName+myMemberId, undoMemberInfo)
		return err
	}

	// add myself as voter in consensus
	_, err = ci.consensusStore.set(consensusVotersName+myMemberId, make([]byte, 0))
	if err != nil {
		return err
	}

	// delete myself out of
	ci.consensusStore.delete(consensusNonVotersName + myMemberId)

	// make sure we have enough voters in the voter pool
	err = ci.ensureConsensusStoreVoters()

	return nil
}

func (ci *clusterImpl) getMemberInfoFromConsensusStore(longMemberId string) (*pb.MemberInfo, error) {
	bites, err := ci.consensusStore.get(consensusMembersRootName + longMemberId)
	if err != nil {
		return nil, err
	}

	if bites == nil {
		return nil, fmt.Errorf("No member with id [%s] found!", longMemberId)
	}

	memberInfo := &pb.MemberInfo{}
	err = proto.Unmarshal(bites, memberInfo)
	return memberInfo, err
}

func (ci *clusterImpl) setMemberInfoInConsensusStore(longMemberId string, memberInfo *pb.MemberInfo) error {
	bites, err := proto.Marshal(memberInfo)
	if err != nil {
		return err
	}

	_, err = ci.consensusStore.set(consensusMembersRootName+longMemberId, bites)
	return err
}

func (ci *clusterImpl) ensureConsensusStoreVoters() error {
	// make sure we have enough voters in our raft cluster
	kvs, err := ci.consensusStore.getPrefix(consensusVotersName)
	if err != nil {
		return fmt.Errorf("Can't get voters: %s", err.Error())
	}

	// compute the number of voters I need to add
	numVoters := len(kvs)
	numVotersIWant := ci.config.NumRaftVoters - numVoters

	//
	// add new voters to raft cluster
	//
	if numVotersIWant > 0 {
		kvs, err := ci.consensusStore.getPrefix(consensusNonVotersName)
		if err != nil {
			return fmt.Errorf("Can't get nonvoters: %s", err.Error())
		}

		// recruit more nodes to be voters
		for _, kv := range kvs {
			mi := &pb.MemberInfo{}
			err := proto.Unmarshal(kv.v, mi)
			if err != nil {
				ci.logger.Infof("Can't unmarshal member info: %s", err.Error())
			}

			raftAddr := fmt.Sprintf("%s:%d", mi.Host, mi.RaftPort)
			err = ci.consensusStore.addVoter(kv.k, raftAddr)
			if err == nil {
				// seems to succesfully added a voter
				// we can decrease the number of voters we want to add
				numVotersIWant--
				if numVotersIWant <= 0 {
					// we're done !!!
					return nil
				}
			}
		}
	}

	return nil
}

func (ci *clusterImpl) addNewMemberToRaftCluster(newMemberId string) error {
	bitesVoter, errVoter := ci.consensusStore.get(consensusVotersName + newMemberId)
	bitesNonvoter, errNonvoter := ci.consensusStore.get(consensusNonVotersName + newMemberId)
	if errVoter != nil || errNonvoter != nil {
		return fmt.Errorf("Error getting voter/nonvoter info: %s %s", errVoter, errNonvoter)
	} else if bitesVoter != nil || bitesNonvoter != nil {
		return fmt.Errorf("Voter / Nonvoter status for member [%s] not set yet!?!?", newMemberId)
	}

	// get the new members info from membership
	info, infoOk := ci.membership.getMemberById(newMemberId)
	if !infoOk {
		return fmt.Errorf("Member info for member %s is not present!?!?!?", newMemberId)
	}

	memberInfoProto, err := ci.convertNodeInfoFromSerfToRaft(newMemberId, info)
	if err != nil {
		return fmt.Errorf("Can't create member info proto: %s", err.Error())
	}

	if memberInfoProto.ShortMemberId <= 0 {
		memberInfoProto, _ = ci.findShortMemberId(newMemberId, memberInfoProto)
	}

	raftAddr := fmt.Sprintf("%s:%d", memberInfoProto.Host, memberInfoProto.RaftPort)

	//
	// add new voters to raft cluster
	//
	// find the current number of voters
	kvs, err := ci.consensusStore.getPrefix(consensusVotersName)
	if err != nil {
		return err
	}

	numVoters := len(kvs)
	// compute the number of voters I want
	numVotersIWant := ci.config.NumRaftVoters - numVoters
	if numVotersIWant > 0 {
		err := ci.consensusStore.addVoter(newMemberId, raftAddr)
		if err == nil {
			// add the new member as voter
			_, err = ci.consensusStore.set(consensusVotersName+newMemberId, make([]byte, 0))
			if err != nil {
				return err
			}

			// marshal the new members member info
			bites, err := proto.Marshal(memberInfoProto)
			if err != nil {
				return err
			}

			// set member info in consensus
			_, err = ci.consensusStore.set(consensusMembersRootName+newMemberId, bites)
			if err != nil {
				return err
			}

			// cuz I'm paranoid delete the new member from nonvoters in consensus
			_, err = ci.consensusStore.delete(consensusNonVotersName + newMemberId)
			if err != nil {
				return err
			}
		}
	} else {
		err := ci.consensusStore.addNonvoter(newMemberId, raftAddr)
		if err == nil {
			// add the new member as nonvoter
			_, err = ci.consensusStore.set(consensusNonVotersName+newMemberId, make([]byte, 0))
			if err != nil {
				return err
			}

			// marshal the new members member info
			bites, err := proto.Marshal(memberInfoProto)
			if err != nil {
				return err
			}

			// set member info in consensus
			_, err = ci.consensusStore.set(consensusMembersRootName+newMemberId, bites)
			if err != nil {
				return err
			}

			// cuz I'm paranoid delete the new member from nonvoters in consensus
			_, err = ci.consensusStore.delete(consensusVotersName + newMemberId)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (ci *clusterImpl) findShortMemberId(memberId string, memberInfo *pb.MemberInfo) (*pb.MemberInfo, error) {
	allMembers, err := ci.consensusStore.getPrefix(consensusMembersRootName)
	if err != nil {
		return nil, err
	}

	a := make([]*pb.MemberInfo, len(allMembers))
	for idx, kv := range allMembers {
		mi := &pb.MemberInfo{}
		err = proto.Unmarshal(kv.v, mi)
		if err != nil {
			return nil, err
		}
		a[idx] = mi
	}

	// sort the node infos by short id
	sort.Sort(byShortMemberId(a))

	// count the sorted node infos up until you find a gap
	newShortMemberId := -1
	for idx := 0; idx < len(a); idx++ {
		if a[idx].ShortMemberId != int32(idx+1) {
			newShortMemberId = idx + 1
			break
		}
	}

	// or take the next node id available
	if newShortMemberId == -1 {
		newShortMemberId = len(a) + 1
	}

	ci.logger.Infof("Assinged short id %d to long id %s", newShortMemberId, memberId)
	memberInfo.ShortMemberId = int32(newShortMemberId)
	return memberInfo, nil
}

func (ci *clusterImpl) convertNodeInfoFromSerfToRaft(myMemberId string, serfInfo map[string]string) (*pb.MemberInfo, error) {
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

	var shortMemberId int
	shortMemberIdStr, ok := serfInfo[serfMDKeyShortMemberId]
	if ok {
		shortMemberId, err = strconv.Atoi(shortMemberIdStr)
		if err != nil {
			return nil, err
		}
	} else {
		shortMemberId = 0
	}

	return &pb.MemberInfo{
		Host:            host,
		LongMemberId:    myMemberId,
		SerfPort:        int32(serfPort),
		RaftPort:        int32(raftPort),
		RaftServicePort: int32(raftServicePort),
		GridPort:        int32(gridPort),
		ShortMemberId:   int32(shortMemberId),
	}, nil
}

/////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////
///////////////////////////////////////////
//         EXECUTED AS RAFT LEADER

/////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////
///////////////////////////////////////////
//      PUBLIC INTERFACE DEFINITIONS

func (ci *clusterImpl) GetMyShortMemberId() int {
	return ci.shortMemberId
}

func (ci *clusterImpl) GetGridMemberChangeEvents() <-chan *GridMemberConnectionEvent {
	return ci.gridMemberInfoChan
}

func (ci *clusterImpl) printClusterState() {
	// state, _ := ci.getRaftClusterState()
	//
	// ci.logger.Info("CLUSTER STATE:")
	// ci.logger.Infof("Voters [%d]:", len(state.Voters))
	// for id := range state.Voters {
	// 	ci.logger.Infof("%s", id)
	// }
	//
	// ci.logger.Infof("Nonvoters [%d]:", len(state.Nonvoters))
	// for id := range state.Nonvoters {
	// 	ci.logger.Infof("%s", id)
	// }
	//
	// ci.logger.Infof("AllNodes [%d]:", len(state.AllNodes))
	// for _, info := range state.AllNodes {
	// 	ci.logger.Infof("%s", info.String())
	// }
}

func (ci *clusterImpl) Close() error {
	err := ci.membership.unmarkLeader()
	if err != nil {
		ci.logger.Errorf("Error unmarking myself as leader (I'm proceeding anyways though): %s", err.Error())
	}

	time.Sleep(100 * time.Millisecond)
	err = ci.membership.close()
	if err != nil {
		ci.logger.Errorf("Error closing membership store (I'm proceeding anyways though): %s", err.Error())
	}

	time.Sleep(100 * time.Millisecond)
	ci.raftService.close()
	err = ci.consensusStore.close()
	if err != nil {
		ci.logger.Errorf("Error closing consensus store (I'm proceeding anyways though): %s", err.Error())
	}

	err = ci.consensusStoreProxy.close()
	if err != nil {
		ci.logger.Errorf("Error closing consensus proxy (I'm proceeding anyways though): %s", err.Error())
	}
	time.Sleep(100 * time.Millisecond)
	return nil
}

// boiler plate code to implement go sort interface
type byShortMemberId []*pb.MemberInfo

func (a byShortMemberId) Len() int {
	return len(a)
}
func (a byShortMemberId) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a byShortMemberId) Less(i, j int) bool {
	return a[i].ShortMemberId < a[j].ShortMemberId
}
