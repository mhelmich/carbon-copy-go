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
	"github.com/hashicorp/raft"
	log "github.com/sirupsen/logrus"
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

const (
	nameSeparator          = "/"
	consensusNamespaceName = "carbon-copy"
	consensusNodesRootName = consensusNamespaceName + nameSeparator + "nodes" + nameSeparator
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
	config         clusterConfig
}

func createNewCluster(config clusterConfig) (*clusterImpl, error) {
	m, err := createNewMembership(config)
	if err != nil {
		return nil, err
	}

	cs, err := createNewConsensusStore(config)
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
			ci.logger.Info("I'm the leader ... wheeee!")
			go ci.leaderLoop()
		} else {
			ci.logger.Info("I'm not the leader ... cruel world!")
		}
	}
}

func (ci *clusterImpl) leaderLoop() {

	if ci.consensusStore.isRaftLeader() {
		// update my own state in serf
		newTags := make(map[string]string)
		newTags[serfMDKeyRaftRole] = raftRoleLeader
		err := ci.membership.updateRaftTag(newTags)
		if err == nil {
			ci.logger.Infof("Updated serf status to reflect me being raft leader.")
			time.Sleep(3 * time.Second)
		} else {
			ci.logger.Infof("Updating serf failed: %s", err.Error())
		}
	}

	for { // ever...
		ci.logger.Infof("isRaftLeader %t", ci.consensusStore.isRaftLeader())
		if ci.consensusStore.isRaftLeader() {
			// make sure we have enough other nodes in the cluster
			clusterSize := ci.membership.getClusterSize()
			numVoters := len(ci.membership.membershipState.raftVoters)
			numNonvoters := len(ci.membership.membershipState.raftNonvoters)
			numNonvotersIWant := 1 - numNonvoters
			numVotersIWant := 3 - numVoters
			ci.logger.Infof("Consensus store only has %d voters and I want %d. I will ask %d nodes to become voter.", numVoters, 5, numVotersIWant)

			//
			// add new nonvoters to raft cluster
			// if we find that there's enough nodes in the cluster
			// to begin with
			//
			if clusterSize-numVotersIWant-numNonvotersIWant > 0 {
				if numNonvotersIWant > 0 {
					// recruit more nonvoters
					numNonvotersIWant = numNonvotersIWant - ci.addNonvotersFrom(numNonvotersIWant, ci.membership.membershipState.raftNones)
				}

				if numNonvotersIWant > 0 {
					ci.logger.Warnf("I did all I could and I'm still %d voters missing.", numNonvotersIWant)
				}
			} else {
				ci.logger.Infof("I'm not even trying to find nonvoters because the cluster is too small: clusterSize [%d] numVotersIWant [%d] numNonvotersIWant [%d]", clusterSize, numVotersIWant, numNonvotersIWant)
			}

			//
			// add new voters to raft cluster
			// do this *after* nonvoter business
			// worst case scenario we add a nonvoter and promote right away
			// that's better than needing voters and demoting them to nonvoters
			// because there aren't enough nodes in the cluster to begin with
			//
			if numVotersIWant > 0 {
				// recruit more nodes to be voters
				numVotersIWant = numVotersIWant - ci.addVotersFrom(numVotersIWant, ci.membership.membershipState.raftNonvoters)
				if numVotersIWant > 0 {
					numVotersIWant = numVotersIWant - ci.addVotersFrom(numVotersIWant, ci.membership.membershipState.raftNones)
				}
			}

			if numVotersIWant > 0 {
				ci.logger.Warnf("I did all I could and I'm still %d voters missing.", numVotersIWant)
			}
		} else {
			ci.logger.Info("I'm not the leader anymore ... Goodbye cruel world!")
			return
		}

		//
		// wait until the cluster topology changes
		// then go through the same loop again
		// or after every so and so seconds for good measure
		//
		select {
		case <-ci.membership.memberJoined:
		case <-ci.membership.memberLeft:
		}
	}
}

func (ci *clusterImpl) addVotersFrom(numVotersToAdd int, source map[string]bool) int {
	numAdded := 0
	for id, _ := range source {
		if id != ci.membership.myNodeId() {
			if numVotersToAdd <= numAdded {
				break
			}

			m, ok := ci.membership.getNodeById(id)
			if ok {
				raftAddr, ok := m[serfMDKeyRaftAddr]
				if ok {
					err := ci.consensusStore.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(raftAddr), 0, 0).Error()
					if err == nil {
						log.Infof("Added (%s - %s) as raft voter", id, raftAddr)
						numAdded++
					} else {
						log.Infof("Couldn't add (%s - %s) as raft voter: %v", id, raftAddr, err.Error())
					}
				}
			}
		}
	}

	return numAdded
}

func (ci *clusterImpl) addNonvotersFrom(numNonvotersToAdd int, source map[string]bool) int {
	numAdded := 0
	for id, _ := range source {
		if id != ci.membership.myNodeId() {
			if numNonvotersToAdd <= numAdded {
				break
			}

			m, ok := ci.membership.getNodeById(id)
			if ok {
				raftAddr, ok := m[serfMDKeyRaftAddr]
				if ok {
					err := ci.consensusStore.raft.AddNonvoter(raft.ServerID(id), raft.ServerAddress(raftAddr), 0, 0).Error()
					if err == nil {
						log.Infof("Added (%s - %s) as raft nonvoter", id, raftAddr)
						numAdded++
					} else {
						log.Warnf("Couldn't add (%s - %s) as raft nonvoter: %s", id, raftAddr, err.Error())
					}
				}
			}
		}
	}

	return numAdded
}

// func startMyNodeIdProvider(ctx context.Context, cc consensusClient) chan int {
// 	ch := make(chan int, 1)

// 	go func() {
// 		for {
// 			kvs, err := cc.getSortedRange(ctx, consensusNodesRootName)
// 			if err != nil {
// 				log.Errorf("Can't connect to consensus %s", err.Error())
// 			}
// 			keySet := make(map[string]bool)

// 			for _, kv := range kvs {
// 				keySet[kv.key] = true
// 			}

// 			i := 1
// 			for i < 16384 {
// 				_, ok := keySet[strconv.Itoa(i)]
// 				if !ok {
// 					b, err := cc.putIfAbsent(ctx, consensusNodesRootName+strconv.Itoa(i), "")
// 					if b && err == nil {
// 						ch <- i
// 						close(ch)
// 						log.Infof("Aqcuired node id %d", i)
// 						return
// 					}
// 				}
// 				i++
// 			}
// 		}
// 	}()

// 	return ch
// }

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

func (ci *clusterImpl) Close() {
	ci.membership.close()
	time.Sleep(1 * time.Second)
	ci.consensusStore.Close()
	time.Sleep(1 * time.Second)
}
