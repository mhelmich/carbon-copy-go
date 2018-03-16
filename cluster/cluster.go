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
	"context"
	"github.com/golang/protobuf/proto"
	"github.com/mhelmich/carbon-copy-go/pb"
	log "github.com/sirupsen/logrus"
	"strconv"
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
	consensus consensusClient
	myNodeId  int
	// this channel will only receive one int
	// after that it is closed
	myNodeIdCh chan int
}

func createNewCluster() (*clusterImpl, error) {
	ctx := context.Background()
	// make an actual connection
	etcd, err := createNewEtcdConsensus(ctx)
	if err != nil {
		return nil, err
	}

	return createNewClusterWithConsensus(ctx, etcd)
}

func createNewClusterWithConsensus(ctx context.Context, cc consensusClient) (*clusterImpl, error) {
	c := &clusterImpl{
		consensus:  cc,
		myNodeIdCh: startMyNodeIdProvider(ctx, cc),
		myNodeId:   -1,
	}

	return c, nil
}

func startMyNodeIdProvider(ctx context.Context, cc consensusClient) chan int {
	ch := make(chan int, 1)

	go func() {
		for {
			kvs, err := cc.getSortedRange(ctx, consensusNodesRootName)
			if err != nil {
				log.Errorf("Can't connect to consensus %s", err.Error())
			}
			keySet := make(map[string]bool)

			for _, kv := range kvs {
				keySet[kv.key] = true
			}

			i := 1
			for i < 16384 {
				_, ok := keySet[strconv.Itoa(i)]
				if !ok {
					b, err := cc.putIfAbsent(ctx, consensusNodesRootName+strconv.Itoa(i), "")
					if b && err == nil {
						ch <- i
						close(ch)
						log.Infof("Aqcuired node id %d", i)
						return
					}
				}
				i++
			}
		}
	}()

	return ch
}

func (ci *clusterImpl) GetMyNodeId() int {
	if ci.myNodeId == -1 {
		ci.myNodeId = <-ci.myNodeIdCh
	}
	return ci.myNodeId
}

func (ci *clusterImpl) GetNodeConnectionInfoUpdates() (<-chan []*NodeConnectionInfo, error) {
	kvBytesChan, err := ci.consensus.watchKeyPrefix(context.Background(), consensusNodesRootName)
	if err != nil {
		return nil, err
	}

	nodeConnInfoChan := make(chan []*NodeConnectionInfo)
	go func() {
		for kvBatchBytes := range kvBytesChan {
			if len(kvBatchBytes) <= 0 {
				close(nodeConnInfoChan)
				return
			}

			nodeInfos := make([]*NodeConnectionInfo, len(kvBatchBytes))
			for idx, kvBytes := range kvBatchBytes {
				nodeInfoProto := &pb.NodeInfo{}
				err = proto.Unmarshal(kvBytes.value, nodeInfoProto)
				if err == nil {
					nodeInfos[idx] = &NodeConnectionInfo{
						nodeId:      int(nodeInfoProto.NodeId),
						nodeAddress: nodeInfoProto.Host,
					}
				}
			}

			nodeConnInfoChan <- nodeInfos
		}
	}()

	return nodeConnInfoChan, nil
}

func (ci *clusterImpl) Close() {
	ci.consensus.close()
}
