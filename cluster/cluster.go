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
	// "github.com/mhelmich/carbon-copy-go/pb"
	"carbon-grid-go/pb"
	log "github.com/sirupsen/logrus"
	"strconv"
	// "github.com/hashicorp/serf/serf"
)

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
