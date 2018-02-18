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
)

const (
	nameSeparator          = "/"
	consensusNamespaceName = "carbon-copy"
	consensusNodesName     = consensusNamespaceName + nameSeparator + "nodes"
)

type clusterImpl struct {
	consensus consensusClient
}

func createNewCluster() (*clusterImpl, error) {
	ctx := context.Background()
	etcd, err := createNewEtcdConsensus(ctx)
	if err != nil {
		return nil, err
	}

	return &clusterImpl{
		consensus: etcd,
	}, nil
}

// func (ci *clusterImpl) allocateNodeId(ctx context.Context, client *clientv3.Client) (int, error) {
// 	resp, err := client.Get(ctx, consensusNodesName+"_", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
// 	if err != nil {
// 		return -1, err
// 	}

// 	myNodeId := -1
// 	idx := -1

// 	for idx, ev := range resp.Kvs {
// 		key := string(ev.Key)
// 		log.Infof("Found node %s", key)
// 		tokens := strings.Split(key, "_")
// 		if len(tokens) == 2 {
// 			nodeId, err := strconv.Atoi(tokens[1])
// 			if err == nil {
// 				if nodeId != idx {
// 					myNodeId = idx
// 					return myNodeId, nil
// 				}
// 			}
// 		}
// 	}

// 	return idx + 1, nil
// }

func (ci *clusterImpl) myNodeId() int {
	return 0
}

func (ci *clusterImpl) getAllocator() GlobalIdAllocator {
	return nil
}
