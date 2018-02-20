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
	log "github.com/sirupsen/logrus"
	"math"
	"strconv"
	"time"
)

const (
	nameSeparator          = "/"
	consensusNamespaceName = "carbon-copy"
	consensusNodesName     = consensusNamespaceName + nameSeparator + "nodes"
	consensusIdAllocator   = consensusNamespaceName + nameSeparator + "cache-line-id"

	// TODO: move this into a config
	idBufferSize = 7
)

type kv struct {
	key   string
	value string
}

type clusterImpl struct {
	consensus consensusClient
	newIdsCh  chan int
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

func createNewClusterWithConsensus(ctx context.Context, etcd consensusClient) (*clusterImpl, error) {
	c := &clusterImpl{
		consensus: etcd,
	}

	c.initIdAllocator()
	return c, nil
}

func (ci *clusterImpl) initIdAllocator() {
	b, err := ci.consensus.putIfAbsent(context.Background(), consensusIdAllocator, strconv.Itoa(math.MinInt64))
	if err != nil {
		log.Infof("Initializing the id allocator failed!", err.Error())
	} else {
		if b {
			log.Infof("Created id allocator base at %d.", math.MinInt64)
		} else {
			log.Info("Id allocator base was present already.")
		}
	}
}

func (ci *clusterImpl) startGlobalIdProvider(ctx context.Context) chan int {
	idChan := make(chan int, idBufferSize/2)
	go func() {
		ci.getIdBatchesForever(ctx, idChan)
	}()
	return idChan
}

func (ci *clusterImpl) getIdBatchesForever(ctx context.Context, idChan chan int) {
	for { // ever...
		low, high, err := ci.getNextIdBatch(ctx)
		if err != nil {
			log.Warnf("Couldn't allocate new id batch %s", err.Error())
			time.Sleep(3 * time.Second)
		} else {
			for i := low; i < high; i++ {
				idChan <- i
			}
		}
	}
}

func (ci *clusterImpl) getNextIdBatch(ctx context.Context) (int, int, error) {
	succeeded := false
	nextHighWatermarkStr := ""
	currentHighWatermarkStr := ""
	var err error
	for !succeeded {
		currentHighWatermarkStr, err = ci.consensus.get(ctx, consensusIdAllocator)
		log.Infof("Got from get %v %v", currentHighWatermarkStr, err)
		currentHighWatermark, err := strconv.Atoi(currentHighWatermarkStr)
		if err != nil {
			return -1, -1, err
		}

		nextHighWatermarkStr = strconv.Itoa(currentHighWatermark + idBufferSize)
		succeeded, err = ci.consensus.compareAndPut(ctx, consensusIdAllocator, currentHighWatermarkStr, nextHighWatermarkStr)
		log.Infof("Got from compareAndPut %v %v", succeeded, err)
		if err != nil {
			return -1, -1, err
		}
	}

	high, err := strconv.Atoi(nextHighWatermarkStr)
	if err != nil {
		return -1, -1, err
	}

	low, err := strconv.Atoi(currentHighWatermarkStr)
	if err != nil {
		return -1, -1, err
	}

	log.Infof("Allocated new id batch low [%d] high [%d]", low, high)
	return low, high, nil
}

func (ci *clusterImpl) myNodeId() int {
	return 0
}

func (ci *clusterImpl) getIdAllocator() chan int {
	return ci.newIdsCh
}

func (ci *clusterImpl) close() {
	ci.consensus.close()
}
