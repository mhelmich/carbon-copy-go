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
	consensusNodesRootName = consensusNamespaceName + nameSeparator + "nodes" + nameSeparator
	consensusIdAllocator   = consensusNamespaceName + nameSeparator + "cache-line-ids"

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
	myNodeId  int
	// this channel will only receive one int
	// after that it is closed
	myNodeIdCh  chan int
	shouldClose bool
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
	initIdAllocator(cc)

	c := &clusterImpl{
		consensus:   cc,
		shouldClose: false,
		newIdsCh:    startGlobalIdProvider(ctx, cc),
		myNodeIdCh:  startMyNodeIdProvider(ctx, cc),
		myNodeId:    -1,
	}

	return c, nil
}

func initIdAllocator(cc consensusClient) {
	b, err := cc.putIfAbsent(context.Background(), consensusIdAllocator, strconv.Itoa(math.MinInt64))
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

			i := 0
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

func startGlobalIdProvider(ctx context.Context, cc consensusClient) chan int {
	idChan := make(chan int, idBufferSize/2)
	go func() {
		getIdBatchesForever(ctx, idChan, cc)
	}()
	return idChan
}

// loops forever (hopefully) in its own go routine
func getIdBatchesForever(ctx context.Context, idChan chan int, cc consensusClient) {
	for { // ever...
		low, high, err := getNextIdBatch(ctx, cc)
		if err != nil {
			log.Warnf("Couldn't allocate new id batch %s", err.Error())
			time.Sleep(3 * time.Second)
		} else {
			for i := low; i < high; i++ {
				idChan <- i

				if cc.isClosed() {
					return
				}
			}
		}

		if cc.isClosed() {
			return
		}
	}
}

// allocates the next available batch of unique cache line ids
func getNextIdBatch(ctx context.Context, cc consensusClient) (int, int, error) {
	succeeded := false
	var nextHighWatermarkStr string = ""
	var currentHighWatermarkStr string = ""
	var err error = nil
	for !succeeded {
		currentHighWatermarkStr, err = cc.get(ctx, consensusIdAllocator)
		log.Infof("Got from get %s %v", currentHighWatermarkStr, err)
		currentHighWatermark, err := strconv.Atoi(currentHighWatermarkStr)
		if err != nil {
			return -1, -1, err
		}

		nextHighWatermarkStr = strconv.Itoa(currentHighWatermark + idBufferSize)
		succeeded, err = cc.compareAndPut(ctx, consensusIdAllocator, currentHighWatermarkStr, nextHighWatermarkStr)
		log.Infof("Got from compareAndPut %t %v", succeeded, err)
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

func (ci *clusterImpl) GetMyNodeId() int {
	if ci.myNodeId == -1 {
		ci.myNodeId = <-ci.myNodeIdCh
	}
	return ci.myNodeId
}

func (ci *clusterImpl) GetIdAllocator() <-chan int {
	return ci.newIdsCh
}

func (ci *clusterImpl) Close() {
	ci.consensus.close()
	// read one id to close the allocator go routine
	<-ci.newIdsCh
}
