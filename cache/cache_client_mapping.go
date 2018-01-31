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

package cache

import (
	"errors"
	"fmt"
	mc "github.com/goburrow/cache"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"sync"
	"time"
)

func newCacheClientMapping() *cacheClientMappingImpl {
	connCache := mc.NewLoadingCache(
		cacheLoader,
		mc.WithExpireAfterAccess(15*time.Minute),
		mc.WithRemovalListener(cacheRemovalListener),
	)

	return &cacheClientMappingImpl{
		connectionCache: connCache,
		nodeIdToAddr:    &sync.Map{},
	}
}

// the underlying connection is completely managed by the cache
// there is no way for consumers to explicitly close a connection
// as they only interact with the client stub
// the only one who's able to close a connection is the cache itself
var cacheLoader = func(addr mc.Key) (mc.Value, error) {
	return grpc.Dial(addr.(string), grpc.WithInsecure())
}

var cacheRemovalListener = func(key mc.Key, value mc.Value) {
	go func() {
		clientConn := value.(*grpc.ClientConn)
		clientConn.Close()
	}()
}

type cacheClientMappingImpl struct {
	connectionCache mc.LoadingCache
	nodeIdToAddr    *sync.Map
}

func (ccm *cacheClientMappingImpl) getClientForNodeId(nodeId int) (CacheClient, error) {
	addr, ok := ccm.nodeIdToAddr.Load(nodeId)
	if !ok {
		return nil, errors.New(fmt.Sprintf("Can't find address for node id %d", nodeId))
	}

	val, err := ccm.connectionCache.Get(addr)
	if err != nil {
		return nil, err
	} else {
		conn := val.(*grpc.ClientConn)
		// create a new stub on the fly
		return createNewCacheClientFromConn(conn)
	}
}

func (ccm *cacheClientMappingImpl) addClientWithNodeId(nodeId int, addr string) {
	ccm.nodeIdToAddr.Store(nodeId, addr)
}

func (ccm *cacheClientMappingImpl) forEachParallel(f func(c CacheClient)) {
	fctn := func(key, value interface{}) bool {
		go func() {
			nodeId := key.(int)
			client, err := ccm.getClientForNodeId(nodeId)
			if err == nil {
				f(client)
			} else {
				log.Errorf(err.Error())
			}
		}()
		return true
	}

	ccm.nodeIdToAddr.Range(fctn)
}

func (ccm *cacheClientMappingImpl) printStats() {
	stats := &mc.Stats{}
	ccm.connectionCache.Stats(stats)
	log.Infof("Connection cache stats: %v", stats)
}

func (ccm *cacheClientMappingImpl) clear() {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	go func() {
		ccm.nodeIdToAddr.Range(func(k interface{}, v interface{}) bool {
			ccm.nodeIdToAddr.Delete(k)
			return true
		})
		wg.Done()
	}()

	go func() {
		ccm.connectionCache.InvalidateAll()
		wg.Done()
	}()

	waitTimeout(wg, 10*time.Second)
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})

	go func() {
		defer close(c)
		wg.Wait()
	}()

	select {
	case <-c:
		return true
	case <-time.After(timeout):
		return false
	}
}
