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
	mc "github.com/goburrow/cache"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"sync"
	"testing"
	"time"
)

func TestTheConnectionCache(t *testing.T) {
	loadedChan := make(chan struct{}, 3)
	removedChan := make(chan struct{}, 1)

	loader := func(addr mc.Key) (mc.Value, error) {
		loadedChan <- struct{}{}
		log.Infof("Running cache loader")
		return new(grpc.ClientConn), nil
	}

	removalListener := func(key mc.Key, value mc.Value) {
		strKey := key.(string)
		assert.Equal(t, "localhost:1234", strKey)
		removedChan <- struct{}{}
		log.Infof("Running removal listener with %s", strKey)
	}

	connCache := mc.NewLoadingCache(
		loader,
		mc.WithExpireAfterAccess(2*time.Second),
		mc.WithRemovalListener(removalListener),
	)

	m := &cacheClientMappingImpl{
		connectionCache: connCache,
		nodeIdToAddr:    &sync.Map{},
	}

	log.Infof("Adding client...")
	m.addClientWithNodeId(1234, "localhost:1234")
	m.getClientForNodeId(1234)
	assert.True(t, waitTimeoutChan(loadedChan, 5*time.Second), "Loading callback wasn't executed")
	// wait for key to expire
	time.Sleep(3 * time.Second)
	log.Infof("Adding second client and hopefully trigger clean up logic...")
	m.addClientWithNodeId(9999, "addr.doesnt.exist:1234")
	m.getClientForNodeId(9999)
	m.getClientForNodeId(9999)
	assert.True(t, waitTimeoutChan(removedChan, 5*time.Second), "Removal listener wasn't executed")
}

func TestFillAndClear(t *testing.T) {
	m := newCacheClientMapping()
	m.addClientWithNodeId(1234, "localhost:1234")
	m.addClientWithNodeId(5678, "localhost:5678")
	m.addClientWithNodeId(4321, "localhost:4321")
	m.addClientWithNodeId(9876, "localhost:9876")

	m.getClientForNodeId(1234)
	m.getClientForNodeId(9876)
	m.getClientForNodeId(4321)
	m.getClientForNodeId(5678)

	m.clear()
}

func waitTimeoutChan(ch chan struct{}, timeout time.Duration) bool {
	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		return false
	}
}
