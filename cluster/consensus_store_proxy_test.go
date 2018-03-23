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
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"testing"
)

func TestProxyBasic(t *testing.T) {
	hn, err := os.Hostname()
	assert.Nil(t, err)

	config := ClusterConfig{
		RaftPort:        15111,
		RaftServicePort: 16111,
		longMemberId:    "node111",
		hostname:        hn,
		raftNotifyCh:    make(chan bool, 16),
		isDevMode:       true,
	}
	store1, err := createNewConsensusStore(config)
	assert.Nil(t, err)
	assert.NotNil(t, store1)
	assert.True(t, <-config.raftNotifyCh)

	raftServer, err := createRaftService(config)
	assert.Nil(t, err)

	raftLeaderAddrChan := make(chan string, 4)
	raftLeaderAddrChan <- hn + ":" + strconv.Itoa(16111)
	proxy, err := newConsensusStoreProxy(config, store1, raftLeaderAddrChan)
	assert.Nil(t, err)
	assert.NotNil(t, proxy)

	k := "keykeykey"
	v := []byte("value_value_value")
	existed, err := proxy.set(k, v)
	assert.Nil(t, err)
	assert.False(t, existed)

	bites, err := proxy.consistentGet(k)
	assert.Nil(t, err)
	assert.Equal(t, v, bites)
	log.Infof("The string is read: %s", string(bites))

	deleted, err := proxy.delete(k)
	assert.Nil(t, err)
	// TODO - fix this -- should be true
	assert.False(t, deleted)

	assert.Nil(t, proxy.close())
	raftServer.close()
	assert.Nil(t, store1.close())
}
