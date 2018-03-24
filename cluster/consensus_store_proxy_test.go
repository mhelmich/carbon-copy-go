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
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestProxyBasic(t *testing.T) {
	hn, err := os.Hostname()
	assert.Nil(t, err)

	config1 := ClusterConfig{
		RaftPort:        15111,
		RaftServicePort: 16111,
		longMemberId:    "node111",
		hostname:        hn,
		raftNotifyCh:    make(chan bool, 16),
		isDevMode:       true,
	}
	store1, err := createNewConsensusStore(config1)
	assert.Nil(t, err)
	assert.NotNil(t, store1)
	assert.True(t, <-config1.raftNotifyCh)

	raftServer, err := createRaftService(config1, store1)
	assert.Nil(t, err)

	raftLeaderAddrChan := make(chan string, 4)
	raftLeaderAddrChan <- hn + ":" + strconv.Itoa(16111)
	proxy, err := newConsensusStoreProxy(config1, store1, raftLeaderAddrChan)
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
	assert.True(t, deleted)

	deleted, err = proxy.delete("some_random_key")
	assert.Nil(t, err)
	assert.False(t, deleted)

	assert.Nil(t, proxy.close())
	raftServer.close()
	assert.Nil(t, store1.close())
}

func TestProxyProxy(t *testing.T) {
	hn, err := os.Hostname()
	assert.Nil(t, err)

	config1, store1, raftServer1, proxy1, raftLeaderAddrChan1 := createStoreServiceProxyProxyTest(t, 15111, 16111, "node111", "")
	assert.True(t, <-config1.raftNotifyCh)
	raftLeaderAddr := fmt.Sprintf("%s:%d", hn, 16111)

	_, store2, raftServer2, proxy2, raftLeaderAddrChan2 := createStoreServiceProxyProxyTest(t, 15222, 16222, "node222", raftLeaderAddr)
	raftPeerAddr := fmt.Sprintf("%s:%d", hn, 15222)
	err = store1.addVoter("node222", raftPeerAddr)
	assert.Nil(t, err)
	// assert.False(t, <-config2.raftNotifyCh)

	// allowing raft to settle
	time.Sleep(1 * time.Second)

	k := "keykeykey"
	v := []byte("value_value_value")
	existed, err := proxy2.set(k, v)
	assert.Nil(t, err)
	assert.False(t, existed)

	bites, err := proxy2.consistentGet(k)
	assert.Nil(t, err)
	assert.Equal(t, v, bites)
	log.Infof("The string is read: %s", string(bites))

	close(raftLeaderAddrChan1)
	assert.Nil(t, proxy1.close())
	raftServer1.close()
	assert.Nil(t, store1.close())

	close(raftLeaderAddrChan2)
	assert.Nil(t, proxy2.close())
	raftServer2.close()
	assert.Nil(t, store2.close())
}

func createStoreServiceProxyProxyTest(t *testing.T, raftPort int, raftServicePort int, nodeId string, leaderAddr string) (ClusterConfig, *consensusStoreImpl, *raftServiceImpl, *consensusStoreProxy, chan string) {
	hn, err := os.Hostname()
	assert.Nil(t, err)

	var peers []string
	if leaderAddr == "" {
		peers = nil
	} else {
		peers = make([]string, 1)
		peers[0] = "Polly wants cracker!"
	}

	config1 := ClusterConfig{
		RaftPort:        raftPort,
		RaftServicePort: raftServicePort,
		longMemberId:    nodeId,
		hostname:        hn,
		raftNotifyCh:    make(chan bool, 16),
		Peers:           peers,
		logger: log.WithFields(log.Fields{
			"nodeId": nodeId,
		}),
		isDevMode: true,
	}
	store1, err := createNewConsensusStore(config1)
	assert.Nil(t, err)
	assert.NotNil(t, store1)

	raftServer1, err := createRaftService(config1, store1)
	assert.Nil(t, err)
	assert.NotNil(t, raftServer1)

	raftLeaderAddrChan1 := make(chan string, 4)
	if leaderAddr == "" {
		raftLeaderAddrChan1 <- hn + ":" + strconv.Itoa(raftServicePort)
	} else {
		raftLeaderAddrChan1 <- leaderAddr
	}
	proxy1, err := newConsensusStoreProxy(config1, store1, raftLeaderAddrChan1)
	assert.Nil(t, err)
	assert.NotNil(t, proxy1)

	return config1, store1, raftServer1, proxy1, raftLeaderAddrChan1
}
