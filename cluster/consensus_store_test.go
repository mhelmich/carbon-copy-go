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
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
)

func TestConsensusStoreBasic(t *testing.T) {
	hn, err := os.Hostname()
	assert.Nil(t, err)

	cfg1 := ClusterConfig{
		RaftPort:        15111,
		RaftServicePort: 16111,
		longMemberId:    "node111",
		hostname:        hn,
		raftNotifyCh:    make(chan bool, 16),
		isDevMode:       true,
	}
	store1, err := createNewConsensusStore(cfg1)
	assert.Nil(t, err)
	assert.NotNil(t, store1)
	assert.True(t, <-cfg1.raftNotifyCh)

	peers := make([]string, 1)
	// as long as this is not nil, we don't care what's
	// actually written in there
	peers[0] = "Polly wants cracker!"
	cfg2 := ClusterConfig{
		RaftPort:        25222,
		RaftServicePort: 26222,
		longMemberId:    "node222",
		hostname:        hn,
		Peers:           peers,
		raftNotifyCh:    make(chan bool, 16),
		isDevMode:       true,
	}
	store2, err := createNewConsensusStore(cfg2)
	assert.Nil(t, err)
	assert.NotNil(t, store2)

	// form the cluster of two rafts
	store2NodeId := store2.config.longMemberId
	store2Addr := fmt.Sprintf("%s:%d", hn, cfg2.RaftPort)
	err = store1.addVoter(store2NodeId, store2Addr)
	assert.Nil(t, err)

	// set a value at raft 1
	key := ulid.MustNew(ulid.Now(), rand.Reader).String()
	value := []byte(ulid.MustNew(ulid.Now(), rand.Reader).String())
	created, err := store1.set(key, value)
	assert.Nil(t, err)
	assert.True(t, created)

	// give it some time to replicate
	time.Sleep(100 * time.Millisecond)

	val, err := store2.get(key)
	assert.Nil(t, err)
	assert.Equal(t, value, val)

	err = store1.close()
	assert.Nil(t, err)
	err = store2.close()
	assert.Nil(t, err)
}

func TestConsensusStoreConsistentGet(t *testing.T) {
	hn, err := os.Hostname()
	assert.Nil(t, err)

	cfg1 := ClusterConfig{
		RaftPort:        9876,
		RaftServicePort: 9877,
		longMemberId:    "node111",
		hostname:        hn,
		raftNotifyCh:    make(chan bool, 16),
		isDevMode:       true,
	}
	store1, err := createNewConsensusStore(cfg1)
	assert.Nil(t, err)
	assert.NotNil(t, store1)
	assert.True(t, <-cfg1.raftNotifyCh)

	peers := make([]string, 1)
	peers[0] = "Polly wants cracker!"
	cfg2 := ClusterConfig{
		RaftPort:        6789,
		RaftServicePort: 6780,
		longMemberId:    "node222",
		hostname:        hn,
		raftNotifyCh:    make(chan bool, 16),
		Peers:           peers,
		isDevMode:       true,
	}
	store2, err := createNewConsensusStore(cfg2)
	assert.Nil(t, err)
	assert.NotNil(t, store2)

	// form the cluster of two rafts
	store2Addr := fmt.Sprintf("%s:%d", hn, cfg2.RaftPort)
	err = store1.addVoter(store2.config.longMemberId, store2Addr)
	assert.Nil(t, err)

	key := ulid.MustNew(ulid.Now(), rand.Reader).String()
	value := []byte(ulid.MustNew(ulid.Now(), rand.Reader).String())
	created, err := store1.set(key, value)
	assert.Nil(t, err)
	assert.True(t, created)

	val, err := store1.consistentGet(key)
	assert.Nil(t, err)
	assert.Equal(t, value, val)

	err = store1.close()
	assert.Nil(t, err)
	err = store2.close()
	assert.Nil(t, err)
}

func TestConsensusStorePersistentStorage(t *testing.T) {
	hn, err := os.Hostname()
	assert.Nil(t, err)
	path := "./db.raft.db1"

	cfg1 := ClusterConfig{
		RaftPort:        9876,
		RaftServicePort: 9877,
		RaftStoreDir:    path,
		longMemberId:    "node111",
		hostname:        hn,
		raftNotifyCh:    make(chan bool, 16),
		isDevMode:       false,
	}
	store1, err := createNewConsensusStore(cfg1)
	assert.Nil(t, err)
	assert.NotNil(t, store1)
	assert.True(t, <-cfg1.raftNotifyCh)

	_, err = os.Stat(path)
	assert.Nil(t, err)

	err = store1.close()
	assert.Nil(t, err)

	err = os.RemoveAll(path)
	assert.Nil(t, err)
}

func TestConsensusStoreWatcher(t *testing.T) {
	hn, err := os.Hostname()
	assert.Nil(t, err)

	cfg1 := ClusterConfig{
		RaftPort:        9876,
		RaftServicePort: 9877,
		longMemberId:    "node111",
		hostname:        hn,
		raftNotifyCh:    make(chan bool, 16),
		isDevMode:       true,
	}
	store1, err := createNewConsensusStore(cfg1)
	assert.Nil(t, err)
	assert.NotNil(t, store1)
	assert.True(t, <-cfg1.raftNotifyCh)

	peers := make([]string, 1)
	peers[0] = "Polly wants cracker!"
	cfg2 := ClusterConfig{
		RaftPort:        6789,
		RaftServicePort: 6780,
		longMemberId:    "node222",
		hostname:        hn,
		raftNotifyCh:    make(chan bool, 16),
		Peers:           peers,
		isDevMode:       true,
	}
	store2, err := createNewConsensusStore(cfg2)
	assert.Nil(t, err)
	assert.NotNil(t, store2)

	// form the cluster of two rafts
	store2Addr := fmt.Sprintf("%s:%d", hn, cfg2.RaftPort)
	err = store1.addVoter(store2.config.longMemberId, store2Addr)
	assert.Nil(t, err)

	watcherFired := make(chan string)
	fn1 := func(key string, value []byte) {
		store2.logger.Infof("Got key in watcher %s", key)
		watcherFired <- key
	}
	store2.addWatcher("prefix___aaa", fn1)

	// the test goes here
	key := "prefix___aaa"
	value := []byte(ulid.MustNew(ulid.Now(), rand.Reader).String())
	created, err := store1.set(key, value)
	assert.Nil(t, err)
	assert.True(t, created)
	assert.Equal(t, key, <-watcherFired)

	err = store1.close()
	assert.Nil(t, err)
	err = store2.close()
	assert.Nil(t, err)
}

func TestConsensusStoreGetPrefix(t *testing.T) {
	hn, err := os.Hostname()
	assert.Nil(t, err)

	cfg1 := ClusterConfig{
		RaftPort:        9876,
		RaftServicePort: 9877,
		longMemberId:    "node111",
		hostname:        hn,
		raftNotifyCh:    make(chan bool, 16),
		isDevMode:       true,
	}
	store1, err := createNewConsensusStore(cfg1)
	assert.Nil(t, err)
	assert.NotNil(t, store1)
	assert.True(t, <-cfg1.raftNotifyCh)

	commonPrefix := "polly_wants_cracker_"

	store1.set(commonPrefix+"1", []byte(ulid.MustNew(ulid.Now(), rand.Reader).String()))
	store1.set(commonPrefix+"2", []byte(ulid.MustNew(ulid.Now(), rand.Reader).String()))
	store1.set(commonPrefix+"3", []byte(ulid.MustNew(ulid.Now(), rand.Reader).String()))
	store1.set("lalalalla_1", []byte(ulid.MustNew(ulid.Now(), rand.Reader).String()))

	kvs, err := store1.getPrefix(commonPrefix)
	assert.Equal(t, 3, len(kvs))
	assert.True(t, isStringIsInKvArray(commonPrefix+"3", kvs))
	assert.True(t, isStringIsInKvArray(commonPrefix+"1", kvs))
	assert.True(t, isStringIsInKvArray(commonPrefix+"2", kvs))
	assert.Nil(t, err)

	kvs, err = store1.getPrefix("la")
	assert.Equal(t, 1, len(kvs))
	assert.True(t, isStringIsInKvArray("lalalalla_1", kvs))
	assert.Nil(t, err)

	kvs, err = store1.getPrefix("narf")
	assert.Equal(t, 0, len(kvs))
	assert.Nil(t, err)

	err = store1.close()
	assert.Nil(t, err)
}

func isStringIsInKvArray(findMe string, inThisArray []*kv) bool {
	for _, item := range inThisArray {
		if item.k == findMe {
			return true
		}
	}
	return false
}
