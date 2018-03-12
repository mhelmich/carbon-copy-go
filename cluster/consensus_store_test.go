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
	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestConsensusStoreBasic(t *testing.T) {
	raftDbPath1 := "./db.raft1.db"
	cfg1 := consensusStoreConfig{
		RaftPort:     9876,
		ServicePort:  9877,
		RaftStoreDir: raftDbPath1,
		isDevMode:    true,
	}
	store1, err := createNewConsensusStore(cfg1)
	assert.Nil(t, err)
	assert.NotNil(t, store1)

	// this is just to give the node some time to settle
	// things are happening pretty much immediately
	// I'm not waiting for anything
	time.Sleep(2 * time.Second)

	peers := make([]string, 1)
	peers[0] = fmt.Sprintf("localhost:%d", cfg1.ServicePort)
	raftDbPath2 := "./db.raft2.db"
	cfg2 := consensusStoreConfig{
		RaftPort:     6789,
		ServicePort:  6780,
		RaftStoreDir: raftDbPath2,
		Peers:        peers,
		isDevMode:    true,
	}
	store2, err := createNewConsensusStore(cfg2)
	assert.Nil(t, err)
	assert.NotNil(t, store2)

	// this is just to give the node some time to settle
	// things are happening pretty much immediately
	// I'm not waiting for anything
	time.Sleep(1 * time.Second)

	key := ulid.MustNew(ulid.Now(), rand.Reader).String()
	value := ulid.MustNew(ulid.Now(), rand.Reader).String()
	err = store1.Set(key, value)
	assert.Nil(t, err)

	time.Sleep(1 * time.Second)

	val, err := store2.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, value, val)

	err = store1.Close()
	assert.Nil(t, err)
	err = store2.Close()
	assert.Nil(t, err)
}

func TestConsensusStoreHopscotch(t *testing.T) {
	raftDbPath1 := "./db.raft1.db"
	cfg1 := consensusStoreConfig{
		RaftPort:     9876,
		ServicePort:  9877,
		RaftStoreDir: raftDbPath1,
		isDevMode:    true,
	}
	store1, err := createNewConsensusStore(cfg1)
	assert.Nil(t, err)
	assert.NotNil(t, store1)

	// this is just to give the node some time to settle
	// things are happening pretty much immediately
	// I'm not waiting for anything
	time.Sleep(2 * time.Second)

	peers := make([]string, 1)
	peers[0] = fmt.Sprintf("localhost:%d", cfg1.ServicePort)
	raftDbPath2 := "./db.raft2.db"
	cfg2 := consensusStoreConfig{
		RaftPort:     6789,
		ServicePort:  6780,
		RaftStoreDir: raftDbPath2,
		Peers:        peers,
		isDevMode:    true,
	}
	store2, err := createNewConsensusStore(cfg2)
	assert.Nil(t, err)
	assert.NotNil(t, store2)

	// this is just to give the node some time to settle
	// things are happening pretty much immediately
	// I'm not waiting for anything
	time.Sleep(2 * time.Second)

	peers2 := make([]string, 1)
	peers2[0] = fmt.Sprintf("localhost:%d", cfg2.ServicePort)
	raftDbPath3 := "./db.raft3.db"
	cfg3 := consensusStoreConfig{
		RaftPort:     4567,
		ServicePort:  7654,
		RaftStoreDir: raftDbPath3,
		Peers:        peers2,
		isDevMode:    true,
	}
	store3, err := createNewConsensusStore(cfg3)
	assert.Nil(t, err)
	assert.NotNil(t, store3)
}
