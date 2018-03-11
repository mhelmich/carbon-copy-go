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
	"github.com/stretchr/testify/assert"
	// "os"
	"testing"
	"time"
)

func TestConsensusStoreBasic(t *testing.T) {
	raftDbPath1 := "./db.raft1.db"
	cfg1 := ConsensusStoreConfig{
		RaftPort:     9876,
		ServicePort:  9877,
		RaftStoreDir: raftDbPath1,
		isDevMode:    true,
	}
	store1, err := createNewConsensusStore(cfg1)
	assert.Nil(t, err)
	assert.NotNil(t, store1)

	time.Sleep(3 * time.Second)

	peers := make([]string, 1)
	peers[0] = fmt.Sprintf("localhost:%d", cfg1.ServicePort)
	raftDbPath2 := "./db.raft2.db"
	cfg2 := ConsensusStoreConfig{
		RaftPort:     6789,
		ServicePort:  6780,
		RaftStoreDir: raftDbPath2,
		Peers:        peers,
		isDevMode:    true,
	}
	store2, err := createNewConsensusStore(cfg2)
	assert.Nil(t, err)
	assert.NotNil(t, store2)

	time.Sleep(5 * time.Second)

	err = store1.Close()
	assert.Nil(t, err)
	err = store2.Close()
	assert.Nil(t, err)
	// assert.Nil(t, os.RemoveAll(raftDbPath1))
	// assert.Nil(t, os.RemoveAll(raftDbPath2))
}
