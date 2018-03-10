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
	"testing"
)

func TestConsensusStoreBasic(t *testing.T) {
	cfg1 := ConsensusStoreConfig{
		RaftPort:     9876,
		ServicePort:  9877,
		RaftStoreDir: "./raft1.db",
	}
	store1, err := createNewConsensusStore(cfg1)
	assert.Nil(t, err)
	assert.NotNil(t, store1)

	peers := make([]string, 1)
	peers[0] = fmt.Sprintf("localhost:%d", cfg1.ServicePort)
	cfg2 := ConsensusStoreConfig{
		RaftPort:     6789,
		ServicePort:  6780,
		RaftStoreDir: "./raft2.db",
		Peers:        peers,
	}
	store2, err := createNewConsensusStore(cfg2)
	assert.Nil(t, err)
	assert.NotNil(t, store2)

	err = store1.Close()
	assert.Nil(t, err)
	err = store2.Close()
	assert.Nil(t, err)
}
