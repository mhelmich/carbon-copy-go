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
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestClusterBasic(t *testing.T) {
	hn := "127.0.0.1"

	cfg1 := ClusterConfig{
		RaftPort:        17111,
		NumRaftVoters:   3,
		Peers:           nil,
		hostname:        hn,
		RaftServicePort: 27111,
		SerfPort:        37111,
		longMemberId:    "node1",
		raftNotifyCh:    make(chan bool, 16),
		logger: log.WithFields(log.Fields{
			"cluster": "AAA",
		}),
		isDevMode: true,
	}
	c1, err := createNewCluster(cfg1)
	assert.Nil(t, err)
	assert.NotNil(t, c1)

	peers := make([]string, 1)
	peers[0] = fmt.Sprintf("%s:%d", hn, 37111)
	cfg2 := ClusterConfig{
		RaftPort:        17222,
		NumRaftVoters:   3,
		Peers:           peers,
		hostname:        hn,
		RaftServicePort: 27222,
		SerfPort:        37222,
		longMemberId:    "node2",
		raftNotifyCh:    make(chan bool, 16),
		logger: log.WithFields(log.Fields{
			"cluster": "BBB",
		}),
		isDevMode: true,
	}
	c2, err := createNewCluster(cfg2)
	assert.Nil(t, err)
	assert.NotNil(t, c2)

	ids := make(map[int]bool)
	ids[1] = true
	ids[2] = true
	_, ok := ids[c1.GetMyShortMemberId()]
	assert.True(t, ok)
	delete(ids, c1.GetMyShortMemberId())
	_, ok = ids[c2.GetMyShortMemberId()]
	assert.True(t, ok)
	delete(ids, c2.GetMyShortMemberId())
	assert.Equal(t, 0, len(ids))

	assert.Nil(t, c1.Close())
	assert.Nil(t, c2.Close())
}
