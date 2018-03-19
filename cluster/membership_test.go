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
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMembershipBasic(t *testing.T) {
	hn := "127.0.0.1"
	nid1 := ulid.MustNew(ulid.Now(), rand.Reader).String()
	c1 := clusterConfig{
		Peers:    make([]string, 0),
		hostname: hn,
		SerfPort: 47474,
		nodeId:   nid1,
		logger: log.WithFields(log.Fields{
			"serf_port": 47474,
			"node_id":   nid1,
		}),
	}

	m1, err := createNewMembership(c1)
	assert.Nil(t, err)
	assert.NotNil(t, m1)

	nid2 := ulid.MustNew(ulid.Now(), rand.Reader).String()
	peers := make([]string, 1)
	peers[0] = fmt.Sprintf("%s:%d", hn, c1.SerfPort)
	c2 := clusterConfig{
		Peers:    peers,
		hostname: hn,
		SerfPort: 57575,
		nodeId:   nid2,
		logger: log.WithFields(log.Fields{
			"serf_port": 57575,
			"node_id":   nid2,
		}),
	}

	m2, err := createNewMembership(c2)
	assert.Nil(t, err)
	assert.NotNil(t, m2)

	// TODO: build better latching
	// or rather saying: build latch at all!!
	time.Sleep(1 * time.Second)

	_, ok := m1.getNodeById(nid1)
	assert.True(t, ok)
	_, ok = m1.getNodeById(nid2)
	assert.True(t, ok)

	_, ok = m2.getNodeById(nid1)
	assert.True(t, ok)
	_, ok = m2.getNodeById(nid2)
	assert.True(t, ok)

	assert.Equal(t, 2, m1.getClusterSize())
	assert.Equal(t, 2, m2.getClusterSize())

	m1.close()
	m2.close()
}
