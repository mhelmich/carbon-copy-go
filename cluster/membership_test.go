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
	"testing"
	"time"
)

func TestMembershipBasic(t *testing.T) {
	hn := "127.0.0.1"
	nid1 := "node111"
	c1 := ClusterConfig{
		Peers:        make([]string, 0),
		hostname:     hn,
		SerfPort:     17111,
		longMemberId: nid1,
		logger: log.WithFields(log.Fields{
			"serf_port": 17111,
			"node_id":   nid1,
		}),
	}

	m1, err := createNewMembership(c1)
	assert.Nil(t, err)
	assert.NotNil(t, m1)
	assertNumMessages(t, m1.memberJoined, 1)

	nid2 := "node222"
	peers := make([]string, 1)
	peers[0] = fmt.Sprintf("%s:%d", hn, c1.SerfPort)
	c2 := ClusterConfig{
		Peers:        peers,
		hostname:     hn,
		SerfPort:     17222,
		longMemberId: nid2,
		logger: log.WithFields(log.Fields{
			"serf_port": 17222,
			"node_id":   nid2,
		}),
	}

	m2, err := createNewMembership(c2)
	assert.Nil(t, err)
	assert.NotNil(t, m2)
	assertNumMessages(t, m1.memberJoined, 1)
	assertNumMessages(t, m2.memberJoined, 2)

	_, ok := m1.getMemberById(nid1)
	assert.True(t, ok)
	_, ok = m1.getMemberById(nid2)
	assert.True(t, ok)

	_, ok = m2.getMemberById(nid1)
	assert.True(t, ok)
	_, ok = m2.getMemberById(nid2)
	assert.True(t, ok)

	assert.Equal(t, 2, m1.getClusterSize())
	assert.Equal(t, 2, m2.getClusterSize())

	m1.close()
	assertNumMessages(t, m1.memberLeft, 1)
	assertNumMessages(t, m2.memberLeft, 1)
	m2.close()
}

func TestMembershipNotificationDedup(t *testing.T) {
	hn := "127.0.0.1"
	nid1 := "node111"
	c1 := ClusterConfig{
		Peers:        make([]string, 0),
		hostname:     hn,
		SerfPort:     17111,
		longMemberId: nid1,
		logger: log.WithFields(log.Fields{
			"serf_port": 17111,
			"node_id":   nid1,
		}),
	}

	m1, err := createNewMembership(c1)
	assert.Nil(t, err)
	assert.NotNil(t, m1)
	assertNumMessages(t, m1.memberJoined, 1)

	nid2 := "node222"
	peers := make([]string, 1)
	peers[0] = fmt.Sprintf("%s:%d", hn, c1.SerfPort)
	c2 := ClusterConfig{
		Peers:        peers,
		hostname:     hn,
		SerfPort:     17222,
		longMemberId: nid2,
		logger: log.WithFields(log.Fields{
			"serf_port": 17222,
			"node_id":   nid2,
		}),
	}

	m2, err := createNewMembership(c2)
	assert.Nil(t, err)
	assert.NotNil(t, m2)
	assertNumMessages(t, m1.memberJoined, 1)
	assertNumMessages(t, m2.memberJoined, 2)

	_, ok := m1.getMemberById(nid1)
	assert.True(t, ok)
	_, ok = m1.getMemberById(nid2)
	assert.True(t, ok)

	_, ok = m2.getMemberById(nid1)
	assert.True(t, ok)
	_, ok = m2.getMemberById(nid2)
	assert.True(t, ok)

	assert.Equal(t, 2, m1.getClusterSize())
	assert.Equal(t, 2, m2.getClusterSize())

	// update tags and see how many messages come out
	newTags := make(map[string]string)
	newTags["key"] = "value"
	err = m1.updateMemberTags(newTags)
	assert.Nil(t, err)
	assertNumMessages(t, m1.memberJoined, 1)
	assertNumMessages(t, m2.memberJoined, 1)

	// update the same node with the same tags
	// see no messages being triggered
	err = m1.updateMemberTags(newTags)
	assert.Nil(t, err)
	assertNumMessages(t, m1.memberJoined, 0)
	assertNumMessages(t, m2.memberJoined, 0)

	// update the other member with these tags
	// see messages being triggered
	// as these tags are new to this node
	err = m2.updateMemberTags(newTags)
	assert.Nil(t, err)
	assertNumMessages(t, m1.memberJoined, 1)
	assertNumMessages(t, m2.memberJoined, 1)

	m1.close()
	assertNumMessages(t, m1.memberLeft, 1)
	assertNumMessages(t, m2.memberLeft, 1)
	m2.close()
}

func assertNumMessages(t *testing.T, c <-chan string, num int) {
	timeout := 1 * time.Second
	if num > 0 {
		numMessagesReceived := 0
		for {
			select {
			case s := <-c:
				if s == "" {
					// channel closed we're done
					assert.Equal(t, num, numMessagesReceived)
					return
				}

				if numMessagesReceived > num {
					// no point in doing more work,
					// we it's wrong already
					assert.FailNow(t, "Received too many messages")
				}

				numMessagesReceived++

			case <-time.After(timeout):
				if numMessagesReceived != num {
					assert.FailNow(t, "received wrong number of messages")
				} else {
					return
				}
			}
		}
	} else if num == 0 {
		select {
		case <-c:
			assert.FailNow(t, "get a message where I didn't expect one")
		case <-time.After(timeout):
		}
	}
}
