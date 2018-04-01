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
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestMembershipBasic(t *testing.T) {
	hn := "127.0.0.1"
	nid1 := "node111"
	c1 := ClusterConfig{
		Peers:        make([]string, 0),
		hostname:     hn,
		SerfPort:     13111,
		longMemberId: nid1,
		logger: log.WithFields(log.Fields{
			"serf_port": 13111,
			"node_id":   nid1,
			"cluster":   "AAA",
		}),
	}

	m1, err := createNewMembership(c1)
	assert.Nil(t, err)
	assert.NotNil(t, m1)
	assertNumMessages(t, m1.memberJoinedOrUpdatedChan, 1)

	memberIds := m1.getAllLongMemberIds()
	assert.Equal(t, 1, len(memberIds))
	assert.True(t, isStringIsInStringArray(m1.myLongMemberId(), memberIds))

	nid2 := "node222"
	peers := make([]string, 1)
	peers[0] = fmt.Sprintf("%s:%d", hn, c1.SerfPort)
	c2 := ClusterConfig{
		Peers:        peers,
		hostname:     hn,
		SerfPort:     13222,
		longMemberId: nid2,
		logger: log.WithFields(log.Fields{
			"serf_port": 13222,
			"node_id":   nid2,
			"cluster":   "BBB",
		}),
	}

	m2, err := createNewMembership(c2)
	assert.Nil(t, err)
	assert.NotNil(t, m2)
	assertNumMessages(t, m1.memberJoinedOrUpdatedChan, 1)
	assertNumMessages(t, m2.memberJoinedOrUpdatedChan, 2)

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

	memberIds = m2.getAllLongMemberIds()
	assert.Equal(t, 2, len(memberIds))
	assert.True(t, isStringIsInStringArray(m1.myLongMemberId(), memberIds))
	assert.True(t, isStringIsInStringArray(m2.myLongMemberId(), memberIds))

	m1.close()
	assertNumMessages(t, m1.memberLeftChan, 1)
	assertNumMessages(t, m2.memberLeftChan, 1)

	memberIds = m2.getAllLongMemberIds()
	assert.Equal(t, 1, len(memberIds))
	assert.True(t, isStringIsInStringArray(m2.myLongMemberId(), memberIds))

	m2.close()
}

func TestMembershipMarkAsLeaderInCluster(t *testing.T) {
	hn := "127.0.0.1"
	nid1 := "node111"
	c1 := ClusterConfig{
		Peers:           make([]string, 0),
		hostname:        hn,
		SerfPort:        14111,
		RaftPort:        15111,
		RaftServicePort: 16111,
		longMemberId:    nid1,
		logger: log.WithFields(log.Fields{
			"serf_port": 14111,
			"node_id":   nid1,
		}),
	}

	m1, err := createNewMembership(c1)
	assert.Nil(t, err)
	assert.NotNil(t, m1)
	assertNumMessages(t, m1.memberJoinedOrUpdatedChan, 1)

	err = m1.markLeader()
	assert.Nil(t, err)
	assertNumMessages(t, m1.memberJoinedOrUpdatedChan, 1)
	assert.Equal(t, fmt.Sprintf("%s:%d", hn, c1.RaftServicePort), <-m1.raftLeaderServiceAddrChan)
	tags, ok := m1.getMemberById(m1.myLongMemberId())
	assert.True(t, ok)
	v, ok := tags[serfMDKeyRaftRole]
	assert.True(t, ok)
	assert.Equal(t, raftRoleLeader, v)

	nid2 := "node222"
	peers := make([]string, 1)
	peers[0] = fmt.Sprintf("%s:%d", hn, c1.SerfPort)
	c2 := ClusterConfig{
		Peers:           peers,
		hostname:        hn,
		SerfPort:        14222,
		RaftPort:        15222,
		RaftServicePort: 16222,
		longMemberId:    nid2,
		logger: log.WithFields(log.Fields{
			"serf_port": 14222,
			"node_id":   nid2,
		}),
	}

	m2, err := createNewMembership(c2)
	assert.Nil(t, err)
	assert.NotNil(t, m2)
	assertNumMessages(t, m1.memberJoinedOrUpdatedChan, 1)
	assertNumMessages(t, m2.memberJoinedOrUpdatedChan, 2)
	assert.Equal(t, fmt.Sprintf("%s:%d", hn, c1.RaftServicePort), <-m2.raftLeaderServiceAddrChan)

	m1.close()
	m2.close()
}

func TestMembershipNotificationDedup(t *testing.T) {
	hn := "127.0.0.1"
	nid1 := "node111"
	c1 := ClusterConfig{
		Peers:        make([]string, 0),
		hostname:     hn,
		SerfPort:     27111,
		longMemberId: nid1,
		logger: log.WithFields(log.Fields{
			"serf_port": 27111,
			"node_id":   nid1,
		}),
	}

	m1, err := createNewMembership(c1)
	assert.Nil(t, err)
	assert.NotNil(t, m1)
	assertNumMessages(t, m1.memberJoinedOrUpdatedChan, 1)

	nid2 := "node222"
	peers := make([]string, 1)
	peers[0] = fmt.Sprintf("%s:%d", hn, c1.SerfPort)
	c2 := ClusterConfig{
		Peers:        peers,
		hostname:     hn,
		SerfPort:     27222,
		longMemberId: nid2,
		logger: log.WithFields(log.Fields{
			"serf_port": 27222,
			"node_id":   nid2,
		}),
	}

	m2, err := createNewMembership(c2)
	assert.Nil(t, err)
	assert.NotNil(t, m2)
	assertNumMessages(t, m1.memberJoinedOrUpdatedChan, 1)
	assertNumMessages(t, m2.memberJoinedOrUpdatedChan, 2)

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
	newTags["key1"] = "value1"
	err = m1.updateMemberTags(newTags)
	assert.Nil(t, err)
	assertNumMessages(t, m1.memberJoinedOrUpdatedChan, 1)
	assertNumMessages(t, m2.memberJoinedOrUpdatedChan, 1)

	m, ok := m1.getMemberById(m1.myLongMemberId())
	assert.True(t, ok)
	assert.Equal(t, "value1", m["key1"])

	m, ok = m2.getMemberById(m1.myLongMemberId())
	assert.True(t, ok)
	assert.Equal(t, "value1", m["key1"])

	// update the same node with the same tags
	// see no messages being triggered
	err = m1.updateMemberTags(newTags)
	assert.Nil(t, err)
	assertNumMessages(t, m1.memberJoinedOrUpdatedChan, 0)
	assertNumMessages(t, m2.memberJoinedOrUpdatedChan, 0)

	// update the other member with these tags
	// see messages being triggered
	// as these tags are new to this node
	err = m2.updateMemberTags(newTags)
	assert.Nil(t, err)
	assertNumMessages(t, m1.memberJoinedOrUpdatedChan, 1)
	assertNumMessages(t, m2.memberJoinedOrUpdatedChan, 1)

	m, ok = m2.getMemberById(m2.myLongMemberId())
	assert.True(t, ok)
	assert.Equal(t, "value1", m["key1"])

	m, ok = m2.getMemberById(m1.myLongMemberId())
	assert.True(t, ok)
	assert.Equal(t, "value1", m["key1"])
	memberIds := m2.getAllLongMemberIds()
	assert.Equal(t, 2, len(memberIds))
	assert.True(t, isStringIsInStringArray(m1.myLongMemberId(), memberIds))
	assert.True(t, isStringIsInStringArray(m2.myLongMemberId(), memberIds))

	m1.close()
	assertNumMessages(t, m1.memberLeftChan, 1)
	assertNumMessages(t, m2.memberLeftChan, 1)
	m2.close()
}

func isStringIsInStringArray(findMe string, inThisArray []string) bool {
	for _, item := range inThisArray {
		if item == findMe {
			return true
		}
	}
	return false
}

func assertNumMessages(t *testing.T, c <-chan string, num int) {
	timeout := 500 * time.Millisecond
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
					msg := fmt.Sprintf("received wrong number of messages want [%d] have [%d]", num, numMessagesReceived)
					assert.FailNow(t, msg)
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
