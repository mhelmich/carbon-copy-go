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

	"github.com/gogo/protobuf/proto"
	"github.com/mhelmich/carbon-copy-go/pb"
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
	go func() {
		ch := c1.GetGridMemberChangeEvents()
		for {
			<-ch
		}
	}()

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
	go func() {
		ch := c2.GetGridMemberChangeEvents()
		for {
			<-ch
		}
	}()

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

	// assert on cluster state
	// I'm constantly asking c2 during this series of assertions
	// see who the leader is - should be c1
	bites, err := c2.consensusStore.get(consensusLeaderName)
	assert.Nil(t, err)
	assert.Equal(t, cfg1.longMemberId, string(bites))
	// get member info of c1 - assert on short id
	bites, err = c2.consensusStore.get(consensusMembersRootName + cfg1.longMemberId)
	assert.Nil(t, err)
	assert.NotNil(t, bites)
	memberInfo := &pb.MemberInfo{}
	err = proto.Unmarshal(bites, memberInfo)
	assert.Nil(t, err)
	assert.Equal(t, cfg1.longMemberId, memberInfo.LongMemberId)
	assert.Equal(t, 1, int(memberInfo.ShortMemberId))
	// get member info of c2 - assert on short id
	bites, err = c2.consensusStore.get(consensusMembersRootName + cfg2.longMemberId)
	assert.Nil(t, err)
	assert.NotNil(t, bites)
	memberInfo = &pb.MemberInfo{}
	err = proto.Unmarshal(bites, memberInfo)
	assert.Nil(t, err)
	assert.Equal(t, cfg2.longMemberId, memberInfo.LongMemberId)
	assert.Equal(t, 2, int(memberInfo.ShortMemberId))
	// assert both have been added as raft voters
	kvs, err := c2.consensusStore.getPrefix(consensusVotersName)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(kvs))
	assert.NotNil(t, bites)

	assert.Nil(t, c1.Close())
	assert.Nil(t, c2.Close())
}

func TestClusterHouseKeeping(t *testing.T) {
	hn := "127.0.0.1"

	cfg1 := ClusterConfig{
		RaftPort:        18111,
		NumRaftVoters:   3,
		Peers:           nil,
		hostname:        hn,
		RaftServicePort: 28111,
		SerfPort:        38111,
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
	gridEvent := <-c1.GetGridMemberChangeEvents()
	assert.Equal(t, MemberJoined, gridEvent.Type)
	assert.Equal(t, 1, gridEvent.ShortMemberId)
	// yupp, no grid port set hence the value is 0
	assert.Equal(t, hn+":0", gridEvent.MemberGridAddress)
	// eat all other messages in this channel
	go func() {
		ch := c1.GetGridMemberChangeEvents()
		for {
			<-ch
		}
	}()

	// assert on consensus store state
	bites, err := c1.consensusStore.get(consensusLeaderName)
	assert.Nil(t, err)
	assert.Equal(t, cfg1.longMemberId, string(bites))
	bites, err = c1.consensusStore.get(consensusMembersRootName + cfg1.longMemberId)
	assert.Nil(t, err)
	assert.NotNil(t, bites)
	memberInfo := &pb.MemberInfo{}
	err = proto.Unmarshal(bites, memberInfo)
	assert.Nil(t, err)
	assert.Equal(t, cfg1.longMemberId, memberInfo.LongMemberId)
	kvs, err := c1.consensusStore.getPrefix(consensusVotersName)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(kvs))
	assert.Equal(t, consensusVotersName+cfg1.longMemberId, kvs[0].k)
	assert.NotNil(t, bites)

	c1.Close()
}
