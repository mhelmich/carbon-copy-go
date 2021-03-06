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
	consumeChannelEmpty(c1.GetGridMemberChangeEvents())

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
	consumeChannelEmpty(c2.GetGridMemberChangeEvents())

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
	consumeChannelEmpty(c1.GetGridMemberChangeEvents())

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

func TestClusterSimpleLeaderFailoverOnlyVoters(t *testing.T) {
	hn := "127.0.0.1"

	cfg1 := ClusterConfig{
		RaftPort:        19111,
		NumRaftVoters:   3,
		Peers:           nil,
		hostname:        hn,
		RaftServicePort: 29111,
		SerfPort:        39111,
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
	consumeChannelEmpty(c1.GetGridMemberChangeEvents())

	time.Sleep(500 * time.Millisecond)
	peers2 := make([]string, 1)
	peers2[0] = fmt.Sprintf("%s:%d", hn, cfg1.SerfPort)
	cfg2 := ClusterConfig{
		RaftPort:        19222,
		NumRaftVoters:   3,
		Peers:           peers2,
		hostname:        hn,
		RaftServicePort: 29222,
		SerfPort:        39222,
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
	consumeChannelEmpty(c2.GetGridMemberChangeEvents())

	time.Sleep(500 * time.Millisecond)
	peers3 := make([]string, 1)
	peers3[0] = fmt.Sprintf("%s:%d", hn, cfg1.SerfPort)
	cfg3 := ClusterConfig{
		RaftPort:        19333,
		NumRaftVoters:   3,
		Peers:           peers3,
		hostname:        hn,
		RaftServicePort: 29333,
		SerfPort:        39333,
		longMemberId:    "node3",
		raftNotifyCh:    make(chan bool, 16),
		logger: log.WithFields(log.Fields{
			"cluster": "CCC",
		}),
		isDevMode: true,
	}

	c3, err := createNewCluster(cfg3)
	assert.Nil(t, err)
	assert.NotNil(t, c3)
	consumeChannelEmpty(c3.GetGridMemberChangeEvents())

	time.Sleep(2000 * time.Millisecond)
	kvs, err := c2.consensusStore.getPrefix(consensusVotersName)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(kvs))

	c1.Close()
	time.Sleep(2000 * time.Millisecond)

	kvs, err = c3.consensusStore.getPrefix(consensusVotersName)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(kvs))

	c2.Close()
	c3.Close()
}

func TestClusterComplicatedLeaderFailoverVotersNonVoters(t *testing.T) {
	hn := "127.0.0.1"

	cfg1 := ClusterConfig{
		RaftPort:        19111,
		NumRaftVoters:   3,
		Peers:           nil,
		hostname:        hn,
		RaftServicePort: 29111,
		SerfPort:        39111,
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
	consumeChannelEmpty(c1.GetGridMemberChangeEvents())

	time.Sleep(500 * time.Millisecond)
	peers2 := make([]string, 1)
	peers2[0] = fmt.Sprintf("%s:%d", hn, cfg1.SerfPort)
	cfg2 := ClusterConfig{
		RaftPort:        19222,
		NumRaftVoters:   3,
		Peers:           peers2,
		hostname:        hn,
		RaftServicePort: 29222,
		SerfPort:        39222,
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
	consumeChannelEmpty(c2.GetGridMemberChangeEvents())

	time.Sleep(500 * time.Millisecond)
	peers3 := make([]string, 1)
	peers3[0] = fmt.Sprintf("%s:%d", hn, cfg1.SerfPort)
	cfg3 := ClusterConfig{
		RaftPort:        19333,
		NumRaftVoters:   3,
		Peers:           peers3,
		hostname:        hn,
		RaftServicePort: 29333,
		SerfPort:        39333,
		longMemberId:    "node3",
		raftNotifyCh:    make(chan bool, 16),
		logger: log.WithFields(log.Fields{
			"cluster": "CCC",
		}),
		isDevMode: true,
	}
	c3, err := createNewCluster(cfg3)
	assert.Nil(t, err)
	assert.NotNil(t, c3)
	consumeChannelEmpty(c3.GetGridMemberChangeEvents())

	time.Sleep(500 * time.Millisecond)
	peers4 := make([]string, 1)
	peers4[0] = fmt.Sprintf("%s:%d", hn, cfg3.SerfPort)
	cfg4 := ClusterConfig{
		RaftPort:        19444,
		NumRaftVoters:   3,
		Peers:           peers3,
		hostname:        hn,
		RaftServicePort: 29444,
		SerfPort:        39444,
		longMemberId:    "node4",
		raftNotifyCh:    make(chan bool, 16),
		logger: log.WithFields(log.Fields{
			"cluster": "DDD",
		}),
		isDevMode: true,
	}
	c4, err := createNewCluster(cfg4)
	assert.Nil(t, err)
	assert.NotNil(t, c4)
	consumeChannelEmpty(c4.GetGridMemberChangeEvents())

	time.Sleep(500 * time.Millisecond)
	peers5 := make([]string, 1)
	peers5[0] = fmt.Sprintf("%s:%d", hn, cfg4.SerfPort)
	cfg5 := ClusterConfig{
		RaftPort:        19555,
		NumRaftVoters:   3,
		Peers:           peers3,
		hostname:        hn,
		RaftServicePort: 29555,
		SerfPort:        39555,
		longMemberId:    "node5",
		raftNotifyCh:    make(chan bool, 16),
		logger: log.WithFields(log.Fields{
			"cluster": "EEE",
		}),
		isDevMode: true,
	}
	c5, err := createNewCluster(cfg5)
	assert.Nil(t, err)
	assert.NotNil(t, c5)
	consumeChannelEmpty(c5.GetGridMemberChangeEvents())

	time.Sleep(500 * time.Millisecond)
	kvs, err := c3.consensusStore.getPrefix(consensusVotersName)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(kvs))
	kvs, err = c3.consensusStore.getPrefix(consensusNonVotersName)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(kvs))

	c3.printClusterState()
	c1.Close()
	time.Sleep(3000 * time.Millisecond)
	c3.printClusterState()

	kvs, err = c3.consensusStore.getPrefix(consensusVotersName)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(kvs))
	kvs, err = c3.consensusStore.getPrefix(consensusNonVotersName)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(kvs))

	c2.Close()
	c3.Close()
	c4.Close()
	c5.Close()
}

func TestClusterVoterNonLeaderDies(t *testing.T) {
	hn := "127.0.0.1"

	cfg1 := ClusterConfig{
		RaftPort:        49111,
		NumRaftVoters:   3,
		Peers:           nil,
		hostname:        hn,
		RaftServicePort: 48111,
		SerfPort:        47111,
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
	consumeChannelEmpty(c1.GetGridMemberChangeEvents())

	time.Sleep(500 * time.Millisecond)
	peers2 := make([]string, 1)
	peers2[0] = fmt.Sprintf("%s:%d", hn, cfg1.SerfPort)
	cfg2 := ClusterConfig{
		RaftPort:        49222,
		NumRaftVoters:   3,
		Peers:           peers2,
		hostname:        hn,
		RaftServicePort: 48222,
		SerfPort:        47222,
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
	consumeChannelEmpty(c2.GetGridMemberChangeEvents())

	time.Sleep(500 * time.Millisecond)
	peers3 := make([]string, 1)
	peers3[0] = fmt.Sprintf("%s:%d", hn, cfg1.SerfPort)
	cfg3 := ClusterConfig{
		RaftPort:        49333,
		NumRaftVoters:   3,
		Peers:           peers3,
		hostname:        hn,
		RaftServicePort: 48333,
		SerfPort:        47333,
		longMemberId:    "node3",
		raftNotifyCh:    make(chan bool, 16),
		logger: log.WithFields(log.Fields{
			"cluster": "CCC",
		}),
		isDevMode: true,
	}
	c3, err := createNewCluster(cfg3)
	assert.Nil(t, err)
	assert.NotNil(t, c3)
	consumeChannelEmpty(c3.GetGridMemberChangeEvents())

	time.Sleep(500 * time.Millisecond)
	kvs, err := c3.consensusStore.getPrefix(consensusVotersName)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(kvs))
	kvs, err = c3.consensusStore.getPrefix(consensusNonVotersName)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(kvs))

	c3.printClusterState()
	c2.Close()
	time.Sleep(3000 * time.Millisecond)
	c3.printClusterState()

	kvs, err = c3.consensusStore.getPrefix(consensusVotersName)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(kvs))
	kvs, err = c3.consensusStore.getPrefix(consensusNonVotersName)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(kvs))

	c1.Close()
	c3.Close()
}

func consumeChannelEmpty(ch <-chan *GridMemberConnectionEvent) {
	go func() {
		for {
			i := <-ch
			if i == nil {
				return
			}
		}
	}()
}
