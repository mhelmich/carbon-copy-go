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
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

////////////////////////////////////////////////////////////////////////
/////
/////  MOCK DEFINITION
/////
////////////////////////////////////////////////////////////////////////

type mockConsensusClient struct {
	mock.Mock
	closed bool
}

func (ec *mockConsensusClient) get(ctx context.Context, key string) (string, error) {
	args := ec.Called(ctx, key)
	return args.String(0), args.Error(1)
}

func (ec *mockConsensusClient) getSortedRange(ctx context.Context, keyPrefix string) ([]kvStr, error) {
	args := ec.Called(ctx, keyPrefix)
	return args.Get(0).([]kvStr), args.Error(1)
}

func (ec *mockConsensusClient) put(ctx context.Context, key string, value string) error {
	args := ec.Called(ctx, key, value)
	return args.Error(0)
}

func (ec *mockConsensusClient) putIfAbsent(ctx context.Context, key string, value string) (bool, error) {
	args := ec.Called(ctx, key, value)
	return args.Bool(0), args.Error(1)
}

func (ec *mockConsensusClient) compareAndPut(ctx context.Context, key string, oldValue string, newValue string) (bool, error) {
	args := ec.Called(ctx, key, oldValue, newValue)
	return args.Bool(0), args.Error(1)
}

func (ec *mockConsensusClient) watchKey(ctx context.Context, key string) (<-chan *kvStr, error) {
	args := ec.Called(ctx, key)
	return args.Get(0).(chan *kvStr), args.Error(1)
}

func (ec *mockConsensusClient) watchKeyPrefix(ctx context.Context, prefix string) (<-chan []*kvBytes, error) {
	args := ec.Called(ctx, prefix)
	return args.Get(0).(chan []*kvBytes), args.Error(1)
}

func (ec *mockConsensusClient) watchKeyPrefixStr(ctx context.Context, prefix string) (<-chan []*kvStr, error) {
	args := ec.Called(ctx, prefix)
	return args.Get(0).(chan []*kvStr), args.Error(1)
}

func (ec *mockConsensusClient) isClosed() bool {
	return ec.closed
}

func (ec *mockConsensusClient) close() error {
	ec.closed = true
	return nil
}

////////////////////////////////////////////////////////////////////////
/////
/////  ACTUAL TEST CODE
/////
////////////////////////////////////////////////////////////////////////

// func TestClusterAllocateMyNodeIdBasic(t *testing.T) {
// 	mockEtcd := &mockConsensusClient{}
// 	// mock node id allocation
// 	kvs := []kvStr{
// 		kvStr{"0", ""},
// 		kvStr{"1", ""},
// 		kvStr{"2", ""},
// 		kvStr{"4", ""},
// 		kvStr{"7", ""},
// 	}
// 	mockEtcd.On("getSortedRange", mock.AnythingOfTypeArgument("*context.emptyCtx"), consensusNodesRootName).Return(kvs, nil)
// 	mockEtcd.On("putIfAbsent", mock.AnythingOfTypeArgument("*context.emptyCtx"), consensusNodesRootName+"3", "").Return(true, nil)

// 	// run test
// 	idChan := startMyNodeIdProvider(context.Background(), mockEtcd)
// 	nodeId := <-idChan
// 	log.Infof("Acquired node id %d", nodeId)
// }

// func TestClusterAllocateMyNodeIdConflict(t *testing.T) {
// 	mockEtcd := &mockConsensusClient{}
// 	// mock node id allocation
// 	kvs := []kvStr{
// 		kvStr{"0", ""},
// 		kvStr{"1", ""},
// 		kvStr{"2", ""},
// 		kvStr{"4", ""},
// 		kvStr{"7", ""},
// 	}
// 	mockEtcd.On("getSortedRange", mock.AnythingOfTypeArgument("*context.emptyCtx"), consensusNodesRootName).Return(kvs, nil)
// 	mockEtcd.On("putIfAbsent", mock.AnythingOfTypeArgument("*context.emptyCtx"), consensusNodesRootName+"3", "").Return(false, nil)
// 	mockEtcd.On("putIfAbsent", mock.AnythingOfTypeArgument("*context.emptyCtx"), consensusNodesRootName+"5", "").Return(true, nil)

// 	// run test
// 	idChan := startMyNodeIdProvider(context.Background(), mockEtcd)
// 	nodeId := <-idChan
// 	log.Infof("Acquired node id %d", nodeId)
// }

// func TestClusterNodeInfoWatcher(t *testing.T) {
// 	mockEtcd := &mockConsensusClient{}
// 	// mock node id allocation
// 	kvs := []kvStr{}
// 	mockEtcd.On("getSortedRange", mock.AnythingOfTypeArgument("*context.emptyCtx"), consensusNodesRootName).Return(kvs, nil)
// 	mockEtcd.On("putIfAbsent", mock.AnythingOfTypeArgument("*context.emptyCtx"), consensusNodesRootName+"1", "").Return(true, nil)

// 	kvBytesChan := make(chan []*kvBytes)
// 	mockEtcd.On("watchKeyPrefix", mock.AnythingOfTypeArgument("*context.emptyCtx"), consensusNodesRootName).Return(kvBytesChan, nil)

// 	cluster, err := createNewClusterWithConsensus(context.Background(), mockEtcd)
// 	assert.Nil(t, err)
// 	nodeInfoChan, err := cluster.GetNodeConnectionInfoUpdates()
// 	assert.Nil(t, err)
// 	assert.NotNil(t, nodeInfoChan)

// 	numNodeInfos := 8
// 	go func() {
// 		allNodeInfos := make([]*kvBytes, numNodeInfos)
// 		for i := 0; i < numNodeInfos; i++ {
// 			uuid, err := uuid.NewRandom()
// 			assert.Nil(t, err)
// 			nodeInfoProto := &pb.NodeInfo{
// 				NodeId: int32(i),
// 				Host:   uuid.String() + "_" + strconv.Itoa(i),
// 			}
// 			buf, err := proto.Marshal(nodeInfoProto)
// 			assert.Nil(t, err)
// 			assert.NotNil(t, buf)
// 			bs := make([]byte, 4)
// 			binary.LittleEndian.PutUint32(bs, uint32(i))
// 			allNodeInfos[i] = &kvBytes{
// 				key:   bs,
// 				value: buf,
// 			}
// 		}

// 		kvBytesChan <- allNodeInfos
// 	}()

// 	nodeInfos := <-nodeInfoChan
// 	assert.Equal(t, numNodeInfos, len(nodeInfos))
// 	assert.True(t, nodeInfos[0].nodeId == 0)
// 	assert.True(t, strings.HasSuffix(nodeInfos[0].nodeAddress, "_0"))
// 	assert.True(t, nodeInfos[3].nodeId == 3)
// 	assert.True(t, strings.HasSuffix(nodeInfos[3].nodeAddress, "_3"))
// 	assert.True(t, nodeInfos[5].nodeId == 5)
// 	assert.True(t, strings.HasSuffix(nodeInfos[5].nodeAddress, "_5"))
// 	assert.True(t, nodeInfos[7].nodeId == 7)
// 	assert.True(t, strings.HasSuffix(nodeInfos[7].nodeAddress, "_7"))
// }

func TestClusterBasic(t *testing.T) {
	cfg1 := ClusterConfig{
		RaftPort:        17171,
		NumRaftVoters:   3,
		Peers:           nil,
		hostname:        "127.0.0.1",
		RaftServicePort: 27272,
		SerfPort:        37373,
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

	c1.printClusterState()
	time.Sleep(3 * time.Second)
	c1.printClusterState()

	peers := make([]string, 1)
	peers[0] = fmt.Sprintf("%s:%d", "127.0.0.1", cfg1.SerfPort)
	cfg2 := ClusterConfig{
		RaftPort:        18181,
		NumRaftVoters:   3,
		Peers:           peers,
		hostname:        "127.0.0.1",
		RaftServicePort: 28282,
		SerfPort:        38383,
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

	time.Sleep(3 * time.Second)
	c2.printClusterState()

	time.Sleep(4 * time.Second)
	c2.printClusterState()
	c1.Close()
	time.Sleep(3 * time.Second)
	c2.Close()
}
