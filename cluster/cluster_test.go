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
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"math"
	"strconv"
	"testing"
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

func (ec *mockConsensusClient) watchKeyPrefix(ctx context.Context, prefix string) (<-chan []*kvStr, error) {
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

func TestClusterAllocateGlobalIds(t *testing.T) {
	mockEtcd := &mockConsensusClient{}
	// mock the id base setting at startup
	mockEtcd.On("putIfAbsent", mock.AnythingOfTypeArgument("*context.emptyCtx"), consensusIdAllocator, strconv.Itoa(math.MinInt64)).Return(true, nil)
	mockEtcd.On("get", mock.AnythingOfTypeArgument("*context.emptyCtx"), consensusIdAllocator).Return("13", nil)
	mockEtcd.On("compareAndPut", mock.AnythingOfTypeArgument("*context.emptyCtx"), consensusIdAllocator, "13", strconv.Itoa(13+idBufferSize)).Return(true, nil)
	// mock node id allocation
	mockEtcd.On("getSortedRange", mock.AnythingOfTypeArgument("*context.emptyCtx"), consensusNodesRootName).Return(make([]kvStr, 0), nil)
	mockEtcd.On("putIfAbsent", mock.AnythingOfTypeArgument("*context.emptyCtx"), consensusNodesRootName+"1", "").Return(true, nil)

	// run test
	cluster, err := createNewClusterWithConsensus(context.Background(), mockEtcd)
	assert.Nil(t, err)
	assert.NotNil(t, cluster)
	idChan := startGlobalIdProvider(context.Background(), cluster.consensus)
	assert.Equal(t, 13, <-idChan)
	assert.Equal(t, 14, <-idChan)
	assert.Equal(t, 15, <-idChan)
	assert.Equal(t, 16, <-idChan)
	cluster.Close()
}

func TestClusterAllocateMyNodeIdBasic(t *testing.T) {
	mockEtcd := &mockConsensusClient{}
	// mock the id base setting at startup
	mockEtcd.On("putIfAbsent", mock.AnythingOfTypeArgument("*context.emptyCtx"), consensusIdAllocator, strconv.Itoa(math.MinInt64)).Return(true, nil)
	// mock node id allocation
	kvs := []kvStr{
		kvStr{"0", ""},
		kvStr{"1", ""},
		kvStr{"2", ""},
		kvStr{"4", ""},
		kvStr{"7", ""},
	}
	mockEtcd.On("getSortedRange", mock.AnythingOfTypeArgument("*context.emptyCtx"), consensusNodesRootName).Return(kvs, nil)
	mockEtcd.On("putIfAbsent", mock.AnythingOfTypeArgument("*context.emptyCtx"), consensusNodesRootName+"3", "").Return(true, nil)

	// run test
	idChan := startMyNodeIdProvider(context.Background(), mockEtcd)
	nodeId := <-idChan
	log.Infof("Acquired node id %d", nodeId)
}

func TestClusterAllocateMyNodeIdConflict(t *testing.T) {
	mockEtcd := &mockConsensusClient{}
	// mock the id base setting at startup
	mockEtcd.On("putIfAbsent", mock.AnythingOfTypeArgument("*context.emptyCtx"), consensusIdAllocator, strconv.Itoa(math.MinInt64)).Return(true, nil)
	// mock node id allocation
	kvs := []kvStr{
		kvStr{"0", ""},
		kvStr{"1", ""},
		kvStr{"2", ""},
		kvStr{"4", ""},
		kvStr{"7", ""},
	}
	mockEtcd.On("getSortedRange", mock.AnythingOfTypeArgument("*context.emptyCtx"), consensusNodesRootName).Return(kvs, nil)
	mockEtcd.On("putIfAbsent", mock.AnythingOfTypeArgument("*context.emptyCtx"), consensusNodesRootName+"3", "").Return(false, nil)
	mockEtcd.On("putIfAbsent", mock.AnythingOfTypeArgument("*context.emptyCtx"), consensusNodesRootName+"5", "").Return(true, nil)

	// run test
	idChan := startMyNodeIdProvider(context.Background(), mockEtcd)
	nodeId := <-idChan
	log.Infof("Acquired node id %d", nodeId)
}
