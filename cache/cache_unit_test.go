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

package cache

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

////////////////////////////////////////////////////////////////////////
/////
/////  MOCK DEFINITIONS
/////
////////////////////////////////////////////////////////////////////////

type mockCacheClient struct {
	mock.Mock
	fGet func() (*Put, *OwnerChanged, error)
}

func (cc *mockCacheClient) SendGet(ctx context.Context, g *Get) (*Put, *OwnerChanged, error) {
	args := cc.Called(ctx, g)

	if cc.fGet == nil {
		// take care of nil pointers
		p := args.Get(0)
		var pr *Put
		if p == nil {
			pr = nil
		} else {
			pr = p.(*Put)
		}

		oc := args.Get(1)
		var ocr *OwnerChanged
		if oc == nil {
			ocr = nil
		} else {
			ocr = oc.(*OwnerChanged)
		}

		return pr, ocr, args.Error(2)
	} else {
		return cc.fGet()
	}
}

func (cc *mockCacheClient) SendGetx(ctx context.Context, g *Getx) (*Putx, *OwnerChanged, error) {
	return nil, nil, nil
}

func (cc *mockCacheClient) SendInvalidate(ctx context.Context, i *Inv) (*InvAck, error) {
	return nil, nil
}

func (cc *mockCacheClient) Close() error {
	return nil
}

type mockCacheClientMapping struct {
	mock.Mock
}

func (ccm *mockCacheClientMapping) getClientForNodeId(nodeId int) (CacheClient, error) {
	args := ccm.Called(nodeId)
	return args.Get(0).(CacheClient), args.Error(1)
}

func (ccm *mockCacheClientMapping) addClientWithNodeId(nodeId int, addr string) {
}

func (ccm *mockCacheClientMapping) forEachParallel(f func(c CacheClient)) {
}

func (ccm *mockCacheClientMapping) printStats() {
}

func (ccm *mockCacheClientMapping) clear() {
}

func mockCache(mapping cacheClientMapping) *cacheImpl {
	myNodeId := 111
	clStore := createNewCacheLineStore()

	cache := &cacheImpl{
		store:         clStore,
		clientMapping: mapping,
		server:        nil,
		myNodeId:      myNodeId,
		port:          9999,
	}

	return cache
}

////////////////////////////////////////////////////////////////////////
/////
/////  ACTUAL TEST CODE
/////
////////////////////////////////////////////////////////////////////////

func TestGetUnit(t *testing.T) {
	lineId := 12345679
	latestBuffer := "testing_test_test_test"
	clientMock := new(mockCacheClient)
	var p *Put
	var oc *OwnerChanged
	p = &Put{
		Error:    CacheError_NoError,
		SenderId: int32(1234),
		LineId:   int64(lineId),
		Version:  int32(2),
		Buffer:   []byte(latestBuffer),
	}
	oc = nil
	clientMock.On("SendGet", mock.AnythingOfTypeArgument("*context.emptyCtx"), mock.AnythingOfTypeArgument("*cache.Get")).Return(p, oc, errors.New("this still works because error conditions are checked last!?!?!"))
	m := new(mockCacheClientMapping)
	m.On("getClientForNodeId", 1234).Return(clientMock, nil)
	cache := mockCache(m)

	line := newCacheLine(lineId, 111, []byte("lalalalalala"))
	line, loaded := cache.store.putIfAbsent(lineId, line)
	assert.False(t, loaded)
	line.cacheLineState = CacheLineState_Invalid
	line.ownerId = 1234

	// now run test
	readBites, err := cache.Get(lineId)
	assert.Nil(t, err)
	assert.Equal(t, latestBuffer, string(readBites))
	l, ok := cache.store.getCacheLineById(lineId)
	assert.True(t, ok)
	assert.Equal(t, 2, l.version)
	assert.Equal(t, CacheLineState_Shared, l.cacheLineState)
	m.AssertNumberOfCalls(t, "getClientForNodeId", 1)
}

func TestGetWithOwnerChangedUnit(t *testing.T) {
	lineId := 12345679
	latestBuffer := "testing_test_test_test"

	p := &Put{
		Error:    CacheError_NoError,
		SenderId: int32(5678),
		LineId:   int64(lineId),
		Version:  int32(2),
		Buffer:   []byte(latestBuffer),
	}
	oc := &OwnerChanged{
		SenderId:   int32(1234),
		LineId:     int64(lineId),
		NewOwnerId: int32(5678),
	}

	clientMock1234 := new(mockCacheClient)
	clientMock1234.On("SendGet", mock.AnythingOfTypeArgument("*context.emptyCtx"), mock.AnythingOfTypeArgument("*cache.Get")).Return(nil, oc, nil)
	clientMock5678 := new(mockCacheClient)
	clientMock5678.On("SendGet", mock.AnythingOfTypeArgument("*context.emptyCtx"), mock.AnythingOfTypeArgument("*cache.Get")).Return(p, nil, nil)

	m := new(mockCacheClientMapping)
	m.On("getClientForNodeId", 1234).Return(clientMock1234, nil)
	m.On("getClientForNodeId", 5678).Return(clientMock5678, nil)
	cache := mockCache(m)

	line := newCacheLine(lineId, 111, []byte("lalalalalala"))
	line, loaded := cache.store.putIfAbsent(lineId, line)
	assert.False(t, loaded)
	line.cacheLineState = CacheLineState_Shared
	line.ownerId = 1234

	// now run test
	readBites, err := cache.Get(lineId)
	assert.Nil(t, err)
	assert.Equal(t, latestBuffer, string(readBites))
	l, ok := cache.store.getCacheLineById(lineId)
	assert.True(t, ok)
	assert.Equal(t, 2, l.version)
	assert.Equal(t, CacheLineState_Shared, l.cacheLineState)
	m.AssertNumberOfCalls(t, "getClientForNodeId", 2)
}
