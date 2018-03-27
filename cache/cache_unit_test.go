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
	"crypto/rand"
	"errors"
	"testing"

	"github.com/mhelmich/carbon-copy-go/pb"
	"github.com/oklog/ulid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

////////////////////////////////////////////////////////////////////////
/////
/////  MOCK DEFINITIONS
/////
////////////////////////////////////////////////////////////////////////

type mockCacheClient struct {
	mock.Mock
}

func (cc *mockCacheClient) SendGet(ctx context.Context, g *pb.Get) (*pb.Put, *pb.OwnerChanged, error) {
	args := cc.Called(ctx, g)
	// take care of nil pointers
	p := args.Get(0)
	var pr *pb.Put
	if p == nil {
		pr = nil
	} else {
		pr = p.(*pb.Put)
	}

	oc := args.Get(1)
	var ocr *pb.OwnerChanged
	if oc == nil {
		ocr = nil
	} else {
		ocr = oc.(*pb.OwnerChanged)
	}

	return pr, ocr, args.Error(2)
}

func (cc *mockCacheClient) SendGets(ctx context.Context, g *pb.Gets) (*pb.Puts, *pb.OwnerChanged, error) {
	args := cc.Called(ctx, g)
	// take care of nil pointers
	p := args.Get(0)
	var pr *pb.Puts
	if p == nil {
		pr = nil
	} else {
		pr = p.(*pb.Puts)
	}

	oc := args.Get(1)
	var ocr *pb.OwnerChanged
	if oc == nil {
		ocr = nil
	} else {
		ocr = oc.(*pb.OwnerChanged)
	}

	return pr, ocr, args.Error(2)
}

func (cc *mockCacheClient) SendGetx(ctx context.Context, g *pb.Getx) (*pb.Putx, *pb.OwnerChanged, error) {
	args := cc.Called(ctx, g)
	// take care of nil pointers
	p := args.Get(0)
	var pr *pb.Putx
	if p == nil {
		pr = nil
	} else {
		pr = p.(*pb.Putx)
	}

	oc := args.Get(1)
	var ocr *pb.OwnerChanged
	if oc == nil {
		ocr = nil
	} else {
		ocr = oc.(*pb.OwnerChanged)
	}

	return pr, ocr, args.Error(2)
}

func (cc *mockCacheClient) SendInvalidate(ctx context.Context, i *pb.Inv) (*pb.InvAck, error) {
	args := cc.Called(ctx, i)
	// take care of nil pointers
	ia := args.Get(0)
	var invAck *pb.InvAck
	if ia == nil {
		invAck = nil
	} else {
		invAck = ia.(*pb.InvAck)
	}

	return invAck, args.Error(1)
}

func (cc *mockCacheClient) Close() error {
	return nil
}

type mockCacheClientMapping struct {
	mock.Mock
}

func (ccm *mockCacheClientMapping) getClientForNodeId(nodeId int) (cacheClient, error) {
	args := ccm.Called(nodeId)
	return args.Get(0).(cacheClient), args.Error(1)
}

func (ccm *mockCacheClientMapping) addClientWithNodeId(nodeId int, addr string) {
}

func (ccm *mockCacheClientMapping) forEachParallel(f func(c cacheClient)) {
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
	lineId := newRandomCacheLineId()
	latestBuffer := "testing_test_test_test"
	clientMock := new(mockCacheClient)
	var p *pb.Put
	var oc *pb.OwnerChanged
	p = &pb.Put{
		Error:    pb.CacheError_NoError,
		SenderId: int32(1234),
		LineId:   lineId.toProtoBuf(),
		Version:  int32(2),
		Buffer:   []byte(latestBuffer),
	}
	oc = nil
	clientMock.On("SendGet", mock.AnythingOfTypeArgument("*context.emptyCtx"), mock.AnythingOfTypeArgument("*pb.Get")).Return(p, oc, errors.New("this still works because error conditions are checked last!?!?!"))
	m := new(mockCacheClientMapping)
	m.On("getClientForNodeId", 1234).Return(clientMock, nil)
	cache := mockCache(m)

	line := newCacheLine(lineId, 111, []byte("lalalalalala"))
	line, loaded := cache.store.putIfAbsent(lineId, line)
	assert.False(t, loaded)
	line.cacheLineState = pb.CacheLineState_Invalid
	line.ownerId = 1234

	// now run test
	readBites, err := cache.Get(lineId)
	assert.Nil(t, err)
	assert.Equal(t, latestBuffer, string(readBites))
	l, ok := cache.store.getCacheLineById(lineId)
	assert.True(t, ok)
	assert.Equal(t, 2, l.version)
	assert.Equal(t, pb.CacheLineState_Shared, l.cacheLineState)
	m.AssertNumberOfCalls(t, "getClientForNodeId", 1)
}

func TestGetOwnedUnit(t *testing.T) {
	lineId := newRandomCacheLineId()
	latestBuffer := "testing_test_test_test"
	clientMock := new(mockCacheClient)
	var p *pb.Put
	var oc *pb.OwnerChanged
	p = &pb.Put{
		Error:    pb.CacheError_NoError,
		SenderId: int32(1234),
		LineId:   lineId.toProtoBuf(),
		Version:  int32(2),
		Buffer:   []byte(latestBuffer),
	}
	oc = nil
	clientMock.On("SendGet", mock.AnythingOfTypeArgument("*context.emptyCtx"), mock.AnythingOfTypeArgument("*pb.Get")).Return(p, oc, errors.New("this still works because error conditions are checked last!?!?!"))
	m := new(mockCacheClientMapping)
	m.On("getClientForNodeId", 1234).Return(clientMock, nil)
	cache := mockCache(m)

	initialBuffer := "lalalalalala"
	line := newCacheLine(lineId, 111, []byte(initialBuffer))
	line, loaded := cache.store.putIfAbsent(lineId, line)
	assert.False(t, loaded)
	line.cacheLineState = pb.CacheLineState_Owned
	line.ownerId = 1234

	// now run test
	readBites, err := cache.Get(lineId)
	assert.Nil(t, err)
	assert.Equal(t, initialBuffer, string(readBites))
	l, ok := cache.store.getCacheLineById(lineId)
	assert.True(t, ok)
	assert.Equal(t, 1, l.version)
	assert.Equal(t, pb.CacheLineState_Owned, l.cacheLineState)
	m.AssertNumberOfCalls(t, "getClientForNodeId", 0)
}

func TestGetWithOwnerChangedUnit(t *testing.T) {
	lineId := newRandomCacheLineId()
	latestBuffer := "testing_test_test_test"

	p := &pb.Put{
		Error:    pb.CacheError_NoError,
		SenderId: int32(5678),
		LineId:   lineId.toProtoBuf(),
		Version:  int32(2),
		Buffer:   []byte(latestBuffer),
	}
	oc := &pb.OwnerChanged{
		SenderId:   int32(1234),
		LineId:     lineId.toProtoBuf(),
		NewOwnerId: int32(5678),
	}

	clientMock1234 := new(mockCacheClient)
	clientMock1234.On("SendGet", mock.AnythingOfTypeArgument("*context.emptyCtx"), mock.AnythingOfTypeArgument("*pb.Get")).Return(nil, oc, nil)
	clientMock5678 := new(mockCacheClient)
	clientMock5678.On("SendGet", mock.AnythingOfTypeArgument("*context.emptyCtx"), mock.AnythingOfTypeArgument("*pb.Get")).Return(p, nil, nil)

	m := new(mockCacheClientMapping)
	m.On("getClientForNodeId", 1234).Return(clientMock1234, nil)
	m.On("getClientForNodeId", 5678).Return(clientMock5678, nil)
	cache := mockCache(m)

	line := newCacheLine(lineId, 111, []byte("lalalalalala"))
	line, loaded := cache.store.putIfAbsent(lineId, line)
	assert.False(t, loaded)
	line.cacheLineState = pb.CacheLineState_Shared
	line.ownerId = 1234

	// now run test
	readBites, err := cache.Get(lineId)
	assert.Nil(t, err)
	assert.Equal(t, latestBuffer, string(readBites))
	l, ok := cache.store.getCacheLineById(lineId)
	assert.True(t, ok)
	assert.Equal(t, 2, l.version)
	assert.Equal(t, pb.CacheLineState_Shared, l.cacheLineState)
	m.AssertNumberOfCalls(t, "getClientForNodeId", 2)
}

func TestGetxUnit(t *testing.T) {
	lineId := newRandomCacheLineId()
	latestBuffer := "testing_test_test_test"
	clientMock := new(mockCacheClient)
	var p *pb.Putx
	var oc *pb.OwnerChanged
	p = &pb.Putx{
		Error:    pb.CacheError_NoError,
		SenderId: int32(1234),
		LineId:   lineId.toProtoBuf(),
		Version:  int32(2),
		Buffer:   []byte(latestBuffer),
	}
	oc = nil
	clientMock.On("SendGetx", mock.AnythingOfTypeArgument("*context.emptyCtx"), mock.AnythingOfTypeArgument("*pb.Getx")).Return(p, oc, errors.New("this still works because error conditions are checked last!?!?!"))
	m := new(mockCacheClientMapping)
	m.On("getClientForNodeId", 1234).Return(clientMock, nil)
	cache := mockCache(m)

	line := newCacheLine(lineId, 111, []byte("lalalalalala"))
	line, loaded := cache.store.putIfAbsent(lineId, line)
	assert.False(t, loaded)
	line.cacheLineState = pb.CacheLineState_Invalid
	line.ownerId = 1234

	// now run test
	txn := cache.NewTransaction()
	readBites, err := cache.Getx(lineId, txn)
	assert.Nil(t, err)
	err = txn.Commit()
	assert.Nil(t, err)
	assert.Equal(t, latestBuffer, string(readBites))
	l, ok := cache.store.getCacheLineById(lineId)
	assert.True(t, ok)
	assert.Equal(t, 2, l.version)
	assert.Equal(t, pb.CacheLineState_Exclusive, l.cacheLineState)
	m.AssertNumberOfCalls(t, "getClientForNodeId", 1)
}

func TestGetxExclusiveUnit(t *testing.T) {
	lineId := newRandomCacheLineId()
	latestBuffer := "testing_test_test_test"
	clientMock := new(mockCacheClient)
	var p *pb.Putx
	var oc *pb.OwnerChanged
	p = &pb.Putx{
		Error:    pb.CacheError_NoError,
		SenderId: int32(1234),
		LineId:   lineId.toProtoBuf(),
		Version:  int32(2),
		Buffer:   []byte(latestBuffer),
	}
	oc = nil
	clientMock.On("SendGetx", mock.AnythingOfTypeArgument("*context.emptyCtx"), mock.AnythingOfTypeArgument("*pb.Getx")).Return(p, oc, errors.New("this still works because error conditions are checked last!?!?!"))
	m := new(mockCacheClientMapping)
	m.On("getClientForNodeId", 1234).Return(clientMock, nil)
	cache := mockCache(m)

	initialBuffer := "lalalalalala"
	line := newCacheLine(lineId, 111, []byte(initialBuffer))
	line, loaded := cache.store.putIfAbsent(lineId, line)
	assert.False(t, loaded)
	line.cacheLineState = pb.CacheLineState_Exclusive
	line.ownerId = 1234

	// now run test
	txn := cache.NewTransaction()
	readBites, err := cache.Getx(lineId, txn)
	assert.Nil(t, err)
	err = txn.Commit()
	assert.Nil(t, err)
	assert.Equal(t, initialBuffer, string(readBites))
	l, ok := cache.store.getCacheLineById(lineId)
	assert.True(t, ok)
	assert.Equal(t, 1, l.version)
	assert.Equal(t, pb.CacheLineState_Exclusive, l.cacheLineState)
	m.AssertNumberOfCalls(t, "getClientForNodeId", 0)
}

func TestGetxOwnedUnit(t *testing.T) {
	lineId := newRandomCacheLineId()
	clientMock := new(mockCacheClient)

	invAck := &pb.InvAck{
		Error:    pb.CacheError_NoError,
		SenderId: int32(1234),
		LineId:   lineId.toProtoBuf(),
	}

	clientMock.On("SendInvalidate", mock.AnythingOfTypeArgument("*context.emptyCtx"), mock.AnythingOfTypeArgument("*pb.Inv")).Return(invAck, nil)
	m := new(mockCacheClientMapping)
	m.On("getClientForNodeId", 1234).Return(clientMock, nil)
	cache := mockCache(m)

	initialBuffer := "lalalalalala"
	line := newCacheLine(lineId, 111, []byte(initialBuffer))
	line, loaded := cache.store.putIfAbsent(lineId, line)
	assert.False(t, loaded)
	line.cacheLineState = pb.CacheLineState_Owned
	line.sharers = []int{1234}
	line.ownerId = 1111

	// now run test
	txn := cache.NewTransaction()
	readBites, err := cache.Getx(lineId, txn)
	assert.Nil(t, err)
	err = txn.Commit()
	assert.Nil(t, err)
	assert.Equal(t, initialBuffer, string(readBites))
	l, ok := cache.store.getCacheLineById(lineId)
	assert.True(t, ok)
	assert.Equal(t, 1, l.version)
	assert.Equal(t, pb.CacheLineState_Exclusive, l.cacheLineState)
	m.AssertNumberOfCalls(t, "getClientForNodeId", 1)
}

func TestGetxSharedUnit(t *testing.T) {
	lineId := newRandomCacheLineId()
	latestBuffer := "testing_test_test_test"

	putx := &pb.Putx{
		Error:    pb.CacheError_NoError,
		SenderId: int32(5678),
		LineId:   lineId.toProtoBuf(),
		Version:  int32(2),
		Sharers:  []int32{int32(1234)},
		Buffer:   []byte(latestBuffer),
	}

	invAck := &pb.InvAck{
		Error:    pb.CacheError_NoError,
		SenderId: int32(1234),
		LineId:   lineId.toProtoBuf(),
	}

	clientMock1234 := new(mockCacheClient)
	clientMock1234.On("SendInvalidate", mock.AnythingOfTypeArgument("*context.emptyCtx"), mock.AnythingOfTypeArgument("*pb.Inv")).Return(invAck, nil)

	clientMock5678 := new(mockCacheClient)
	clientMock5678.On("SendGetx", mock.AnythingOfTypeArgument("*context.emptyCtx"), mock.AnythingOfTypeArgument("*pb.Getx")).Return(putx, nil, nil)

	m := new(mockCacheClientMapping)
	m.On("getClientForNodeId", 1234).Return(clientMock1234, nil)
	m.On("getClientForNodeId", 5678).Return(clientMock5678, nil)
	cache := mockCache(m)

	initialBuffer := "lalalalalala"
	line := newCacheLine(lineId, 111, []byte(initialBuffer))
	line, loaded := cache.store.putIfAbsent(lineId, line)
	assert.False(t, loaded)
	line.cacheLineState = pb.CacheLineState_Shared
	line.sharers = []int{1234}
	line.ownerId = 5678

	// now run test
	txn := cache.NewTransaction()
	readBites, err := cache.Getx(lineId, txn)
	assert.Nil(t, err)
	err = txn.Commit()
	assert.Nil(t, err)
	assert.Equal(t, latestBuffer, string(readBites))
	l, ok := cache.store.getCacheLineById(lineId)
	assert.True(t, ok)
	assert.Equal(t, 2, l.version)
	assert.Equal(t, pb.CacheLineState_Exclusive, l.cacheLineState)
	m.AssertNumberOfCalls(t, "getClientForNodeId", 2)
}

func TestPutUnit(t *testing.T) {
	lineId := newRandomCacheLineId()
	latestBuffer := "testing_test_test_test"

	putx := &pb.Putx{
		Error:    pb.CacheError_NoError,
		SenderId: int32(5678),
		LineId:   lineId.toProtoBuf(),
		Version:  int32(2),
		Sharers:  []int32{int32(1234)},
		Buffer:   []byte(latestBuffer),
	}

	invAck := &pb.InvAck{
		Error:    pb.CacheError_NoError,
		SenderId: int32(1234),
		LineId:   lineId.toProtoBuf(),
	}

	clientMock1234 := new(mockCacheClient)
	clientMock1234.On("SendInvalidate", mock.AnythingOfTypeArgument("*context.emptyCtx"), mock.AnythingOfTypeArgument("*pb.Inv")).Return(invAck, nil)

	clientMock5678 := new(mockCacheClient)
	clientMock5678.On("SendGetx", mock.AnythingOfTypeArgument("*context.emptyCtx"), mock.AnythingOfTypeArgument("*pb.Getx")).Return(putx, nil, nil)

	m := new(mockCacheClientMapping)
	m.On("getClientForNodeId", 1234).Return(clientMock1234, nil)
	m.On("getClientForNodeId", 5678).Return(clientMock5678, nil)
	cache := mockCache(m)

	initialBuffer := "lalalalalala"
	line := newCacheLine(lineId, 111, []byte(initialBuffer))
	line, loaded := cache.store.putIfAbsent(lineId, line)
	assert.False(t, loaded)
	line.cacheLineState = pb.CacheLineState_Invalid
	line.ownerId = 5678

	// now run test
	txn := cache.NewTransaction()
	err := cache.Put(lineId, []byte(latestBuffer), txn)
	assert.Nil(t, err)
	err = txn.Commit()
	assert.Nil(t, err)
	l, ok := cache.store.getCacheLineById(lineId)
	assert.True(t, ok)
	assert.Equal(t, 3, l.version)
	assert.Equal(t, pb.CacheLineState_Exclusive, l.cacheLineState)
	m.AssertNumberOfCalls(t, "getClientForNodeId", 2)
}

func TestCacheUlidMatshalling(t *testing.T) {
	id := ulid.MustNew(ulid.Now(), rand.Reader)
	log.Infof("New id %s", id.String())
}
