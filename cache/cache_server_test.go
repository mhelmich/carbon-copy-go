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
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

func TestServerGetLineNotPresent(t *testing.T) {
	lineId := 654321
	serverNodeId := 111
	clStore := createNewCacheLineStore()
	cacheServer := &cacheServerImpl{
		myNodeId:   int32(serverNodeId),
		grpcServer: nil,
		store:      clStore,
	}

	req := &Get{
		SenderId: int32(555),
		LineId:   int64(lineId),
	}
	resp, err := cacheServer.Get(context.Background(), req)
	assert.Nil(t, err)
	ack := resp.GetAck()
	assert.NotNil(t, ack)
}

func TestServerGetLineNotOwned(t *testing.T) {
	lineId := 654321
	serverNodeId := 111
	clStore := createNewCacheLineStore()
	cacheServer := &cacheServerImpl{
		myNodeId:   int32(serverNodeId),
		grpcServer: nil,
		store:      clStore,
	}

	line := newCacheLine(lineId, serverNodeId, []byte("lalalalalala"))
	line, loaded := clStore.putIfAbsent(lineId, line)
	assert.False(t, loaded)
	line.cacheLineState = CacheLineState_Shared
	line.ownerId = 258

	req := &Get{
		SenderId: int32(555),
		LineId:   int64(lineId),
	}
	resp, err := cacheServer.Get(context.Background(), req)
	assert.Nil(t, err)
	oc := resp.GetOwnerChanged()
	assert.NotNil(t, oc)
	assert.Equal(t, lineId, int(oc.LineId))
	assert.Equal(t, 258, int(oc.NewOwnerId))
	assert.Equal(t, 111, int(oc.SenderId))
}

func TestServerGetLineOwned(t *testing.T) {
	lineId := 654321
	serverNodeId := 111
	clStore := createNewCacheLineStore()
	cacheServer := &cacheServerImpl{
		myNodeId:   int32(serverNodeId),
		grpcServer: nil,
		store:      clStore,
	}

	lineBuffer := []byte("lalalalalala")
	line := newCacheLine(lineId, serverNodeId, lineBuffer)
	line, loaded := clStore.putIfAbsent(lineId, line)
	assert.False(t, loaded)
	line.cacheLineState = CacheLineState_Owned
	line.ownerId = 258

	req := &Get{
		SenderId: int32(555),
		LineId:   int64(lineId),
	}
	resp, err := cacheServer.Get(context.Background(), req)
	assert.Nil(t, err)
	put := resp.GetPut()
	assert.NotNil(t, put)
	assert.Equal(t, lineId, int(put.LineId))
	assert.Equal(t, lineBuffer, put.Buffer)
	assert.Equal(t, 111, int(put.SenderId))
}

func TestServerGetsLineExclusive(t *testing.T) {
	lineId := 654321
	serverNodeId := 111
	clStore := createNewCacheLineStore()
	cacheServer := &cacheServerImpl{
		myNodeId:   int32(serverNodeId),
		grpcServer: nil,
		store:      clStore,
	}

	lineBuffer := []byte("lalalalalala")
	line := newCacheLine(lineId, serverNodeId, lineBuffer)
	line, loaded := clStore.putIfAbsent(lineId, line)
	assert.False(t, loaded)
	line.cacheLineState = CacheLineState_Exclusive
	line.ownerId = serverNodeId

	req := &Gets{
		SenderId: int32(555),
		LineId:   int64(lineId),
	}
	resp, err := cacheServer.Gets(context.Background(), req)
	assert.Nil(t, err)
	put := resp.GetPuts()
	assert.NotNil(t, put)

	sort.Ints(line.sharers)
	idxToInsert := sort.SearchInts(line.sharers, 555)
	assert.True(t, line.sharers[idxToInsert] == 555)
	assert.Equal(t, CacheLineState_Owned, line.cacheLineState)
}

func TestServerGetsLineOwned(t *testing.T) {
	lineId := 654321
	serverNodeId := 111
	clStore := createNewCacheLineStore()
	cacheServer := &cacheServerImpl{
		myNodeId:   int32(serverNodeId),
		grpcServer: nil,
		store:      clStore,
	}

	lineBuffer := []byte("lalalalalala")
	line := newCacheLine(lineId, serverNodeId, lineBuffer)
	line, loaded := clStore.putIfAbsent(lineId, line)
	assert.False(t, loaded)
	line.cacheLineState = CacheLineState_Owned
	line.ownerId = serverNodeId

	req := &Gets{
		SenderId: int32(555),
		LineId:   int64(lineId),
	}
	resp, err := cacheServer.Gets(context.Background(), req)
	assert.Nil(t, err)
	put := resp.GetPuts()
	assert.NotNil(t, put)

	sort.Ints(line.sharers)
	idxToInsert := sort.SearchInts(line.sharers, 555)
	assert.True(t, line.sharers[idxToInsert] == 555)
	assert.Equal(t, CacheLineState_Owned, line.cacheLineState)
}

func TestServerGetsLineShared(t *testing.T) {
	lineId := 654321
	serverNodeId := 111
	clStore := createNewCacheLineStore()
	cacheServer := &cacheServerImpl{
		myNodeId:   int32(serverNodeId),
		grpcServer: nil,
		store:      clStore,
	}

	lineBuffer := []byte("lalalalalala")
	line := newCacheLine(lineId, serverNodeId, lineBuffer)
	line, loaded := clStore.putIfAbsent(lineId, line)
	assert.False(t, loaded)
	line.cacheLineState = CacheLineState_Shared
	line.ownerId = 258

	req := &Gets{
		SenderId: int32(555),
		LineId:   int64(lineId),
	}
	resp, err := cacheServer.Gets(context.Background(), req)
	assert.Nil(t, err)
	oc := resp.GetOwnerChanged()
	assert.NotNil(t, oc)
	assert.Equal(t, int32(258), oc.NewOwnerId)
}

func TestServerGetsLineDoesntExist(t *testing.T) {
	lineId := 654321
	serverNodeId := 111
	clStore := createNewCacheLineStore()
	cacheServer := &cacheServerImpl{
		myNodeId:   int32(serverNodeId),
		grpcServer: nil,
		store:      clStore,
	}

	req := &Gets{
		SenderId: int32(555),
		LineId:   int64(lineId),
	}
	resp, err := cacheServer.Gets(context.Background(), req)
	assert.Nil(t, err)
	ack := resp.GetAck()
	assert.NotNil(t, ack)
}

func TestServerGetxLineOwned(t *testing.T) {
	lineId := 654321
	serverNodeId := 111
	clStore := createNewCacheLineStore()
	cacheServer := &cacheServerImpl{
		myNodeId:   int32(serverNodeId),
		grpcServer: nil,
		store:      clStore,
	}

	lineBuffer := []byte("lalalalalala")
	line := newCacheLine(lineId, serverNodeId, lineBuffer)
	line, loaded := clStore.putIfAbsent(lineId, line)
	assert.False(t, loaded)
	line.cacheLineState = CacheLineState_Owned
	line.ownerId = serverNodeId

	req := &Getx{
		SenderId: int32(555),
		LineId:   int64(lineId),
	}
	resp, err := cacheServer.Getx(context.Background(), req)
	assert.Nil(t, err)
	putx := resp.GetPutx()
	assert.NotNil(t, putx)
	assert.Equal(t, lineBuffer, putx.Buffer)
	assert.Equal(t, CacheLineState_Invalid, line.cacheLineState)
}

func TestServerGetxLineInvalid(t *testing.T) {
	lineId := 654321
	serverNodeId := 111
	clStore := createNewCacheLineStore()
	cacheServer := &cacheServerImpl{
		myNodeId:   int32(serverNodeId),
		grpcServer: nil,
		store:      clStore,
	}

	lineBuffer := []byte("lalalalalala")
	line := newCacheLine(lineId, serverNodeId, lineBuffer)
	line, loaded := clStore.putIfAbsent(lineId, line)
	assert.False(t, loaded)
	line.cacheLineState = CacheLineState_Invalid
	line.ownerId = 258

	req := &Getx{
		SenderId: int32(555),
		LineId:   int64(lineId),
	}
	resp, err := cacheServer.Getx(context.Background(), req)
	assert.Nil(t, err)
	oc := resp.GetOwnerChanged()
	assert.NotNil(t, oc)
	assert.Equal(t, CacheLineState_Invalid, line.cacheLineState)
	assert.Equal(t, int32(258), oc.NewOwnerId)
}

func TestServerGetxLineDoesntExist(t *testing.T) {
	lineId := 654321
	serverNodeId := 111
	clStore := createNewCacheLineStore()
	cacheServer := &cacheServerImpl{
		myNodeId:   int32(serverNodeId),
		grpcServer: nil,
		store:      clStore,
	}

	req := &Getx{
		SenderId: int32(555),
		LineId:   int64(lineId),
	}
	resp, err := cacheServer.Getx(context.Background(), req)
	assert.Nil(t, err)
	ack := resp.GetAck()
	assert.NotNil(t, ack)
}
