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
