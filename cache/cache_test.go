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
	"github.com/mhelmich/carbon-copy-go/pb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAllocateNewAndGet(t *testing.T) {
	c, err := NewCache(111, 6666)
	assert.Nil(t, err, "Can't create cache")
	assert.NotNil(t, c, "There is no client")

	value := "lalalalalala"
	CacheLineId, err := c.AllocateWithData([]byte(value), nil)
	if !assert.Nil(t, err, "Can't create []byte") {
		c.Stop()
		return
	}

	readBites, err := c.Get(CacheLineId)
	if !assert.Nil(t, err, "Can't get locally") {
		c.Stop()
		return
	}
	assert.Equal(t, value, string(readBites), "Bytes aren't the same")

	c.Stop()
}

func TestGetPutInTwoCaches(t *testing.T) {
	cache1, err := createNewCache(111, 6666)
	assert.Nil(t, err, "Can't create cache")
	assert.NotNil(t, cache1, "There is no client")

	cache2, err := createNewCache(999, 7777)
	if !assert.Nil(t, err, "Can't create cache") {
		cache1.Stop()
		return
	}
	assert.NotNil(t, cache2, "There is no client")

	cache1.addPeerNode(999, "localhost:7777")
	cache2.addPeerNode(111, "localhost:6666")

	value := "lalalalalala"
	CacheLineId, err := cache1.AllocateWithData([]byte(value), nil)
	assert.Nil(t, err, "Can't create []byte")
	readBites, err := cache1.Get(CacheLineId)
	if !assert.Nil(t, err, "Can't get locally") {
		cache1.Stop()
		cache2.Stop()
		return
	}
	assert.Equal(t, value, string(readBites), "Bytes aren't the same")

	cache2Bites, err := cache2.Get(CacheLineId)
	if !assert.Nil(t, err, "Can't get remotely") {
		cache1.Stop()
		cache2.Stop()
		return
	}
	assert.Equal(t, value, string(cache2Bites), "Bytes aren't the same")

	cache1.Stop()
	cache2.Stop()
}

func TestGetPutInThreeCaches(t *testing.T) {
	cache1, cache2, cache3, err := threeCaches(t)

	value := "lalalalalala"
	CacheLineId, err := cache1.AllocateWithData([]byte(value), nil)
	if !assert.Nil(t, err, "Can't create []byte") {
		stopThreeCaches(cache1, cache2, cache3)
		return
	}
	// assert that cache1 owns the line exclusively
	v, ok := cache1.store.getCacheLineById(CacheLineId)
	if !assert.True(t, ok) {
		stopThreeCaches(cache1, cache2, cache3)
		return
	}
	assert.Equal(t, pb.CacheLineState_Exclusive, v.cacheLineState)

	readBites, err := cache2.Getx(CacheLineId, nil)
	if !assert.Nil(t, err, "Can't getx remotely") {
		stopThreeCaches(cache1, cache2, cache3)
		return
	}
	assert.Equal(t, value, string(readBites), "Bytes aren't the same")
	// assert on cache line state in cache1
	// should be invalid
	v, ok = cache1.store.getCacheLineById(CacheLineId)
	if !assert.True(t, ok) {
		stopThreeCaches(cache1, cache2, cache3)
		return
	}
	assert.Equal(t, pb.CacheLineState_Invalid, v.cacheLineState)
	// assert on cache line state in cache2
	// should own the line exclusively
	v, ok = cache2.store.getCacheLineById(CacheLineId)
	if !assert.True(t, ok) {
		stopThreeCaches(cache1, cache2, cache3)
		return
	}
	assert.Equal(t, pb.CacheLineState_Exclusive, v.cacheLineState)
	stopThreeCaches(cache1, cache2, cache3)
}

func TestOwnerChanged(t *testing.T) {
	cache1, cache2, cache3, err := threeCaches(t)
	assert.Nil(t, err)

	value := "lalalalalala"
	// create line in cache1
	CacheLineId, err := cache1.AllocateWithData([]byte(value), nil)
	// let cache3 know that this line exists
	cache3.Get(CacheLineId)
	// move ownership to cache2
	readBites, err := cache2.Getx(CacheLineId, nil)
	assert.Nil(t, err)
	v, ok := cache1.store.getCacheLineById(CacheLineId)
	assert.True(t, ok)
	assert.Equal(t, pb.CacheLineState_Invalid, v.cacheLineState)
	v, ok = cache2.store.getCacheLineById(CacheLineId)
	assert.True(t, ok)
	assert.Equal(t, pb.CacheLineState_Exclusive, v.cacheLineState)

	// now I go ahead and let cache3 ask cache1 for that line
	// cache1 should answer with "owner changed to cache2"
	// and cache3 should send a second get transparently
	readBites, err = cache3.Get(CacheLineId)
	assert.Nil(t, err)
	assert.Equal(t, value, string(readBites), "Value is not the same")

	stopThreeCaches(cache1, cache2, cache3)
}

func TestStartStop(t *testing.T) {
	c, err := NewCache(111, 6666)
	assert.Nil(t, err)
	c.Stop()
}

func TestStartConnectStop(t *testing.T) {
	clientId := 111
	serverId := 123
	c, err := NewCache(serverId, 6666)
	assert.Nil(t, err)

	client, err := createNewCacheClientFromAddr("localhost:6666")
	assert.Nil(t, err)

	inv := &pb.Inv{
		SenderId: int32(clientId),
		LineId:   newRandomCacheLineId().toProtoBuf(),
	}
	invAck, err := client.SendInvalidate(context.Background(), inv)
	assert.Nil(t, err)
	assert.NotNil(t, invAck)
	assert.Equal(t, inv.LineId, invAck.LineId)
	assert.Equal(t, int32(serverId), invAck.SenderId)

	err = client.Close()
	assert.Nil(t, err)

	c.Stop()
}

func threeCaches(t *testing.T) (*cacheImpl, *cacheImpl, *cacheImpl, error) {
	cache1, err := createNewCache(111, 6666)
	assert.Nil(t, err, "Can't create cache")
	assert.NotNil(t, cache1, "There is no cache1")

	cache2, err := createNewCache(222, 7777)
	assert.Nil(t, err, "Can't create cache")
	assert.NotNil(t, cache2, "There is no cache2")

	cache3, err := createNewCache(333, 8888)
	assert.Nil(t, err, "Can't create cache")
	assert.NotNil(t, cache3, "There is no cache3")

	cache1.addPeerNode(222, "localhost:7777")
	cache1.addPeerNode(333, "localhost:8888")

	cache2.addPeerNode(111, "localhost:6666")
	cache2.addPeerNode(333, "localhost:8888")

	cache3.addPeerNode(111, "localhost:6666")
	cache3.addPeerNode(222, "localhost:7777")

	return cache1, cache2, cache3, nil
}

func stopThreeCaches(c1, c2, c3 *cacheImpl) {
	c1.Stop()
	c2.Stop()
	c3.Stop()
}
