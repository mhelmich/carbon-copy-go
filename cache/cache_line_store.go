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
	"github.com/mhelmich/carbon-copy-go/pb"
	"sync"
)

func createNewCacheLineStore() *cacheLineStore {
	return &cacheLineStore{
		cacheLineMap: &sync.Map{},
	}
}

type cacheLineStore struct {
	// this map stores the cache lines on this node
	// the map has type map[string]cacheLine
	// the key into the map is the string id of the cache line
	cacheLineMap *sync.Map
}

func (cls *cacheLineStore) getCacheLineById(lineId CacheLineId) (*cacheLine, bool) {
	cl, ok := cls.cacheLineMap.Load(lineId.toIdString())

	if ok {
		return cl.(*cacheLine), true
	} else {
		return nil, false
	}
}

func (cls *cacheLineStore) putIfAbsent(lineId CacheLineId, line *cacheLine) (*cacheLine, bool) {
	val, loaded := cls.cacheLineMap.LoadOrStore(lineId.toIdString(), line)
	return val.(*cacheLine), loaded
}

func (cls *cacheLineStore) addCacheLineToLocalCache(line *cacheLine) {
	cls.cacheLineMap.Store(line.id.toIdString(), line)
}

// gut decision to put this function here
// I only knew it couldn't stay in cache
func (c *cacheLineStore) applyChangesFromPut(line *cacheLine, put *pb.Put) {
	line.ownerId = int(put.SenderId)
	line.cacheLineState = pb.CacheLineState_Shared
	line.version = int(put.Version)
	line.buffer = put.Buffer
}

// gut decision to put this function here
// I only knew it couldn't stay in cache
func (c *cacheLineStore) applyChangesFromPutx(line *cacheLine, p *pb.Putx, myNodeId int) {
	line.ownerId = myNodeId
	line.cacheLineState = pb.CacheLineState_Owned
	line.version = int(p.Version)
	line.sharers = concertInt32ArrayToIntArray(p.Sharers)
	line.buffer = p.Buffer
}

// gut decision to put this function here
// I only knew it couldn't stay in cache
func (cls *cacheLineStore) createCacheLineFromPut(put *pb.Put) *cacheLine {
	line := &cacheLine{
		id:             cacheLineIdFromProtoBuf(put.LineId),
		cacheLineState: pb.CacheLineState_Shared,
		version:        int(put.Version),
		ownerId:        int(put.SenderId),
		buffer:         put.Buffer,
		mutex:          &sync.Mutex{},
	}
	return line
}

// gut decision to put this function here
// I only knew it couldn't stay in cache
func (cls *cacheLineStore) createCacheLineFromPutx(p *pb.Putx, myNodeId int) *cacheLine {
	line := &cacheLine{
		id:             cacheLineIdFromProtoBuf(p.LineId),
		cacheLineState: pb.CacheLineState_Exclusive,
		version:        int(p.Version),
		ownerId:        myNodeId,
		buffer:         p.Buffer,
		mutex:          &sync.Mutex{},
	}
	return line
}

func concertInt32ArrayToIntArray(in []int32) []int {
	out := make([]int, len(in))
	for idx, val := range in {
		out[idx] = int(val)
	}
	return out
}
