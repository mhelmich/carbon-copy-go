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

func (cls *cacheLineStore) getCacheLineById(lineId CacheLineId) (*CacheLine, bool) {
	cl, ok := cls.cacheLineMap.Load(lineId.string())
	if ok {
		return cl.(*CacheLine), true
	} else {
		return nil, false
	}
}

func (cls *cacheLineStore) putIfAbsent(lineId CacheLineId, line *CacheLine) (*CacheLine, bool) {
	val, loaded := cls.cacheLineMap.LoadOrStore(lineId.string(), line)
	return val.(*CacheLine), loaded
}

func (cls *cacheLineStore) addCacheLineToLocalCache(line *CacheLine) {
	cls.cacheLineMap.Store(line.id.string(), line)
}

// gut decision to put this function here
// I only knew it couldn't stay in cache
func (c *cacheLineStore) applyChangesFromPut(line *CacheLine, put *pb.Put) {
	line.lock()
	line.ownerId = int(put.SenderId)
	line.cacheLineState = pb.CacheLineState_Shared
	line.version = int(put.Version)
	line.buffer = put.Buffer
	line.unlock()
}

// gut decision to put this function here
// I only knew it couldn't stay in cache
func (c *cacheLineStore) applyChangesFromPutx(line *CacheLine, p *pb.Putx, myNodeId int) {
	line.lock()
	line.ownerId = myNodeId
	line.cacheLineState = pb.CacheLineState_Owned
	line.version = int(p.Version)
	line.sharers = concertInt32ArrayToIntArray(p.Sharers)
	line.buffer = p.Buffer
	line.unlock()
}

// gut decision to put this function here
// I only knew it couldn't stay in cache
func (cls *cacheLineStore) createCacheLineFromPut(put *pb.Put) *CacheLine {
	line := &CacheLine{
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
func (cls *cacheLineStore) createCacheLineFromPutx(p *pb.Putx, myNodeId int) *CacheLine {
	line := &CacheLine{
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
