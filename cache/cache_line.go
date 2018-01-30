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
	"fmt"
	"sync"
)

func newCacheLine(id int, myNodeId int, buffer []byte) *CacheLine {
	return &CacheLine{
		id:             id,
		cacheLineState: CacheLineState_Exclusive,
		version:        1,
		ownerId:        myNodeId,
		buffer:         buffer,
		mutex:          &sync.Mutex{},
	}
}

type CacheLine struct {
	id             int
	cacheLineState CacheLineState
	version        int
	ownerId        int
	sharers        []int
	buffer         []byte
	// used as a boolean
	// 0 => false
	// 1 => true
	isLocked int
	mutex    *sync.Mutex
}

func (cl *CacheLine) lock() {
	cl.mutex.Lock()
}

func (cl *CacheLine) unlock() {
	cl.mutex.Unlock()
}

func (cl *CacheLine) String() string {
	return fmt.Sprintf("<id: %d state: %s version: %d owner: %d buffer length: %d>", cl.id, cl.cacheLineState.String(), cl.version, cl.ownerId, len(cl.buffer))
}
