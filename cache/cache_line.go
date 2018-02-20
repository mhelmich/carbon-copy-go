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
		locked:         false,
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
	locked         bool
	mutex          *sync.Mutex
}

func (cl *CacheLine) lock() {
	cl.mutex.Lock()
	cl.locked = true
}

func (cl *CacheLine) unlock() {
	cl.locked = false
	cl.mutex.Unlock()
}

func (cl *CacheLine) isLocked() bool {
	return cl.locked
}

func (cl *CacheLine) String() string {
	return fmt.Sprintf("<id: %d state: %s version: %d owner: %d locked: %t buffer length: %d>", cl.id, cl.cacheLineState.String(), cl.version, cl.ownerId, cl.locked, len(cl.buffer))
}
