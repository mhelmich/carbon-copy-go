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
	"container/list"
	"fmt"
	"github.com/mhelmich/carbon-copy-go/pb"
)

func createNewTransaction() *transactionImpl {
	return &transactionImpl{
		undo: list.New(),
	}
}

type transactionImpl struct {
	undo *list.List
}

type undo struct {
	line    *CacheLine
	version int
	buf     []byte
}

func (t *transactionImpl) Commit() error {
	for e := t.undo.Front(); e != nil; e = e.Next() {
		undo := e.Value.(*undo)
		err := t.canCommit(undo.line)
		if err != nil {
			t.releaseAllLocks()
			return err
		}
	}

	for e := t.undo.Front(); e != nil; e = e.Next() {
		undo := e.Value.(*undo)
		undo.line.version = undo.version
		undo.line.buffer = undo.buf
	}

	t.releaseAllLocks()
	return nil
}

func (t *transactionImpl) canCommit(line *CacheLine) error {
	if !line.isLocked() {
		return CarbonGridError(fmt.Sprintf("Line %d is not locked", line.id))
	}

	if line.cacheLineState != pb.CacheLineState_Exclusive {
		return CarbonGridError(fmt.Sprintf("Line %d is in state %v instead exclusive", line.id, line.cacheLineState))
	}

	return nil
}

func (t *transactionImpl) Rollback() error {
	t.releaseAllLocks()
	return nil
}

func (t *transactionImpl) addToTxn(cl *CacheLine, newBuffer []byte) {
	u := &undo{
		line:    cl,
		version: cl.version + 1,
		buf:     newBuffer,
	}
	cl.lock()
	t.undo.PushBack(u)
}

func (t *transactionImpl) releaseAllLocks() {
	for e := t.undo.Front(); e != nil; e = e.Next() {
		undo := e.Value.(*undo)
		undo.line.unlock()
	}
}
