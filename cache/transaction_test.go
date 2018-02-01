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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTxnCommit(t *testing.T) {
	txn := createNewTransaction()
	buf := "testing_test_test_test"
	cl := newCacheLine(123456789, 4444, []byte(buf))
	newBuf := "new_buffer_bwahahaha"
	txn.addToTxn(cl, []byte(newBuf))
	txn.Commit()
	assert.Equal(t, newBuf, string(cl.buffer))
	assert.Equal(t, 2, cl.version)
}

func TestTxnRollback(t *testing.T) {
	txn := createNewTransaction()
	buf := "testing_test_test_test"
	cl := newCacheLine(123456789, 4444, []byte(buf))
	newBuf := "new_buffer_bwahahaha"
	txn.addToTxn(cl, []byte(newBuf))
	txn.Rollback()
	assert.Equal(t, buf, string(cl.buffer))
	assert.Equal(t, 1, cl.version)
}
