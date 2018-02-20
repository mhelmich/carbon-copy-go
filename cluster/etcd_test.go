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

package cluster

import (
	"context"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func _TestEtcdCreateConsensus(t *testing.T) {
	etcd, err := createNewEtcdConsensus(context.Background())
	assert.Nil(t, err)
	assert.NotNil(t, etcd.etcdSession.Lease())
	assert.Nil(t, etcd.close())
}

func _TestEtcdPutGet(t *testing.T) {
	key := "key_key_key"
	val := "val_val_val"

	etcd, err := createNewEtcdConsensus(context.Background())
	assert.Nil(t, err)
	assert.Nil(t, etcd.put(context.Background(), key, val))
	v, err := etcd.get(context.Background(), key)
	assert.Nil(t, err)
	assert.Equal(t, val, v)
	assert.Nil(t, etcd.close())
}

func _TestEtcdPutGetExpireGet(t *testing.T) {
	key := "key_key_key"
	val := "val_val_val"

	etcd1, err := createNewEtcdConsensus(context.Background())
	assert.Nil(t, err)
	assert.Nil(t, etcd1.put(context.Background(), key, val))
	v, err := etcd1.get(context.Background(), key)
	assert.Nil(t, err)
	assert.Equal(t, val, v)
	assert.Nil(t, etcd1.close())

	etcd2, err := createNewEtcdConsensus(context.Background())
	assert.Nil(t, err)
	v, err = etcd1.get(context.Background(), key)
	assert.NotNil(t, err)
	assert.Equal(t, "", v)
	assert.Nil(t, etcd2.close())
}

func _TestEtcdPutIfAbsent(t *testing.T) {
	key := "key_key_key"
	val := "val_val_val"

	etcd1, err := createNewEtcdConsensus(context.Background())
	assert.Nil(t, err)
	didPut, err := etcd1.putIfAbsent(context.Background(), key, val)
	assert.Nil(t, err)
	assert.True(t, didPut)

	v, err := etcd1.get(context.Background(), key)
	assert.Nil(t, err)
	assert.Equal(t, val, v)

	didPut, err = etcd1.putIfAbsent(context.Background(), key, "narf_narf_narf")
	assert.Nil(t, err)
	assert.False(t, didPut)

	v, err = etcd1.get(context.Background(), key)
	assert.Nil(t, err)
	assert.Equal(t, val, v)

	assert.Nil(t, etcd1.close())
}

func _TestEtcdGetSortedRange(t *testing.T) {
	count := 17
	key := "key_key_key_"
	etcd1, err := createNewEtcdConsensus(context.Background())
	assert.Nil(t, err)

	for i := 0; i < count; i++ {
		val, err := uuid.NewRandom()
		assert.Nil(t, err)
		didPut, err := etcd1.putIfAbsent(context.Background(), key+strconv.Itoa(i), val.String())
		assert.Nil(t, err)
		assert.True(t, didPut)
	}

	kvs, err := etcd1.getSortedRange(context.Background(), key)
	assert.Nil(t, err)
	assert.Equal(t, count, len(kvs))

	assert.Nil(t, etcd1.close())
}
