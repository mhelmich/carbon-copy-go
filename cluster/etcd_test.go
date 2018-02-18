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
	// log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEtcdCreateConsensus(t *testing.T) {
	etcd, err := createNewEtcdConsensus(context.Background())
	assert.Nil(t, err)
	assert.NotNil(t, etcd.etcdSession.Lease())
	assert.Nil(t, etcd.Close())
}

func TestEtcdPutGet(t *testing.T) {
	key := "key_key_key"
	val := "val_val_val"

	etcd, err := createNewEtcdConsensus(context.Background())
	assert.Nil(t, err)
	assert.Nil(t, etcd.Put(context.Background(), key, val))
	v, err := etcd.Get(context.Background(), key)
	assert.Nil(t, err)
	assert.Equal(t, val, v)
	assert.Nil(t, etcd.Close())
}

func TestEtcdPutGetExpireGet(t *testing.T) {
	key := "key_key_key"
	val := "val_val_val"

	etcd1, err := createNewEtcdConsensus(context.Background())
	assert.Nil(t, err)
	assert.Nil(t, etcd1.Put(context.Background(), key, val))
	v, err := etcd1.Get(context.Background(), key)
	assert.Nil(t, err)
	assert.Equal(t, val, v)
	assert.Nil(t, etcd1.Close())

	etcd2, err := createNewEtcdConsensus(context.Background())
	assert.Nil(t, err)
	v, err = etcd1.Get(context.Background(), key)
	assert.NotNil(t, err)
	assert.Equal(t, "", v)
	assert.Nil(t, etcd2.Close())
}

func TestEtcdPutIfAbsent(t *testing.T) {
	key := "key_key_key"
	val := "val_val_val"

	etcd1, err := createNewEtcdConsensus(context.Background())
	assert.Nil(t, err)
	didPut, err := etcd1.PutIfAbsent(context.Background(), key, val)
	assert.Nil(t, err)
	assert.True(t, didPut)

	v, err := etcd1.Get(context.Background(), key)
	assert.Nil(t, err)
	assert.Equal(t, val, v)

	didPut, err = etcd1.PutIfAbsent(context.Background(), key, "narf_narf_narf")
	assert.Nil(t, err)
	assert.False(t, didPut)

	v, err = etcd1.Get(context.Background(), key)
	assert.Nil(t, err)
	assert.Equal(t, val, v)

	assert.Nil(t, etcd1.Close())
}
