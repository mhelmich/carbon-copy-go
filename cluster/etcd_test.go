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
	"crypto/rand"
	"github.com/oklog/ulid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
	// I could toy around with this one day
	// "github.com/araddon/qlbridge"
	// "github.com/couchbase/blance"
	// "github.com/couchbase/moss"
	// "github.com/hashicorp/raft"
	// raftboltdb "github.com/hashicorp/raft-boltdb"
	// "github.com/hashicorp/serf/serf"
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
		val := ulid.MustNew(ulid.Now(), rand.Reader)
		didPut, err := etcd1.putIfAbsent(context.Background(), key+strconv.Itoa(i), val.String())
		assert.Nil(t, err)
		assert.True(t, didPut)
	}

	kvs, err := etcd1.getSortedRange(context.Background(), key)
	assert.Nil(t, err)
	assert.Equal(t, count, len(kvs))

	assert.Nil(t, etcd1.close())
}

func _TestEtcdWatchKeyPrefixValuesPresentOnly(t *testing.T) {
	commonPrefix := "prefix___"
	etcd1, err := createNewEtcdConsensus(context.Background())
	assert.Nil(t, err)

	didPut, err := etcd1.putIfAbsent(context.Background(), commonPrefix+"55", ulid.MustNew(ulid.Now(), rand.Reader).String())
	assert.Nil(t, err)
	assert.True(t, didPut)

	watcherContext, watcherCancel := context.WithCancel(context.Background())
	kvCh, err := etcd1.watchKeyPrefixStr(watcherContext, commonPrefix)
	assert.Nil(t, err)
	assert.NotNil(t, kvCh)

	var continueLoop = true
	numPacketsReceived := 0
	for continueLoop { // ever...
		select {
		case kvPacket := <-kvCh:
			log.Infof("Received kv packet with size %d", len(kvPacket))
			numPacketsReceived++
			if numPacketsReceived > 0 {
				continueLoop = false
			}
		case <-time.After(3 * time.Second):
			log.Errorf("Read from watcher channel timed out")
			assert.Failf(t, "Read from watcher channel timed out", "msg")
			continueLoop = false
		}
	}

	watcherCancel()
	assert.Equal(t, 1, numPacketsReceived)
	assert.Nil(t, etcd1.close())
}

func _TestEtcdWatchKeyPrefixValuesPresentAndNewOnesSet(t *testing.T) {
	commonPrefix := "prefix___"
	etcd1, err := createNewEtcdConsensus(context.Background())
	assert.Nil(t, err)

	didPut, err := etcd1.putIfAbsent(context.Background(), commonPrefix+"55", ulid.MustNew(ulid.Now(), rand.Reader).String())
	assert.Nil(t, err)
	assert.True(t, didPut)

	watcherContext, watcherCancel := context.WithCancel(context.Background())
	kvCh, err := etcd1.watchKeyPrefixStr(watcherContext, commonPrefix)
	assert.Nil(t, err)
	assert.NotNil(t, kvCh)

	// yeah yeah yeah, I know
	// the test relies on the fact that watchers can be attached before
	// this time ticked away
	// I should be writing a mock that calls the original code
	// and only returns after a certain condition is met
	// (and for the sake of complete testing I should go through all possible situations)
	// but I'm too lazy to do this now
	time.Sleep(300 * time.Millisecond)

	didPut, err = etcd1.putIfAbsent(context.Background(), commonPrefix+"99", ulid.MustNew(ulid.Now(), rand.Reader).String())
	assert.Nil(t, err)
	assert.True(t, didPut)
	if err != nil || !didPut {
		assert.Failf(t, "Couldn't put value", "msg")
	}

	var continueLoop = true
	numPacketsReceived := 0
	for continueLoop { // ever...
		select {
		case kvPacket := <-kvCh:
			log.Infof("Received kv packet with size %d", len(kvPacket))
			numPacketsReceived++
			if numPacketsReceived > 1 {
				continueLoop = false
			}
		case <-time.After(3 * time.Second):
			log.Errorf("Read from watcher channel timed out")
			assert.Failf(t, "Read from watcher channel timed out", "msg")
			continueLoop = false
		}
	}

	watcherCancel()
	assert.Equal(t, 2, numPacketsReceived)
	assert.Nil(t, etcd1.close())
}
