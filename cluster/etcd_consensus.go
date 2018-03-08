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
	"errors"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	log "github.com/sirupsen/logrus"
	"time"
)

func createNewEtcdConsensus(ctx context.Context) (*etcdConsensus, error) {
	cfg := clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 3 * time.Second,
	}

	etcdClient, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}

	etcdSession, err := concurrency.NewSession(etcdClient)
	if err != nil {
		return nil, err
	}

	log.Infof("Created etcd session for %v", cfg.Endpoints)
	return &etcdConsensus{
		etcdSession: etcdSession,
		closed:      false,
	}, nil
}

type etcdConsensus struct {
	etcdSession *concurrency.Session
	closed      bool
}

func (ec *etcdConsensus) get(ctx context.Context, key string) (string, error) {
	resp, err := ec.etcdSession.Client().Get(ctx, key, clientv3.WithLimit(1))
	if err != nil {
		return "", err
	}

	if resp.Count == int64(0) {
		return "", errors.New("Couldn't find key '" + key + "'")
	} else {
		return string(resp.Kvs[0].Value), nil
	}
}

func (ec *etcdConsensus) getSortedRange(ctx context.Context, keyPrefix string) ([]kvStr, error) {
	resp, err := ec.etcdSession.Client().Get(ctx, keyPrefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return nil, err
	} else {
		if resp.Count == int64(0) {
			return make([]kvStr, 0), nil
		} else {
			strs := make([]kvStr, len(resp.Kvs))
			for idx, ev := range resp.Kvs {
				strs[idx].key = string(ev.Key)
				strs[idx].value = string(ev.Value)
			}
			return strs, nil
		}
	}
}

func (ec *etcdConsensus) put(ctx context.Context, key string, value string) error {
	_, err := ec.etcdSession.Client().Put(ctx, key, value, clientv3.WithLease(ec.etcdSession.Lease()))
	return err
}

func (ec *etcdConsensus) putIfAbsent(ctx context.Context, key string, value string) (bool, error) {
	resp, err := ec.etcdSession.Client().Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), ">", 0)).
		Then().
		Else(clientv3.OpPut(key, value, clientv3.WithLease(ec.etcdSession.Lease()))).
		Commit()

	if err != nil {
		return false, err
	} else {
		return !resp.Succeeded, nil
	}
}

func (ec *etcdConsensus) compareAndPut(ctx context.Context, key string, oldValue string, newValue string) (bool, error) {
	resp, err := ec.etcdSession.Client().Txn(ctx).
		If(clientv3.Compare(clientv3.Value(key), "=", oldValue)).
		Then(clientv3.OpPut(key, newValue)).
		Else().
		Commit()

	if err != nil {
		return false, err
	} else {
		return resp.Succeeded, nil
	}
}

func (ec *etcdConsensus) watchKey(ctx context.Context, key string) (<-chan *kvStr, error) {
	return nil, errors.New("Not implemented yet!")
}

func (ec *etcdConsensus) watchKeyPrefix(ctx context.Context, prefix string) (<-chan []*kvStr, error) {
	kvChan := make(chan []*kvStr)

	go func() {
		// start the watcher first
		watcherCh := ec.etcdSession.Client().Watch(ctx, prefix, clientv3.WithPrefix())
		// then get the "current" state
		// worst case we're getting a newer revision in get and in older one in the watcher
		// but we will be processing everything out of the watcher anyways
		// meaning we could go from time t3 -> t2 -> t3
		// we can obviously just look at the revision of every
		// watcher event and make sure we only move forwards in time
		resp, err := ec.etcdSession.Client().Get(ctx, prefix, clientv3.WithPrefix())
		if err != nil {
			log.Errorf("Can't watch key prefix '%s' because of %s", prefix, err.Error())
			return
		}

		initialRevision := resp.Header.GetRevision()
		kvPacket := make([]*kvStr, len(resp.Kvs))
		for idx, ev := range resp.Kvs {
			kvPacket[idx] = &kvStr{
				key:   string(ev.Key),
				value: string(ev.Value),
			}
		}
		kvChan <- kvPacket

		for { // listen to the channel forever
			for resp := range watcherCh {

				if len(resp.Events) == 0 && resp.Err() == nil {
					// channel was closed
					// we're done
					close(kvChan)
					return
				}

				if resp.Header.GetRevision() > initialRevision {
					kvPacket := make([]*kvStr, len(resp.Events))
					for idx, event := range resp.Events {
						kvPacket[idx] = &kvStr{
							key:   string(event.Kv.Key),
							value: string(event.Kv.Value),
						}
					}
					kvChan <- kvPacket
				}
			}
		}
	}()

	log.Infof("Watching for key prefix '%s'", prefix)
	return kvChan, nil
}

func (ec *etcdConsensus) isClosed() bool {
	return ec.closed
}

func (ec *etcdConsensus) close() error {
	ec.closed = true
	err := ec.etcdSession.Close()
	if err != nil {
		log.Warnf("Too bad: error while shutting down: %s", err.Error())
	}
	return ec.etcdSession.Client().Close()
}
