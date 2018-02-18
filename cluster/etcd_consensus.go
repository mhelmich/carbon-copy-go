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
	}, nil
}

type etcdConsensus struct {
	etcdSession *concurrency.Session
}

func (ec *etcdConsensus) Get(ctx context.Context, key string) (string, error) {
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

func (ec *etcdConsensus) GetSortedRange(ctx context.Context, keyPrefix string) ([]string, error) {
	return nil, errors.New("Not implemented yet!")
}

func (ec *etcdConsensus) Put(ctx context.Context, key string, value string) error {
	_, err := ec.etcdSession.Client().Put(ctx, key, value, clientv3.WithLease(ec.etcdSession.Lease()))
	return err
}

func (ec *etcdConsensus) PutIfAbsent(ctx context.Context, key string, value string) (bool, error) {
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

func (ec *etcdConsensus) Close() error {
	return ec.etcdSession.Close()
}
