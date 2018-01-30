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
	"golang.org/x/net/context"
)

type Transaction interface {
	Commit() error
	Rollback() error
}

type Cache interface {
	AllocateWithData(buffer []byte, txn *Transaction) (int, error)
	Get(lineId int) ([]byte, error)
	Gets(lineId int, txn *Transaction) ([]byte, error)
	Getx(lineId int, txn *Transaction) ([]byte, error)
	Put(lineId int, buffer []byte, txn *Transaction)
	Putx(lineId int, buffer []byte, txn *Transaction)
	NewTransaction() *Transaction
	Stop()
}

func NewCache(myNodeId int, serverPort int) (Cache, error) {
	return createNewCache(myNodeId, serverPort)
}

// TODO make this interface package private
type CacheClient interface {
	SendGet(ctx context.Context, g *Get) (*Put, *OwnerChanged, error)
	SendGetx(ctx context.Context, g *Getx) (*Putx, *OwnerChanged, error)
	SendInvalidate(ctx context.Context, i *Inv) (*InvAck, error)
	Close() error
}

// TODO make this interface package private
type CacheServer interface {
	Get(ctx context.Context, req *Get) (*GetResponse, error)
	Getx(ctx context.Context, req *Getx) (*GetxResponse, error)
	Invalidate(ctx context.Context, in *Inv) (*InvAck, error)
	Stop()
}
