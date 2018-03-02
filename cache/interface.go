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

type CarbonGridError string

func (e CarbonGridError) Error() string { return string(e) }

// This error is returned when remote operations time out.
const TimeoutError = CarbonGridError("Timeout")

type Transaction interface {
	// Makes all operations that are part of this transaction durable.
	Commit() error
	// Reverts all operations being done as part of this transaction.
	Rollback() error
}

type Cache interface {
	// Allocates a new item and returns its newly created id.
	AllocateWithData(buffer []byte, txn Transaction) (CacheLineId, error)
	// Retrieves a particular item (if necessary remotely) and returns its contents.
	Get(lineId CacheLineId) ([]byte, error)
	// Retrieves a particular item and registers to be a sharer of this item with the owner.
	Gets(lineId CacheLineId, txn Transaction) ([]byte, error)
	// Retrieves a particular item cluster-exclusively.
	// All other copies of this item will be invalidated.
	Getx(lineId CacheLineId, txn Transaction) ([]byte, error)
	// Not needed...
	Put(lineId CacheLineId, buffer []byte, txn Transaction) error
	// Acquires exclusive ownership of an item and overrides its contents.
	Putx(lineId CacheLineId, buffer []byte, txn Transaction) error
	// Creates a new transaction.
	NewTransaction() Transaction
	// Stops the operation of this cache.
	Stop()
}

type CacheLineId interface {
	toProtoBuf() *LineId
	string() string
}

// Constructor-type function creating a cache instance.
func NewCache(myNodeId int, serverPort int) (Cache, error) {
	return createNewCache(myNodeId, serverPort)
}

type cacheClient interface {
	SendGet(ctx context.Context, g *Get) (*Put, *OwnerChanged, error)
	SendGets(ctx context.Context, g *Gets) (*Puts, *OwnerChanged, error)
	SendGetx(ctx context.Context, g *Getx) (*Putx, *OwnerChanged, error)
	SendInvalidate(ctx context.Context, i *Inv) (*InvAck, error)
	Close() error
}

type cacheServer interface {
	Get(ctx context.Context, req *Get) (*GetResponse, error)
	Gets(ctx context.Context, req *Gets) (*GetsResponse, error)
	Getx(ctx context.Context, req *Getx) (*GetxResponse, error)
	Invalidate(ctx context.Context, in *Inv) (*InvAck, error)
	Stop()
}

type cacheClientMapping interface {
	getClientForNodeId(nodeId int) (cacheClient, error)
	addClientWithNodeId(nodeId int, addr string)
	forEachParallel(f func(c cacheClient))
	printStats()
	clear()
}
