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
	"github.com/mhelmich/carbon-copy-go/pb"
	"golang.org/x/net/context"
)

type CarbonGridError string

func (e CarbonGridError) Error() string { return string(e) }

// This error is returned when remote operations time out.
const TimeoutError = CarbonGridError("Timeout")
const TxnNilError = CarbonGridError("Txn cannot be nil")

type Transaction interface {
	// Makes all operations that are part of this transaction durable.
	Commit() error
	// Reverts all operations being done as part of this transaction.
	Rollback() error

	addToTxn(cl *CacheLine, newBuffer []byte)
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
	toProtoBuf() *pb.CacheLineId
	String() string
}

type NodeId interface {
	toProtoBuf() *pb.NodeId
	string() string
}

// Constructor-type function creating a cache instance.
func NewCache(myNodeId int, serverPort int) (Cache, error) {
	return createNewCache(myNodeId, serverPort)
}

type cacheClient interface {
	SendGet(ctx context.Context, g *pb.Get) (*pb.Put, *pb.OwnerChanged, error)
	SendGets(ctx context.Context, g *pb.Gets) (*pb.Puts, *pb.OwnerChanged, error)
	SendGetx(ctx context.Context, g *pb.Getx) (*pb.Putx, *pb.OwnerChanged, error)
	SendInvalidate(ctx context.Context, i *pb.Inv) (*pb.InvAck, error)
	Close() error
}

type cacheServer interface {
	Get(ctx context.Context, req *pb.Get) (*pb.GetResponse, error)
	Gets(ctx context.Context, req *pb.Gets) (*pb.GetsResponse, error)
	Getx(ctx context.Context, req *pb.Getx) (*pb.GetxResponse, error)
	Invalidate(ctx context.Context, in *pb.Inv) (*pb.InvAck, error)
	Stop()
}

type cacheClientMapping interface {
	getClientForNodeId(nodeId int) (cacheClient, error)
	addClientWithNodeId(nodeId int, addr string)
	forEachParallel(f func(c cacheClient))
	printStats()
	clear()
}
