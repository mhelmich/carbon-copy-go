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
	"context"
	"errors"
	"fmt"
	"github.com/mhelmich/carbon-copy-go/pb"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

func createNewCache(myNodeId int, serverPort int) (*cacheImpl, error) {
	clStore := createNewCacheLineStore()
	srv, err := createNewServer(myNodeId, serverPort, clStore)
	if err != nil {
		return nil, err
	} else {
		return &cacheImpl{
			store:         clStore,
			clientMapping: newCacheClientMapping(),
			server:        srv,
			myNodeId:      myNodeId,
			port:          serverPort,
		}, nil
	}
}

type cacheImpl struct {
	store         *cacheLineStore
	clientMapping cacheClientMapping
	server        cacheServer
	myNodeId      int
	port          int
}

////////////////////////////////////////////////////////////////////////
/////
/////  INTERFACE DEFINITIONS
/////
////////////////////////////////////////////////////////////////////////

func (c *cacheImpl) AllocateWithData(buffer []byte, txn Transaction) (CacheLineId, error) {
	if txn == nil {
		return nil, TxnNilError
	}

	newLineId := newRandomCacheLineId()
	line := newCacheLine(newLineId, c.myNodeId, buffer)
	txn.addToTxn(line, buffer)
	return newLineId, nil
}

func (c *cacheImpl) Get(lineId CacheLineId) ([]byte, error) {
	line, ok := c.store.getCacheLineById(lineId)

	if ok {
		switch line.cacheLineState {

		case pb.CacheLineState_Exclusive, pb.CacheLineState_Owned:
			// I have the most current version that means we're good to go
			return line.buffer, nil

		case pb.CacheLineState_Shared, pb.CacheLineState_Invalid:
			// need to get latest version of the line as I don't have it
			g := &pb.Get{
				SenderId: int32(c.myNodeId),
				LineId:   lineId.toProtoBuf(),
			}

			put, err := c.unicastGet(context.Background(), line.ownerId, g)
			if err != nil {
				log.Errorf(err.Error())
				return nil, err
			}

			c.store.applyChangesFromPut(line, put)
			return line.buffer, nil

		default:
			return nil, errors.New(fmt.Sprintf("Not a cache line state I like %v", line.cacheLineState))
		}
	} else {
		// multi cast to everybody I know whether anyone knows this line
		g := &pb.Get{
			SenderId: int32(c.myNodeId),
			LineId:   lineId.toProtoBuf(),
		}

		put, err := c.multicastGet(context.Background(), g)
		if err != nil {
			log.Errorf(err.Error())
			return nil, err
		}

		line = c.store.createCacheLineFromPut(put)
		val, loaded := c.store.putIfAbsent(lineId, line)
		if loaded {
			c.store.applyChangesFromPut(val, put)
		}

		return line.buffer, nil
	}
}

func (c *cacheImpl) Gets(lineId CacheLineId, txn Transaction) ([]byte, error) {
	if txn == nil {
		return nil, TxnNilError
	}

	return nil, nil
}

func (c *cacheImpl) Getx(lineId CacheLineId, txn Transaction) ([]byte, error) {
	if txn == nil {
		return nil, TxnNilError
	}

	line, ok := c.store.getCacheLineById(lineId)

	if ok {
		txn.addToLockedLines(line)
		switch line.cacheLineState {

		case pb.CacheLineState_Exclusive:
			return line.buffer, nil

		case pb.CacheLineState_Shared, pb.CacheLineState_Invalid:
			err := c.unicastExclusiveGet(context.Background(), line)
			if err != nil {
				log.Errorf(err.Error())
				return nil, err
			}
			// shabang#!
			// fall through this case and invalidate the line
			fallthrough

		case pb.CacheLineState_Owned:
			err := c.elevateOwnedToExclusive(line)
			if err == nil {
				return line.buffer, nil
			} else {
				log.Errorf(err.Error())
				return nil, err
			}
		}
	} else {
		// if we don't know about the line,
		// let's ask around and see whether somebody else knows it
		err := c.multicastExclusiveGet(context.Background(), lineId)
		if err != nil {
			log.Errorf(err.Error())
			return nil, err
		}

		line, ok := c.store.getCacheLineById(lineId)
		if ok {
			txn.addToLockedLines(line)
			return line.buffer, nil
		} else {
			err := errors.New(fmt.Sprintf("Can't find line with id %d even after get.", lineId))
			log.Errorf(err.Error())
			return nil, err
		}

	}
	return nil, errors.New("Unknown error!")
}

func (c *cacheImpl) Put(lineId CacheLineId, buffer []byte, txn Transaction) error {
	if txn == nil {
		return TxnNilError
	}

	line, ok := c.store.getCacheLineById(lineId)

	if ok {
		switch line.cacheLineState {
		case pb.CacheLineState_Shared, pb.CacheLineState_Invalid:
			err := c.unicastExclusiveGet(context.Background(), line)
			if err != nil {
				log.Errorf(err.Error())
				return err
			}
			// shabang#!
			// fall through this case and invalidate the line
			fallthrough
		case pb.CacheLineState_Owned:
			err := c.elevateOwnedToExclusive(line)
			if err != nil {
				log.Errorf(err.Error())
				return err
			}
			// shabang#!
			// fall through this case and invalidate the line
			fallthrough
		case pb.CacheLineState_Exclusive:
			line.version++
			line.buffer = buffer
		default:
			log.Infof("No interesting state in put %v", line.cacheLineState)
		}

		return nil
	} else {
		return errors.New("Not implemented yet!")
	}
}

func (c *cacheImpl) Putx(lineId CacheLineId, buffer []byte, txn Transaction) error {
	if txn == nil {
		return TxnNilError
	}

	return errors.New("Not implemented yet!")
}

func (c *cacheImpl) NewTransaction() Transaction {
	return createNewTransaction(c)
}

func (c *cacheImpl) Stop() {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	go func() {
		c.server.Stop()
		wg.Done()
	}()

	go func() {
		c.clientMapping.clear()
		wg.Done()
	}()

	wg.Wait()
}

////////////////////////////////////////////////////////////////////////
/////
/////  INTERNAL HELPERS
/////
////////////////////////////////////////////////////////////////////////

func (c *cacheImpl) unicastExclusiveGet(ctx context.Context, line *CacheLine) error {
	getx := &pb.Getx{
		SenderId: int32(c.myNodeId),
		LineId:   line.id.toProtoBuf(),
	}

	putx, err := c.unicastGetx(context.Background(), line.ownerId, getx)
	if err != nil {
		return err
	}

	c.store.applyChangesFromPutx(line, putx, c.myNodeId)
	return nil
}

func (c *cacheImpl) multicastExclusiveGet(ctx context.Context, lineId CacheLineId) error {
	g := &pb.Getx{
		SenderId: int32(c.myNodeId),
		LineId:   lineId.toProtoBuf(),
	}

	putx, err := c.multicastGetx(ctx, g)
	if err != nil {
		return err
	}

	line := c.store.createCacheLineFromPutx(putx, c.myNodeId)
	val, loaded := c.store.putIfAbsent(lineId, line)
	if loaded {
		c.store.applyChangesFromPutx(val, putx, c.myNodeId)
	}
	return nil
}

func (c *cacheImpl) elevateOwnedToExclusive(line *CacheLine) error {
	inv := &pb.Inv{
		SenderId: int32(c.myNodeId),
		LineId:   line.id.toProtoBuf(),
	}

	log.Infof("sharers %v", line.sharers)
	err := c.multicastInvalidate(context.Background(), line.sharers, inv)
	if err == nil {
		line.cacheLineState = pb.CacheLineState_Exclusive
		return nil
	} else {
		return err
	}
}

func (c *cacheImpl) addPeerNode(nodeId int, addr string) {
	c.clientMapping.addClientWithNodeId(nodeId, addr)
}

func (c *cacheImpl) unicastGet(ctx context.Context, nodeId int, get *pb.Get) (*pb.Put, error) {
	var p *pb.Put
	var oc *pb.OwnerChanged
	var err error
	var client cacheClient

	// as long as we're getting owner changed messages,
	// we keep iterating and try to find the actual line
	for { // ever...
		client, err = c.clientMapping.getClientForNodeId(nodeId)
		if err != nil {
			log.Errorf(err.Error())
			return nil, err
		}

		p, oc, err = client.SendGet(ctx, get)
		if oc != nil {
			nodeId = int(oc.NewOwnerId)
		} else if p != nil {
			return p, nil
		} else {
			return nil, err
		}
	}

	return nil, errors.New(fmt.Sprintf("Couldn't find cache lines for %v", get))
}

func (c *cacheImpl) multicastGet(ctx context.Context, get *pb.Get) (*pb.Put, error) {
	ch := make(chan *pb.Put)

	fctn := func(client cacheClient) {
		put, _, err := client.SendGet(ctx, get)
		if err == nil && put != nil {
			ch <- put
		}
	}

	c.clientMapping.forEachParallel(fctn)

	select {
	case p := <-ch:
		return p, nil
	case <-time.After(5 * time.Second):
		return nil, TimeoutError
	}
}

func (c *cacheImpl) unicastGetx(ctx context.Context, nodeId int, getx *pb.Getx) (*pb.Putx, error) {
	var p *pb.Putx
	var oc *pb.OwnerChanged
	var err error
	var client cacheClient

	for { //ever...
		client, err = c.clientMapping.getClientForNodeId(nodeId)
		if err != nil {
			log.Errorf(err.Error())
			return nil, err
		}

		p, oc, err = client.SendGetx(ctx, getx)
		if oc != nil {
			log.Infof("Received owner changed %v", oc)
			nodeId = int(oc.NewOwnerId)
		} else if p != nil {
			return p, nil
		} else {
			return nil, err
		}
	}

	return nil, errors.New(fmt.Sprintf("Couldn't find cache lines for %v", getx))
}

func (c *cacheImpl) multicastGetx(ctx context.Context, getx *pb.Getx) (*pb.Putx, error) {
	ch := make(chan *pb.Putx)

	fctn := func(client cacheClient) {
		putx, _, err := client.SendGetx(ctx, getx)
		if err == nil && putx != nil {
			ch <- putx
		}
	}

	c.clientMapping.forEachParallel(fctn)
	select {
	case p := <-ch:
		return p, nil
	case <-time.After(5 * time.Second):
		return nil, TimeoutError
	}
}

func (c *cacheImpl) multicastInvalidate(ctx context.Context, sharers []int, inv *pb.Inv) error {
	ch := make(chan interface{}, len(sharers))

	for _, sharerId := range sharers {
		go func() {
			client, err := c.clientMapping.getClientForNodeId(sharerId)
			if err == nil {
				log.Infof("Send invalidate to %d", sharerId)
				_, err := client.SendInvalidate(ctx, inv)
				if err != nil {
					log.Errorf("Couldn't invalidate %v with %d because of %s", inv, sharerId, err)
				}
				// send to channel anyways
				// that way we're not stopping the entire train
				ch <- 1
			}
		}()
	}

	// TODO: waiting for everyone might be too strict and too slow
	// what happens if one node isn't reachable
	msgCount := 0
	for msgCount < len(sharers) {
		select {
		case <-ch:
			msgCount++
		case <-time.After(5 * time.Second):
			return TimeoutError
		}
	}
	return nil
}
