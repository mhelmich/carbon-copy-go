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
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/mhelmich/carbon-copy-go/pb"
	log "github.com/sirupsen/logrus"
)

func defaultCacheConfig(myNodeId int, config CacheConfig) CacheConfig {
	host, err := os.Hostname()
	if err != nil {
		log.Errorf("Can't get hostname: %s", err.Error())
	}

	if config.hostname == "" {
		config.hostname = host
	}

	if config.logger == nil {
		config.logger = log.WithFields(log.Fields{
			"host":         host,
			"component":    "cache",
			"longMemberId": myNodeId,
		})
	}

	return config
}

func createNewCache(myNodeId int, serverPort int, config CacheConfig) (*cacheImpl, error) {
	config = defaultCacheConfig(myNodeId, config)
	clStore := createNewCacheLineStore()
	srv, err := createNewServer(myNodeId, serverPort, clStore)
	if err != nil {
		return nil, err
	}

	return &cacheImpl{
		store:         clStore,
		clientMapping: newCacheClientMapping(),
		server:        srv,
		myNodeId:      myNodeId,
		port:          serverPort,
		config:        config,
		logger: config.logger.WithFields(log.Fields{
			"class": "cache",
		}),
	}, nil
}

type cacheImpl struct {
	store         *cacheLineStore
	clientMapping cacheClientMapping
	server        cacheServer
	myNodeId      int
	port          int
	config        CacheConfig
	logger        *log.Entry
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
				return nil, fmt.Errorf("Can't unicast get: %s", err.Error())
			}

			c.store.applyChangesFromPut(line, put)
			return line.buffer, nil

		default:
			return nil, CarbonGridError(fmt.Sprintf("Not a cache line state I like %v", line.cacheLineState))
		}
	} else {
		// multi cast to everybody I know whether anyone knows this line
		g := &pb.Get{
			SenderId: int32(c.myNodeId),
			LineId:   lineId.toProtoBuf(),
		}

		put, err := c.multicastGet(context.Background(), g)
		if err != nil {
			return nil, fmt.Errorf("Can't multicast get: %s", err.Error())
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

	return nil, fmt.Errorf("Not implmented yet")
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
				return nil, fmt.Errorf("Can't unicast exclusive get: %s", err.Error())
			}
			// shabang#!
			// fall through this case and invalidate the line
			fallthrough

		case pb.CacheLineState_Owned:
			err := c.elevateOwnedToExclusive(line)
			if err != nil {
				return nil, fmt.Errorf("Can't elevate owned to exclusive: %s", lineId.String())
			}

			return line.buffer, nil
		}
	} else {
		// if we don't know about the line,
		// let's ask around and see whether somebody else knows it
		err := c.multicastExclusiveGet(context.Background(), lineId)
		if err != nil {
			return nil, fmt.Errorf("Can't multicast exclusive get: %s", err.Error())
		}

		line, ok := c.store.getCacheLineById(lineId)
		if !ok {
			return nil, fmt.Errorf("Can't find line with id [%s] even after get.", lineId.String())
		}

		txn.addToLockedLines(line)
		return line.buffer, nil
	}
	return nil, fmt.Errorf("Unknown error!")
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
				return fmt.Errorf("Can't unicast exclusive get: %s", err.Error())
			}
			// shabang#!
			// fall through this case and invalidate the line
			fallthrough
		case pb.CacheLineState_Owned:
			err := c.elevateOwnedToExclusive(line)
			if err != nil {
				return fmt.Errorf("Can't elevate owned to exclusive: %s", lineId.String())
			}
			// shabang#!
			// fall through this case and invalidate the line
			fallthrough
		case pb.CacheLineState_Exclusive:
			line.version++
			line.buffer = buffer
		default:
			c.logger.Infof("No interesting state in put %v", line.cacheLineState)
		}

		return nil
	} else {
		return fmt.Errorf("Not implemented yet!")
	}
}

func (c *cacheImpl) Putx(lineId CacheLineId, buffer []byte, txn Transaction) error {
	if txn == nil {
		return TxnNilError
	}

	return fmt.Errorf("Not implemented yet!")
}

func (c *cacheImpl) NewTransaction() Transaction {
	return createNewTransaction(c)
}

func (c *cacheImpl) Stop() {
	wg := sync.WaitGroup{}
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

func (c *cacheImpl) unicastExclusiveGet(ctx context.Context, line *cacheLine) error {
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

func (c *cacheImpl) elevateOwnedToExclusive(line *cacheLine) error {
	inv := &pb.Inv{
		SenderId: int32(c.myNodeId),
		LineId:   line.id.toProtoBuf(),
	}

	err := c.multicastInvalidate(context.Background(), line.sharers, inv)
	if err != nil {
		return err
	}

	line.cacheLineState = pb.CacheLineState_Exclusive
	return nil
}

func (c *cacheImpl) AddPeerNode(nodeId int, addr string) {
	c.clientMapping.addClientWithNodeId(nodeId, addr)
}

func (c *cacheImpl) RemovePeerNode(nodeId int) {
	c.clientMapping.removeClientWithNodeId(nodeId)
}

func (c *cacheImpl) unicastGet(ctx context.Context, nodeId int, get *pb.Get) (*pb.Put, error) {
	var p *pb.Put
	var oc *pb.OwnerChanged
	var err error
	var client cacheClient
	var nextNodeIdToContact int
	nextNodeIdToContact = nodeId

	// as long as we're getting owner changed messages,
	// we keep iterating and try to find the actual line
	for { // ever...
		client, err = c.clientMapping.getClientForNodeId(nextNodeIdToContact)
		if err != nil {
			return nil, fmt.Errorf("Can't find client for node [%d] in client mapping!", nextNodeIdToContact)
		}

		p, oc, err = client.SendGet(ctx, get)
		if oc != nil {
			// if we're getting owner changed messages
			// we try to contact the new owner in an infinite loop
			nextNodeIdToContact = int(oc.NewOwnerId)
		} else if p != nil {
			// when we're getting a put that means we found the owner
			// we're done with our work
			return p, nil
		} else {
			// when we get an error we return that and we're done
			return nil, err
		}
	}
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
	var nextNodeIdToContact int
	nextNodeIdToContact = nodeId

	for { //ever...
		client, err = c.clientMapping.getClientForNodeId(nodeId)
		if err != nil {
			return nil, fmt.Errorf("Can't find client for node [%d] in client mapping!", nextNodeIdToContact)
		}

		p, oc, err = client.SendGetx(ctx, getx)
		if oc != nil {
			// if we're getting owner changed messages
			// we try to contact the new owner in an infinite loop
			nextNodeIdToContact = int(oc.NewOwnerId)
		} else if p != nil {
			// when we're getting a put that means we found the owner
			// we're done with our work
			return p, nil
		} else {
			return nil, err
		}
	}
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
		// create new int to not point into the array
		sId := sharerId
		go func() {
			client, err := c.clientMapping.getClientForNodeId(sId)
			if err == nil {
				c.logger.Infof("Send invalidate message to node id [%d]", sId)
				_, err := client.SendInvalidate(ctx, inv)
				if err != nil {
					c.logger.Errorf("Couldn't invalidate %v with %d because of %s", inv, sId, err)
				}
				// send to channel anyways
				// that way we're not stopping the entire train
				ch <- 1
			}
		}()
	}

	// TODO: waiting for everyone might be too strict and too slow
	// what happens if one node isn't reachable?
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
