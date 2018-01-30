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
	log "github.com/sirupsen/logrus"
	"math/rand"
	"sync"
	"time"
)

func getNextLineId() int {
	return rand.Int()
}

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
	clientMapping *cacheClientMappingImpl
	server        CacheServer
	myNodeId      int
	port          int
}

func (c *cacheImpl) AllocateWithData(buffer []byte, txn *Transaction) (int, error) {
	newLineId := getNextLineId()
	line := newCacheLine(newLineId, c.myNodeId, buffer)
	c.store.addCacheLineToLocalCache(line)
	return newLineId, nil
}

func (c *cacheImpl) Get(lineId int) ([]byte, error) {
	line, ok := c.store.getCacheLineById(lineId)

	if ok {
		switch line.cacheLineState {

		case CacheLineState_Exclusive, CacheLineState_Owned:
			return line.buffer, nil

		case CacheLineState_Shared, CacheLineState_Invalid:
			g := &Get{
				SenderId: int32(c.myNodeId),
				LineId:   int64(lineId),
			}

			put, err := c.unicastGet(context.Background(), line.ownerId, g)
			if err != nil {
				msg := fmt.Sprintf("Didn't get valid response for get request for line %d", lineId)
				log.Errorf(msg)
				return nil, errors.New(msg)
			}

			c.store.applyChangesFromPut(line, put)
			return line.buffer, nil

		default:
			return nil, errors.New(fmt.Sprintf("Not a cache line state I like %v", line.cacheLineState))
		}
	} else {
		// multi cast to everybody I know whether anyone knows this line
		g := &Get{
			SenderId: int32(c.myNodeId),
			LineId:   int64(lineId),
		}

		put, err := c.multicastGet(context.Background(), g)
		if err != nil {
			msg := fmt.Sprintf("Couldn't find cache line with id %d", lineId)
			log.Errorf(msg)
			return nil, errors.New(msg)
		}

		line = c.store.createCacheLineFromPut(lineId, put)
		val, loaded := c.store.putIfAbsent(lineId, line)
		if loaded {
			c.store.applyChangesFromPut(val, put)
		}

		return line.buffer, nil
	}
}

func (c *cacheImpl) Gets(lineId int, txn *Transaction) ([]byte, error) {
	return nil, nil
}

func (c *cacheImpl) Getx(lineId int, txn *Transaction) ([]byte, error) {
	line, ok := c.store.getCacheLineById(lineId)

	if ok {
		switch line.cacheLineState {
		case CacheLineState_Exclusive:
			return line.buffer, nil
		case CacheLineState_Owned:
			// elevate to exclusive
			inv := &Inv{
				SenderId: int32(c.myNodeId),
				LineId:   int64(line.id),
			}
			c.multicastInvalidate(context.Background(), line.sharers, inv)
			return line.buffer, nil
		case CacheLineState_Shared, CacheLineState_Invalid:
			getx := &Getx{
				SenderId: int32(c.myNodeId),
				LineId:   int64(line.id),
			}

			putx, err := c.unicastGetx(context.Background(), line.ownerId, getx)
			if err != nil {
				msg := fmt.Sprintf("Didn't get valid response for getx request for line %d", lineId)
				log.Errorf(msg)
				return nil, errors.New(msg)
			}
			c.store.applyChangesFromPutx(line, putx, c.myNodeId)
			return line.buffer, nil

		}
	} else {
		// multi cast to everybody I know whether anyone knows this line
		g := &Getx{
			SenderId: int32(c.myNodeId),
			LineId:   int64(lineId),
		}

		putx, err := c.multicastGetx(context.Background(), g)
		if err != nil {
			msg := fmt.Sprintf("Couldn't find cache line with id %d", lineId)
			log.Errorf(msg)
			return nil, errors.New(msg)
		}

		line = c.store.createCacheLineFromPutx(lineId, putx, c.myNodeId)
		val, loaded := c.store.putIfAbsent(lineId, line)
		if loaded {
			c.store.applyChangesFromPutx(val, putx, c.myNodeId)
		}

		return line.buffer, nil
	}
	return nil, nil
}

func (c *cacheImpl) Put(lineId int, buffer []byte, txn *Transaction) {
	line, ok := c.store.getCacheLineById(lineId)

	if ok {
		switch line.cacheLineState {
		case CacheLineState_Exclusive:
			line.lock()
			line.version++
			line.buffer = buffer
			line.unlock()
		default:
			log.Infof("No interesting state in put %v", line.cacheLineState)
		}
	} else {
	}
}

func (c *cacheImpl) Putx(lineId int, buffer []byte, txn *Transaction) {

}

func (c *cacheImpl) NewTransaction() *Transaction {
	return nil
}

func (c *cacheImpl) invalidate(lineId int) {

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

func (c *cacheImpl) addPeerNode(nodeId int, addr string) {
	c.clientMapping.addClientWithNodeId(nodeId, addr)
}

func (c *cacheImpl) unicastGet(ctx context.Context, nodeId int, get *Get) (*Put, error) {
	var p *Put
	var oc *OwnerChanged
	var err error
	var client CacheClient

	for p == nil {
		client, err = c.clientMapping.getClientForNodeId(nodeId)
		if err != nil {
			msg := fmt.Sprintf("Can't find client with node id %d", nodeId)
			log.Errorf(msg)
			return nil, errors.New(msg)
		}

		p, oc, err = client.SendGet(ctx, get)
		if oc != nil {
			log.Infof("Received owner changed %v", oc)
			nodeId = int(oc.NewOwnerId)
		} else if p != nil {
			return p, nil
		} else {
			return nil, err
		}
	}

	return nil, errors.New(fmt.Sprintf("Couldn't find cache lines for %v", get))
}

func (c *cacheImpl) unicastGetx(ctx context.Context, nodeId int, getx *Getx) (*Putx, error) {
	var p *Putx
	var oc *OwnerChanged
	var err error
	var client CacheClient

	for p == nil {
		client, err = c.clientMapping.getClientForNodeId(nodeId)
		if err != nil {
			msg := fmt.Sprintf("Can't find client with node id %d", nodeId)
			log.Errorf(msg)
			return nil, errors.New(msg)
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

func (c *cacheImpl) multicastGet(ctx context.Context, get *Get) (*Put, error) {
	ch := make(chan *Put)

	fctn := func(key, value interface{}) bool {
		go func() {
			nodeId := key.(int)
			client, err := c.clientMapping.getClientForNodeId(nodeId)
			if err == nil {
				put, _, err := client.SendGet(ctx, get)
				if err == nil && put != nil {
					ch <- put
				}
			}
		}()
		return true
	}

	c.clientMapping.nodeIdToAddr.Range(fctn)
	select {
	case p := <-ch:
		return p, nil
	case <-time.After(5 * time.Second):
		return nil, errors.New("Timeout")
	}
}

func (c *cacheImpl) multicastGetx(ctx context.Context, getx *Getx) (*Putx, error) {
	ch := make(chan *Putx)

	fctn := func(key, value interface{}) bool {
		go func() {
			nodeId := key.(int)
			client, err := c.clientMapping.getClientForNodeId(nodeId)
			if err == nil {
				putx, _, err := client.SendGetx(ctx, getx)
				// TODO: write better code
				if err == nil && putx != nil {
					ch <- putx
				}
			}
		}()
		return true
	}

	c.clientMapping.nodeIdToAddr.Range(fctn)
	select {
	case p := <-ch:
		return p, nil
	case <-time.After(5 * time.Second):
		return nil, errors.New("Timeout")
	}
}

func (c *cacheImpl) multicastInvalidate(ctx context.Context, sharers []int, inv *Inv) {
	for sharerId, _ := range sharers {
		go func() {
			client, err := c.clientMapping.getClientForNodeId(sharerId)
			if err == nil {
				_, err := client.SendInvalidate(ctx, inv)
				if err != nil {
					log.Errorf("Couldn't invalidate %v with %d because of %s", inv, sharerId, err)
				}
			}
		}()
	}
}
