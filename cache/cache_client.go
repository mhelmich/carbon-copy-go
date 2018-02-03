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
	"errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// CLIENT IMPLEMENTATION

func createNewCacheClientFromConn(conn *grpc.ClientConn) (*cacheClientImpl, error) {
	return &cacheClientImpl{
		client: NewCacheCommClient(conn),
	}, nil
}

type cacheClientImpl struct {
	client CacheCommClient
}

func (cc *cacheClientImpl) SendGet(ctx context.Context, g *Get) (*Put, *OwnerChanged, error) {
	getResp, err := cc.client.Get(ctx, g)
	return cc.handleGetResponse(getResp, err)
}

func (cc *cacheClientImpl) SendGets(ctx context.Context, g *Gets) (*Puts, *OwnerChanged, error) {
	getsResp, err := cc.client.Gets(ctx, g)
	return cc.handleGetsResponse(getsResp, err)
}

func (cc *cacheClientImpl) SendGetx(ctx context.Context, g *Getx) (*Putx, *OwnerChanged, error) {
	getxResp, err := cc.client.Getx(ctx, g)
	return cc.handleGetxResponse(getxResp, err)
}

func (cc *cacheClientImpl) SendInvalidate(ctx context.Context, i *Inv) (*InvAck, error) {
	invAck, err := cc.client.Invalidate(ctx, i)
	if err != nil {
		return nil, err
	} else {
		return invAck, nil
	}
}

func (cc *cacheClientImpl) Close() error {
	return nil
}

func (cc *cacheClientImpl) handleGetResponse(getResp *GetResponse, err error) (*Put, *OwnerChanged, error) {
	switch {
	case err != nil:
		log.Errorf("%v", err)
		return nil, nil, err
	case getResp.GetAck() != nil:
		log.Infof("ack: %s", getResp.GetAck().String())
		// TODO -- solve this better :/
		// I don't like the way I'm (mis-)using the error for this non-error case
		return nil, nil, errors.New("response was ack")
	case getResp.GetPut() != nil:
		log.Infof("put: %s", getResp.GetPut().String())
		return getResp.GetPut(), nil, nil
	case getResp.GetOwnerChanged() != nil:
		return nil, getResp.GetOwnerChanged(), nil
	}

	return nil, nil, errors.New("Unknown response!")
}

func (cc *cacheClientImpl) handleGetsResponse(getsResp *GetsResponse, err error) (*Puts, *OwnerChanged, error) {
	switch {
	case err != nil:
		log.Errorf("%v", err)
		return nil, nil, err
	case getsResp.GetAck() != nil:
		log.Infof("ack: %s", getsResp.GetAck().String())
		// TODO -- solve this better :/
		// I don't like the way I'm (mis-)using the error for this non-error case
		return nil, nil, errors.New("response was ack")
	case getsResp.GetPuts() != nil:
		log.Infof("puts: %s", getsResp.GetPuts().String())
		return getsResp.GetPuts(), nil, nil
	case getsResp.GetOwnerChanged() != nil:
		return nil, getsResp.GetOwnerChanged(), nil
	}

	return nil, nil, errors.New("Unknown response!")
}

func (cc *cacheClientImpl) handleGetxResponse(getxResp *GetxResponse, err error) (*Putx, *OwnerChanged, error) {
	switch {
	case err != nil:
		log.Errorf("%v", err)
		return nil, nil, err
	case getxResp.GetAck() != nil:
		log.Infof("ack: %s", getxResp.GetAck().String())
		// TODO -- solve this better :/
		// I don't like the way I'm (mis-)using the error for this non-error case
		return nil, nil, errors.New("response was ack")
	case getxResp.GetPutx() != nil:
		log.Infof("putx: %s", getxResp.GetPutx().String())
		return getxResp.GetPutx(), nil, nil
	case getxResp.GetOwnerChanged() != nil:
		return nil, getxResp.GetOwnerChanged(), nil
	}

	return nil, nil, errors.New("Unknown response!")
}
