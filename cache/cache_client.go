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
	"github.com/mhelmich/carbon-copy-go/pb"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// CLIENT IMPLEMENTATION

func createNewCacheClientFromConn(conn *grpc.ClientConn) (*cacheClientImpl, error) {
	return &cacheClientImpl{
		client: pb.NewCacheCommClient(conn),
	}, nil
}

type cacheClientImpl struct {
	client pb.CacheCommClient
}

func (cc *cacheClientImpl) SendGet(ctx context.Context, g *pb.Get) (*pb.Put, *pb.OwnerChanged, error) {
	getResp, err := cc.client.Get(ctx, g)
	return cc.handleGetResponse(getResp, err)
}

func (cc *cacheClientImpl) SendGets(ctx context.Context, g *pb.Gets) (*pb.Puts, *pb.OwnerChanged, error) {
	getsResp, err := cc.client.Gets(ctx, g)
	return cc.handleGetsResponse(getsResp, err)
}

func (cc *cacheClientImpl) SendGetx(ctx context.Context, g *pb.Getx) (*pb.Putx, *pb.OwnerChanged, error) {
	getxResp, err := cc.client.Getx(ctx, g)
	return cc.handleGetxResponse(getxResp, err)
}

func (cc *cacheClientImpl) SendInvalidate(ctx context.Context, i *pb.Inv) (*pb.InvAck, error) {
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

func (cc *cacheClientImpl) handleGetResponse(getResp *pb.GetResponse, err error) (*pb.Put, *pb.OwnerChanged, error) {
	switch {
	case err != nil:
		log.Errorf("%v", err)
		return nil, nil, err
	case getResp.GetAck() != nil:
		// TODO -- solve this better :/
		// I don't like the way I'm (mis-)using the error for this non-error case
		return nil, nil, errors.New("response was ack")
	case getResp.GetPut() != nil:
		return getResp.GetPut(), nil, nil
	case getResp.GetOwnerChanged() != nil:
		return nil, getResp.GetOwnerChanged(), nil
	}

	return nil, nil, errors.New("Unknown response!")
}

func (cc *cacheClientImpl) handleGetsResponse(getsResp *pb.GetsResponse, err error) (*pb.Puts, *pb.OwnerChanged, error) {
	switch {
	case err != nil:
		log.Errorf("%v", err)
		return nil, nil, err
	case getsResp.GetAck() != nil:
		// TODO -- solve this better :/
		// I don't like the way I'm (mis-)using the error for this non-error case
		return nil, nil, errors.New("response was ack")
	case getsResp.GetPuts() != nil:
		return getsResp.GetPuts(), nil, nil
	case getsResp.GetOwnerChanged() != nil:
		return nil, getsResp.GetOwnerChanged(), nil
	}

	return nil, nil, errors.New("Unknown response!")
}

func (cc *cacheClientImpl) handleGetxResponse(getxResp *pb.GetxResponse, err error) (*pb.Putx, *pb.OwnerChanged, error) {
	switch {
	case err != nil:
		log.Errorf("%v", err)
		return nil, nil, err
	case getxResp.GetAck() != nil:
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
