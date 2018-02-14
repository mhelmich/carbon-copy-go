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
	"fmt"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	// "google.golang.org/grpc/codes"
	"errors"
	"net"
)

// SERVER IMPLEMENTATION

func createNewServer(myNodeId int, serverPort int, store *cacheLineStore) (*cacheServerImpl, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", serverPort))
	if err != nil {
		return nil, err
	}

	grpcServer := grpc.NewServer()
	cacheServer := &cacheServerImpl{
		myNodeId:   int32(myNodeId),
		grpcServer: grpcServer,
		store:      store,
	}

	RegisterCacheCommServer(grpcServer, cacheServer)
	go grpcServer.Serve(lis)
	return cacheServer, nil
}

type cacheServerImpl struct {
	myNodeId   int32
	grpcServer *grpc.Server
	store      *cacheLineStore
}

func (cs *cacheServerImpl) Get(ctx context.Context, req *Get) (*GetResponse, error) {
	cl, ok := cs.store.getCacheLineById(int(req.LineId))

	if ok {
		if cl.cacheLineState == CacheLineState_Exclusive || cl.cacheLineState == CacheLineState_Owned {
			put := &Put{
				Error:    CacheError_NoError,
				SenderId: cs.myNodeId,
				LineId:   int64(cl.id),
				Version:  int32(cl.version),
				Buffer:   cl.buffer,
			}

			resp := &GetResponse{
				InnerMessage: &GetResponse_Put{
					Put: put,
				},
			}

			log.Infof("Get request from %d for line %d fulfilled with %s", req.SenderId, req.LineId, "put")
			return resp, nil
		} else {
			oc := &OwnerChanged{
				SenderId:            cs.myNodeId,
				LineId:              req.LineId,
				NewOwnerId:          int32(cl.ownerId),
				OriginalMessageType: 0,
			}

			resp := &GetResponse{
				InnerMessage: &GetResponse_OwnerChanged{
					OwnerChanged: oc,
				},
			}

			log.Infof("Get request from %d for line %d fulfilled with %s", req.SenderId, req.LineId, "owner_changed")
			return resp, nil
		}
	} else {
		ack := &Ack{
			SenderId: cs.myNodeId,
			LineId:   req.LineId,
		}

		resp := &GetResponse{
			InnerMessage: &GetResponse_Ack{
				Ack: ack,
			},
		}

		log.Infof("Get request from %d for line %d fulfilled with %s", req.SenderId, req.LineId, "ack")
		return resp, nil
	}
}

func (cs *cacheServerImpl) Gets(ctx context.Context, req *Gets) (*GetsResponse, error) {
	cl, ok := cs.store.getCacheLineById(int(req.LineId))

	if ok {
		switch cl.cacheLineState {
		case CacheLineState_Exclusive:
			cl.lock()

			puts := &Puts{
				Error:    CacheError_NoError,
				SenderId: cs.myNodeId,
				LineId:   int64(cl.id),
				Version:  int32(cl.version),
				Sharers:  convertIntTo32Array(cl.sharers),
				Buffer:   cl.buffer,
			}

			resp := &GetsResponse{
				InnerMessage: &GetsResponse_Puts{
					Puts: puts,
				},
			}

			cl.cacheLineState = CacheLineState_Owned
			cl.sharers = append(cl.sharers, int(req.SenderId))
			cl.unlock()
			return resp, nil

		case CacheLineState_Owned:
			cl.lock()

			puts := &Puts{
				Error:    CacheError_NoError,
				SenderId: cs.myNodeId,
				LineId:   int64(cl.id),
				Version:  int32(cl.version),
				Sharers:  convertIntTo32Array(cl.sharers),
				Buffer:   cl.buffer,
			}

			resp := &GetsResponse{
				InnerMessage: &GetsResponse_Puts{
					Puts: puts,
				},
			}

			cl.sharers = append(cl.sharers, int(req.SenderId))
			cl.unlock()
			return resp, nil

		case CacheLineState_Shared, CacheLineState_Invalid:
			oc := &OwnerChanged{
				SenderId:            cs.myNodeId,
				LineId:              req.LineId,
				NewOwnerId:          int32(cl.ownerId),
				OriginalMessageType: 0,
			}

			resp := &GetsResponse{
				InnerMessage: &GetsResponse_OwnerChanged{
					OwnerChanged: oc,
				},
			}

			return resp, nil

		default:
			return nil, errors.New(fmt.Sprintf("Cacheline %d is in invalid state %v", req.LineId, cl.cacheLineState))
		}
	} else {
		ack := &Ack{
			SenderId: cs.myNodeId,
			LineId:   req.LineId,
		}

		resp := &GetsResponse{
			InnerMessage: &GetsResponse_Ack{
				Ack: ack,
			},
		}

		return resp, nil
	}
}

func (cs *cacheServerImpl) Getx(ctx context.Context, req *Getx) (*GetxResponse, error) {
	cl, ok := cs.store.getCacheLineById(int(req.LineId))

	if ok {
		if cl.cacheLineState == CacheLineState_Exclusive || cl.cacheLineState == CacheLineState_Owned {
			cl.lock()

			putx := &Putx{
				Error:    CacheError_NoError,
				SenderId: cs.myNodeId,
				LineId:   int64(cl.id),
				Version:  int32(cl.version),
				Sharers:  convertIntTo32Array(cl.sharers),
				Buffer:   cl.buffer,
			}

			resp := &GetxResponse{
				InnerMessage: &GetxResponse_Putx{
					Putx: putx,
				},
			}

			cl.cacheLineState = CacheLineState_Invalid
			cl.sharers = nil
			cl.buffer = nil
			cl.ownerId = int(req.SenderId)
			cl.version = 0

			cl.unlock()
			return resp, nil
		} else {
			oc := &OwnerChanged{
				SenderId:            cs.myNodeId,
				LineId:              req.LineId,
				NewOwnerId:          int32(cl.ownerId),
				OriginalMessageType: 0,
			}

			resp := &GetxResponse{
				InnerMessage: &GetxResponse_OwnerChanged{
					OwnerChanged: oc,
				},
			}

			log.Infof("Get request from %d for line %d fulfilled with %s", req.SenderId, req.LineId, "owner_changed")
			return resp, nil
		}
	} else {
		ack := &Ack{
			SenderId: cs.myNodeId,
			LineId:   req.LineId,
		}

		resp := &GetxResponse{
			InnerMessage: &GetxResponse_Ack{
				Ack: ack,
			},
		}

		log.Infof("Get request from %d for line %d fulfilled with %s", req.SenderId, req.LineId, "ack")
		return resp, nil
	}
}

func (cs *cacheServerImpl) Invalidate(ctx context.Context, req *Inv) (*InvAck, error) {
	log.Info("Answering invalidate call")
	cl, ok := cs.store.getCacheLineById(int(req.LineId))
	if ok {
		cl.lock()
		cl.cacheLineState = CacheLineState_Invalid
		cl.sharers = nil
		cl.buffer = nil
		cl.unlock()
	}
	return &InvAck{
		SenderId: cs.myNodeId,
		LineId:   req.LineId,
	}, nil
}

func (cs *cacheServerImpl) Stop() {
	cs.grpcServer.GracefulStop()
}

func convertIntTo32Array(in []int) []int32 {
	out := make([]int32, len(in))
	for idx, val := range in {
		out[idx] = int32(val)
	}
	return out
}
