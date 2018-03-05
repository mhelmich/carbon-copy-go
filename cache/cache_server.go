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
	"github.com/mhelmich/carbon-copy-go/pb"
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

	pb.RegisterCacheCommServer(grpcServer, cacheServer)
	go grpcServer.Serve(lis)
	return cacheServer, nil
}

type cacheServerImpl struct {
	myNodeId   int32
	grpcServer *grpc.Server
	store      *cacheLineStore
}

func (cs *cacheServerImpl) Get(ctx context.Context, req *pb.Get) (*pb.GetResponse, error) {
	cl, ok := cs.store.getCacheLineById(cacheLineIdFromProtoBuf(req.LineId))

	if ok {
		if cl.cacheLineState == pb.CacheLineState_Exclusive || cl.cacheLineState == pb.CacheLineState_Owned {
			put := &pb.Put{
				Error:    pb.CacheError_NoError,
				SenderId: cs.myNodeId,
				LineId:   cl.id.toProtoBuf(),
				Version:  int32(cl.version),
				Buffer:   cl.buffer,
			}

			resp := &pb.GetResponse{
				InnerMessage: &pb.GetResponse_Put{
					Put: put,
				},
			}

			log.Infof("Get request from %d for line %s fulfilled with %s", req.SenderId, cacheLineIdFromProtoBuf(req.LineId).string(), "put")
			return resp, nil
		} else {
			oc := &pb.OwnerChanged{
				SenderId:            cs.myNodeId,
				LineId:              req.LineId,
				NewOwnerId:          int32(cl.ownerId),
				OriginalMessageType: 0,
			}

			resp := &pb.GetResponse{
				InnerMessage: &pb.GetResponse_OwnerChanged{
					OwnerChanged: oc,
				},
			}

			log.Infof("Get request from %d for line %s fulfilled with %s", req.SenderId, cacheLineIdFromProtoBuf(req.LineId).string(), "owner_changed")
			return resp, nil
		}
	} else {
		ack := &pb.Ack{
			SenderId: cs.myNodeId,
			LineId:   req.LineId,
		}

		resp := &pb.GetResponse{
			InnerMessage: &pb.GetResponse_Ack{
				Ack: ack,
			},
		}

		log.Infof("Get request from %d for line %s fulfilled with %s", req.SenderId, cacheLineIdFromProtoBuf(req.LineId).string(), "ack")
		return resp, nil
	}
}

func (cs *cacheServerImpl) Gets(ctx context.Context, req *pb.Gets) (*pb.GetsResponse, error) {
	cl, ok := cs.store.getCacheLineById(cacheLineIdFromProtoBuf(req.LineId))

	if ok {
		switch cl.cacheLineState {
		case pb.CacheLineState_Exclusive:
			cl.lock()

			puts := &pb.Puts{
				Error:    pb.CacheError_NoError,
				SenderId: cs.myNodeId,
				LineId:   cl.id.toProtoBuf(),
				Version:  int32(cl.version),
				Sharers:  convertIntTo32Array(cl.sharers),
				Buffer:   cl.buffer,
			}

			resp := &pb.GetsResponse{
				InnerMessage: &pb.GetsResponse_Puts{
					Puts: puts,
				},
			}

			cl.cacheLineState = pb.CacheLineState_Owned
			cl.sharers = append(cl.sharers, int(req.SenderId))
			cl.unlock()
			return resp, nil

		case pb.CacheLineState_Owned:
			cl.lock()

			puts := &pb.Puts{
				Error:    pb.CacheError_NoError,
				SenderId: cs.myNodeId,
				LineId:   cl.id.toProtoBuf(),
				Version:  int32(cl.version),
				Sharers:  convertIntTo32Array(cl.sharers),
				Buffer:   cl.buffer,
			}

			resp := &pb.GetsResponse{
				InnerMessage: &pb.GetsResponse_Puts{
					Puts: puts,
				},
			}

			cl.sharers = append(cl.sharers, int(req.SenderId))
			cl.unlock()
			return resp, nil

		case pb.CacheLineState_Shared, pb.CacheLineState_Invalid:
			oc := &pb.OwnerChanged{
				SenderId:            cs.myNodeId,
				LineId:              req.LineId,
				NewOwnerId:          int32(cl.ownerId),
				OriginalMessageType: 0,
			}

			resp := &pb.GetsResponse{
				InnerMessage: &pb.GetsResponse_OwnerChanged{
					OwnerChanged: oc,
				},
			}

			return resp, nil

		default:
			return nil, errors.New(fmt.Sprintf("Cacheline %d is in invalid state %v", req.LineId, cl.cacheLineState))
		}
	} else {
		ack := &pb.Ack{
			SenderId: cs.myNodeId,
			LineId:   req.LineId,
		}

		resp := &pb.GetsResponse{
			InnerMessage: &pb.GetsResponse_Ack{
				Ack: ack,
			},
		}

		return resp, nil
	}
}

func (cs *cacheServerImpl) Getx(ctx context.Context, req *pb.Getx) (*pb.GetxResponse, error) {
	cl, ok := cs.store.getCacheLineById(cacheLineIdFromProtoBuf(req.LineId))

	if ok {
		if cl.cacheLineState == pb.CacheLineState_Exclusive || cl.cacheLineState == pb.CacheLineState_Owned {
			cl.lock()

			putx := &pb.Putx{
				Error:    pb.CacheError_NoError,
				SenderId: cs.myNodeId,
				LineId:   cl.id.toProtoBuf(),
				Version:  int32(cl.version),
				Sharers:  convertIntTo32Array(cl.sharers),
				Buffer:   cl.buffer,
			}

			resp := &pb.GetxResponse{
				InnerMessage: &pb.GetxResponse_Putx{
					Putx: putx,
				},
			}

			cl.cacheLineState = pb.CacheLineState_Invalid
			cl.sharers = nil
			cl.buffer = nil
			cl.ownerId = int(req.SenderId)
			cl.version = 0

			cl.unlock()
			return resp, nil
		} else {
			oc := &pb.OwnerChanged{
				SenderId:            cs.myNodeId,
				LineId:              req.LineId,
				NewOwnerId:          int32(cl.ownerId),
				OriginalMessageType: 0,
			}

			resp := &pb.GetxResponse{
				InnerMessage: &pb.GetxResponse_OwnerChanged{
					OwnerChanged: oc,
				},
			}

			log.Infof("Get request from %d for line %d fulfilled with %s", req.SenderId, cacheLineIdFromProtoBuf(req.LineId).string(), "owner_changed")
			return resp, nil
		}
	} else {
		ack := &pb.Ack{
			SenderId: cs.myNodeId,
			LineId:   req.LineId,
		}

		resp := &pb.GetxResponse{
			InnerMessage: &pb.GetxResponse_Ack{
				Ack: ack,
			},
		}

		log.Infof("Get request from %d for line %d fulfilled with %s", req.SenderId, cacheLineIdFromProtoBuf(req.LineId).string(), "ack")
		return resp, nil
	}
}

func (cs *cacheServerImpl) Invalidate(ctx context.Context, req *pb.Inv) (*pb.InvAck, error) {
	cl, ok := cs.store.getCacheLineById(cacheLineIdFromProtoBuf(req.LineId))
	if ok {
		cl.lock()
		cl.cacheLineState = pb.CacheLineState_Invalid
		cl.sharers = nil
		cl.buffer = nil
		cl.unlock()
	}
	return &pb.InvAck{
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
