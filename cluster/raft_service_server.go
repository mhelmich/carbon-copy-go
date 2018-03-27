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

package cluster

import (
	"context"

	"github.com/mhelmich/carbon-copy-go/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type raftServiceImpl struct {
	localConsensusStore *consensusStoreImpl
	grpcServer          *grpc.Server
	logger              *log.Entry
}

func (rs *raftServiceImpl) Get(ctx context.Context, req *pb.GetReq) (*pb.GetResp, error) {
	bites, err := rs.localConsensusStore.get(req.Key)
	if err != nil {
		return nil, err
	}

	return &pb.GetResp{
		Error: pb.RaftServiceError_NoRaftError,
		Value: bites,
	}, nil
}

func (rs *raftServiceImpl) Set(ctx context.Context, req *pb.SetReq) (*pb.SetResp, error) {
	if !rs.localConsensusStore.isRaftLeader() {
		return &pb.SetResp{
			Error:   pb.RaftServiceError_NotLeaderRaftError,
			Created: false,
		}, nil
	}

	existed, err := rs.localConsensusStore.set(req.Key, req.Value)
	if err != nil {
		return nil, err
	}

	return &pb.SetResp{
		Error:   pb.RaftServiceError_NoRaftError,
		Created: !existed,
	}, nil
}

func (rs *raftServiceImpl) Delete(ctx context.Context, req *pb.DeleteReq) (*pb.DeleteResp, error) {
	if !rs.localConsensusStore.isRaftLeader() {
		return &pb.DeleteResp{
			Error:   pb.RaftServiceError_NotLeaderRaftError,
			Deleted: false,
		}, nil
	}

	del, err := rs.localConsensusStore.delete(req.Key)
	if err != nil {
		return nil, err
	}

	return &pb.DeleteResp{
		Error:   pb.RaftServiceError_NoRaftError,
		Deleted: del,
	}, nil
}

func (rs *raftServiceImpl) ConsistentGet(ctx context.Context, req *pb.GetReq) (*pb.GetResp, error) {
	if !rs.localConsensusStore.isRaftLeader() {
		return &pb.GetResp{
			Error: pb.RaftServiceError_NotLeaderRaftError,
			Value: nil,
		}, nil
	}

	return rs.Get(ctx, req)
}

func (rs *raftServiceImpl) close() {
	rs.grpcServer.Stop()
	rs.localConsensusStore = nil
}
