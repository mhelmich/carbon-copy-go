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
	// "github.com/mhelmich/carbon-copy-go/pb"
	"carbon-grid-go/pb"
	"google.golang.org/grpc"
)

type raftClusterClientImpl struct{}

func (rcc *raftClusterClientImpl) JoinRaftCluster(ctx context.Context, in *pb.RaftJoinRequest, opts ...grpc.CallOption) (*pb.RaftJoinResponse, error) {
	return nil, nil
}

func (rcc *raftClusterClientImpl) Get(ctx context.Context, in *pb.GetReq, opts ...grpc.CallOption) (*pb.GetResp, error) {
	return nil, nil
}

func (rcc *raftClusterClientImpl) Set(ctx context.Context, in *pb.SetReq, opts ...grpc.CallOption) (*pb.SetResp, error) {
	return nil, nil
}

func (rcc *raftClusterClientImpl) Delete(ctx context.Context, in *pb.DeleteReq, opts ...grpc.CallOption) (*pb.DeleteResp, error) {
	return nil, nil
}
