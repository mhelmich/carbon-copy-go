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
	"fmt"
	"github.com/hashicorp/raft"
	log "github.com/sirupsen/logrus"
	"strings"
)

type raftClusterServerImpl struct {
	r *raft.Raft
}

func (rcs *raftClusterServerImpl) JoinRaftCluster(ctx context.Context, req *pb.RaftJoinRequest) (*pb.RaftJoinResponse, error) {
	if rcs.r.State() == raft.Leader {
		f := rcs.r.AddVoter(raft.ServerID(req.Id), raft.ServerAddress(fmt.Sprint("%s:%d", req.Host, req.Port)), 0, 0)
		err := f.Error()

		return &pb.RaftJoinResponse{
			Ok: err == nil,
		}, nil
	} else {
		leaderAddr := rcs.r.Leader()
		log.Infof("Leader address: %s", leaderAddr)
		tokens := strings.Split(string(leaderAddr), ":")
		if len(tokens) >= 1 {
			host := tokens[0]
			log.Infof("Hostname: %s", host)
		} else {
			log.Warnf("Leader address looks weird: %s", leaderAddr)
		}
	}
	return nil, nil
}

func (rcs *raftClusterServerImpl) Get(ctx context.Context, req *pb.GetReq) (*pb.GetResp, error) {
	return nil, nil
}

func (rcs *raftClusterServerImpl) Set(ctx context.Context, req *pb.SetReq) (*pb.SetResp, error) {
	return nil, nil
}

func (rcs *raftClusterServerImpl) Delete(ctx context.Context, req *pb.DeleteReq) (*pb.DeleteResp, error) {
	return nil, nil
}
