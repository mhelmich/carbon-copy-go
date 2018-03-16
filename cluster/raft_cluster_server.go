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
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/mhelmich/carbon-copy-go/pb"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
)

type raftServiceImpl struct {
	r          *raft.Raft
	raftNodeId string
}

func (rcs *raftServiceImpl) JoinRaftCluster(ctx context.Context, req *pb.RaftJoinRequest) (*pb.RaftJoinResponse, error) {
	logger := rcs.setupLogger()
	if rcs.r.State() == raft.Leader {
		addr := fmt.Sprintf("%s:%d", req.Host, req.Port)
		logger.Infof("Adding new peer with id: %s addr: %s", req.Id, addr)
		f := rcs.r.AddVoter(raft.ServerID(req.Id), raft.ServerAddress(addr), 0, raftTimeout)
		err := f.Error()

		if err != nil {
			logger.Infof("Couldn't add peer %s: %s", addr, err)
		}

		return &pb.RaftJoinResponse{
			Ok: err == nil,
		}, nil
	} else {
		leaderAddr := rcs.r.Leader()
		logger.Infof("Leader address: %s", leaderAddr)
		tokens := strings.Split(string(leaderAddr), ":")
		if len(tokens) >= 1 {
			host := tokens[0]
			logger.Infof("Hostname: %s", host)
		} else {
			logger.Warnf("Leader address looks weird: %s", leaderAddr)
		}
	}

	// by default we report unsuccessful processing of a join request
	return &pb.RaftJoinResponse{
		Ok: false,
	}, nil
}

func (rcs *raftServiceImpl) Get(ctx context.Context, req *pb.GetReq) (*pb.GetResp, error) {
	return nil, nil
}

func (rcs *raftServiceImpl) Set(ctx context.Context, req *pb.SetReq) (*pb.SetResp, error) {
	return nil, nil
}

func (rcs *raftServiceImpl) Delete(ctx context.Context, req *pb.DeleteReq) (*pb.DeleteResp, error) {
	return nil, nil
}

func (rcs *raftServiceImpl) setupLogger() *log.Entry {
	hn, _ := os.Hostname()
	return log.WithFields(log.Fields{
		"hostname":   hn,
		"raftState":  rcs.r.State().String(),
		"raft":       rcs.r.String(),
		"raftNodeId": rcs.raftNodeId,
	})
}
