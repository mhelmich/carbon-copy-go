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
	// "github.com/mhelmich/carbon-copy-go/pb"
	"carbon-grid-go/pb"
	"context"
	"crypto/rand"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/oklog/ulid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
	"os"
	"path/filepath"
	"time"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
	raftDbFileName      = "raft.db"
)

func createNewConsensusStore(config ConsensusStoreConfig) (*consensusStoreImpl, error) {
	raftNodeId := ulid.MustNew(ulid.Now(), rand.Reader)
	// creating the actual raft instance
	r, raftFsm, err := createRaft(config, raftNodeId.String())
	if err != nil {
		return nil, err
	}

	// create the service providing non-consensus nodes with values
	grpcServer, err := createValueService(config, r)
	if err != nil {
		return nil, err
	}

	return &consensusStoreImpl{
		raft:            r,
		raftFsm:         raftFsm,
		raftValueServer: grpcServer,
	}, nil
}

// see this example or rather reference usage
// https://github.com/otoolep/hraftd/blob/master/store/store.go

type consensusStoreImpl struct {
	logger          log.Logger
	raft            *raft.Raft
	raftFsm         *fsm
	raftValueServer *grpc.Server
}

func createRaft(config ConsensusStoreConfig, raftNodeId string) (*raft.Raft, *fsm, error) {
	// Setup Raft configuration.
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(raftNodeId)
	raftConfig.StartAsLeader = config.Peers == nil || len(config.Peers) == 0

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", config.RaftPort))
	if err != nil {
		return nil, nil, err
	}

	transport, err := raft.NewTCPTransport(fmt.Sprintf(":%d", config.RaftPort), addr, 3, raftTimeout, os.Stderr)
	if err != nil {
		return nil, nil, err
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(config.RaftStoreDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return nil, nil, fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	if err := os.MkdirAll(config.RaftStoreDir, 0755); err != nil {
		return nil, nil, fmt.Errorf("couldn't create dirs: %s", err)
	}
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(config.RaftStoreDir, raftDbFileName))
	if err != nil {
		return nil, nil, fmt.Errorf("new bolt store: %s", err)
	}

	fsm := &fsm{}

	// Instantiate the Raft systems.
	r, err := raft.NewRaft(raftConfig, fsm, logStore, logStore, snapshots, transport)
	return r, fsm, err
}

func createValueService(config ConsensusStoreConfig, r *raft.Raft) (*grpc.Server, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.ServicePort))
	if err != nil {
		return nil, err
	}

	for _, peer := range config.Peers {
		conn, err := grpc.Dial(peer, grpc.WithInsecure())
		if err != nil {
			log.Warnf("Couldnt' connect to %s: %s", peer, err)
			continue
		}

		client := pb.NewRaftClusterClient(conn)
		joinReq := &pb.RaftJoinRequest{}
		joinResp, err := client.JoinRaftCluster(context.Background(), joinReq)
		if err == nil && joinResp.Ok {
			break
		}
	}

	grpcServer := grpc.NewServer()
	raftServer := &raftClusterServerImpl{}

	pb.RegisterRaftClusterServer(grpcServer, raftServer)
	go grpcServer.Serve(lis)
	return grpcServer, nil
}

func (cs *consensusStoreImpl) Get(key string) (string, error) {
	cs.raftFsm.mutex.Lock()
	defer cs.raftFsm.mutex.Unlock()
	return cs.raftFsm.state[key], nil
}

func (cs *consensusStoreImpl) Set(key string, value string) error {
	cmd := &pb.RaftCommand{
		Cmd:   pb.RaftOps_Set,
		Key:   key,
		Value: value,
	}

	return cs.raftApply(cmd)
}

func (cs *consensusStoreImpl) Delete(key string) error {
	cmd := &pb.RaftCommand{
		Cmd: pb.RaftOps_Delete,
		Key: key,
	}

	return cs.raftApply(cmd)
}

func (cs *consensusStoreImpl) raftApply(cmd *pb.RaftCommand) error {
	if cs.raft.State() != raft.Leader {
		return fmt.Errorf("I'm not the leader")
	}

	buf, err := proto.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("Can't marshall proto: %s", err)
	}

	f := cs.raft.Apply(buf, raftTimeout)
	return f.Error()
}

func (cs *consensusStoreImpl) Close() error {
	f := cs.raft.Shutdown()
	return f.Error()
}
