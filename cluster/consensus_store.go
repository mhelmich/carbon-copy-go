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
	"crypto/rand"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/oklog/ulid"
	log "github.com/sirupsen/logrus"
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

func createNewConsensusStore(nodeConfig ConsensusStoreConfig) (*consensusStoreImpl, error) {
	nodeId := ulid.MustNew(ulid.Now(), rand.Reader)

	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeId.String())

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", nodeConfig.bindAddr)
	if err != nil {
		return nil, err
	}

	transport, err := raft.NewTCPTransport(nodeConfig.bindAddr, addr, 3, raftTimeout, os.Stderr)
	if err != nil {
		return nil, err
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(nodeConfig.raftStoreDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	if err := os.MkdirAll(nodeConfig.raftStoreDir, 0755); err != nil {
		return nil, fmt.Errorf("couldn't create dirs: %s", err)
	}
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(nodeConfig.raftStoreDir, raftDbFileName))
	if err != nil {
		return nil, fmt.Errorf("new bolt store: %s", err)
	}

	fsm := &fsm{}

	// Instantiate the Raft systems.
	raft, err := raft.NewRaft(config, fsm, logStore, logStore, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("new raft: %s", err)
	}

	return &consensusStoreImpl{
		raft:          raft,
		raftStore:     logStore,
		raftTransport: transport,
		raftFsm:       fsm,
	}, nil
}

// see this example or rather reference usage
// https://github.com/otoolep/hraftd/blob/master/store/store.go

type consensusStoreImpl struct {
	logger        log.Logger
	raft          *raft.Raft
	raftStore     *raftboltdb.BoltStore
	raftTransport *raft.NetworkTransport
	raftFsm       *fsm
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
