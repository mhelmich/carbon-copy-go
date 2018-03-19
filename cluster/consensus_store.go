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
	"crypto/rand"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/mhelmich/carbon-copy-go/pb"
	"github.com/oklog/ulid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	golanglog "log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
	raftDbFileName      = "raft.db"
	raftLogCacheSize    = 512
)

func createNewConsensusStore(config clusterConfig) (*consensusStoreImpl, error) {
	raftNodeId := ulid.MustNew(ulid.Now(), rand.Reader).String()
	config.nodeId = raftNodeId
	config.raftNotifyCh = make(chan bool, 16)
	hn, _ := os.Hostname()
	config.hostname = hn
	config.logger = log.WithFields(log.Fields{
		"raftNodeId": raftNodeId,
		"hostname":   config.hostname,
		"raftPort":   config.RaftPort,
	})

	// creating the actual raft instance
	r, raftFsm, err := createRaft(config)
	if err != nil {
		return nil, err
	}

	// create the service providing non-consensus nodes with values
	grpcServer, err := createRaftService(config, r, raftNodeId)
	if err != nil {
		return nil, err
	}

	consensusStore := &consensusStoreImpl{
		raft:            r,
		raftFsm:         raftFsm,
		raftValueServer: grpcServer,
		logger:          config.logger,
		raftNotifyCh:    config.raftNotifyCh,
	}

	return consensusStore, nil
}

// see this example or rather reference usage
// https://github.com/otoolep/hraftd/blob/master/store/store.go

type consensusStoreImpl struct {
	logger          *log.Entry
	raft            *raft.Raft
	raftFsm         *fsm
	raftValueServer *grpc.Server
	raftNotifyCh    <-chan bool
}

func createRaft(config clusterConfig) (*raft.Raft, *fsm, error) {
	var err error

	// setup Raft configuration
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.nodeId)
	raftConfig.Logger = golanglog.New(config.logger.Writer(), "raft ", 0)
	raftConfig.NotifyCh = config.raftNotifyCh

	localhost := fmt.Sprintf("%s:%d", config.hostname, config.RaftPort)
	// setup Raft communication
	addr, err := net.ResolveTCPAddr("tcp", localhost)
	if err != nil {
		return nil, nil, err
	}

	transport, err := raft.NewTCPTransport(localhost, addr, 3, raftTimeout, config.logger.Writer())
	if err != nil {
		return nil, nil, err
	}

	var snapshotStore raft.SnapshotStore
	var logStore raft.LogStore
	var stableStore raft.StableStore

	if config.isDevMode {
		logStore = raft.NewInmemStore()
		stableStore = raft.NewInmemStore()
		snapshotStore = raft.NewInmemSnapshotStore()
	} else {
		// create the snapshot store
		// this allows the Raft to truncate the log
		snapshotStore, err = raft.NewFileSnapshotStore(config.RaftStoreDir, retainSnapshotCount, config.logger.Writer())
		if err != nil {
			return nil, nil, fmt.Errorf("file snapshot store: %s", err)
		}

		// create the log store and stable store
		if err := os.MkdirAll(config.RaftStoreDir, 0755); err != nil {
			return nil, nil, fmt.Errorf("couldn't create dirs: %s", err)
		}
		// create a durable bolt store
		var boltStore *raftboltdb.BoltStore
		boltStore, err := raftboltdb.NewBoltStore(filepath.Join(config.RaftStoreDir, raftDbFileName))
		if err != nil {
			return nil, nil, fmt.Errorf("new bolt store: %s", err)
		}

		// boltstore implements all sorts of raft interfaces
		// we use it as a stable store and as backing for the log cache
		stableStore = boltStore
		logStore, err = raft.NewLogCache(raftLogCacheSize, boltStore)
		if err != nil {
			return nil, nil, fmt.Errorf("new log cache: %s", err)
		}
	}

	fsm := &fsm{
		logger: config.logger,
		state:  make(map[string][]byte),
		mutex:  sync.RWMutex{},
	}

	// instantiate the Raft systems
	newRaft, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, nil, err
	}

	// we all assume this is a brandnew cluster if no peers are being
	// supplied in the config
	if config.Peers == nil || len(config.Peers) == 0 {
		hasExistingState, _ := raft.HasExistingState(logStore, stableStore, snapshotStore)
		if !hasExistingState {
			config.logger.Infof("Bootstrapping cluster with %v and %v", raft.ServerAddress(localhost), raftConfig.LocalID)
			configuration := raft.Configuration{
				Servers: []raft.Server{
					{
						ID:      raftConfig.LocalID,
						Address: raft.ServerAddress(localhost),
					},
				},
			}

			f := newRaft.BootstrapCluster(configuration)
			err := f.Error()
			if err != nil {
				return nil, nil, err
			}
		} else {
			config.logger.Info("Raft has existing state. Bootstrapping new cluster interrupted.")
		}
	}

	return newRaft, fsm, nil
}

func createRaftService(config clusterConfig, r *raft.Raft, raftNodeId string) (*grpc.Server, error) {
	// if config.Peers != nil && len(config.Peers) > 0 {
	// 	joinedRaft := false
	// 	joinReq := &pb.RaftJoinRequest{
	// 		Host: config.hostname,
	// 		Port: int32(config.RaftPort),
	// 		Id:   raftNodeId,
	// 	}

	// 	for _, peer := range config.Peers {
	// 		config.logger.Infof("Connecting to peer %s", peer)
	// 		conn, err := grpc.Dial(peer, grpc.WithInsecure())
	// 		defer conn.Close()

	// 		if err == nil {
	// 			client := pb.NewRaftServiceClient(conn)
	// 			joinResp, err := client.JoinRaftCluster(context.Background(), joinReq)
	// 			if err == nil && joinResp.Ok {
	// 				config.logger.Infof("Asked %s to join raft answer: %t", peer, joinResp.Ok)
	// 				joinedRaft = true
	// 				break
	// 			} else {
	// 				config.logger.Warnf("Asked %s to join raft answer: %v", peer, joinResp.Ok)
	// 			}
	// 		} else {
	// 			config.logger.Warnf("Couldn't connect to %s: %s", peer, err)
	// 		}
	// 	}

	// 	// After we looped through all peers and we're not able to connect to the master,
	// 	// then we're dead in water.
	// 	if !joinedRaft {
	// 		return nil, fmt.Errorf("Wasn't able to talk to any of the peers %v", config.Peers)
	// 	}
	// }

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", config.hostname, config.RaftServicePort))
	if err != nil {
		return nil, err
	}

	grpcServer := grpc.NewServer()
	raftServer := &raftServiceImpl{
		raft:       r,
		raftNodeId: raftNodeId,
	}

	pb.RegisterRaftServiceServer(grpcServer, raftServer)
	go grpcServer.Serve(lis)
	return grpcServer, nil
}

func (cs *consensusStoreImpl) AcquireUniqueShortNodeId() (int, error) {
	return -1, nil
}

func (cs *consensusStoreImpl) ConsistentGet(key string) ([]byte, error) {
	cmd := &pb.RaftCommand{
		Cmd: &pb.RaftCommand_GetCmd{
			GetCmd: &pb.GetCommand{
				Key: key,
			},
		},
	}

	f := cs.raftApply(cmd)
	if f.Error() != nil {
		return nil, f.Error()
	} else {
		resp := f.Response().(raftApplyResponse)
		return resp.value, resp.err
	}
}

func (cs *consensusStoreImpl) Get(key string) ([]byte, error) {
	cs.raftFsm.mutex.RLock()
	defer cs.raftFsm.mutex.RUnlock()
	// this might be a stale read :/
	return cs.raftFsm.state[key], nil
}

func (cs *consensusStoreImpl) Set(key string, value []byte) error {
	cmd := &pb.RaftCommand{
		Cmd: &pb.RaftCommand_SetCmd{
			SetCmd: &pb.SetCommand{
				Key:   key,
				Value: value,
			},
		},
	}

	return cs.raftApply(cmd).Error()
}

func (cs *consensusStoreImpl) Delete(key string) error {
	cmd := &pb.RaftCommand{
		Cmd: &pb.RaftCommand_DeleteCmd{
			DeleteCmd: &pb.DeleteCommand{
				Key: key,
			},
		},
	}

	return cs.raftApply(cmd).Error()
}

func (cs *consensusStoreImpl) raftApply(cmd *pb.RaftCommand) raft.ApplyFuture {
	if cs.raft.State() != raft.Leader {
		return localApplyFuture{fmt.Errorf("I'm not the leader")}
	}

	buf, err := proto.Marshal(cmd)
	if err != nil {
		return localApplyFuture{fmt.Errorf("Can't marshall proto: %s", err)}
	}

	f := cs.raft.Apply(buf, raftTimeout)

	switch v := f.(type) {
	case raft.ApplyFuture:
		return v
	default:
		return localApplyFuture{}
	}
}

func (cs *consensusStoreImpl) isRaftLeader() bool {
	return cs.raft.State() == raft.Leader
}

func (cs *consensusStoreImpl) Close() error {
	f := cs.raft.Shutdown()
	cs.raftValueServer.Stop()
	return f.Error()
}

type localApplyFuture struct {
	err error
}

func (f localApplyFuture) Error() error {
	return f.err
}

func (f localApplyFuture) Response() interface{} {
	return nil
}

func (f localApplyFuture) Index() uint64 {
	return 0
}
