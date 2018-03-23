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
	"github.com/mhelmich/carbon-copy-go/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"sync"
)

func newConsensusStoreProxy(config ClusterConfig, store *consensusStoreImpl, raftLeaderAddrChan <-chan string) (*consensusStoreProxy, error) {
	proxy := &consensusStoreProxy{
		store:              store,
		connectionMutex:    sync.RWMutex{},
		raftLeaderAddrChan: raftLeaderAddrChan,
		logger:             config.logger,
		// these two are being set in updateLeaderConnection()
		// leaderClient:       leaderClient,
		// leaderConnection:   leaderConnection,
	}

	// we do it once to complete setting up the object
	proxy.updateLeaderConnection()
	// then we spin up a co routine to keep the connection
	// to the leader up to date
	go proxy.updateLeaderConnection()

	return proxy, nil
}

type consensusStoreProxy struct {
	store              *consensusStoreImpl
	leaderConnection   *grpc.ClientConn
	connectionMutex    sync.RWMutex
	raftLeaderAddrChan <-chan string
	logger             *log.Entry
}

func (csp *consensusStoreProxy) updateLeaderConnection() error {
	leaderAddr := <-csp.raftLeaderAddrChan
	leaderConnection, err := grpc.Dial(leaderAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}

	var tmpConn *grpc.ClientConn

	csp.connectionMutex.Lock()
	tmpConn = csp.leaderConnection
	csp.leaderConnection = leaderConnection
	csp.connectionMutex.Unlock()

	if tmpConn != nil {
		tmpConn.Close()
	}
	return nil
}

func (csp *consensusStoreProxy) acquireUniqueShortNodeId() (int, error) {
	return 0, nil
}

func (csp *consensusStoreProxy) get(key string) ([]byte, error) {
	return csp.store.get(key)
}

func (csp *consensusStoreProxy) consistentGet(key string) ([]byte, error) {
	if csp.store.isRaftLeader() {
		return csp.store.get(key)
	} else {
		ctx := context.Background()
		req := &pb.GetReq{
			Key: key,
		}

		leaderClient := pb.NewRaftServiceClient(csp.leaderConnection)
		resp, err := leaderClient.ConsistentGet(ctx, req)

		if err != nil {
			return nil, err
		} else if resp.Error != pb.RaftServiceError_NoRaftError {
			return nil, fmt.Errorf("consistent get: %s", resp.Error.String())
		} else {
			return resp.Value, nil
		}
	}
}

func (csp *consensusStoreProxy) set(key string, value []byte) (bool, error) {
	if csp.store.isRaftLeader() {
		err := csp.store.set(key, value)
		return false, err
	} else {
		ctx := context.Background()
		req := &pb.SetReq{
			Key:   key,
			Value: value,
		}

		leaderClient := pb.NewRaftServiceClient(csp.leaderConnection)
		resp, err := leaderClient.Set(ctx, req)

		csp.logger.Infof("Got response %s", resp.String())

		if err != nil {
			csp.logger.Errorf("Error sending set request: %s", err.Error())
			return false, err
		} else if resp.Error != pb.RaftServiceError_NoRaftError {
			return false, fmt.Errorf("set failed: %s", resp.Error.String())
		} else {
			return resp.Created, nil
		}
	}
}

func (csp *consensusStoreProxy) delete(key string) (bool, error) {
	if csp.store.isRaftLeader() {
		deleted, err := csp.store.delete(key)
		return deleted, err
	} else {
		ctx := context.Background()
		req := &pb.DeleteReq{
			Key: key,
		}

		leaderClient := pb.NewRaftServiceClient(csp.leaderConnection)
		resp, err := leaderClient.Delete(ctx, req)

		if err != nil {
			return false, err
		} else if resp.Error != pb.RaftServiceError_NoRaftError {
			return false, fmt.Errorf("delete failed: %s", resp.Error.String())
		} else {
			return resp.Deleted, nil
		}
	}
}

func (csp *consensusStoreProxy) close() error {
	return csp.leaderConnection.Close()
}
