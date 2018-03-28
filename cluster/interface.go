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
	log "github.com/sirupsen/logrus"
)

type NodeConnectionInfo struct {
	nodeId      int
	nodeAddress string
}

type ClusterConfig struct {
	RaftPort         int
	RaftStoreDir     string
	Peers            []string
	hostname         string
	RaftServicePort  int
	SerfPort         int
	GridPort         int
	SerfSnapshotPath string
	longMemberId     string
	raftNotifyCh     chan bool
	logger           *log.Entry
	NumRaftVoters    int
	isDevMode        bool
}

type Cluster interface {
	GetMyShortMemberId() int
	GetNodeConnectionInfoUpdates() (<-chan []*NodeConnectionInfo, error)
	Close() error
}

func NewCluster(config ClusterConfig) (Cluster, error) {
	return createNewCluster(config)
}

type consensusStore interface {
	get(key string) ([]byte, error)
	set(key string, value []byte) error
	delete(key string) error
	close() error
}
