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

type EventType int

const (
	MemberJoined EventType = iota
	MemberLeft
)

type GridMemberConnectionEvent struct {
	Type              EventType
	ShortMemberId     int
	MemberGridAddress string
}

type ClusterConfig struct {
	// The port on which raft communicates.
	RaftPort int
	// The directory under which raft will write its persistent metadata.
	RaftStoreDir string
	// An array of string addresses indicating where the serf service runs.
	// One other peers is enough to bootstrap this member. However, multiple are
	// advisable for fault tolerance.
	Peers []string
	// The port on which the service operates.
	RaftServicePort int
	// The port on which serf operates. This address is supposed to be given
	// as peer information to bootstrap a new member.
	SerfPort int
	// Port on which the Grid service operates.
	GridPort int
	// Path under which serf writes its metadata.
	SerfSnapshotPath string
	// The number of raft voters that a cluster should have.
	// All other members will be added as Nonvoters.
	NumRaftVoters int
	// Will be set internally.
	hostname string
	// Will be set internally.
	longMemberId string
	// Will be set internally.
	raftNotifyCh chan bool
	// Will be set internally.
	logger *log.Entry
	// Will be set internally.
	isDevMode bool
}

// This is what a cluster looks like to the rest of the world.
type Cluster interface {
	// This method will return this members short id.
	// This id is guaranteed to be unique in the cluster.
	GetMyShortMemberId() int
	// Take care: Calling this method multiple times will return the same channel!!
	GetGridMemberChangeEvents() <-chan *GridMemberConnectionEvent
	// Closes this cluster.
	Close() error
}

// Creates a new cluster from the given config.
func NewCluster(config ClusterConfig) (Cluster, error) {
	return createNewCluster(config)
}

// internal interface
type consensusStore interface {
	get(key string) ([]byte, error)
	consistentGet(key string) ([]byte, error)
	set(key string, value []byte) (bool, error)
	delete(key string) (bool, error)
	addVoter(serverId string, serverAddress string) error
	addNonvoter(serverId string, serverAddress string) error
	removeVoter(serverId string, serverAddress string)
	addWatcher(prefix string, fn func(string, []byte))
	removeWatcher(prefix string)
	close() error
}
