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
	"fmt"
	"github.com/hashicorp/serf/serf"
	log "github.com/sirupsen/logrus"
	golanglog "log"
	"os"
)

const (
	serfEventChannelBufferSize = 256

	serfMDKeySerfAddr        = "serf_addr"
	serfMDKeyRaftAddr        = "raft_addr"
	serfMDKeyRaftServiceAddr = "raft_service_addr"
	serfMDKeyRaftRole        = "raft_role"
	serfMDKeyGridAddr        = "grid_addr"

	raftRoleLeader   = "l"
	raftRoleVoter    = "v"
	raftRoleNonvoter = "n"
	raftRoleNone     = "x"
)

func createSerf(config clusterConfig) (*membership, error) {
	serfConfig := serf.DefaultConfig()
	serfConfig.Logger = golanglog.New(config.logger.Writer(), "serf ", 0)
	// it's important that this channel never blocks
	// if it blocks, the sender will block and therefore stop applying log entries
	// which means we're not up to date with the current cluster state anymore
	serfEventCh := make(chan serf.Event, serfEventChannelBufferSize)
	serfConfig.EventCh = serfEventCh
	serfConfig.NodeName = config.nodeId
	serfConfig.EnableNameConflictResolution = false
	serfConfig.MemberlistConfig.BindAddr = config.hostname
	serfConfig.MemberlistConfig.BindPort = config.SerfPort

	if config.SerfSnapshotPath != "" {
		if err := os.MkdirAll(config.SerfSnapshotPath, 0755); err != nil {
			return nil, fmt.Errorf("couldn't create dirs: %s", err)
		}
		serfConfig.SnapshotPath = config.SerfSnapshotPath
	} else {
		config.logger.Warn("Starting serf without a persistent snapshot!")
	}

	serfConfig.Tags = make(map[string]string)
	serfConfig.Tags["role"] = "carbon-copy"
	serfConfig.Tags[serfMDKeySerfAddr] = fmt.Sprintf("%s:%d", config.hostname, config.SerfPort)
	serfConfig.Tags[serfMDKeyRaftAddr] = fmt.Sprintf("%s:%d", config.hostname, config.RaftPort)
	serfConfig.Tags[serfMDKeyRaftServiceAddr] = fmt.Sprintf("%s:%d", config.hostname, config.RaftServicePort)
	serfConfig.Tags[serfMDKeyRaftRole] = raftRoleNone

	surf, err := serf.Create(serfConfig)
	if err != nil {
		return nil, err
	}

	// if we have peers, let's join the cluster
	if config.Peers != nil && len(config.Peers) > 0 {
		config.logger.Infof("Peers to contact: %v", config.Peers)
		numContactedNodes, err := surf.Join(config.Peers, true)

		if err != nil {
			return nil, fmt.Errorf("Can't connect to serf nodes: %s", err.Error())
		} else if numContactedNodes == 0 {
			return nil, fmt.Errorf("Wasn't able to connect to any serf node: %v", config.Peers)
		} else {
			config.logger.Infof("Contacted %d serf nodes! List: %v", numContactedNodes, config.Peers)
		}
	} else {
		config.logger.Info("No peers defined - starting a brandnew cluster!")
	}

	nodeJoined := make(chan string, serfEventChannelBufferSize)
	nodeLeft := make(chan string, serfEventChannelBufferSize)

	m := &membership{
		serf:         surf,
		logger:       config.logger,
		clusterState: newMembershipState(config.logger),
		nodeJoined:   nodeJoined,
		nodeLeft:     nodeLeft,
	}

	go m.handleSerfEvents(serfEventCh, nodeJoined, nodeLeft, nodeUpdated)
	return m, nil
}

type membership struct {
	serf         *serf.Serf
	logger       *log.Entry
	clusterState *membershipState

	nodeJoined chan<- string
	nodeLeft   chan<- string
}

func (m *membership) handleSerfEvents(ch <-chan serf.Event, nodeJoined chan<- string, nodeLeft chan<- string) {
	for { // ever...
		select {
		case e := <-ch:
			if e == nil {
				// seems the channel was closed
				// let's stop this go routine
				break
			}

			switch e.EventType() {
			case serf.EventMemberJoin, serf.EventMemberUpdate:
				m.handleMemberJoinEvent(e.(serf.MemberEvent), nodeJoined)
			case serf.EventMemberLeave, serf.EventMemberFailed:
				m.handleMemberLeaveEvent(e.(serf.MemberEvent), nodeLeft)
			}
		case <-m.serf.ShutdownCh():
			return
		}
	}
}

func (m *membership) handleMemberJoinEvent(me serf.MemberEvent, nodeJoined chan<- string) {
	for _, item := range me.Members {
		m.clusterState.updateMember(item.Name, item.Tags)
		nodeJoined <- item.Name
	}
}

func (m *membership) handleMemberLeaveEvent(me serf.MemberEvent, nodeLeft chan<- string) {
	for _, item := range me.Members {
		m.clusterState.removeMember(item.Name)
		nodeLeft <- item.Name
	}
}

func (m *membership) getNodeById(nodeId string) (map[string]string, bool) {
	// v, ok := m.currentCluster[nodeId]
	// return v, ok
	return m.clusterState.getMemberById(nodeId)
}

func (m *membership) updateMyRaftStatus(raftRole string) {
	tags := make(map[string]string)
	tags[serfMDKeyRaftRole] = raftRole
	// this will update the nodes metadata and broadcast it out
	// blocks until broadcasting was successful or timed out
	m.serf.SetTags(tags)
}

func (m *membership) updateRaftTag(newTags map[string]string) {
	// this will update the nodes metadata and broadcast it out
	// blocks until broadcasting was successful or timed out
	m.serf.SetTags(newTags)
}

// this is only used for testing right now
func (m *membership) getClusterSize() int {
	return m.clusterState.getNumMembers()
}

func (m *membership) close() {
	m.serf.Leave()
	m.serf.Shutdown()
}
