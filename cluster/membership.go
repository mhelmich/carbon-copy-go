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
	// golanglog "log"
	"strconv"
)

const (
	serfEventChannelBufferSize = 256

	serfMDKeyHost            = "host"
	serfMDKeySerfPort        = "serf_port"
	serfMDKeyRaftPort        = "raft_port"
	serfMDKeyRaftServicePort = "raft_service_port"
	serfMDKeyRaftRole        = "raft_role"
	serfMDKeyGridPort        = "grid_port"

	// TODO: this can be changed to use ints instead
	raftRoleLeader   = "l"
	raftRoleVoter    = "v"
	raftRoleNonvoter = "n"
	raftRoleNone     = "x"
)

func createNewMembership(config clusterConfig) (*membership, error) {
	serfConfig := serf.DefaultConfig()
	// serfConfig.Logger = golanglog.New(config.logger.Writer(), "serf ", 0)
	// it's important that this channel never blocks
	// if it blocks, the sender will block and therefore stop applying log entries
	// which means we're not up to date with the current cluster state anymore
	serfEventCh := make(chan serf.Event, serfEventChannelBufferSize)
	serfConfig.EventCh = serfEventCh
	serfConfig.NodeName = config.nodeId
	serfConfig.EnableNameConflictResolution = false
	serfConfig.MemberlistConfig.BindAddr = config.hostname
	serfConfig.MemberlistConfig.BindPort = config.SerfPort

	if config.SerfSnapshotPath != "" && config.isDevMode == false {
		serfConfig.SnapshotPath = config.SerfSnapshotPath
	} else {
		config.logger.Warn("Starting serf without a persistent snapshot!")
	}

	serfConfig.Tags = make(map[string]string)
	serfConfig.Tags["role"] = "carbon-copy"
	serfConfig.Tags[serfMDKeyHost] = config.hostname
	serfConfig.Tags[serfMDKeySerfPort] = strconv.Itoa(config.SerfPort)
	serfConfig.Tags[serfMDKeyRaftPort] = strconv.Itoa(config.RaftPort)
	serfConfig.Tags[serfMDKeyRaftServicePort] = strconv.Itoa(config.RaftServicePort)
	serfConfig.Tags[serfMDKeyGridPort] = strconv.Itoa(0)

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

	memberJoined := make(chan string)
	memberLeft := make(chan string)

	m := &membership{
		serf:            surf,
		logger:          config.logger,
		membershipState: newMembershipState(config.logger),
		memberJoined:    memberJoined,
		memberLeft:      memberLeft,
		config:          config,
	}

	go m.handleSerfEvents(serfEventCh, memberJoined, memberLeft)
	return m, nil
}

type membership struct {
	serf            *serf.Serf
	logger          *log.Entry
	membershipState *membershipState
	config          clusterConfig

	memberJoined <-chan string
	memberLeft   <-chan string
}

func (m *membership) handleSerfEvents(ch <-chan serf.Event, memberJoined chan<- string, memberLeft chan<- string) {
	for { // ever...
		select {
		case e := <-ch:
			if e == nil {
				// seems the channel was closed
				// let's stop this go routine
				return
			}

			//
			// Obviously we receive these events multiple times per actual event.
			// That means we need to do some sort of diffing.
			//
			switch e.EventType() {
			case serf.EventMemberJoin, serf.EventMemberUpdate:
				m.handleMemberJoinEvent(e.(serf.MemberEvent), memberJoined)
			case serf.EventMemberLeave, serf.EventMemberFailed:
				m.handleMemberLeaveEvent(e.(serf.MemberEvent), memberLeft)
			}
		case <-m.serf.ShutdownCh():
			return
		}
	}
}

func (m *membership) handleMemberJoinEvent(me serf.MemberEvent, memberJoined chan<- string) {
	for _, item := range me.Members {
		updated := m.membershipState.updateMember(item.Name, item.Tags)
		if updated {
			memberJoined <- item.Name
		}
	}
}

func (m *membership) handleMemberLeaveEvent(me serf.MemberEvent, memberLeft chan<- string) {
	for _, item := range me.Members {
		removed := m.membershipState.removeMember(item.Name)
		if removed {
			memberLeft <- item.Name
		}
	}
}

func (m *membership) getRaftLeaderTags() (map[string]string, bool) {
	return m.membershipState.getMemberById(m.membershipState.raftLeader)
}

func (m *membership) getNodeById(nodeId string) (map[string]string, bool) {
	return m.membershipState.getMemberById(nodeId)
}

func (m *membership) updateRaftTag(newTags map[string]string) error {
	// this will update the nodes metadata and broadcast it out
	// blocks until broadcasting was successful or timed out
	tags, ok := m.getNodeById(m.myNodeId())
	if ok {
		for k, v := range newTags {
			tags[k] = v
		}
	} else {
		tags = newTags
	}

	err := m.serf.SetTags(tags)
	m.membershipState.updateMember(m.myNodeId(), tags)
	return err
}

func (m *membership) myNodeId() string {
	return m.config.nodeId
}

func (m *membership) getClusterSize() int {
	return m.membershipState.getNumMembers()
}

func (m *membership) close() {
	m.serf.Leave()
	m.serf.Shutdown()
}
