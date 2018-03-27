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
	golanglog "log"
	"strconv"

	"github.com/hashicorp/serf/serf"
	log "github.com/sirupsen/logrus"
)

const (
	serfEventChannelBufferSize = 64

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
)

func createNewMembership(config ClusterConfig) (*membership, error) {
	serfConfig := serf.DefaultConfig()
	serfConfig.Logger = golanglog.New(config.logger.Writer(), "serf ", 0)
	// it's important that this channel never blocks
	// if it blocks, the sender will block and therefore stop applying log entries
	// which means we're not up to date with the current cluster state anymore
	serfEventCh := make(chan serf.Event, serfEventChannelBufferSize)
	serfConfig.EventCh = serfEventCh
	serfConfig.NodeName = config.longMemberId
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
	serfConfig.Tags[serfMDKeyGridPort] = strconv.Itoa(config.GridPort)

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

	//
	// These channels are really sensitive.
	// If they block, membership can't do any updates.
	// No changes to memberships states will ever be processed.
	//
	memberJoinedOrUpdated := make(chan string)
	memberLeft := make(chan string)
	raftLeaderServiceAddrChan := make(chan string)

	m := &membership{
		serf:                      surf,
		logger:                    config.logger,
		membershipState:           newMembershipState(config.logger),
		memberJoinedOrUpdatedChan: memberJoinedOrUpdated,
		memberLeftChan:            memberLeft,
		raftLeaderServiceAddrChan: raftLeaderServiceAddrChan,
		config: config,
	}

	go m.handleSerfEvents(serfEventCh, memberJoinedOrUpdated, memberLeft, raftLeaderServiceAddrChan)
	return m, nil
}

type membership struct {
	serf            *serf.Serf
	logger          *log.Entry
	membershipState *membershipState
	config          ClusterConfig

	//
	// These channels are really sensitive.
	// If they block, membership can't do any updates.
	// No changes to memberships states will ever be processed.
	//
	memberJoinedOrUpdatedChan <-chan string
	memberLeftChan            <-chan string
	raftLeaderServiceAddrChan <-chan string
}

func (m *membership) handleSerfEvents(serfEventChannel <-chan serf.Event, memberJoined chan<- string, memberLeft chan<- string, raftLeaderServiceAddrChan chan<- string) {
	for { // ever...
		select {
		case serfEvent := <-serfEventChannel:
			if serfEvent == nil {
				// seems the channel was closed
				// let's stop this go routine
				close(memberJoined)
				close(memberLeft)
				close(raftLeaderServiceAddrChan)
				return
			}

			//
			// Obviously we receive these events multiple times per actual event.
			// That means we need to do some sort of diffing.
			//
			switch serfEvent.EventType() {
			case serf.EventMemberJoin, serf.EventMemberUpdate:
				m.handleMemberJoinEvent(serfEvent.(serf.MemberEvent), memberJoined, raftLeaderServiceAddrChan)
			case serf.EventMemberLeave, serf.EventMemberFailed:
				m.handleMemberLeaveEvent(serfEvent.(serf.MemberEvent), memberLeft)
			}
		case <-m.serf.ShutdownCh():
			close(memberJoined)
			close(memberLeft)
			close(raftLeaderServiceAddrChan)
			return
		}
	}
}

func (m *membership) handleMemberJoinEvent(me serf.MemberEvent, memberJoined chan<- string, raftLeaderServiceAddrChan chan<- string) {
	for _, item := range me.Members {
		updated := m.membershipState.updateMember(item.Name, item.Tags)
		if updated {
			memberJoined <- item.Name

			go func() {
				role, roleOk := item.Tags[serfMDKeyRaftRole]
				host, hostOk := item.Tags[serfMDKeyHost]
				raftPort, portOk := item.Tags[serfMDKeyRaftServicePort]
				if roleOk && hostOk && portOk && role == raftRoleLeader {
					raftLeaderServiceAddrChan <- host + ":" + raftPort
				}
			}()
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

func (m *membership) getMemberById(memberId string) (map[string]string, bool) {
	return m.membershipState.getMemberById(memberId)
}

func (m *membership) markLeader() error {
	newTags := make(map[string]string)
	newTags[serfMDKeyRaftRole] = raftRoleLeader
	return m.updateMemberTags(newTags)
}

func (m *membership) unmarkLeader() error {
	newTags := make(map[string]string)
	newTags[serfMDKeyRaftRole] = ""
	return m.updateMemberTags(newTags)
}

func (m *membership) updateMemberTags(newTags map[string]string) error {
	// this will update the nodes metadata and broadcast it out
	// blocks until broadcasting was successful or timed out
	tags, ok := m.getMemberById(m.myMemberId())
	if ok {
		for k, v := range newTags {
			tags[k] = v
		}
	} else {
		tags = newTags
	}

	// this update will be processed via the regular membership event processing
	return m.serf.SetTags(tags)
}

func (m *membership) myMemberId() string {
	return m.config.longMemberId
}

func (m *membership) getClusterSize() int {
	return m.membershipState.getNumMembers()
}

func (m *membership) close() {
	m.serf.Leave()
	m.serf.Shutdown()
}
