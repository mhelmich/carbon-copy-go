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
)

func createSerf(config clusterConfig) (*membership, error) {
	serfConfig := serf.DefaultConfig()
	serfConfig.Logger = golanglog.New(config.logger.Writer(), "serf", 0)
	// it's important that this guy never blocks
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
	serfConfig.Tags["raft_addr"] = fmt.Sprintf("%s:%d", config.hostname, config.RaftPort)
	serfConfig.Tags["serf_addr"] = fmt.Sprintf("%s:%d", config.hostname, config.SerfPort)
	serfConfig.Tags["grid_addr"] = fmt.Sprintf("%s:%d", config.hostname, config.ServicePort)

	s, err := serf.Create(serfConfig)
	if err != nil {
		return nil, err
	}

	// if we have peers, let's join the cluster
	if config.Peers != nil && len(config.Peers) > 0 {
		numContactedNodes, err := s.Join(config.Peers, true)

		if err != nil {
			return nil, fmt.Errorf("Can't connect to serf nodes: %s", err.Error())
		} else if numContactedNodes == 0 {
			return nil, fmt.Errorf("Wasn't able to connect to any serf node: %v", config.Peers)
		} else {
			config.logger.Infof("Contacted %d serf nodes! List: %v", numContactedNodes, config.Peers)
		}
	} else {
		config.logger.Info("No peers defined")
	}

	m := &membership{
		serf:           s,
		logger:         config.logger,
		currentCluster: make(map[string]map[string]string),
	}

	go m.handleSerfEvents(serfEventCh)
	return m, nil
}

type membership struct {
	serf           *serf.Serf
	currentCluster map[string]map[string]string
	logger         *log.Entry
}

func (m *membership) handleSerfEvents(ch <-chan serf.Event) {
	for {
		select {
		case e := <-ch:
			if e == nil {
				// seems the channel was closed
				// let's stop this go routine
				break
			}

			switch e.EventType() {
			case serf.EventMemberJoin:
				m.handleMemberChangeEvent(e.(serf.MemberEvent))
			case serf.EventMemberLeave:
				m.handleMemberChangeEvent(e.(serf.MemberEvent))
			case serf.EventMemberFailed:
				m.handleMemberChangeEvent(e.(serf.MemberEvent))
			}
		case <-m.serf.ShutdownCh():
			return
		}
	}
}

func (m *membership) handleMemberChangeEvent(me serf.MemberEvent) {
	m.logger.Infof("Member event: current members: %v", me.Members)
	for _, item := range me.Members {
		m.currentCluster[item.Name] = item.Tags
	}
}

func (m *membership) getNodeById(nodeId string) (map[string]string, bool) {
	v, ok := m.currentCluster[nodeId]
	return v, ok
}

func (m *membership) updateRaftStatus(status string) {
	tags := make(map[string]string)
	tags["raft_status"] = status
	// this will update the nodes metadata and broadcast it out
	// blocks until broadcasting was successful or timed out
	m.serf.SetTags(tags)
}

func (m *membership) close() {
	m.serf.Leave()
	m.serf.Shutdown()
}
