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
	"github.com/hashicorp/serf/serf"
	log "github.com/sirupsen/logrus"
	golanglog "log"
)

const (
	serfEventChannelBufferSize = 256
)

func createSerf(config clusterConfig) (*membership, error) {
	serfConfig := serf.DefaultConfig()
	serfConfig.Logger = golanglog.New(config.logger.Writer(), "serf", 0)
	// it's important that this guy never blocks
	serfConfig.EventCh = make(chan serf.Event, serfEventChannelBufferSize)
	serfConfig.MemberlistConfig.BindAddr = config.hostname
	serfConfig.MemberlistConfig.BindPort = config.SerfPort

	s, err := serf.Create(serfConfig)
	if err != nil {
		return nil, err
	}

	m := &membership{
		serf:   s,
		logger: config.logger,
	}

	go m.handleSerfEvents(serfConfig.EventCh)
	return m, nil
}

type membership struct {
	serf   *serf.Serf
	logger *log.Entry
}

func (m *membership) handleSerfEvents(ch chan serf.Event) {
	for {
		e := <-ch
		switch e.EventType() {
		case serf.EventMemberJoin:
			m.handleMemberJoined(e.(serf.MemberEvent))
		case serf.EventMemberLeave:
			m.handleMemberLeave(e.(serf.MemberEvent))
		case serf.EventMemberFailed:
			m.handleMemberFailed(e.(serf.MemberEvent))
		}
	}
}

func (m *membership) handleMemberJoined(me serf.MemberEvent) {
	m.logger.Infof("Member joined: %v", me)
}

func (m *membership) handleMemberLeave(me serf.MemberEvent) {
	m.logger.Infof("Member left: %v", me)
}

func (m *membership) handleMemberFailed(me serf.MemberEvent) {
	m.logger.Infof("Member failed: %v", me)
}
