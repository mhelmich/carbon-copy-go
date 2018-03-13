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

func createSerf(config clusterConfig) (*serf.Serf, error) {
	serfEventCh := make(chan serf.Event, 256)

	serfConfig := serf.DefaultConfig()
	serfConfig.Logger = golanglog.New(config.logger.Writer(), "serf", 0)
	serfConfig.EventCh = serfEventCh
	serfConfig.MemberlistConfig.BindAddr = config.AdvertizedHostname
	serfConfig.MemberlistConfig.BindPort = config.SerfPort

	go handleSerfEvents(serfEventCh)

	return serf.Create(serfConfig)
}

type membership struct {
	s *serf.Serf
}

func handleSerfEvents(ch chan serf.Event) {
	for {
		e := <-ch
		switch e.EventType() {
		case serf.EventMemberJoin:
			handleMemberJoined(e.(serf.MemberEvent))
		case serf.EventMemberLeave:
			handleMemberLeave(e.(serf.MemberEvent))
		case serf.EventMemberFailed:
			handleMemberFailed(e.(serf.MemberEvent))
		}
	}
}

func handleMemberJoined(me serf.MemberEvent) {}

func handleMemberLeave(me serf.MemberEvent) {}

func handleMemberFailed(me serf.MemberEvent) {}
