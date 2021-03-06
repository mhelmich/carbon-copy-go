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
	"reflect"
	"sync"

	log "github.com/sirupsen/logrus"
)

func newMembershipState(logger *log.Entry) *membershipState {
	return &membershipState{
		mutex:          &sync.RWMutex{},
		currentMembers: make(map[string]map[string]string),
		logger:         logger,
	}
}

type membershipState struct {
	mutex          *sync.RWMutex
	currentMembers map[string]map[string]string
	logger         *log.Entry
}

func (cs *membershipState) updateMember(memberId string, tags map[string]string) bool {
	cs.mutex.RLock()
	_, existing := cs.currentMembers[memberId]
	equal := reflect.DeepEqual(cs.currentMembers[memberId], tags)
	cs.mutex.RUnlock()

	if existing && equal {
		// it seems we have seen this member event already
		return false
	}

	cs.mutex.Lock()
	// carry over all tags
	cs.currentMembers[memberId] = tags
	cs.mutex.Unlock()

	return true
}

func (cs *membershipState) removeMember(memberId string) bool {
	cs.mutex.RLock()
	_, existing := cs.currentMembers[memberId]
	cs.mutex.RUnlock()

	if !existing {
		return false
	}

	cs.mutex.Lock()
	// just be sure to not leave dead bodies in our basement
	delete(cs.currentMembers, memberId)
	cs.mutex.Unlock()

	return true
}

func (cs *membershipState) getMemberById(memberId string) (map[string]string, bool) {
	newMap := make(map[string]string)
	cs.mutex.RLock()
	m, ok := cs.currentMembers[memberId]
	if ok {
		for k, v := range m {
			newMap[k] = v
		}
	}
	cs.mutex.RUnlock()

	if !ok {
		return nil, ok
	}

	return newMap, ok
}

func (cs *membershipState) getAllLongMemberIds() []string {
	// thanks to read locking nobody can change the map while I iterate over it
	cs.mutex.RLock()
	memberIds := make([]string, len(cs.currentMembers))
	i := 0
	for id := range cs.currentMembers {
		memberIds[i] = id
		i++
	}
	cs.mutex.RUnlock()
	return memberIds
}

func (cs *membershipState) getNumMembers() int {
	cs.mutex.RLock()
	i := len(cs.currentMembers)
	cs.mutex.RUnlock()
	return i
}

func (cs *membershipState) printMemberState() {}
