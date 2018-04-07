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
	"sort"
	"strconv"

	"github.com/gogo/protobuf/proto"
	"github.com/mhelmich/carbon-copy-go/pb"
	log "github.com/sirupsen/logrus"
)

type cluster2 struct {
	consensusStore      *consensusStoreImpl
	consensusStoreProxy *consensusStoreProxy
	membership          *membership
	raftService         *raftServiceImpl
	logger              *log.Logger
	config              *ClusterConfig
	shortMemberId       int
	gridMemberInfoChan  chan *GridMemberConnectionEvent
	longIdsToShortIds   map[string]int
}

func createNewCluster2(config ClusterConfig) (*cluster2, error) {
	config = defaultClusterConfig(config)
	// the consensus store comes first
	cs, err := createNewConsensusStore(config)
	if err != nil {
		return nil, err
	}

	shortMemberIdChan := shortMemberIdChan(cs, config)
	raftLeaderAddressChan := raftLeaderAddressChan(cs, config)

	// then we need membership to announce our presence to others (or not)
	m, err := createNewMembership(config)
	if err != nil {
		return nil, err
	}

	// create the service providing non-consensus nodes with values
	raftServer, err := createRaftService(config, cs)
	if err != nil {
		return nil, err
	}

	c := &cluster2{
		membership:          m,
		consensusStore:      cs,
		raftService:         raftServer,
		consensusStoreProxy: nil,
		logger:              config.logger.Logger,
		config:              &config,
		shortMemberId:       -1,
		longIdsToShortIds:   make(map[string]int),
	}
	go c.eventProcessorLoop()

	// at last create the proxy
	proxy, err := newConsensusStoreProxy(config, cs, raftLeaderAddressChan)
	if err != nil {
		return nil, err
	}

	c.consensusStoreProxy = proxy
	c.shortMemberId = <-shortMemberIdChan

	return c, nil
}

func shortMemberIdChan(cs *consensusStoreImpl, config ClusterConfig) chan int {
	shortMemberIdChan := make(chan int)

	cs.addWatcher(consensusMembersRootName+config.longMemberId, func(key string, value []byte) {
		if key == consensusMembersRootName+config.longMemberId {
			mi := &pb.MemberInfo{}
			errUnMarshall := proto.Unmarshal(value, mi)
			if errUnMarshall != nil {
				config.logger.Errorf("Can't acquire short member id: %s", errUnMarshall.Error())
			}

			shortMemberId := int(mi.ShortMemberId)
			if shortMemberId > 0 {
				shortMemberIdChan <- int(mi.ShortMemberId)
				cs.removeWatcher(consensusMembersRootName + config.longMemberId)
				close(shortMemberIdChan)
			}
		}
	})

	return shortMemberIdChan
}

func raftLeaderAddressChan(cs *consensusStoreImpl, config ClusterConfig) chan string {
	raftLeaderAddrChan := make(chan string)

	go func() {
		lb, _ := cs.get(consensusLeaderName)
		s, err := consensusLeaderAddrFromConsensus(lb, cs)
		if err == nil {
			raftLeaderAddrChan <- s
		}
	}()

	cs.addWatcher(consensusLeaderName, func(key string, value []byte) {
		s, err := consensusLeaderAddrFromConsensus(value, cs)
		if err == nil {
			raftLeaderAddrChan <- s
		}
	})

	return raftLeaderAddrChan
}

func consensusLeaderAddrFromConsensus(leaderIdBites []byte, cs *consensusStoreImpl) (string, error) {
	lid := string(leaderIdBites)
	b, err := cs.get(consensusMembersRootName + lid)
	if err != nil {
		return "", err
	}

	mi := &pb.MemberInfo{}
	err = proto.Unmarshal(b, mi)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%d", mi.Host, mi.RaftServicePort), nil
}

func (c *cluster2) eventProcessorLoop() {
	for { // ever...
		var err error
		select {
		case isLeader := <-c.consensusStore.raftLeaderChangeNotifyCh:
			if isLeader {
				c.handleNewLeaderHousekeeping()
			}

		case memberJoined := <-c.membership.memberJoinedChan:
			if memberJoined == "" {
				// the channel was closed, we're done
				c.logger.Warn("Member joined channel is closed stopping event processor loop")
				return
			}

			err = c.handleLeaderMemberJoined(memberJoined)
			if err != nil {
				c.logger.Errorf("Handle member joined [%s] failed: %s", memberJoined, err.Error())
			}

		case memberUpdated := <-c.membership.memberUpdatedChan:
			if memberUpdated == "" {
				// the channel was closed, we're done
				c.logger.Warn("Member updated channel is closed stopping event processor loop")
				close(c.gridMemberInfoChan)
				return
			}

			err = c.handleMemberUpdated(memberUpdated)
			if err != nil {
				c.logger.Errorf("Handle member updated [%s] failed: %s", memberUpdated, err.Error())
			}

		case memberLeft := <-c.membership.memberLeftChan:
			if memberLeft == "" {
				// the channel was closed, we're done
				c.logger.Warn("Member left channel is closed stopping event processor loop")
				close(c.gridMemberInfoChan)
				return
			}

			err = c.handleMemberLeft(memberLeft)
			if err != nil {
				c.logger.Errorf("Handle member left [%s] failed: %s", memberLeft, err.Error())
			}
		}
	}
}

func (c *cluster2) handleNewLeaderHousekeeping() error {
	if !c.consensusStore.isRaftLeader() {
		return fmt.Errorf("Not leader!!")
	}

	// remove old leader from consensus
	// add myself as new leader everywhere
	err := c.doConsensusBookkeeping()
	if err != nil {
		return err
	}

	// balance out cluster
	return c.balanceOutCluster()
}

func (c *cluster2) handleLeaderMemberJoined(memberId string) error {
	if !c.consensusStore.isRaftLeader() {
		return nil
	}

	// get the new members info from membership
	info, infoOk := c.membership.getMemberById(memberId)
	if !infoOk {
		return fmt.Errorf("Member info for member [%s] is not present!?!?!?", memberId)
	}

	mi, err := c.convertNodeInfoFromMembershipToConsensus(memberId, info)
	if err != nil {
		return fmt.Errorf("Can't create member info proto: %s", err.Error())
	}

	shortId, err := c.findShortMemberId()
	if err != nil {
		return fmt.Errorf("Failed to find short member id for member [%s]: %s", memberId, err.Error())
	}

	mi.ShortMemberId = int32(shortId)
	voters, err := c.consensusStore.getVoters()
	if err != nil {
		return fmt.Errorf("Can't get voters: %s", err.Error())
	}

	numVoters := len(voters)
	// compute the number of voters I want
	numVotersIWant := c.config.NumRaftVoters - numVoters
	if numVotersIWant > 0 {
		err := c.markVoter(mi)
		if err != nil {
			return fmt.Errorf("Can't mark member [%s] voter: %s", memberId, err.Error())
		}
	} else {
		err := c.markNonvoter(mi)
		if err != nil {
			return fmt.Errorf("Can't mark member [%s] nonvoter: %s", memberId, err.Error())
		}
	}

	return nil
}

func (c *cluster2) handleMemberUpdated(memberId string) error {
	tags, ok := c.membership.getMemberById(memberId)
	if ok {
		shortMid, shortMidOk := tags[serfMDKeyShortMemberId]
		host, hostOk := tags[serfMDKeyHost]
		gridPort, gridPortOk := tags[serfMDKeyGridPort]
		if shortMidOk && hostOk && gridPortOk {
			id, err := strconv.Atoi(shortMid)
			if err != nil {
				c.logger.Errorf("Can't convert short id [%s] to int: %s", shortMid, err.Error())
			} else {
				// save for member left use case
				c.longIdsToShortIds[memberId] = id

				c.gridMemberInfoChan <- &GridMemberConnectionEvent{
					Type:              MemberJoined,
					ShortMemberId:     id,
					MemberGridAddress: host + ":" + gridPort,
				}
			}
		}
	}

	return nil
}

func (c *cluster2) handleMemberLeft(memberId string) error {
	if c.consensusStore.isRaftLeader() {
		c.consensusStore.delete(consensusMembersRootName + memberId)
		c.consensusStore.delete(consensusVotersName + memberId)
		c.consensusStore.delete(consensusNonVotersName + memberId)

		c.consensusStore.removeMemberById(memberId)

		err := c.balanceOutCluster()
		if err != nil {
			c.logger.Errorf("Can't rebalance cluster: %s", err.Error())
		}
	}

	// drop the member leave into the channel
	c.gridMemberInfoChan <- &GridMemberConnectionEvent{
		Type:          MemberLeft,
		ShortMemberId: int(c.longIdsToShortIds[memberId]),
	}

	delete(c.longIdsToShortIds, memberId)
	return nil
}

// this method does all the work necessary to remove all traces
// of the previous leader and set myself up as the new leader
func (c *cluster2) doConsensusBookkeeping() error {
	// get old leader id
	oldLeaderIdBites, err := c.consensusStore.get(consensusLeaderName)
	if err != nil {
		return err
	}

	// this removes the old leader from the consensus internal
	// server config list
	oldLeaderId := string(oldLeaderIdBites)
	err = c.consensusStore.removeMemberById(oldLeaderId)
	if err != nil {
		return err
	}

	// this removes the old leader from my bookkeeping
	_, err = c.consensusStore.delete(consensusVotersName + oldLeaderId)
	if err != nil {
		return err
	}

	if c.shortMemberId <= 0 {
		c.shortMemberId, err = c.findShortMemberId()
		if err != nil {
			return err
		}
		c.logger.Infof("Assinged short id %d to long id %s", c.shortMemberId, c.config.longMemberId)
	}

	mi := &pb.MemberInfo{
		LongMemberId:    c.config.longMemberId,
		Host:            c.config.hostname,
		ShortMemberId:   int32(c.shortMemberId),
		RaftPort:        int32(c.config.RaftPort),
		RaftServicePort: int32(c.config.RaftServicePort),
		GridPort:        int32(c.config.GridPort),
		SerfPort:        int32(c.config.SerfPort),
		RaftState:       pb.RaftState_Leader,
	}

	err = c.setMemberInfoInConsensusStore(mi)
	if err != nil {
		return err
	}

	// update me as leader in consensus
	_, err = c.consensusStore.set(consensusLeaderName, []byte(mi.LongMemberId))
	if err != nil {
		return err
	}

	// add myself as voter in consensus
	_, err = c.consensusStore.set(consensusVotersName+mi.LongMemberId, make([]byte, 0))
	if err != nil {
		return err
	}

	// delete myself out of
	c.consensusStore.delete(consensusNonVotersName + mi.LongMemberId)
	return nil
}

func (c *cluster2) setMemberInfoInConsensusStore(memberInfo *pb.MemberInfo) error {
	bites, err := proto.Marshal(memberInfo)
	if err != nil {
		return err
	}

	_, err = c.consensusStore.set(consensusMembersRootName+memberInfo.LongMemberId, bites)
	return err
}

func (c *cluster2) balanceOutCluster() error {
	voters, err := c.consensusStore.getVoters()
	if err != nil {
		return err
	}

	// compute the number of voters I need to add
	numVoters := len(voters)
	numVotersIWant := c.config.NumRaftVoters - numVoters

	if numVotersIWant > 0 {
		nonvoters, err := c.consensusStore.getNonvoters()
		if err != nil {
			return err
		}

		for id := range nonvoters {
			mi, err := c.getMemberInfoFromConsensusStore(id)
			if err != nil {
				c.logger.Errorf("Can't get member [%s] from consensus store: %s", id, err.Error())
			} else {
				err = c.markVoter(mi)
				if err != nil {
					c.logger.Errorf("Failed to mark member [%s] as voter: %s", id, err.Error())
				}
			}
		}
	}

	return nil
}

func (c *cluster2) markVoter(mi *pb.MemberInfo) error {
	var err error

	err = c.consensusStore.addVoter(mi.LongMemberId, fmt.Sprintf("%s:%d", mi.Host, mi.RaftPort))
	if err != nil {
		return err
	}

	mi.RaftState = pb.RaftState_Voter

	// add member as voter in consensus
	_, err = c.consensusStore.set(consensusVotersName+mi.LongMemberId, make([]byte, 0))
	if err != nil {
		return err
	}

	// override member info in consensus
	err = c.setMemberInfoInConsensusStore(mi)
	if err != nil {
		return err
	}

	_, err = c.consensusStore.delete(consensusNonVotersName + mi.LongMemberId)
	if err != nil {
		return err
	}

	return nil
}

func (c *cluster2) markNonvoter(mi *pb.MemberInfo) error {
	var err error

	err = c.consensusStore.addNonvoter(mi.LongMemberId, fmt.Sprintf("%s:%d", mi.Host, mi.RaftPort))
	if err != nil {
		return err
	}

	mi.RaftState = pb.RaftState_Nonvoter

	// add member as voter in consensus
	_, err = c.consensusStore.set(consensusNonVotersName+mi.LongMemberId, make([]byte, 0))
	if err != nil {
		return err
	}

	// override member info in consensus
	err = c.setMemberInfoInConsensusStore(mi)
	if err != nil {
		return err
	}

	_, err = c.consensusStore.delete(consensusVotersName + mi.LongMemberId)
	if err != nil {
		return err
	}

	return nil
}

func (c *cluster2) getMemberInfoFromConsensusStore(longMemberId string) (*pb.MemberInfo, error) {
	bites, err := c.consensusStore.get(consensusMembersRootName + longMemberId)
	if err != nil {
		return nil, err
	}

	if bites == nil {
		return nil, fmt.Errorf("No member with id [%s] found!", longMemberId)
	}

	memberInfo := &pb.MemberInfo{}
	err = proto.Unmarshal(bites, memberInfo)
	return memberInfo, err
}

func (c *cluster2) convertNodeInfoFromMembershipToConsensus(myMemberId string, serfInfo map[string]string) (*pb.MemberInfo, error) {
	host := serfInfo[serfMDKeyHost]

	serfPort, err := strconv.Atoi(serfInfo[serfMDKeySerfPort])
	if err != nil {
		return nil, err
	}

	raftPort, err := strconv.Atoi(serfInfo[serfMDKeyRaftPort])
	if err != nil {
		return nil, err
	}

	raftServicePort, err := strconv.Atoi(serfInfo[serfMDKeyRaftServicePort])
	if err != nil {
		return nil, err
	}

	gridPort, err := strconv.Atoi(serfInfo[serfMDKeyGridPort])
	if err != nil {
		return nil, err
	}

	return &pb.MemberInfo{
		Host:            host,
		LongMemberId:    myMemberId,
		SerfPort:        int32(serfPort),
		RaftPort:        int32(raftPort),
		RaftServicePort: int32(raftServicePort),
		GridPort:        int32(gridPort),
	}, nil
}

func (c *cluster2) findShortMemberId() (int, error) {
	allMembers, err := c.consensusStore.getPrefix(consensusMembersRootName)
	if err != nil {
		return -1, err
	}

	a := make([]*pb.MemberInfo, len(allMembers))
	for idx, kv := range allMembers {
		mi := &pb.MemberInfo{}
		err = proto.Unmarshal(kv.v, mi)
		if err != nil {
			return -1, err
		}
		a[idx] = mi
	}

	// sort the node infos by short id
	sort.Sort(byShortMemberId(a))

	// count the sorted node infos up until you find a gap
	newShortMemberId := -1
	for idx := 0; idx < len(a); idx++ {
		if a[idx].ShortMemberId != int32(idx+1) {
			newShortMemberId = idx + 1
			break
		}
	}

	// or take the next node id available
	if newShortMemberId == -1 {
		newShortMemberId = len(a) + 1
	}

	return newShortMemberId, nil
}

/////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////
///////////////////////////////////////////
//      PUBLIC INTERFACE DEFINITIONS

func (c *cluster2) GetMyShortMemberId() int {
	return c.shortMemberId
}

func (c *cluster2) GetGridMemberChangeEvents() <-chan *GridMemberConnectionEvent {
	return c.gridMemberInfoChan
}

func (c *cluster2) Close() error {
	return nil
}
