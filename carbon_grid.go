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

package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"sync"

	"github.com/mhelmich/carbon-copy-go/cache"
	"github.com/mhelmich/carbon-copy-go/cluster"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func createNewGrid(configFileName string) (*carbonGridImpl, error) {
	gridConfig, err := loadConfig(configFileName)

	clustr, err := cluster.NewCluster(gridConfig.cluster)
	if err != nil {
		return nil, err
	}

	// blocks until cluster becomes available...somewhat
	myMemberId := clustr.GetMyShortMemberId()
	if myMemberId <= 0 {
		return nil, fmt.Errorf("My member id can't be %d", myMemberId)
	}

	cache, err := cache.NewCache(myMemberId, gridConfig.cluster.GridPort, gridConfig.cache)
	if err != nil {
		return nil, err
	}

	// spin up a goroutine that constantly listens to changes in the cluster
	// and adds (or removes) peer nodes in the connection cache
	go func() {
		ch := clustr.GetGridMemberChangeEvents()
		for {
			switch event := <-ch; event.Type {
			case cluster.MemberJoined:
				cache.AddPeerNode(event.ShortMemberId, event.MemberGridAddress)
			case cluster.MemberLeft:
				cache.RemovePeerNode(event.ShortMemberId)
			}
		}
	}()

	return &carbonGridImpl{
		cache:   cache,
		cluster: clustr,
	}, nil
}

func loadConfig(configFileName string) (*CarbonGridConfig, error) {
	b, err := ioutil.ReadFile(configFileName)
	if err != nil {
		log.Panicf("Can't read config file: %s", err)
	}

	cfg := viper.New()
	cfg.SetConfigType("yaml")
	setConfigDefaults(cfg)
	err = cfg.ReadConfig(bytes.NewBuffer(b))
	if err != nil {
		return nil, fmt.Errorf("Couldn't read config file %s", err)
	}

	var clusterConfig cluster.ClusterConfig
	err = cfg.UnmarshalKey("carbongrid.cluster", &clusterConfig)
	if err != nil {
		return nil, fmt.Errorf("Couldn't unmarshall cluster config %s", err)
	}

	var cacheConfig cache.CacheConfig
	err = cfg.UnmarshalKey("carbongrid.cache", &cacheConfig)
	if err != nil {
		return nil, fmt.Errorf("Couldn't unmarshall cache config %s", err)
	}

	return &CarbonGridConfig{
		cluster: clusterConfig,
		cache:   cacheConfig,
		logger:  log.WithFields(log.Fields{}),
	}, nil
}

func setConfigDefaults(cfg *viper.Viper) {
	cfg.SetDefault("carbongrid.cluster.SerfSnapshotPath", clusterDefaultDir+"/serf")
	cfg.SetDefault("carbongrid.cluster.RaftStoreDir", clusterDefaultDir+"/raft")
	cfg.SetDefault("carbongrid.cluster.NumRaftVoters", 3)
	cfg.SetDefault("carbongrid.cache.MaxCacheLineSizeBytes", 1024)
	cfg.SetDefault("carbongrid.cache.MaxCacheSizeBytes", 1073741824)
}

type carbonGridImpl struct {
	cache   cache.Cache
	cluster cluster.Cluster
}

func (cgi *carbonGridImpl) GetCache() cache.Cache {
	return cgi.cache
}

func (cgi *carbonGridImpl) Close() {
	wg := &sync.WaitGroup{}
	wg.Add(2)
	log.Infof("Shutting down grid")

	go func() {
		if cgi != nil && cgi.cache != nil {
			cgi.cache.Stop()
		}
		wg.Done()
	}()

	go func() {
		if cgi != nil && cgi.cluster != nil {
			cgi.cluster.Close()
		}
		wg.Done()
	}()

	wg.Wait()
}
