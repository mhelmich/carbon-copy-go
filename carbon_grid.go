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
	"errors"
	"fmt"
	"github.com/mhelmich/carbon-copy-go/cache"
	"github.com/mhelmich/carbon-copy-go/cluster"
	log "github.com/sirupsen/logrus"
	"sync"
)

func createNewGrid() (*carbonGridImpl, error) {
	clustr, err := cluster.NewCluster(cluster.ClusterConfig{})
	if err != nil {
		return nil, err
	}

	// blocks until cluster becomes available
	myNodeId := clustr.GetMyNodeId()
	if myNodeId <= 0 {
		return nil, errors.New(fmt.Sprintf("My node id can't be %d", myNodeId))
	}

	cache, err := cache.NewCache(myNodeId, 9876)
	if err != nil {
		return nil, err
	}

	return &carbonGridImpl{
		cache:   cache,
		cluster: clustr,
	}, nil
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
