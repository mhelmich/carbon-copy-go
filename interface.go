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
	"github.com/mhelmich/carbon-copy-go/cache"
	"github.com/mhelmich/carbon-copy-go/cluster"
	log "github.com/sirupsen/logrus"
)

const (
	clusterDefaultDir = "./db.carbon.grid"
)

type CarbonGridConfig struct {
	cluster cluster.ClusterConfig
	cache   cache.CacheConfig

	logger *log.Entry
}

type Grid interface {
	GetCache() cache.Cache
	Close()
}

func NewGrid(configFileName string) (Grid, error) {
	return createNewGrid(configFileName)
}
