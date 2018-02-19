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
	"context"
)

type Cluster interface {
	myNodeId() int
	getAllocator() GlobalIdAllocator
}

type GlobalIdAllocator interface {
	nextId() int
}

func NewCluster() (Cluster, error) {
	return createNewCluster()
}

type consensusClient interface {
	Get(ctx context.Context, key string) (string, error)
	GetSortedRange(ctx context.Context, keyPrefix string) ([]kv, error)
	Put(ctx context.Context, key string, value string) error
	PutIfAbsent(ctx context.Context, key string, value string) (bool, error)
	Close() error
}
