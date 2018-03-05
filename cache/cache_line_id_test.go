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

package cache

import (
	"github.com/golang/protobuf/proto"
	"github.com/mhelmich/carbon-copy-go/pb"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCacheLineIdBasic(t *testing.T) {
	id := newRandomCacheLineId()
	protobuf := id.toProtoBuf()

	data, err := proto.Marshal(protobuf)
	assert.Nil(t, err)
	log.Infof("Marshalled data size %d", len(data))

	newPb := &pb.CacheLineId{}
	err = proto.Unmarshal(data, newPb)
	assert.Nil(t, err)
	proto.Equal(protobuf, newPb)
	newId := cacheLineIdFromProtoBuf(newPb)
	assert.True(t, id.equal(newId))
}
