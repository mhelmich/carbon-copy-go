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
	"github.com/mhelmich/carbon-copy-go/pb"
	"strconv"
)

type nodeIdImpl struct {
	id int
}

func (ni *nodeIdImpl) toProtoBuf() *pb.NodeId {
	return &pb.NodeId{
		Id: int32(ni.id),
	}
}

func (ni *nodeIdImpl) equal(that *nodeIdImpl) bool {
	return ni.id == that.id
}

func (ni *nodeIdImpl) string() string {
	return strconv.Itoa(ni.id)
}
