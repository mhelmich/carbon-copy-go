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
	"bytes"
	"crypto/rand"
	"github.com/mhelmich/carbon-copy-go/pb"
	"github.com/oklog/ulid"
	log "github.com/sirupsen/logrus"
)

func cacheLineIdFromProtoBuf(lineId *pb.LineId) *cacheLineIdImpl {
	id, err := ulid.New(lineId.Time, bytes.NewReader(lineId.Entropy))
	if err != nil {
		log.Fatalf("Can't parse cache line id out of []byte: %s", err.Error())
	}
	return &cacheLineIdImpl{
		ulid: id,
	}
}

func newRandomCacheLineId() *cacheLineIdImpl {
	id := ulid.MustNew(ulid.Now(), rand.Reader)
	return &cacheLineIdImpl{
		ulid: id,
	}
}

type cacheLineIdImpl struct {
	ulid ulid.ULID
}

func (cli *cacheLineIdImpl) toProtoBuf() *pb.LineId {
	return &pb.LineId{
		Time:    cli.ulid.Time(),
		Entropy: cli.ulid.Entropy(),
	}
}

func (cli *cacheLineIdImpl) equal(that *cacheLineIdImpl) bool {
	return cli.ulid.Compare(that.ulid) == 0
}

func (cli *cacheLineIdImpl) string() string {
	return cli.ulid.String()
}
