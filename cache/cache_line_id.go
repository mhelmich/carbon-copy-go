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
	"github.com/oklog/ulid"
)

func cacheLineIdFromProtoBuf(lineId *LineId) (*cacheLineIdImpl, error) {
	id, err := ulid.New(lineId.Time, bytes.NewReader(lineId.Entropy))
	if err != nil {
		return nil, err
	}

	return &cacheLineIdImpl{
		ulid: id,
	}, nil
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

func (cli *cacheLineIdImpl) toProtoBuf() LineId {
	return LineId{
		Time:    cli.ulid.Time(),
		Entropy: cli.ulid.Entropy(),
	}
}

func (cli *cacheLineIdImpl) equal(that *cacheLineIdImpl) bool {
	return cli.ulid.Compare(that.ulid) == 0
}
