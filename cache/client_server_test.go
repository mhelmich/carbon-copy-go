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

// +build all unit

package cache

import (
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"testing"
)

func TestServerClient(t *testing.T) {
	server, err := createNewServer(111, 6666, createNewCacheLineStore())
	assert.Nil(t, err, "Couldn't start server")
	client, err := createNewCacheClientFromAddr("localhost:6666")
	assert.Nil(t, err, "Couldn't create client stub")

	inv := &Inv{
		SenderId: 555,
		LineId:   123456789,
	}
	invAck, err := client.SendInvalidate(context.Background(), inv)
	assert.Nil(t, err, "Couldn't send invalidate")
	assert.NotNil(t, invAck, "invAck is nil")
	assert.Equal(t, int32(111), invAck.SenderId)
	assert.Equal(t, int64(123456789), invAck.LineId)

	get := &Get{
		SenderId: 555,
		LineId:   123456789,
	}
	_, _, err = client.SendGet(context.Background(), get)
	assert.NotNil(t, err, "Couldn't send get")

	err = client.Close()
	assert.Nil(t, err, "Couldn't close client")
	server.Stop()
}

func createNewCacheClientFromAddr(addr string) (*cacheClientImpl, error) {
	connection, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Errorf("fail to dial: %v\n", err)
		return nil, err
	}

	return createNewCacheClientFromConn(connection)
}
