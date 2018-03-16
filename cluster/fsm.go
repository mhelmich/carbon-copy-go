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
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/mhelmich/carbon-copy-go/pb"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"sync"
)

// I'm sneaking this struct in here as response for a consistent read
type raftApplyResponse struct {
	err   error
	value []byte
}

type fsm struct {
	state  map[string][]byte
	mutex  sync.Mutex
	logger *log.Entry
}

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	cmdProto := &pb.RaftCommand{}
	if err := proto.Unmarshal(l.Data, cmdProto); err != nil {
		f.logger.Panicf("Shutting down as I can't apply a raft change: %s", err)
	}

	switch cmdProto.Cmd {
	case pb.RaftOps_Set:
		return f.applySet(cmdProto.GetKey(), cmdProto.GetValue())
	case pb.RaftOps_Delete:
		return f.applyDelete(cmdProto.GetKey())
	case pb.RaftOps_ConsistentGet:
		return f.applyConsistentGet(cmdProto.GetKey())
	default:
		return &raftApplyResponse{
			err: fmt.Errorf("Unknown command: %v", cmdProto.Cmd),
		}
	}

	// switch cmd := cmdProto.cmd.(type) {
	// case *pb.GetCommand:
	// case *pb.SetCommand:
	// }
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	// copy the entire map
	o := make(map[string][]byte)
	for k, v := range f.state {
		o[k] = v
	}

	return &fsmSnapshot{store: o}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	buf, err := ioutil.ReadAll(rc)
	if err != nil {
		return err
	}

	snapProto := &pb.RaftSnapshot{}
	if err := proto.Unmarshal(buf, snapProto); err != nil {
		return err
	}

	// setting the state can just happen
	// because the docs seem to allow it
	f.state = snapProto.Snap
	return nil
}

func (f *fsm) applyConsistentGet(key string) interface{} {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	buf, ok := f.state[key]
	f.logger.Infof("applyConsistentGet -- key: %s - ok: %t - buf: %v", key, ok, buf)
	return &raftApplyResponse{
		err:   nil,
		value: buf,
	}
}

func (f *fsm) applySet(key string, value []byte) interface{} {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.state[key] = value
	return nil
}

func (f *fsm) applyDelete(key string) interface{} {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	delete(f.state, key)
	return nil
}

type fsmSnapshot struct {
	store map[string][]byte
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		snapProto := &pb.RaftSnapshot{
			Snap: f.store,
		}

		buffer, err := proto.Marshal(snapProto)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(buffer); err != nil {
			return err
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {

}
