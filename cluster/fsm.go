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
	"io"
	"io/ioutil"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/mhelmich/carbon-copy-go/pb"
	log "github.com/sirupsen/logrus"
)

// I'm sneaking this struct in here as response for a consistent read
// this is the generic return type of an apply step
// I hope I can press all possible responses into this format
// at least this way I have boil type-safety problems down to casting
// to the same thing always and then I have to deal with putting state
// back together from a byte array
type raftApplyResponse struct {
	err   error
	value []byte
}

// the state machine implementation
type fsm struct {
	state        map[string][]byte
	mutex        sync.RWMutex
	logger       *log.Entry
	watcherMutex sync.RWMutex
	watchers     map[string]func(string, []byte)
}

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	cmdProto := &pb.RaftCommand{}
	if err := proto.Unmarshal(l.Data, cmdProto); err != nil {
		f.logger.Panicf("Shutting down as I can't apply a raft change: %s", err)
	}

	switch cmdProto.GetCmd().(type) {
	case *pb.RaftCommand_SetCmd:
		set := cmdProto.GetSetCmd()
		return f.applySet(set.GetKey(), set.GetValue())
	case *pb.RaftCommand_DeleteCmd:
		del := cmdProto.GetDeleteCmd()
		return f.applyDelete(del.GetKey())
	case *pb.RaftCommand_GetCmd:
		get := cmdProto.GetGetCmd()
		return f.applyConsistentGet(get.GetKey())
	default:
		return &raftApplyResponse{
			err: fmt.Errorf("Unknown command: %v", cmdProto.Cmd),
		}
	}
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
	f.mutex.RLock()
	buf, ok := f.state[key]
	f.mutex.RUnlock()

	if !ok {
		return &raftApplyResponse{
			err:   fmt.Errorf("Key doesn't exist: %s", key),
			value: nil,
		}
	}

	return &raftApplyResponse{
		err:   nil,
		value: buf,
	}
}

func (f *fsm) applySet(key string, value []byte) interface{} {
	f.mutex.Lock()
	_, ok := f.state[key]
	f.state[key] = value
	f.mutex.Unlock()

	// meh, this is consistent enough
	// I don't want to block the apply mutex too long
	// and I don't want to put in the work to improve locking now
	if f.watchers != nil {
		for prefix, fn := range f.watchers {
			if strings.HasPrefix(key, prefix) {
				go fn(key, value)
			}
		}
	}

	// this byte has a "created" semantic
	// 1 means the key didn't exist before and was created
	// 0 means the key did exist before and was NOT created
	b := make([]byte, 1)

	if ok {
		b[0] = 0
	} else {
		b[0] = 1
	}

	return &raftApplyResponse{
		err:   nil,
		value: b,
	}
}

// the apply response contains one byte
// if the byte is zero it indicates false
// (as in there key didn't exist and couldn't be deleted)
func (f *fsm) applyDelete(key string) interface{} {
	f.mutex.Lock()
	_, ok := f.state[key]
	delete(f.state, key)
	f.mutex.Unlock()

	// meh, this is consistent enough
	// I don't want to block the apply mutex too long
	// and I don't want to put in the work to improve locking now
	if f.watchers != nil {
		for prefix, fn := range f.watchers {
			if strings.HasPrefix(key, prefix) {
				go fn(key, nil)
			}
		}
	}

	b := make([]byte, 1)

	if ok {
		b[0] = 1
	} else {
		b[0] = 0
	}

	return &raftApplyResponse{
		err:   nil,
		value: b,
	}
}

func (f *fsm) addWatcher(prefix string, fn func(string, []byte)) {
	if f.watchers == nil {
		f.watchers = make(map[string]func(string, []byte))
	}
	f.watchers[prefix] = fn
}

func (f *fsm) removeWatcher(prefix string) {
	delete(f.watchers, prefix)
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
