// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	// added
	offset uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	offset, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	stabled, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	ents, err := storage.Entries(offset, stabled + 1)
	if err != nil {
		panic(err)
	}
	return &RaftLog {
		storage: storage,
		stabled: stabled,
		entries: ents,
		offset: offset,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.stabled == l.LastIndex() {
		return make([]pb.Entry, 0)
	}
	ents, err := l.Slice(l.stabled + 1, l.LastIndex() + 1)
	if err != nil {
		panic(err)
	}
	return ents
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if l.applied == l.LastIndex() {
		return make([]pb.Entry, 0)
	}
	ents, err := l.Slice(l.applied + 1, l.committed + 1)
	if err != nil {
		panic(err)
	}
	return ents
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.offset + uint64(len(l.entries)) - 1
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return i
}

// LastTerm return the last term of the log entries
func (l *RaftLog) LastTerm() uint64 {
	term, err := l.Term(l.LastIndex())
	if err != nil {
		return 0
	}
	return term
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i > l.LastIndex() {
		return 0, nil
	}
	if len(l.entries) > 0 && i >= l.offset {
		return l.entries[i-l.offset].Term, nil
	}
	return l.storage.Term(i)
}

//append entries in the right place
func (l *RaftLog) append(entries ...pb.Entry) {
	if len(entries) == 0 {
		return
	}
	preIdx := entries[0].Index - 1
	if len(l.entries) > 0 {
		switch {
		case preIdx == l.offset + uint64(len(l.entries) - 1):
			l.entries = append(l.entries, entries...)
		case preIdx < l.offset:
			l.offset = preIdx + 1
			l.entries = entries
		default:
			l.entries = append([]pb.Entry{}, l.entries[0:preIdx + 1 - l.offset]...)
			l.entries = append(l.entries, entries...)
		}
	} else {
		l.offset = preIdx + 1
		l.entries = entries
	}
	if l.stabled > preIdx {
		l.stabled = preIdx
	}
}

// Slice return a entries slice based on identified intervals
func (l *RaftLog) Slice(lo, hi uint64) ([]pb.Entry, error) {
	if lo == hi {
		return nil, nil
	}
	if len(l.entries) > 0 {
		var ents []pb.Entry
		if lo < l.offset {
			storageEntries, err := l.storage.Entries(lo, min(l.offset, hi))
			if err != nil {
				panic(err)
			}
			ents = storageEntries
		}
		if hi > l.offset {
			memEntries := l.entries[max(lo, l.offset) - l.offset : hi - l.offset]
			if len(ents) > 0 {
				res := make([]pb.Entry, len(ents) + len(memEntries))
				n := copy(res, ents)
				copy(res[n:], memEntries)
				return res, nil
			} else {
				res := make([]pb.Entry, len(memEntries))
				copy(res, memEntries)
				return res, nil
			}
		}
	} else {
		storageEntries, err := l.storage.Entries(lo, hi)
		if err != nil {
			panic(err)
		}
		return storageEntries, nil
	}
	return nil, nil
}