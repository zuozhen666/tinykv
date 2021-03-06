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
	"errors"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// actual election interval
	randomelectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	var raft *Raft
	raft = &Raft{
		id:               c.ID,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		votes:			  make(map[uint64]bool, 0),
	}
	state, confstate, _ := raft.RaftLog.storage.InitialState()
	if c.peers == nil {
		c.peers = confstate.Nodes
	}
	li := raft.RaftLog.LastIndex()
	for _, i := range(c.peers) {
		if i == raft.id {
			raft.Prs[i] = &Progress{Next: li + 1, Match: li}
		} else {
			raft.Prs[i] = &Progress{Next: li + 1, Match: 0}
		}
	}
	raft.becomeFollower(0, None)
	raft.randomelectionTimeout = raft.electionTimeout + rand.Intn(raft.electionTimeout)
	raft.Term, raft.Vote, raft.RaftLog.committed, raft.RaftLog.applied = state.Term, state.Vote, state.Commit, c.Applied
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevIndex := r.Prs[to].Next - 1
	prevLogTerm, _ := r.RaftLog.Term(prevIndex)
	entries, _ := r.RaftLog.Slice(r.Prs[to].Next, r.RaftLog.LastIndex() + 1)
	ents := make([]*pb.Entry, 0)
	for i, _ := range entries {
		ents = append(ents, &entries[i])
	}
	m := pb.Message {
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:	 r.id,
		Term:	 r.Term,
		LogTerm: prevLogTerm,
		Index:	 prevIndex,
		Entries: ents,
		Commit:	 r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	m := pb.Message {
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, m)
}

// sendHeartbeatResponse sends a Heartbeat response to the given peer.
func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	m := pb.Message {
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:	 r.id,
		To:		 to,
		Term:	 r.Term,
		Reject:	 reject,
	}
	r.msgs = append(r.msgs, m)
}

// sendRequestVote sends a RequestVote RPC to the given peer.
func (r *Raft) sendRequestVote(to uint64) {
	li, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	m := pb.Message {
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: li,
		Index:   r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, m)
}

// sendRequestVoteResponse sends a RequestVote response to the given peer.
func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	m := pb.Message {
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, m)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, From: r.id})
		}
	} else {
		r.electionElapsed++
		if r.electionElapsed >= r.randomelectionTimeout {
			r.electionElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id})
		}
	}
}

// reset when state changes
func (r *Raft) reset(term uint64) {
	r.Term = term
	r.Vote = None
	r.Lead = None
	r.electionElapsed, r.heartbeatElapsed = 0, 0
	r.randomelectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.votes = make(map[uint64]bool, 0)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.State = StateFollower
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.reset(r.Term + 1)
	r.State = StateCandidate
	r.Vote = r.id
	r.votes[r.id] = true
	if len(r.Prs) == 1 { // pass project2aa
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.reset(r.Term)
	r.State = StateLeader
	r.Lead = r.id
	// initialise r.Prs
	li := r.RaftLog.LastIndex()
	for peer := range r.Prs {
		if peer == r.id {
			r.Prs[peer] = &Progress {Next: li + 1, Match: li}
		} else {
			r.Prs[peer] = &Progress {Next: li + 1, Match: 0}
		}
	}
	// append a noop entry on its term
	r.leaderappendEntries([]*pb.Entry {
		{
			EntryType: pb.EntryType_EntryNormal,
			Data:	   nil,
		},
	}...)
	r.isUpdateCommit() // pass project2ac-test1
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

// Follower handle message
func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		for peer := range r.Prs {
			if peer != r.id {
				r.sendRequestVote(peer)
			}
		}
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	}
	return nil
}

// Candidate handle message
func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		for peer := range r.Prs {
			if peer != r.id {
				r.sendRequestVote(peer)
			}
		}
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleResquestVoteResponse(m)
	}
	return nil
}

// leader handle message
func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		for peer := range r.Prs {
			if peer != r.id {
				r.sendHeartbeat(peer)
			}
		}
	case pb.MessageType_MsgPropose:
		r.leaderappendEntries(m.Entries...)
		r.isUpdateCommit()
		for peer := range r.Prs {
			if peer != r.id {
				r.sendAppend(peer)
			}
		}
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	}
	return nil
}

// leader append Entries
func (r *Raft) leaderappendEntries(entries ...*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()
	ents := make([]pb.Entry, 0)
	for i := range entries {
		entries[i].Index = lastIndex + uint64(i) + 1
		entries[i].Term = r.Term
		ents = append(ents, *entries[i])
	}
	r.RaftLog.append(ents...)
	// update its own node information
	pr := r.Prs[r.id]
	pr.Match = r.RaftLog.LastIndex()
	pr.Next = pr.Match + 1
}

// handle RequestVote
func (r *Raft) handleRequestVote(m pb.Message) {
	var reject bool
	li, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	if r.Vote == None || r.Vote == m.From {
		if m.LogTerm > li || (m.LogTerm == li && m.Index >= r.RaftLog.LastIndex()) {
			reject = false
			r.Vote = m.From
		} else {
			reject = true
		}
	} else {
		reject = true
	}
	r.sendRequestVoteResponse(m.From, reject)
}

// handle ResquestVoteResponse
func (r *Raft) handleResquestVoteResponse(m pb.Message) {
	r.votes[m.From] = !m.Reject
	res := r.CountVotes()
	switch res {
	case VoteWon:
		r.becomeLeader()
		for peer := range r.Prs {
			if peer != r.id {
				r.sendHeartbeat(peer)
			}
		}
	case VoteLost:
		r.becomeFollower(r.Term, None)
	}
}

// work for vote
type VoteResType uint64

const (
	VoteWon VoteResType = iota
	VotePending
	VoteLost
)

func (r *Raft) CountVotes() (VoteRes VoteResType) {
	agree, disagree := 0, 0
	for i := 1; i <= len(r.Prs); i++ {
		result, isexit := r.votes[uint64(i)]
		if !isexit {
			continue
		}
		if result {
			agree++
		} else {
			disagree++
		}
	}
	if agree > len(r.Prs)/2 {
		return VoteWon
	}
	if disagree >len(r.Prs)/2 {
		return VoteLost
	}
	return VotePending
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.Lead = m.From
	response := pb.Message {
		MsgType: pb.MessageType_MsgAppendResponse,
		To:		 m.From,
		From:	 r.id,
		Term:	 r.Term,
		Index: 	 m.Index,
	}
	term, _ := r.RaftLog.Term(m.Index)
	if term != m.LogTerm {
		response.Reject = true
		r.msgs = append(r.msgs, response)
		return
	}
	ents := make([]pb.Entry, 0)
	for _, ent := range m.Entries {
		term, _ := r.RaftLog.Term(ent.Index)
		if term != ent.Term {
			ents = append(ents, *ent)
		}
	}
	r.RaftLog.append(ents...)
	response.Index = r.RaftLog.LastIndex()
	response.Reject = false
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Index + uint64(len(m.Entries)), m.Commit)
	}
	r.msgs = append(r.msgs, response)
	return
}

// handle AppendResponse
func (r *Raft) handleAppendResponse(m pb.Message) {
	prs := r.Prs[m.From]
	if m.Reject {
			prs.Next--
			r.sendAppend(m.From)
	} else {
		prs.Match = m.Index
		prs.Next = m.Index + 1
		// update commit
		if r.isUpdateCommit() {
			for peer := range r.Prs {
				if peer != r.id {
					r.sendAppend(peer)
				}
			}
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	r.Lead = m.From
	r.electionElapsed = 0
	r.randomelectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.sendHeartbeatResponse(m.From, false)
}

// Slice return a entries slice based on identified intervals
func (l *RaftLog) Slice(lo, hi uint64) ([]pb.Entry, error) {
	if lo == hi {
		return nil, nil
	}
	if len(l.entries) > 0 {
		var ents []pb.Entry
		if lo < l.preindex {
			ents, _ = l.storage.Entries(lo, min(l.preindex, hi))
		}
		if hi > l.preindex {
			memEntries := l.entries[max(lo, l.preindex) - l.preindex : hi - l.preindex]
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
		storageEntries, _ := l.storage.Entries(lo, hi)
		return storageEntries, nil
	}
	return nil, nil
}

// append entries in the right place
func (l *RaftLog) append(entries ...pb.Entry) {
	if len(entries) == 0 {
		return
	}
	preIdx := entries[0].Index - 1
	if len(l.entries) > 0 {
		switch {
		case preIdx == l.preindex + uint64(len(l.entries) - 1):
			l.entries = append(l.entries, entries...)
		case preIdx < l.preindex:
			l.preindex = preIdx + 1
			l.entries = entries
		default:
			l.entries = append([]pb.Entry{}, l.entries[0:preIdx + 1 - l.preindex]...)
			l.entries = append(l.entries, entries...)
		}
	} else {
		l.preindex = preIdx + 1
		l.entries = entries
	}
	if l.stabled > preIdx {
		l.stabled = preIdx
	}
}

// Determine whether to update Commit
func (r *Raft) isUpdateCommit() bool {
	matches := make([]uint64, len(r.Prs))
	for i, prs := range r.Prs {
		if i == r.id {
			matches[i - 1] = r.RaftLog.LastIndex()
		} else {
			matches[i - 1] = prs.Match
		}
	}
	bubbleSort(matches, len(matches))
	N := matches[(len(matches) - 1)/2]
	if N > r.RaftLog.committed && N <= r.RaftLog.LastIndex() {
		NTerm, err := r.RaftLog.Term(N)
		if err != nil {
			return false
		}
		if NTerm == r.Term {
			r.RaftLog.committed = N
			return true
		}
	}
	return false
}

// work for isUpdateCommit
func bubbleSort(arr []uint64, len int) {
	if len == 1 {
		return
	}
	for i := 0; i < len-1; i++ {
		if arr[i] > arr[i+1] {
			arr[i], arr[i+1] = arr[i+1], arr[i]
		}
	}
	bubbleSort(arr, len-1)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
