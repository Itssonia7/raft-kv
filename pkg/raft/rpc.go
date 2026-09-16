package raft

import (
	"context"
	"time"

	raftpb "raft-kv/pkg/proto"
)

// RequestVote handles incoming vote requests from candidate peers.
func (rn *RaftNode) RequestVote(ctx context.Context, req *raftpb.RequestVoteArgs) (*raftpb.RequestVoteReply, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply := &raftpb.RequestVoteReply{
		Term:        rn.currentTerm,
		VoteGranted: false,
	}

	// Rule 1: Reject request if candidate's term is older than our term.
	if req.Term < rn.currentTerm {
		return reply, nil
	}

	// Rule 2: If RPC term is higher, step down to Follower and update term.
	if req.Term > rn.currentTerm {
		rn.currentTerm = req.Term
		rn.role = Follower
		rn.votedFor = ""
	}

	// Rule 3: Check log up-to-date criteria (§5.4.1).
	lastLog := rn.log[len(rn.log)-1]
	lastLogIndex := lastLog.Index
	lastLogTerm := lastLog.Term

	logUpToDate := false
	if req.LastLogTerm > lastLogTerm {
		logUpToDate = true
	} else if req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex {
		logUpToDate = true
	}

	// Rule 4: Grant vote if we haven't voted yet (or voted for this peer) and its log is up-to-date.
	canVote := rn.votedFor == "" || rn.votedFor == req.CandidateId
	if canVote && logUpToDate {
		rn.votedFor = req.CandidateId
		reply.VoteGranted = true
		rn.lastHeartbeat = rn.lastHeartbeat.Add(0) // keep timestamp fresh
		rn.resetElectionTimeout()
	}

	reply.Term = rn.currentTerm
	return reply, nil
}

// AppendEntries handles log replication and heartbeat RPCs from the leader.
func (rn *RaftNode) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesArgs) (*raftpb.AppendEntriesReply, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply := &raftpb.AppendEntriesReply{
		Term:    rn.currentTerm,
		Success: false,
	}

	// Rule 1: Reject entries from a stale leader
	if req.Term < rn.currentTerm {
		return reply, nil
	}

	// Rule 2: If term is greater, update term and step down to Follower
	if req.Term > rn.currentTerm {
		rn.currentTerm = req.Term
		rn.role = Follower
		rn.votedFor = ""
	}

	// A valid leader contacted us; transition to Follower if Candidate and reset timer
	rn.role = Follower
	rn.lastHeartbeat = time.Now()
	rn.resetElectionTimeout()

	// Rule 3: Check if our log contains an entry at prevLogIndex matching prevLogTerm (§5.3)
	lastLogIndex := uint64(len(rn.log) - 1)
	if req.PrevLogIndex > lastLogIndex {
		// Log is shorter than prevLogIndex
		reply.ConflictIndex = lastLogIndex + 1
		reply.ConflictTerm = 0
		return reply, nil
	}

	if rn.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		// Conflict at prevLogIndex
		reply.ConflictTerm = rn.log[req.PrevLogIndex].Term
		// Find first index that had this conflicting term for fast rollback
		firstIndex := req.PrevLogIndex
		for firstIndex > 0 && rn.log[firstIndex-1].Term == reply.ConflictTerm {
			firstIndex--
		}
		reply.ConflictIndex = firstIndex
		return reply, nil
	}

	// Rule 4: Append new entries, truncating existing conflicting entries (§5.3)
	for i, entry := range req.Entries {
		index := req.PrevLogIndex + 1 + uint64(i)
		if index < uint64(len(rn.log)) {
			if rn.log[index].Term != entry.Term {
				// Term mismatch: truncate log from this index onward
				rn.log = rn.log[:index]
				rn.log = append(rn.log, entry)
			}
		} else {
			// Append remaining new entries
			rn.log = append(rn.log, entry)
		}
	}

	// Rule 5: Update commitIndex if leaderCommit > commitIndex
	if req.LeaderCommit > rn.commitIndex {
		lastNewIndex := req.PrevLogIndex + uint64(len(req.Entries))
		if req.LeaderCommit < lastNewIndex {
			rn.commitIndex = req.LeaderCommit
		} else {
			rn.commitIndex = lastNewIndex
		}
	}

	reply.Success = true
	reply.Term = rn.currentTerm
	return reply, nil
}
