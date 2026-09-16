package raft

import (
	"context"

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