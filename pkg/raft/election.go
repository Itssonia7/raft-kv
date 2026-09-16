package raft

import (
	"context"
	"sync/atomic"
	"time"

	raftpb "raft-kv/pkg/proto"
)

// Start begins the background event tickers for timeouts and heartbeats.
func (rn *RaftNode) Start() {
	go rn.ticker()
}

// ticker monitors election timeouts and triggers heartbeats for leaders.
func (rn *RaftNode) ticker() {
	for {
		time.Sleep(15 * time.Millisecond)

		rn.mu.Lock()
		role := rn.role
		elapsed := time.Since(rn.lastHeartbeat)
		timeout := rn.electionTimeout
		rn.mu.Unlock()

		if role == Leader {
			if elapsed >= rn.heartbeatInterval {
				go rn.broadcastHeartbeats()
			}
		} else if elapsed >= timeout {
			rn.startElection()
		}
	}
}

// startElection transitions the node to Candidate and requests votes concurrently.
func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	rn.role = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id
	rn.resetElectionTimeout()
	rn.lastHeartbeat = time.Now()

	term := rn.currentTerm
	lastLog := rn.log[len(rn.log)-1]
	lastLogIndex := lastLog.Index
	lastLogTerm := lastLog.Term

	// Snapshot peer list to avoid holding mutex across network calls
	peers := make(map[string]raftpb.RaftServiceClient, len(rn.peers))
	for id, client := range rn.peers {
		peers[id] = client
	}
	rn.mu.Unlock()

	totalNodes := len(peers) + 1
	votesNeeded := totalNodes/2 + 1
	var votesReceived int32 = 1 // Voted for self

	req := &raftpb.RequestVoteArgs{
		Term:         term,
		CandidateId:  rn.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	for peerID, client := range peers {
		go func(id string, cli raftpb.RaftServiceClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			reply, err := cli.RequestVote(ctx, req)
			if err != nil {
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			// Ignore stale response if term changed or node stepped down
			if rn.currentTerm != term || rn.role != Candidate {
				return
			}

			// Rule: Step down immediately if encountering higher term
			if reply.Term > rn.currentTerm {
				rn.currentTerm = reply.Term
				rn.role = Follower
				rn.votedFor = ""
				rn.resetElectionTimeout()
				return
			}

			if reply.VoteGranted {
				votes := atomic.AddInt32(&votesReceived, 1)
				if int(votes) >= votesNeeded && rn.role == Candidate {
					rn.becomeLeader()
				}
			}
		}(peerID, client)
	}
}

// becomeLeader initializes leader state and immediately sends heartbeats.
func (rn *RaftNode) becomeLeader() {
	rn.role = Leader
	lastLogIndex := uint64(len(rn.log) - 1)
	for peerID := range rn.peers {
		rn.nextIndex[peerID] = lastLogIndex + 1
		rn.matchIndex[peerID] = 0
	}
	rn.lastHeartbeat = time.Now()
	go rn.broadcastHeartbeats()
}

// broadcastHeartbeats sends empty AppendEntries RPCs to all peers.
func (rn *RaftNode) broadcastHeartbeats() {
	rn.mu.Lock()
	if rn.role != Leader {
		rn.mu.Unlock()
		return
	}
	rn.lastHeartbeat = time.Now()
	term := rn.currentTerm
	commitIndex := rn.commitIndex

	peers := make(map[string]raftpb.RaftServiceClient, len(rn.peers))
	for id, client := range rn.peers {
		peers[id] = client
	}
	rn.mu.Unlock()

	for peerID, client := range peers {
		go func(id string, cli raftpb.RaftServiceClient) {
			rn.mu.Lock()
			if rn.role != Leader || rn.currentTerm != term {
				rn.mu.Unlock()
				return
			}
			prevIndex := rn.nextIndex[id] - 1
			prevTerm := rn.log[prevIndex].Term
			req := &raftpb.AppendEntriesArgs{
				Term:         term,
				LeaderId:     rn.id,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
				Entries:      nil, // Heartbeat carries no entries
				LeaderCommit: commitIndex,
			}
			rn.mu.Unlock()

			ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
			defer cancel()

			reply, err := cli.AppendEntries(ctx, req)
			if err != nil {
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			if reply.Term > rn.currentTerm {
				rn.currentTerm = reply.Term
				rn.role = Follower
				rn.votedFor = ""
				rn.resetElectionTimeout()
			}
		}(peerID, client)
	}
}
