package raft

import (
	"math/rand"
	"sync"
	"time"

	raftpb "raft-kv/pkg/proto"
)

// NodeRole represents the operational role of a Raft consensus node.
type NodeRole int

const (
	Follower NodeRole = iota
	Candidate
	Leader
)

func (r NodeRole) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// ApplyMsg represents committed entries or snapshots sent across applyCh to the state machine.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex uint64
	CommandTerm  uint64

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  uint64
	SnapshotIndex uint64
}

// RaftNode represents an individual peer executing the consensus protocol.
type RaftNode struct {
	raftpb.UnimplementedRaftServiceServer
	mu sync.Mutex	

	// Identity and network topology
	id    string
	peers map[string]raftpb.RaftServiceClient

	// Persistent state on all servers
	currentTerm uint64
	votedFor    string
	log         []*raftpb.LogEntry

	// Volatile state on all servers
	commitIndex uint64
	lastApplied uint64
	role        NodeRole

	// Volatile state on leaders
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Decoupled state machine execution
	applyCh chan ApplyMsg

	// Timer coordination
	heartbeatInterval time.Duration
	electionTimeout   time.Duration
	lastHeartbeat     time.Time
}



// NewRaftNode creates and initializes a new peer node in the Follower state.
func NewRaftNode(id string, peers map[string]raftpb.RaftServiceClient, applyCh chan ApplyMsg) *RaftNode {
	rn := &RaftNode{
		id:                id,
		peers:             peers,
		currentTerm:       0,
		votedFor:          "",
		role:              Follower,
		commitIndex:       0,
		lastApplied:       0,
		applyCh:           applyCh,
		nextIndex:         make(map[string]uint64),
		matchIndex:        make(map[string]uint64),
		heartbeatInterval: 50 * time.Millisecond,
		lastHeartbeat:     time.Now(),
	}

	// 1-based log indexing: initialize index 0 with a dummy entry
	rn.log = append(rn.log, &raftpb.LogEntry{
		Index: 0,
		Term:  0,
		Data:  nil,
	})

	rn.resetElectionTimeout()
	return rn
}

// GetState returns the current term and whether this node believes it is the leader.
func (rn *RaftNode) GetState() (uint64, bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.currentTerm, rn.role == Leader
}

// resetElectionTimeout randomizes the election timeout between 150ms and 300ms.
// The caller must hold rn.mu or invoke this during initialization.
func (rn *RaftNode) resetElectionTimeout() {
	rn.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
}