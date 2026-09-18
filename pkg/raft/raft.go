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
	applyCh   chan ApplyMsg
	applyCond *sync.Cond

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
	rn.applyCond = sync.NewCond(&rn.mu)

	// 1-based log indexing: initialize index 0 with a dummy entry
	rn.log = append(rn.log, &raftpb.LogEntry{
		Index: 0,
		Term:  0,
		Data:  nil,
	})

	rn.resetElectionTimeout()
	go rn.applyEntriesLoop()

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

// Propose appends a client command to the leader's replicated log.
// Returns the assigned log index, term, and true if this node is the leader.
func (rn *RaftNode) Propose(command []byte) (uint64, uint64, bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.role != Leader {
		return 0, rn.currentTerm, false
	}

	index := uint64(len(rn.log))
	term := rn.currentTerm

	rn.log = append(rn.log, &raftpb.LogEntry{
		Index: index,
		Term:  term,
		Data:  command,
	})

	rn.matchIndex[rn.id] = index

	// Replicate immediately rather than waiting for next heartbeat tick
	go rn.broadcastHeartbeats()

	return index, term, true
}

// applyEntriesLoop continuously streams committed log entries across applyCh.
// It releases rn.mu before sending to prevent deadlocks with slow consumers.
func (rn *RaftNode) applyEntriesLoop() {
	for {
		rn.mu.Lock()
		for rn.commitIndex <= rn.lastApplied {
			rn.applyCond.Wait()
		}

		var toApply []*raftpb.LogEntry
		for i := rn.lastApplied + 1; i <= rn.commitIndex; i++ {
			toApply = append(toApply, rn.log[i])
		}
		rn.lastApplied = rn.commitIndex
		rn.mu.Unlock()

		for _, entry := range toApply {
			rn.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Data,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
		}
	}
}
