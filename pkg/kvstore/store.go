package kvstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"raft-kv/pkg/raft"
)

// ErrNotLeader is returned when a client attempts a mutation on a follower node.
var ErrNotLeader = errors.New("node is not the cluster leader")

// ErrTimeout is returned if consensus quorum takes longer than the operation deadline.
var ErrTimeout = errors.New("operation timed out waiting for consensus commitment")

// OpType identifies the state machine mutation operation.
type OpType string

const (
	OpPut    OpType = "PUT"
	OpGet    OpType = "GET"
	OpDelete OpType = "DELETE"
)

// Command represents a replicated state machine instruction serialized across Raft logs.
type Command struct {
	Op       OpType `json:"op"`
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`
	ClientID string `json:"client_id"`
	ReqID    uint64 `json:"req_id"`
}

// CommandResult represents the outcome of executing a committed command.
type CommandResult struct {
	Value   string
	Success bool
	Err     error
}

// KVStore is the replicated in-memory storage engine driven by Raft consensus.
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string

	// Consensus engine hook
	rf      *raft.RaftNode
	applyCh chan raft.ApplyMsg

	// Client notification channels: map[logIndex] -> chan CommandResult
	waitChans map[uint64]chan CommandResult

	// Client deduplication table: ClientID -> highest processed ReqID
	lastExecuted map[string]uint64

	stopCh chan struct{}
}

// NewKVStore initializes an in-memory key-value engine connected to Raft's applyCh.
func NewKVStore(rf *raft.RaftNode, applyCh chan raft.ApplyMsg) *KVStore {
	kv := &KVStore{
		data:         make(map[string]string),
		rf:           rf,
		applyCh:      applyCh,
		waitChans:    make(map[uint64]chan CommandResult),
		lastExecuted: make(map[string]uint64),
		stopCh:       make(chan struct{}),
	}

	go kv.applyLoop()
	return kv
}

// Close terminates background apply routines.
func (kv *KVStore) Close() {
	close(kv.stopCh)
}

// Execute processes a client command through Raft consensus and waits for commitment.
func (kv *KVStore) Execute(cmd Command, timeout time.Duration) (CommandResult, error) {
	encoded, err := json.Marshal(cmd)
	if err != nil {
		return CommandResult{}, fmt.Errorf("failed to encode command: %w", err)
	}

	// 1. Submit command to Raft consensus engine
	index, _, isLeader := kv.rf.Propose(encoded)
	if !isLeader {
		return CommandResult{}, ErrNotLeader
	}

	// 2. Register a wait channel for this log index
	kv.mu.Lock()
	ch := make(chan CommandResult, 1)
	kv.waitChans[index] = ch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.waitChans, index)
		kv.mu.Unlock()
	}()

	// 3. Await quorum commitment via applyCh or timeout
	select {
	case result := <-ch:
		return result, result.Err
	case <-time.After(timeout):
		return CommandResult{}, ErrTimeout
	case <-kv.stopCh:
		return CommandResult{}, errors.New("server stopped")
	}
}

// applyLoop reads committed entries from Raft's applyCh and executes them sequentially.
func (kv *KVStore) applyLoop() {
	for {
		select {
		case <-kv.stopCh:
			return
		case msg := <-kv.applyCh:
			if !msg.CommandValid {
				continue
			}

			kv.mu.Lock()
			var cmd Command
			if err := json.Unmarshal(msg.Command, &cmd); err != nil {
				kv.mu.Unlock()
				continue
			}

			result := CommandResult{Success: true}

			// Linearizable deduplication check (§8)
			isDuplicate := cmd.ClientID != "" && kv.lastExecuted[cmd.ClientID] >= cmd.ReqID

			if !isDuplicate {
				switch cmd.Op {
				case OpPut:
					kv.data[cmd.Key] = cmd.Value
				case OpDelete:
					delete(kv.data, cmd.Key)
				case OpGet:
					// Value read is fetched directly from state
				}

				if cmd.ClientID != "" {
					kv.lastExecuted[cmd.ClientID] = cmd.ReqID
				}
			}

			// Value for GET is retrieved from current committed state
			if val, exists := kv.data[cmd.Key]; exists {
				result.Value = val
			} else if cmd.Op == OpGet {
				result.Success = false
				result.Err = errors.New("key not found")
			}

			// Notify waiting RPC client handler if this node proposed the entry
			if ch, exists := kv.waitChans[msg.CommandIndex]; exists {
				ch <- result
			}

			kv.mu.Unlock()
		}
	}
}
