package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	raftpb "raft-kv/pkg/proto"
)

// PersistentState holds the Raft invariants that must survive node crashes.
type PersistentState struct {
	CurrentTerm uint64
	VotedFor    string
	Log         []*raftpb.LogEntry
}

// Storage handles durable disk persistence for a Raft peer.
type Storage struct {
	mu       sync.Mutex
	dir      string
	walPath  string
	snapPath string
}

// NewStorage initializes the storage directory and paths for a given peer ID.
func NewStorage(baseDir string, peerID string) (*Storage, error) {
	nodeDir := filepath.Join(baseDir, peerID)
	if err := os.MkdirAll(nodeDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage dir: %w", err)
	}

	return &Storage{
		dir:      nodeDir,
		walPath:  filepath.Join(nodeDir, "raft.wal"),
		snapPath: filepath.Join(nodeDir, "state.snap"),
	}, nil
}

// SaveRaftState atomically writes and flushes Raft persistent state to disk using fsync.
func (s *Storage) SaveRaftState(term uint64, votedFor string, log []*raftpb.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	state := PersistentState{
		CurrentTerm: term,
		VotedFor:    votedFor,
		Log:         log,
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(state); err != nil {
		return fmt.Errorf("failed to encode raft state: %w", err)
	}

	// Atomic write pattern: write to a temporary file, fsync, then rename.
	tmpFile := s.walPath + ".tmp"
	f, err := os.OpenFile(tmpFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open wal tmp file: %w", err)
	}

	if _, err := f.Write(buf.Bytes()); err != nil {
		_ = f.Close()
		return fmt.Errorf("failed to write wal data: %w", err)
	}

	// Flush dirty pages to disk hardware
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("failed to fsync wal: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close tmp wal: %w", err)
	}

	// Atomic rename replaces the existing WAL safely
	return os.Rename(tmpFile, s.walPath)
}

// ReadRaftState loads persisted Raft invariants on node reboot.
// Returns nil if no state file exists (clean cold boot).
func (s *Storage) ReadRaftState() (*PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.walPath)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to read wal file: %w", err)
	}

	var state PersistentState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&state); err != nil {
		return nil, fmt.Errorf("failed to decode wal data: %w", err)
	}

	return &state, nil
}