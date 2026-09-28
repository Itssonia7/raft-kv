package raft

import (
	"bytes"
	"fmt"
	"net"
	"testing"
	"time"

	raftpb "raft-kv/pkg/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type clusterNode struct {
	id       string
	node     *RaftNode
	server   *grpc.Server
	listener net.Listener
	applyCh  chan ApplyMsg
}

func setupCluster(t *testing.T, count int) ([]*clusterNode, func()) {
	nodes := make([]*clusterNode, count)
	listeners := make([]net.Listener, count)
	addrs := make([]string, count)

	// 1. Allocate listeners on random OS-assigned ports
	for i := 0; i < count; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to listen: %v", err)
		}
		listeners[i] = lis
		addrs[i] = lis.Addr().String()
	}

	// 2. Initialize node containers
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("node-%d", i)
		applyCh := make(chan ApplyMsg, 100)
		grpcServer := grpc.NewServer()

		nodes[i] = &clusterNode{
			id:       id,
			server:   grpcServer,
			listener: listeners[i],
			applyCh:  applyCh,
		}
	}

	// 3. Wire gRPC clients to each peer and attach consensus node
	for i := 0; i < count; i++ {
		peers := make(map[string]raftpb.RaftServiceClient)
		for j := 0; j < count; j++ {
			if i == j {
				continue
			}
			peerID := fmt.Sprintf("node-%d", j)
			conn, err := grpc.NewClient(addrs[j], grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatalf("failed to dial peer %s: %v", addrs[j], err)
			}
			peers[peerID] = raftpb.NewRaftServiceClient(conn)
		}

		nodes[i].node = NewRaftNode(nodes[i].id, peers, nodes[i].applyCh)
		raftpb.RegisterRaftServiceServer(nodes[i].server, nodes[i].node)
	}

	// 4. Start serving gRPC RPCs concurrently
	for i := 0; i < count; i++ {
		go func(cn *clusterNode) {
			_ = cn.server.Serve(cn.listener)
		}(nodes[i])
	}

	cleanup := func() {
		for _, n := range nodes {
			n.server.Stop()
			_ = n.listener.Close()
		}
	}

	return nodes, cleanup
}

func TestLeaderElection(t *testing.T) {
	nodes, cleanup := setupCluster(t, 3)
	defer cleanup()

	for _, cn := range nodes {
		cn.node.Start()
	}

	time.Sleep(1 * time.Second)

	leaderCount := 0
	var leaderTerm uint64

	for _, cn := range nodes {
		term, isLeader := cn.node.GetState()
		if isLeader {
			leaderCount++
			leaderTerm = term
		}
	}

	if leaderCount != 1 {
		t.Fatalf("expected exactly 1 leader in cluster, found %d", leaderCount)
	}

	if leaderTerm == 0 {
		t.Fatalf("expected leader term >= 1, got %d", leaderTerm)
	}
}

func TestReplication(t *testing.T) {
	nodes, cleanup := setupCluster(t, 3)
	defer cleanup()

	for _, cn := range nodes {
		cn.node.Start()
	}

	time.Sleep(1 * time.Second)

	var leader *clusterNode
	for _, cn := range nodes {
		_, isLeader := cn.node.GetState()
		if isLeader {
			leader = cn
			break
		}
	}

	if leader == nil {
		t.Fatalf("no leader elected")
	}

	cmd := []byte("set user_id 42")
	index, _, isLeader := leader.node.Propose(cmd)
	if !isLeader {
		t.Fatalf("node unexpectedly stepped down as leader")
	}

	if index != 1 {
		t.Fatalf("expected first entry index to be 1, got %d", index)
	}

	// Verify all cluster nodes commit and stream the exact command across applyCh
	for _, cn := range nodes {
		select {
		case msg := <-cn.applyCh:
			if !msg.CommandValid {
				t.Fatalf("node %s received invalid command message", cn.id)
			}
			if msg.CommandIndex != 1 {
				t.Fatalf("node %s expected command index 1, got %d", cn.id, msg.CommandIndex)
			}
			if !bytes.Equal(msg.Command, cmd) {
				t.Fatalf("node %s command payload mismatch: expected %s, got %s", cn.id, string(cmd), string(msg.Command))
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for node %s to apply entry at index %d", cn.id, index)
		}
	}
}
