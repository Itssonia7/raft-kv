package raft

import (
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

	// Start consensus tickers on all nodes
	for _, cn := range nodes {
		cn.node.Start()
	}

	// Allow election timeouts (150-300ms) and initial heartbeats to settle
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
