package kvstore

import (
	"fmt"
	"net"
	"testing"
	"time"

	raftpb "raft-kv/pkg/proto"
	"raft-kv/pkg/raft"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type testNode struct {
	id         string
	raftNode   *raft.RaftNode
	store      *KVStore
	grpcServer *grpc.Server
	listener   net.Listener
	applyCh    chan raft.ApplyMsg
}

func setupKVCluster(t *testing.T, count int) ([]*testNode, func()) {
	nodes := make([]*testNode, count)
	listeners := make([]net.Listener, count)
	addrs := make([]string, count)

	for i := 0; i < count; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to listen: %v", err)
		}
		listeners[i] = lis
		addrs[i] = lis.Addr().String()
	}

	for i := 0; i < count; i++ {
		nodes[i] = &testNode{
			id:         fmt.Sprintf("node-%d", i),
			grpcServer: grpc.NewServer(),
			listener:   listeners[i],
			applyCh:    make(chan raft.ApplyMsg, 100),
		}
	}

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

		nodes[i].raftNode = raft.NewRaftNode(nodes[i].id, peers, nodes[i].applyCh)
		raftpb.RegisterRaftServiceServer(nodes[i].grpcServer, nodes[i].raftNode)
		nodes[i].store = NewKVStore(nodes[i].raftNode, nodes[i].applyCh)
	}

	for i := 0; i < count; i++ {
		go func(n *testNode) {
			_ = n.grpcServer.Serve(n.listener)
		}(nodes[i])
		nodes[i].raftNode.Start()
	}

	cleanup := func() {
		for _, n := range nodes {
			n.store.Close()
			n.grpcServer.Stop()
			_ = n.listener.Close()
		}
	}

	return nodes, cleanup
}

func findLeader(nodes []*testNode) *testNode {
	for _, n := range nodes {
		_, isLeader := n.raftNode.GetState()
		if isLeader {
			return n
		}
	}
	return nil
}

func TestKVStoreOperations(t *testing.T) {
	nodes, cleanup := setupKVCluster(t, 3)
	defer cleanup()

	time.Sleep(1 * time.Second)

	leader := findLeader(nodes)
	if leader == nil {
		t.Fatalf("expected leader to be elected")
	}

	// 1. Test PUT operation
	putCmd := Command{
		Op:       OpPut,
		Key:      "cluster_name",
		Value:    "raft-kv-alpha",
		ClientID: "client-1",
		ReqID:    1,
	}
	res, err := leader.store.Execute(putCmd, 2*time.Second)
	if err != nil || !res.Success {
		t.Fatalf("PUT failed: %v", err)
	}

	// 2. Test GET operation on leader
	getCmd := Command{
		Op:       OpGet,
		Key:      "cluster_name",
		ClientID: "client-1",
		ReqID:    2,
	}
	res, err = leader.store.Execute(getCmd, 2*time.Second)
	if err != nil || !res.Success {
		t.Fatalf("GET failed: %v", err)
	}
	if res.Value != "raft-kv-alpha" {
		t.Fatalf("expected 'raft-kv-alpha', got '%s'", res.Value)
	}

	// 3. Verify replication to followers' in-memory state
	time.Sleep(200 * time.Millisecond)
	for _, n := range nodes {
		n.store.mu.RLock()
		val := n.store.data["cluster_name"]
		n.store.mu.RUnlock()
		if val != "raft-kv-alpha" {
			t.Fatalf("node %s did not replicate state; found: '%s'", n.id, val)
		}
	}

	// 4. Test DELETE operation
	delCmd := Command{
		Op:       OpDelete,
		Key:      "cluster_name",
		ClientID: "client-1",
		ReqID:    3,
	}
	res, err = leader.store.Execute(delCmd, 2*time.Second)
	if err != nil || !res.Success {
		t.Fatalf("DELETE failed: %v", err)
	}

	// 5. Test GET after DELETE
	getDeleted := Command{
		Op:       OpGet,
		Key:      "cluster_name",
		ClientID: "client-1",
		ReqID:    4,
	}
	res, err = leader.store.Execute(getDeleted, 2*time.Second)
	if err == nil {
		t.Fatalf("expected key not found error, got value: %s", res.Value)
	}
}

func TestKVDeduplication(t *testing.T) {
	nodes, cleanup := setupKVCluster(t, 3)
	defer cleanup()

	time.Sleep(1 * time.Second)

	leader := findLeader(nodes)
	if leader == nil {
		t.Fatalf("expected leader to be elected")
	}

	// Send initial write with ReqID 100
	writeCmd := Command{
		Op:       OpPut,
		Key:      "account_balance",
		Value:    "500",
		ClientID: "client-bank",
		ReqID:    100,
	}
	_, err := leader.store.Execute(writeCmd, 2*time.Second)
	if err != nil {
		t.Fatalf("initial write failed: %v", err)
	}

	// Mutate key to a new value with ReqID 101
	updateCmd := Command{
		Op:       OpPut,
		Key:      "account_balance",
		Value:    "200",
		ClientID: "client-bank",
		ReqID:    101,
	}
	_, err = leader.store.Execute(updateCmd, 2*time.Second)
	if err != nil {
		t.Fatalf("update write failed: %v", err)
	}

	// Replay duplicate stale request (ReqID 100) — must be ignored by state machine
	duplicateCmd := Command{
		Op:       OpPut,
		Key:      "account_balance",
		Value:    "9999",
		ClientID: "client-bank",
		ReqID:    100,
	}
	_, _ = leader.store.Execute(duplicateCmd, 2*time.Second)

	// Verify balance is still 200 across all nodes
	time.Sleep(200 * time.Millisecond)
	for _, n := range nodes {
		n.store.mu.RLock()
		val := n.store.data["account_balance"]
		n.store.mu.RUnlock()
		if val != "200" {
			t.Fatalf("linearizability violation: expected '200', got '%s'", val)
		}
	}
}
