# raft-kv

[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?style=flat&logo=go)](https://golang.org)
[![Consensus](https://img.shields.io/badge/Consensus-Raft-blue)](https://raft.github.io/)
[![Transport](https://img.shields.io/badge/Transport-gRPC%20%2F%20Protobuf-darkgreen)](https://grpc.io/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A distributed, fault-tolerant, replicated Key-Value store built from scratch in Go, powered by the **Raft Consensus Protocol** (Ongaro & Ousterhout).

Designed to explore low-level systems engineering: linearizable state machine replication, network partition resilience, asynchronous RPC pipelines, and synchronous disk persistence.

---

## Architecture Overview

`raft-kv` follows the classic **Replicated State Machine (RSM)** pattern. Consensus is completely decoupled from state machine execution and storage.

+---------------------------------------------+
   |               Client (CLI/API)              |
   +---------------------------------------------+
                          │
                          │ gRPC (Put / Get / Delete)
                          ▼

+───────────────────────────────────────────────────────────+
│                        RAFT NODE                          │
│                                                           │
│  +─────────────────────────────────────────────────────+  │
│  │                 Raft Consensus Engine               │  │
│  │  - Leader Election       - Quorum Tracker           │  │
│  │  - Randomized Timers     - Replicated Log           │  │
│  +─────────────────────────────────────────────────────+  │
│         │                                   │             │
│         │ gRPC RPCs                         │ applyCh     │
│         │ (Vote, AppendEntries)             │ (channel)   │
│         ▼                                   ▼             │
│  +─────────────────────+         +─────────────────────+  │
│  │   Transport Layer   │         │     State Machine   │  │
│  │  (gRPC & Interceptor│         │  (In-Memory Store)  │  │
│  +─────────────────────+         +─────────────────────+  │
│         │                                   │             │
│         ▼                                   ▼             │
│  +─────────────────────────────────────────────────────+  │
│  │        Write-Ahead Log (WAL) & Snapshots (fsync)    │  │
│  +─────────────────────────────────────────────────────+  │
+───────────────────────────────────────────────────────────+



---

## Core Technical Features

* **Raft Consensus Core**: Leader election with randomized timeouts, term-based voting safety, log replication, and heartbeats.
* **Concurrency & Safety**: Strictly no blocking network I/O held under mutex locks to prevent deadlocks and head-of-line blocking.
* **Linearizability**: Safe read and write semantics guaranteeing strongly consistent client interactions across partitions.
* **Decoupled State Machine**: KV engine consumes committed entries asynchronously via a Go channel interface (`applyCh`).
* **Crash Resilience & Durability**: Synchronous write-ahead log (WAL) on disk via POSIX `fsync` and snapshot compaction.
* **Chaos Engineering Harness**: Fault-injection proxy simulating asymmetric network partitions, dropped packets, and latency jitter.

---

## Repository Structure


raft-kv/
├── cmd/
│   ├── server/          # Cluster node binary entrypoint
│   └── client/          # CLI client for cluster interaction
├── pkg/
│   ├── raft/            # Core Raft consensus protocol logic
│   ├── kvstore/         # In-memory key-value state machine
│   ├── proto/           # Protobuf contracts and generated gRPC stubs
│   └── chaos/           # Network chaos and partition testing harness
├── go.mod               # Module configuration
├── go.sum               # Checksums for external dependencies
└── README.md

---

## Project Roadmap

- [x] **Phase 0: Workspace Scaffolding** — Directory layout, Go module setup, Protobuf toolchain.
- [x] **Phase 1: Wire Protocol** — Protocol Buffer RPC schemas (`RequestVote`, `AppendEntries`, `InstallSnapshot`).
- [ ] **Phase 2: Raft Node State** — Term management, role transitions (Follower, Candidate, Leader), and thread-safe timers.
- [ ] **Phase 3: Leader Election** — Randomized election timeouts, candidate voting campaigns, and heartbeat broadcasts.
- [ ] **Phase 4: Log Replication** — Quorum acknowledgments, log consistency checks, and commit index advance.
- [ ] **Phase 5: Key-Value State Machine** — Applying committed entries, handling client `Get`/`Put`/`Delete` requests.
- [ ] **Phase 6: Persistence & Compaction** — Disk-backed WAL, log compaction, and snapshot distribution.
- [ ] **Phase 7: Fault-Tolerance & Chaos** — Network partition injection and recovery testing.

---

## Tech Stack

| Component | Technology |
|---|---|
| **Language** | Go (1.22+) |
| **RPC & Serialization** | gRPC / Protocol Buffers (proto3) |
| **Concurrency Model** | Goroutines, Channels, sync/atomic primitives |
| **Persistence** | Custom POSIX WAL (`os.File` + `fsync`) |

---

## References

* Ongaro, D., & Ousterhout, J. (2014). *In Search of an Understandable Consensus Algorithm (Extended Version)*. USENIX ATC '14.
