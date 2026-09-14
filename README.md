python3 -c "
import urllib.request
content = '''# raft-kv

A distributed, fault-tolerant Key-Value store built from scratch in Go using the **Raft Consensus Protocol** (Ongaro & Ousterhout).

Engineered to explore low-level systems fundamentals: consensus invariants, linearizability, network partition handling, and synchronous disk persistence.

---

## System Overview

\`raft-kv\` implements a Replicated State Machine (RSM) architecture:
* **Consensus Engine (\`pkg/raft\`)**: Pure Raft consensus managing leader election, log replication, and quorum commitments.
* **State Machine (\`pkg/kvstore\`)**: Decoupled in-memory key-value engine listening to committed commands via Go channels (\`applyCh\`).
* **Transport (\`pkg/proto\`)**: High-throughput inter-node RPC communication over gRPC and Protocol Buffers.
* **Storage (\`pkg/kvstore\`)**: Synchronous write-ahead log (WAL) on disk via \`os.File\` and \`fsync\` for crash recovery.
* **Chaos Harness (\`pkg/chaos\`)**: Network interceptor simulating packet loss, asymmetric latency, and split-brain network partitions.

---

## High-Level Architecture

\`\`\`text
       +---------------------------------------------+
       |                 Client CLI                  |
       +---------------------------------------------+
                              │ gRPC (Put / Get / Delete)
                              ▼
+───────────────────────────────────────────────────────────+
│                        NODE SERVER                        │
│                                                           │
│  +─────────────────────────────────────────────────────+  │
│  │             Raft Consensus Core (Engine)            │  │
│  │   [Role State]     [Timers]      [Replicated Log]   │  │
│  +─────────────────────────────────────────────────────+  │
│         │                                   │             │
│         │ gRPC RPCs                         │ applyCh     │
│         │ (Vote, AppendEntries)             │             │
│         ▼                                   ▼             │
│  +─────────────────────+         +─────────────────────+  │
│  │  Transport Layer    │         │  KV State Machine   │  │
│  │  (gRPC + Chaos)     │         │  (In-Memory Engine) │  │
│  +─────────────────────+         +─────────────────────+  │
│         │                                   │             │
│         ▼                                   ▼             │
│  +─────────────────────────────────────────────────────+  │
│  │         Persistent WAL & Point-in-Time Snapshot     │  │
│  +─────────────────────────────────────────────────────+  │
+───────────────────────────────────────────────────────────+
\`\`\`

---

## Project Directory Structure

\`\`\`text
raft-kv/
├── cmd/
│   ├── server/          # Server daemon entrypoint
│   └── client/          # Interactive CLI client
├── pkg/
│   ├── raft/            # Core Raft consensus implementation
│   ├── kvstore/         # State machine & WAL storage engine
│   ├── proto/           # Protobuf definitions & generated gRPC stubs
│   └── chaos/           # Network fault injection proxy / test harness
├── go.mod
├── go.sum
└── README.md
\`\`\`

---

## Roadmap & Milestones

- [x] **Phase 0: Foundation** — Toolchain setup, workspace scaffolding, module initialization.
- [ ] **Phase 1: Wire Protocol & Node State** — Protobuf contracts, core node struct, and \`RequestVote\` RPC.
- [ ] **Phase 2: Leader Election & Heartbeats** — Randomized election timers, candidate campaigns, and role transitions.
- [ ] **Phase 3: Log Replication & Quorum** — \`AppendEntries\` pipeline, log matching consistency checks, and commit advance.
- [ ] **Phase 4: Key-Value Engine** — Decoupled state machine execution across \`applyCh\`.
- [ ] **Phase 5: Durability & Compaction** — Disk WAL with synchronous \`fsync\` and snapshot compaction.
- [ ] **Phase 6: Chaos & Split-Brain Testing** — Partition injection, dropped packets, and recovery verification.
- [ ] **Phase 7: Client CLI & Routing** — Public API with linearizable reads and follower-to-leader redirects.

---

## Prerequisites

* **Go**: \`1.22+\`
* **Protobuf Compiler**: \`protoc\` (v3+)
* **Go Protoc Plugins**: \`protoc-gen-go\`, \`protoc-gen-go-grpc\`

---

## References

* Ongaro, D., & Ousterhout, J. (2014). *In Search of an Understandable Consensus Algorithm (Extended Version)*. USENIX ATC '14.
'''

with open('README.md', 'w') as f:
    f.write(content)
"