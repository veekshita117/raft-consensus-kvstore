# Distributed Raft Consensus Key-Value Store

A production-grade, distributed, consensus-based key-value store implemented in Go using gRPC. This system features leader elections, client request forwarding proxies, and replicated log consensus built from scratch in accordance with the core Raft spec.

---

## Architecture Overview

```text
                      +-------------------+
                      |    Client CLI     |
                      +-------------------+
                                |
             (Forward request if sent to follower)
                                |
                                v
                      +-------------------+
                      |   Follower Proxy  | (Transparent Write Routing)
                      +-------------------+
                                |
                                | (gRPC Forward)
                                v
                      +-------------------+
                      |      Leader       | (Raft Leader State Machine)
                      +-------------------+
                         /             \
       (Replicate Log)  /               \  (Replicate Log)
                       v                 v
            +------------+             +------------+
            | Follower 1 |             | Follower 2 |
            +------------+             +------------+
                  |                          |
    (Apply Commit)|            (Apply Commit)|
                  v                          v
            +------------+             +------------+
            |  KV Store  |             |  KV Store  | (Thread-Safe In-Memory)
            +------------+             +------------+
```

### Component Details

1. **Client CLI**: Allows clients to execute mutations (`PUT`/`DELETE`) and queries (`GET`) against any cluster node.
2. **Follower Proxy**: Non-leader nodes intercept write commands and automatically forward them to the current Leader, achieving full write-transparency.
3. **Consensus Engine**: Implemented via Raft election states, term increments, randomized timeouts (150ms-300ms), and 50ms heartbeat tickers.
4. **Log Replication**: Ensures all writes are written to a quorum of nodes (2 out of 3) before committing and executing them on the thread-safe `KVStore` state machine.

---

## System Specifications

### Protocol Rules & Constraints

- **Language & Runtime**: Go (Golang) utilizing clean, idiomatic concurrency patterns.
- **Communication Layer**: High-performance gRPC and Protocol Buffers (`.proto`). No raw TCP sockets.
- **Cluster Topography**: 3 separate node instances running locally via network loops over specific assignments (ports: `8001`, `8002`, `8003`).

### Core State Variables (Per-Node)

- `currentTerm`: Latest term server has seen (initialized to `0`).
- `votedFor`: Candidate ID that received a vote in the current term (or null/unset).
- `log[]`: Log entries containing state machine mutation commands and the matching term metadata when received.
- `commitIndex`: Index of the highest log entry known by the node to be committed.
- `role`: State assignment indicator tracking whether a node is a Follower, Candidate, or Leader.

### Implementation Guardrails

- **Concurrency Primitives**: Absolute memory safety achieved by isolating all internal state variable adjustments behind a mutual exclusion lock (`sync.Mutex`).
- **Split-Brain Mitigation**: Randomized election tickers set explicitly between 150ms–300ms per instance to maximize variance and mitigate vote splits.
- **Authority Maintenance**: Active leaders generate and dispatch `AppendEntries` RPC heartbeats strictly every 50ms to suppress follower timeouts.

---

## Directory Structure

```text
├── client/            # CLI implementation for testing cluster actions
├── pb/                # Protobuf file and Go compiled gRPC stubs
├── server/            # Core Raft state machine and gRPC server
├── store/             # Thread-safe in-memory key-value database
├── Dockerfile         # Multi-stage optimized production builder
└── docker-compose.yml # 3-node container orchestration spec
```

---

## Quickstart

### Prerequisites

- Go 1.22+
- Docker Engine & Docker Compose V2

### Method 1: Local Native Launch

1. **Build the server and client binaries**:
   ```bash
   go build -o kv-server ./server/main.go
   go build -o kv-client ./client/main.go
   ```

2. **Start the 3 local nodes in separate terminal windows**:
   - **Terminal 1**:
     ```bash
     ./kv-server --id 1 --port 8001 --peers "2:localhost:8002,3:localhost:8003"
     ```
   - **Terminal 2**:
     ```bash
     ./kv-server --id 2 --port 8002 --peers "1:localhost:8001,3:localhost:8003"
     ```
   - **Terminal 3**:
     ```bash
     ./kv-server --id 3 --port 8003 --peers "1:localhost:8001,2:localhost:8002"
     ```

3. **Verify the cluster using the client**:
   - Write to the cluster leader node:
     ```bash
     ./kv-client --addr localhost:8002 put raftKey raftValue
     ```
   - Fetch the value from a follower node to verify consensus replication:
     ```bash
     ./kv-client --addr localhost:8001 get raftKey
     ```

### Method 2: Docker Compose Orchestration

1. **Start the cluster**:
   ```bash
   sudo docker compose up --build
   ```

2. **Execute commands using the client**:
   ```bash
   ./kv-client --addr localhost:8001 put dockerKey dockerValue
   ./kv-client --addr localhost:8003 get dockerKey
   ```

---

## Verification Logs

### 1. Leader Election

```log
2026/06/16 16:56:35 [Node 2] Started election for Term 60
2026/06/16 16:56:35 [Node 2] Received vote from Peer 1, total votes: 2
2026/06/16 16:56:35 [Node 2] Became Leader for Term 60!
```

### 2. Client Proxy Forwarding & Log Replication

**Client command & output:**
```bash
$ ./kv-client --addr localhost:8003 put proxyKey proxyValue
Put Success: true, Message: Key successfully replicated and committed
```

**Node 2 Logs (Leader):**
```log
2026/06/16 16:57:04 [Node 2] Leader appended PUT at log index 2. Key: "proxyKey"
2026/06/16 16:57:04 [Node 2] Leader updated commitIndex from 1 to 2
2026/06/16 16:57:04 [Node 2] Applied PUT key: "proxyKey", value: "proxyValue" at log index 2
```
