# Distributed Raft Consensus Key-Value Store

A production-grade, distributed, consensus-based key-value store implemented in Go using gRPC. This system features leader elections, client request forwarding proxies, and replicated log consensus built from scratch in accordance with the core Raft spec.

---

## Architecture Overview

```
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
1. **Client CLI**: Allows clients to execute mutations (PUT/DELETE) and queries (GET) against any cluster node.
2. **Follower Proxy**: Non-leader nodes intercept write commands and automatically forward them to the current Leader, achieving full write-transparency.
3. **Consensus Engine**: Implemented via Raft election states, term increments, randomized timeouts (150ms-300ms), and 50ms heartbeat tickers.
4. **Log Replication**: Ensures all writes are written to a quorum of nodes (2 out of 3) before committing and executing them on the thread-safe `KVStore` state machine.

---

## Directory Structure

```
├── client/          # CLI implementation for testing cluster actions
├── pb/              # Protobuf file and Go compiled gRPC stubs
├── server/          # Core Raft state machine and gRPC server
├── store/           # Thread-safe in-memory key-value database
├── Dockerfile       # Multi-stage optimized production builder
└── docker-compose.yml # 3-node container orchestration spec
```

---

## Quickstart

### Prerequisites
- Go 1.20+
- Docker & Docker Compose (optional)

### Method 1: Local Launch (Pre-compiled)
1. **Build the server and client binaries**:
   ```bash
   go build -o kv-server ./server
   go build -o kv-client ./client
   ```

2. **Start the 3 local nodes in separate terminal windows (or background)**:
   ```bash
   ./kv-server --id 1 --port 8001 --peers "2:localhost:8002,3:localhost:8003" > node1.log 2>&1 &
   ./kv-server --id 2 --port 8002 --peers "1:localhost:8001,3:localhost:8003" > node2.log 2>&1 &
   ./kv-server --id 3 --port 8003 --peers "1:localhost:8001,2:localhost:8002" > node3.log 2>&1 &
   ```

3. **Verify the cluster using the client**:
   - Write to the leader node:
     ```bash
     ./kv-client --addr localhost:8003 put appKey appValue
     ```
   - Fetch the value from a follower node:
     ```bash
     ./kv-client --addr localhost:8001 get appKey
     ```

### Method 2: Docker Compose Orchestration
1. **Start the cluster**:
   ```bash
   docker-compose up --build
   ```
   This will spin up three containerized server nodes mapping host ports `8001`, `8002`, and `8003` to the cluster on an isolated bridge network.

2. **Execute commands using the client**:
   ```bash
   go build -o kv-client ./client
   ./kv-client --addr localhost:8001 put dockerKey dockerValue
   ./kv-client --addr localhost:8002 get dockerKey
   ```

---

## Verification Logs

Below are real logs and terminal outputs captured during the verification phases:

### 1. Leader Election (Term 1)
When the nodes boot up, they start in `Follower` state. Node 3 times out first, starts an election, gets votes from Node 1 and Node 2, and becomes Leader:
```log
# Node 3 Logs:
2026/06/16 15:38:13 [Node 3] Started election for Term 1
2026/06/16 15:38:13 [Node 3] Received vote from Peer 1, total votes: 2
2026/06/16 15:38:13 [Node 3] Became Leader for Term 1!

# Node 1 Logs:
2026/06/16 15:38:13 [Node 1] Received RequestVote from Candidate 3 for Term 1. Local Term: 0, votedFor: -1
2026/06/16 15:38:13 [Node 1] Stepping down to Follower. Term: 0 -> 1
2026/06/16 15:38:13 [Node 1] Granted vote to Candidate 3 for Term 1
```

### 2. Client Proxy Forwarding & Log replication
A client sends a `PUT` command to Node 1 (a Follower). Node 1 forwards it to Node 3 (the Leader). The Leader appends, replicates it, and commits:
```log
# Client output:
$ ./kv-client --addr localhost:8001 put forwardedKey forwardedValue
Put Success: true, Message: Key successfully replicated and committed

# Node 1 Logs (Follower Proxy):
2026/06/16 15:39:03 [Node 1] Forwarding Put request to Leader 3
2026/06/16 15:39:03 [Node 1] Follower updated commitIndex from 1 to 2
2026/06/16 15:39:03 [Node 1] Applied PUT key: "forwardedKey", value: "forwardedValue" at log index 2

# Node 3 Logs (Leader):
2026/06/16 15:39:03 [Node 3] Leader appended PUT at log index 2. Key: "forwardedKey"
2026/06/16 15:39:03 [Node 3] Leader updated commitIndex from 1 to 2
2026/06/16 15:39:03 [Node 3] Applied PUT key: "forwardedKey", value: "forwardedValue" at log index 2
```

### 3. Failover Re-election (Term 2)
When the active leader is terminated, the followers detect the timeout and re-elect a new leader:
```log
# Leader Node 2 is killed. After ~200ms, Node 1 times out:
2026/06/16 15:19:31 [Node 1] Started election for Term 2
2026/06/16 15:19:31 [Node 1] Received vote from Peer 3, total votes: 2
2026/06/16 15:19:31 [Node 1] Became Leader for Term 2!

# Node 3 grants its vote to Node 1:
2026/06/16 15:19:31 [Node 3] Received RequestVote from Candidate 1 for Term 2. Local Term: 1, votedFor: 2
2026/06/16 15:19:31 [Node 3] Stepping down to Follower. Term: 1 -> 2
2026/06/16 15:19:31 [Node 3] Granted vote to Candidate 1 for Term 2
```
