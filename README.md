# Distributed Raft Consensus Key-Value Store

A production-grade, distributed, consensus-based key-value store implemented in Go using gRPC. This system features leader elections, client request forwarding proxies, and replicated log consensus built from scratch in accordance with the core Raft specification, enhanced with production-ready optimizations.

---

## **Architecture Overview**

```text
                      +-------------------+
                      |    Client CLI     |
                      +-------------------+
                                |
               (Forward request if sent to follower)
                                |
                                v
                      +-------------------+
                      |   Follower Proxy  | (Transparent Read/Write Routing)
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

### **Component Details**

1. **Client CLI**: Connects to any cluster node to perform read (`GET`) and write (`PUT`, `DELETE`) operations.
2. **Transparent Forwarding Proxy**: Follower nodes intercept all mutations (`PUT`, `DELETE`) and read (`GET`) requests, forwarding them transparently via gRPC to the active leader, preventing dirty reads and enforcing strict linearizability.
3. **Consensus Engine**: Drives state changes between Follower, Candidate, and Leader roles with randomized election timeouts (150ms–300ms) monitored by a highly precise 10ms ticker to eliminate drift.
4. **Dynamic Quorum & Log Commit**: Supports arbitrary odd or even cluster configurations. Commits logs only when replicated to a dynamic majority of nodes.
5. **Crash-Safe Persistence**: Synchronously flushes critical server state variables to disk to prevent safety violations (such as double-voting) across node restarts.
6. **Fast Log Catch-Up Recovery**: Leaders bypass mismatched follower log sequences in a single round-trip using conflict term and index indicators, rather than step-by-step backtracking.

---

## **System Specifications**

### **Protocol Rules & Constraints**
- **Runtime**: Go (Golang) with clean, concurrent-safe logic and full synchronization structures.
- **Communication Layer**: Protobuf and gRPC interfaces for cluster-wide Consensus and Key-Value services.
- **Topographical Flexibility**: Supports arbitrary cluster configurations. Local environments default to 3 nodes bound on TCP loopbacks (ports `8001`, `8002`, `8003`).

### **Core State Variables (Per-Node)**
- `currentTerm`: Current term number (persisted, initialized to `0`).
- `votedFor`: Candidate identifier that received a vote in this term (persisted, defaults to `-1`).
- `log[]`: Log entries containing state machine mutation commands and term metadata.
- `commitIndex`: Index of the highest log entry known to be committed.
- `lastApplied`: Index of the highest log entry applied to the state machine.
- `role`: Role identifier (`Follower`, `Candidate`, or `Leader`).

### **Robust Implementation Specifications**
- **Linearizable Follower Reads**: Follower nodes intercept `GET` operations and forward them via gRPC to the active leader. The leader serves the read from its local state machine, ensuring stale follower engines never return dirty data.
- **Lightweight State Persistence**: Every time `currentTerm` or `votedFor` changes, the server synchronously flushes a JSON payload to a dedicated local file `raft_state_[id].json`. During node boot initialization, this state is restored prior to starting listeners to maintain Raft safety guarantees.
- **Dynamic Quorum Calculations**: Replaces hardcoded majority checks. Dynamic quorum size is determined as:
  $$\text{quorum} = \frac{\text{len(peers)} + 1}{2} + 1$$
  During commit log updates, matches are sorted ascending, and the median index matched by a quorum is selected using `N := matches[len(matches)-quorum]`.
- **Fast-Log Catch-Up Backoff**: When a follower rejects an `AppendEntries` request:
  - If its log is too short, it returns `ConflictTerm = -1` and `ConflictIndex = len(log)`.
  - If a term mismatch occurs, it returns `ConflictTerm` of the mismatched entry and the first index it stores for that term.
  - The leader evaluates this feedback and immediately moves the peer's `nextIndex` back to bypass the entire mismatched block in one round-trip.

---

## **Directory Structure**

```text
├── client/            # CLI client tool for key-value queries and mutations
├── pb/                # Protobuf API definitions and compiled gRPC code stubs
├── server/            # Core Raft state machine and gRPC server engine
├── store/             # Thread-safe in-memory key-value database engine
├── Dockerfile         # Multi-stage container builder
└── docker-compose.yml # 3-node container deployment orchestrator
```

---

## **Quickstart**

### **Prerequisites**
- Go 1.22+
- Docker Engine & Docker Compose V2

### **Method 1: Local Native Launch**

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
   - Write to a cluster node:
     ```bash
     ./kv-client --addr localhost:8002 put raftKey raftValue
     ```
   - Retrieve the value (proxied to the leader, ensuring consistency):
     ```bash
     ./kv-client --addr localhost:8001 get raftKey
     ```

### **Method 2: Docker Compose Orchestration**

1. **Start the containerized cluster**:
   ```bash
   docker compose up --build
   ```

2. **Execute client requests against the network**:
   ```bash
   ./kv-client --addr localhost:8001 put dockerKey dockerValue
   ./kv-client --addr localhost:8003 get dockerKey
   ```

---

## **Verification Logs**

### **1. Leader Election (Term 233)**
The candidate starts an election, increments its term to 233, writes its state to disk, and gathers votes to establish leadership.

```log
2026/06/16 22:05:10 [Node 1] Started election for Term 233
2026/06/16 22:05:10 [Node 1] Received vote from Peer 2, total votes: 2
2026/06/16 22:05:10 [Node 1] Became Leader for Term 233!
```

### **2. Client Read/Write Routing & Log Replication**

**Client Command Execution:**
```bash
$ ./kv-client --addr localhost:8003 put testKey testValue
Put Success: true, Message: Key successfully replicated and committed
```

**Node 3 Logs (Follower):**
```log
2026/06/16 22:05:40 [Node 3] Forwarding Put request to Leader 1
```

**Node 1 Logs (Leader):**
```log
2026/06/16 22:05:40 [Node 1] Leader appended PUT at log index 12. Key: "testKey"
2026/06/16 22:05:40 [Node 1] Leader updated commitIndex from 11 to 12
2026/06/16 22:05:40 [Node 1] Applied PUT key: "testKey", value: "testValue" at log index 12
```

**Client Read Consistency Verification (GET Query):**
```bash
$ ./kv-client --addr localhost:8002 get testKey
Get Success: true, Value: testValue, Message: Key found
```

**Node 2 Logs (Follower):**
```log
2026/06/16 22:05:48 [Node 2] Forwarding Get request to Leader 1
```
## Known Limitations & Future Scope

### 1. Log Compaction & Snapshotting (Raft §7)
* **Limitation**: Logs grow unboundedly in memory. A restarted or heavily lagging node must replay the entire log history entry-by-entry from the leader to catch up.
* **Future Scope**: Implement checkpoint snapshotting to periodically serialize the KV-store state to disk and truncate old logs, enabling instant state transfers.

### 2. Startup Initialization Safety
* **Limitation**: The disk-restore function (`readPersistedState`) reads the JSON state file on startup without holding the mutex lock. It is safe during sequential bootstrap before listeners start, but introduces a latent data race if refactored into runtime logic later.
* **Future Scope**: Wrap the initialization sequence inside explicit lock/unlock blocks to ensure absolute, future-proof thread safety.

### 3. Static Cluster Membership (Raft §6)
* **Limitation**: The cluster configuration is static, requiring fixed peer network configurations at boot. Adding or removing nodes dynamically without cluster downtime is unsupported.
* **Future Scope**: Implement Raft joint consensus to allow dynamic, online cluster resizing and membership changes.