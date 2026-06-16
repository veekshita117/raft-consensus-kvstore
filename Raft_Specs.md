# Target Specification: Distributed Raft Consensus-Based Key-Value Store

## Architecture Rules
1. Language: Go (Golang). Clean, idiomatic concurrency.
2. Communication Layer: gRPC and Protocol Buffers (.proto). No raw TCP sockets.
3. Cluster Size: 3 separate node instances running on localhost (ports: 8001, 8002, 8003).

## Core State Variables per Node
- currentTerm: Latest term server has seen (initialized to 0).
- votedFor: CandidateId that received vote in current term (or null).
- log[]: Log entries containing state machine commands and the term when received.
- commitIndex: Index of highest log entry known to be committed.
- role: Follower, Candidate, or Leader.

## Core Implementation Requirements
- Concurrency: Protect all shared memory state adjustments using sync.Mutex.
- Random Timers: Election timeout must be randomized between 150ms–300ms per node to avoid split-brain ties.
- Heartbeats: Leader must send AppendEntries RPC heartbeats exactly every 50ms to maintain authority.