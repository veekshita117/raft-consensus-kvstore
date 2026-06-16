package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/raft-kv/pb"
	"github.com/raft-kv/store"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Role string

const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

type Command struct {
	Op    string `json:"op"` // "PUT" or "DELETE"
	Key   string `json:"key"`
	Value string `json:"val,omitempty"`
}

type peer struct {
	conn       *grpc.ClientConn
	raftClient pb.RaftServiceClient
	kvClient   pb.KVStoreClient
}

type RaftState struct {
	CurrentTerm int32 `json:"currentTerm"`
	VotedFor    int32 `json:"votedFor"`
}

type server struct {
	pb.UnimplementedKVStoreServer
	pb.UnimplementedRaftServiceServer
	id   int
	port string
	kv   *store.KVStore

	mu              sync.Mutex
	peers           map[int]*peer
	role            Role
	currentTerm     int32
	votedFor        int32
	leaderId        int
	lastResetTime   time.Time
	electionTimeout time.Duration

	// Raft State Logs & Commit Pointers
	log         []*pb.LogEntry
	commitIndex int
	lastApplied int
	commitCond  *sync.Cond

	// Leader State
	nextIndex  map[int]int
	matchIndex map[int]int
}

func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	s.mu.Lock()
	if s.role != Leader {
		leaderId := s.leaderId
		s.mu.Unlock()
		if leaderId == -1 {
			return &pb.PutResponse{Success: false, Message: "No active leader in cluster"}, nil
		}
		s.mu.Lock()
		p, exists := s.peers[leaderId]
		s.mu.Unlock()
		if !exists {
			return &pb.PutResponse{Success: false, Message: fmt.Sprintf("Leader %d connection not found", leaderId)}, nil
		}
		log.Printf("[Node %d] Forwarding Put request to Leader %d", s.id, leaderId)
		return p.kvClient.Put(ctx, req)
	}

	cmd := Command{Op: "PUT", Key: req.Key, Value: req.Value}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		s.mu.Unlock()
		return &pb.PutResponse{Success: false, Message: "Internal JSON marshal error"}, nil
	}

	entry := &pb.LogEntry{
		Index:   int32(len(s.log)),
		Term:    s.currentTerm,
		Command: string(cmdBytes),
	}
	s.log = append(s.log, entry)
	index := int(entry.Index)
	s.mu.Unlock()

	log.Printf("[Node %d] Leader appended PUT at log index %d. Key: %q", s.id, index, req.Key)

	// Trigger replication immediately
	s.triggerReplication()

	s.mu.Lock()
	defer s.mu.Unlock()
	for s.commitIndex < index && s.role == Leader {
		s.commitCond.Wait()
	}

	if s.role != Leader {
		return &pb.PutResponse{Success: false, Message: "Lost leadership during replication"}, nil
	}

	return &pb.PutResponse{
		Success: true,
		Message: "Key successfully replicated and committed",
	}, nil
}

func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	s.mu.Lock()
	if s.role != Leader {
		leaderId := s.leaderId
		s.mu.Unlock()
		if leaderId == -1 {
			return &pb.GetResponse{Value: "", Found: false, Message: "No active leader in cluster"}, nil
		}
		s.mu.Lock()
		p, exists := s.peers[leaderId]
		s.mu.Unlock()
		if !exists {
			return &pb.GetResponse{Value: "", Found: false, Message: fmt.Sprintf("Leader %d connection not found", leaderId)}, nil
		}
		log.Printf("[Node %d] Forwarding Get request to Leader %d", s.id, leaderId)
		return p.kvClient.Get(ctx, req)
	}
	s.mu.Unlock()

	log.Printf("[Node %d] Get key: %q", s.id, req.Key)
	val, exists := s.kv.Get(req.Key)
	if !exists {
		return &pb.GetResponse{
			Value:   "",
			Found:   false,
			Message: "Key not found",
		}, nil
	}
	return &pb.GetResponse{
		Value:   val,
		Found:   true,
		Message: "Key found",
	}, nil
}

func (s *server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	s.mu.Lock()
	if s.role != Leader {
		leaderId := s.leaderId
		s.mu.Unlock()
		if leaderId == -1 {
			return &pb.DeleteResponse{Success: false, Message: "No active leader in cluster"}, nil
		}
		s.mu.Lock()
		p, exists := s.peers[leaderId]
		s.mu.Unlock()
		if !exists {
			return &pb.DeleteResponse{Success: false, Message: fmt.Sprintf("Leader %d connection not found", leaderId)}, nil
		}
		log.Printf("[Node %d] Forwarding Delete request to Leader %d", s.id, leaderId)
		return p.kvClient.Delete(ctx, req)
	}

	cmd := Command{Op: "DELETE", Key: req.Key}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		s.mu.Unlock()
		return &pb.DeleteResponse{Success: false, Message: "Internal JSON marshal error"}, nil
	}

	entry := &pb.LogEntry{
		Index:   int32(len(s.log)),
		Term:    s.currentTerm,
		Command: string(cmdBytes),
	}
	s.log = append(s.log, entry)
	index := int(entry.Index)
	s.mu.Unlock()

	log.Printf("[Node %d] Leader appended DELETE at log index %d. Key: %q", s.id, index, req.Key)

	// Trigger replication immediately
	s.triggerReplication()

	s.mu.Lock()
	defer s.mu.Unlock()
	for s.commitIndex < index && s.role == Leader {
		s.commitCond.Wait()
	}

	if s.role != Leader {
		return &pb.DeleteResponse{Success: false, Message: "Lost leadership during replication"}, nil
	}

	return &pb.DeleteResponse{
		Success: true,
		Message: "Key successfully replicated and committed",
	}, nil
}

// must be called with s.mu held
func (s *server) resetElectionTimeout() {
	s.lastResetTime = time.Now()
	s.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// must be called with s.mu held
func (s *server) persistState() {
	state := RaftState{
		CurrentTerm: s.currentTerm,
		VotedFor:    s.votedFor,
	}
	data, err := json.Marshal(state)
	if err != nil {
		log.Printf("[Node %d] Error marshaling state: %v", s.id, err)
		return
	}
	filename := fmt.Sprintf("raft_state_%d.json", s.id)
	err = os.WriteFile(filename, data, 0644)
	if err != nil {
		log.Printf("[Node %d] Error writing state to file: %v", s.id, err)
	}
}

// must be called during initialization before starting listeners
func (s *server) readPersistedState() {
	filename := fmt.Sprintf("raft_state_%d.json", s.id)
	data, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Printf("[Node %d] Error reading state file: %v", s.id, err)
		return
	}
	var state RaftState
	if err := json.Unmarshal(data, &state); err != nil {
		log.Printf("[Node %d] Error unmarshaling persisted state: %v", s.id, err)
		return
	}
	s.currentTerm = state.CurrentTerm
	s.votedFor = state.VotedFor
	log.Printf("[Node %d] Restored state from disk: currentTerm=%d, votedFor=%d", s.id, s.currentTerm, s.votedFor)
}

// RequestVote RPC handler
func (s *server) RequestVote(ctx context.Context, req *pb.RequestVoteArgs) (*pb.RequestVoteReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("[Node %d] Received RequestVote from Candidate %d for Term %d. Local Term: %d, votedFor: %d",
		s.id, req.CandidateId, req.Term, s.currentTerm, s.votedFor)

	if req.Term < s.currentTerm {
		return &pb.RequestVoteReply{
			Term:        s.currentTerm,
			VoteGranted: false,
		}, nil
	}

	if req.Term > s.currentTerm {
		s.stepDown(req.Term)
	}

	lastLogIndex := len(s.log) - 1
	lastLogTerm := s.log[lastLogIndex].Term

	logUpToDate := false
	if req.LastLogTerm > lastLogTerm {
		logUpToDate = true
	} else if req.LastLogTerm == lastLogTerm && req.LastLogIndex >= int32(lastLogIndex) {
		logUpToDate = true
	}

	if (s.votedFor == -1 || s.votedFor == req.CandidateId) && logUpToDate {
		s.votedFor = req.CandidateId
		s.resetElectionTimeout()
		s.persistState()
		log.Printf("[Node %d] Granted vote to Candidate %d for Term %d", s.id, req.CandidateId, req.Term)
		return &pb.RequestVoteReply{
			Term:        s.currentTerm,
			VoteGranted: true,
		}, nil
	}

	log.Printf("[Node %d] Denied vote to Candidate %d for Term %d. LogUpToDate: %v", s.id, req.CandidateId, req.Term, logUpToDate)
	return &pb.RequestVoteReply{
		Term:        s.currentTerm,
		VoteGranted: false,
	}, nil
}

// AppendEntries RPC handler
func (s *server) AppendEntries(ctx context.Context, req *pb.AppendEntriesArgs) (*pb.AppendEntriesReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Term < s.currentTerm {
		return &pb.AppendEntriesReply{
			Term:    s.currentTerm,
			Success: false,
		}, nil
	}

	if req.Term > s.currentTerm || s.role == Candidate {
		s.stepDown(req.Term)
	}

	s.leaderId = int(req.LeaderId)
	s.resetElectionTimeout()

	// Validate prevLogIndex & prevLogTerm
	if int(req.PrevLogIndex) >= len(s.log) {
		return &pb.AppendEntriesReply{
			Term:          s.currentTerm,
			Success:       false,
			ConflictTerm:  -1,
			ConflictIndex: int32(len(s.log)),
		}, nil
	}
	if s.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		conflictTerm := s.log[req.PrevLogIndex].Term
		conflictIndex := req.PrevLogIndex
		for conflictIndex > 0 && s.log[conflictIndex-1].Term == conflictTerm {
			conflictIndex--
		}
		return &pb.AppendEntriesReply{
			Term:          s.currentTerm,
			Success:       false,
			ConflictTerm:  conflictTerm,
			ConflictIndex: int32(conflictIndex),
		}, nil
	}

	// Append entries / truncate conflicts
	for _, entry := range req.Entries {
		idx := int(entry.Index)
		if idx < len(s.log) {
			if s.log[idx].Term != entry.Term {
				log.Printf("[Node %d] Conflicting entry at index %d. Local Term: %d, Leader Term: %d. Truncating.",
					s.id, idx, s.log[idx].Term, entry.Term)
				s.log = s.log[:idx]
				s.log = append(s.log, entry)
			}
		} else {
			s.log = append(s.log, entry)
		}
	}

	// Update commitIndex
	if req.LeaderCommit > int32(s.commitIndex) {
		lastNewIndex := int(req.PrevLogIndex) + len(req.Entries)
		if len(req.Entries) == 0 {
			lastNewIndex = int(req.PrevLogIndex)
		}
		
		newCommitIndex := int(req.LeaderCommit)
		if lastNewIndex < newCommitIndex {
			newCommitIndex = lastNewIndex
		}
		
		if newCommitIndex > s.commitIndex {
			log.Printf("[Node %d] Follower updated commitIndex from %d to %d", s.id, s.commitIndex, newCommitIndex)
			s.commitIndex = newCommitIndex
			s.commitCond.Broadcast()
		}
	}

	return &pb.AppendEntriesReply{
		Term:    s.currentTerm,
		Success: true,
	}, nil
}

// stepDown must be called with lock held
func (s *server) stepDown(newTerm int32) {
	log.Printf("[Node %d] Stepping down to Follower. Term: %d -> %d", s.id, s.currentTerm, newTerm)
	s.role = Follower
	s.currentTerm = newTerm
	s.votedFor = -1
	s.leaderId = -1
	s.resetElectionTimeout()
	s.persistState()
	s.commitCond.Broadcast()
}

func (s *server) becomeLeader() {
	if s.role == Leader {
		return
	}
	log.Printf("[Node %d] Became Leader for Term %d!", s.id, s.currentTerm)
	s.role = Leader
	s.leaderId = s.id

	// Initialize leader state maps
	s.nextIndex = make(map[int]int)
	s.matchIndex = make(map[int]int)
	lastLogIndex := len(s.log) - 1
	for peerID := range s.peers {
		s.nextIndex[peerID] = lastLogIndex + 1
		s.matchIndex[peerID] = 0
	}

	go s.heartbeatLoop(s.currentTerm)
}

func (s *server) startElection() {
	s.role = Candidate
	s.currentTerm++
	s.votedFor = int32(s.id)
	s.resetElectionTimeout()
	s.persistState()

	term := s.currentTerm
	candidateId := s.id
	log.Printf("[Node %d] Started election for Term %d", s.id, term)

	lastLogIndex := len(s.log) - 1
	lastLogTerm := s.log[lastLogIndex].Term

	votesReceived := 1
	var voteMu sync.Mutex

	for peerID, p := range s.peers {
		go func(pID int, client pb.RaftServiceClient) {
			req := &pb.RequestVoteArgs{
				Term:         term,
				CandidateId:  int32(candidateId),
				LastLogIndex: int32(lastLogIndex),
				LastLogTerm:  lastLogTerm,
			}
			reply, err := client.RequestVote(context.Background(), req)
			if err != nil {
				return
			}

			s.mu.Lock()
			defer s.mu.Unlock()

			if reply.Term > s.currentTerm {
				s.stepDown(reply.Term)
				return
			}

			if s.role == Candidate && s.currentTerm == term && reply.VoteGranted {
				voteMu.Lock()
				votesReceived++
				totalVotes := votesReceived
				voteMu.Unlock()

				log.Printf("[Node %d] Received vote from Peer %d, total votes: %d", candidateId, pID, totalVotes)

				quorum := (len(s.peers) + 1)/2 + 1
				if totalVotes >= quorum {
					s.becomeLeader()
				}
			}
		}(peerID, p.raftClient)
	}
}

func (s *server) electionLoop() {
	s.mu.Lock()
	s.resetElectionTimeout()
	s.mu.Unlock()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.mu.Lock()
			if s.role != Leader {
				if time.Since(s.lastResetTime) >= s.electionTimeout {
					s.startElection()
				}
			}
			s.mu.Unlock()
		}
	}
}

func (s *server) heartbeatLoop(term int32) {
	s.sendHeartbeats(term)

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.mu.Lock()
			if s.role != Leader || s.currentTerm != term {
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()
			s.sendHeartbeats(term)
		}
	}
}

func (s *server) triggerReplication() {
	s.mu.Lock()
	term := s.currentTerm
	role := s.role
	s.mu.Unlock()
	if role == Leader {
		go s.sendHeartbeats(term)
	}
}

func (s *server) sendHeartbeats(term int32) {
	s.mu.Lock()
	if s.role != Leader || s.currentTerm != term {
		s.mu.Unlock()
		return
	}
	leaderId := s.id
	commitIndex := s.commitIndex

	type peerReplicationInfo struct {
		pID          int
		client       pb.RaftServiceClient
		prevLogIndex int
		prevLogTerm  int32
		entries      []*pb.LogEntry
	}

	var reps []peerReplicationInfo
	for id, p := range s.peers {
		prevIndex := s.nextIndex[id] - 1
		prevTerm := s.log[prevIndex].Term

		var entries []*pb.LogEntry
		if prevIndex+1 < len(s.log) {
			entries = s.log[prevIndex+1:]
		}

		reps = append(reps, peerReplicationInfo{
			pID:          id,
			client:       p.raftClient,
			prevLogIndex: prevIndex,
			prevLogTerm:  prevTerm,
			entries:      entries,
		})
	}
	s.mu.Unlock()

	for _, rep := range reps {
		go func(r peerReplicationInfo) {
			req := &pb.AppendEntriesArgs{
				Term:         term,
				LeaderId:     int32(leaderId),
				PrevLogIndex: int32(r.prevLogIndex),
				PrevLogTerm:  r.prevLogTerm,
				Entries:      r.entries,
				LeaderCommit: int32(commitIndex),
			}
			reply, err := r.client.AppendEntries(context.Background(), req)
			if err != nil {
				return
			}

			s.mu.Lock()
			defer s.mu.Unlock()

			if reply.Term > s.currentTerm {
				s.stepDown(reply.Term)
				return
			}

			if s.role == Leader && s.currentTerm == term {
				if reply.Success {
					s.nextIndex[r.pID] = r.prevLogIndex + len(r.entries) + 1
					s.matchIndex[r.pID] = r.prevLogIndex + len(r.entries)
					s.updateLeaderCommitIndex()
				} else {
					if reply.ConflictTerm == -1 {
						s.nextIndex[r.pID] = int(reply.ConflictIndex)
					} else {
						lastIndexForTerm := -1
						for idx := len(s.log) - 1; idx >= 0; idx-- {
							if s.log[idx].Term == reply.ConflictTerm {
								lastIndexForTerm = idx
								break
							}
						}
						if lastIndexForTerm != -1 {
							s.nextIndex[r.pID] = lastIndexForTerm + 1
						} else {
							s.nextIndex[r.pID] = int(reply.ConflictIndex)
						}
					}
					if s.nextIndex[r.pID] < 1 {
						s.nextIndex[r.pID] = 1
					}
				}
			}
		}(rep)
	}
}

// updateLeaderCommitIndex must be called with lock held
func (s *server) updateLeaderCommitIndex() {
	matches := make([]int, 0, len(s.peers)+1)
	matches = append(matches, len(s.log)-1)
	for _, m := range s.matchIndex {
		matches = append(matches, m)
	}

	sort.Ints(matches)
	quorum := (len(s.peers) + 1)/2 + 1
	N := matches[len(matches)-quorum]

	if N > s.commitIndex && s.log[N].Term == s.currentTerm {
		log.Printf("[Node %d] Leader updated commitIndex from %d to %d", s.id, s.commitIndex, N)
		s.commitIndex = N
		s.commitCond.Broadcast()
	}
}

func (s *server) applyLoop() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		<-ticker.C
		s.mu.Lock()
		for s.commitIndex > s.lastApplied {
			s.lastApplied++
			entry := s.log[s.lastApplied]
			s.applyEntry(entry)
		}
		s.mu.Unlock()
	}
}

func (s *server) applyEntry(entry *pb.LogEntry) {
	var cmd Command
	if err := json.Unmarshal([]byte(entry.Command), &cmd); err != nil {
		log.Printf("[Node %d] Failed to unmarshal log command at index %d: %v", s.id, entry.Index, err)
		return
	}

	switch cmd.Op {
	case "PUT":
		s.kv.Put(cmd.Key, cmd.Value)
		log.Printf("[Node %d] Applied PUT key: %q, value: %q at log index %d", s.id, cmd.Key, cmd.Value, entry.Index)
	case "DELETE":
		s.kv.Delete(cmd.Key)
		log.Printf("[Node %d] Applied DELETE key: %q at log index %d", s.id, cmd.Key, entry.Index)
	default:
		log.Printf("[Node %d] Unknown command operation: %q at log index %d", s.id, cmd.Op, entry.Index)
	}
}

func parsePeers(peersStr string) (map[int]string, error) {
	peers := make(map[int]string)
	if peersStr == "" {
		return peers, nil
	}
	parts := strings.Split(peersStr, ",")
	for _, part := range parts {
		subparts := strings.SplitN(part, ":", 2)
		if len(subparts) != 2 {
			return nil, fmt.Errorf("invalid peer format: %s", part)
		}
		id, err := strconv.Atoi(subparts[0])
		if err != nil {
			return nil, fmt.Errorf("invalid peer ID: %s", subparts[0])
		}
		peers[id] = subparts[1]
	}
	return peers, nil
}

func (s *server) connectPeers(peerAddrs map[int]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for peerID, addr := range peerAddrs {
		log.Printf("[Node %d] Connecting to Peer %d at %s", s.id, peerID, addr)
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("failed to connect to peer %d at %s: %v", peerID, addr, err)
		}
		s.peers[peerID] = &peer{
			conn:       conn,
			raftClient: pb.NewRaftServiceClient(conn),
			kvClient:   pb.NewKVStoreClient(conn),
		}
	}
	return nil
}

func (s *server) closeConnections() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, p := range s.peers {
		log.Printf("[Node %d] Closing connection to peer %d", s.id, id)
		p.conn.Close()
	}
}

func main() {
	id := flag.Int("id", 1, "Unique node identifier")
	portStr := flag.String("port", "8001", "Port to bind/listen")
	peersStr := flag.String("peers", "", "Comma-separated peer mapping (e.g. 2:localhost:8002,3:localhost:8003)")
	flag.Parse()

	bindAddr := *portStr
	if !strings.HasPrefix(bindAddr, ":") {
		bindAddr = ":" + bindAddr
	}

	lis, err := net.Listen("tcp", bindAddr)
	if err != nil {
		log.Fatalf("[Node %d] failed to listen on %s: %v", *id, bindAddr, err)
	}

	srv := &server{
		id:            *id,
		port:          *portStr,
		kv:            store.NewKVStore(),
		peers:         make(map[int]*peer),
		role:          Follower,
		currentTerm:   0,
		votedFor:      -1,
		leaderId:      -1,
		lastResetTime: time.Now(),
		log: []*pb.LogEntry{
			{
				Index:   0,
				Term:    0,
				Command: "",
			},
		},
		commitIndex: 0,
		lastApplied: 0,
	}
	srv.commitCond = sync.NewCond(&srv.mu)

	// Restore persisted state from file if it exists
	srv.readPersistedState()

	peerAddrs, err := parsePeers(*peersStr)
	if err != nil {
		log.Fatalf("[Node %d] failed to parse peers: %v", *id, err)
	}

	if err := srv.connectPeers(peerAddrs); err != nil {
		log.Fatalf("[Node %d] failed to connect to peers: %v", *id, err)
	}
	defer srv.closeConnections()

	go srv.electionLoop()
	go srv.applyLoop()

	s := grpc.NewServer()
	pb.RegisterKVStoreServer(s, srv)
	pb.RegisterRaftServiceServer(s, srv)

	log.Printf("[Node %d] gRPC Server listening on %s...", *id, bindAddr)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("[Node %d] failed to serve: %v", *id, err)
	}
}
