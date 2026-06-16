package main

import (
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/raft-kv/pb"
)

func TestParsePeers(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected map[int]string
		wantErr  bool
	}{
		{
			name:     "Empty input",
			input:    "",
			expected: map[int]string{},
			wantErr:  false,
		},
		{
			name:     "Single peer",
			input:    "2:localhost:8002",
			expected: map[int]string{2: "localhost:8002"},
			wantErr:  false,
		},
		{
			name:     "Multiple peers",
			input:    "2:localhost:8002,3:localhost:8003",
			expected: map[int]string{2: "localhost:8002", 3: "localhost:8003"},
			wantErr:  false,
		},
		{
			name:     "Invalid peer ID format",
			input:    "abc:localhost:8002",
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "Invalid structure format",
			input:    "2-localhost:8002",
			expected: nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePeers(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parsePeers() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("parsePeers() = %v, expected %v", got, tt.expected)
			}
		})
	}
}

func TestRaftPersistenceAndTimeout(t *testing.T) {
	s := &server{
		id:          99,
		currentTerm: 5,
		votedFor:    2,
	}

	// Clean up after test
	defer os.Remove("raft_state_99.json")

	s.persistState()

	// Create new server to restore state
	s2 := &server{
		id:          99,
		currentTerm: 0,
		votedFor:    -1,
	}
	s2.readPersistedState()

	if s2.currentTerm != 5 {
		t.Errorf("Expected currentTerm 5, got %d", s2.currentTerm)
	}
	if s2.votedFor != 2 {
		t.Errorf("Expected votedFor 2, got %d", s2.votedFor)
	}

	// Test election loop timeout randomization
	s.resetElectionTimeout()
	if s.electionTimeout < 150*time.Millisecond || s.electionTimeout > 300*time.Millisecond {
		t.Errorf("Expected electionTimeout between 150ms and 300ms, got %v", s.electionTimeout)
	}
}

func TestRaftQuorumAndCommitMatching(t *testing.T) {
	s := &server{
		id:    1,
		peers: map[int]*peer{
			2: {},
			3: {},
		},
		log: []*pb.LogEntry{
			{Index: 0, Term: 0},
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
			{Index: 3, Term: 2},
		},
		currentTerm: 2,
		commitIndex: 0,
		matchIndex: map[int]int{
			2: 2,
			3: 3,
		},
	}
	s.commitCond = sync.NewCond(&s.mu)

	// Quorum for 3 nodes (leader + 2 peers) is 2.
	// Matches: leader log index (3), peer 2 (2), peer 3 (3).
	// Sorted: [2, 3, 3].
	// quorum := (len(peers) + 1)/2 + 1 = 2.
	// len(matches) - quorum = 3 - 2 = 1.
	// matches[1] = 3.
	// Since s.log[3].Term == s.currentTerm (2), commitIndex should update to 3.
	s.updateLeaderCommitIndex()

	if s.commitIndex != 3 {
		t.Errorf("Expected commitIndex to update to 3, got %d", s.commitIndex)
	}
}
