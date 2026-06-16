package main

import (
	"reflect"
	"testing"
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
