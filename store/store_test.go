package store

import (
	"fmt"
	"sync"
	"testing"
)

func TestKVStore_BasicOperations(t *testing.T) {
	s := NewKVStore()

	// Test Get on empty store
	val, found := s.Get("key1")
	if found {
		t.Errorf("Expected found to be false for non-existent key, got true")
	}

	// Test Put and Get
	s.Put("key1", "value1")
	val, found = s.Get("key1")
	if !found {
		t.Fatalf("Expected to find key1, but not found")
	}
	if val != "value1" {
		t.Errorf("Expected value1, got %s", val)
	}

	// Test Delete
	deleted := s.Delete("key1")
	if !deleted {
		t.Errorf("Expected delete of key1 to return true, got false")
	}

	val, found = s.Get("key1")
	if found {
		t.Errorf("Expected key1 to be deleted, but still found")
	}

	// Test Delete of non-existent key
	deleted = s.Delete("key1")
	if deleted {
		t.Errorf("Expected delete of non-existent key to return false, got true")
	}
}

func TestKVStore_ConcurrentAccess(t *testing.T) {
	s := NewKVStore()

	var wg sync.WaitGroup
	numRoutines := 50
	numOperations := 100

	// Concurrent writes
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				s.Put(key, "value")
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				_, _ = s.Get(key)
			}
		}(i)
	}

	// Concurrent deletes
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				s.Delete(key)
			}
		}(i)
	}

	wg.Wait()
}
