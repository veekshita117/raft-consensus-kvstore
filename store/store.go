package store

import "sync"

// KVStore represents a thread-safe in-memory key-value store.
type KVStore struct {
	mu sync.RWMutex
	db map[string]string
}

// NewKVStore creates and returns a new KVStore instance.
func NewKVStore() *KVStore {
	return &KVStore{
		db: make(map[string]string),
	}
}

// Put inserts or updates a key-value pair in the store.
func (s *KVStore) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.db[key] = value
}

// Get retrieves a value from the store by key.
// Returns the value and a boolean indicating if the key was found.
func (s *KVStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, exists := s.db[key]
	return val, exists
}

// Delete removes a key-value pair from the store.
// Returns a boolean indicating if the key existed and was deleted.
func (s *KVStore) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.db[key]
	if !exists {
		return false
	}
	delete(s.db, key)
	return true
}
