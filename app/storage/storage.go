package storage

import "sync"

// Database is a simple, thread-safe, in-memory key-value store
type Database struct {
	data map[string]string
	mu   sync.RWMutex
}

func NewDatabase() *Database {
	return &Database{
		data: make(map[string]string),
		mu:   sync.RWMutex{},
	}
}

func (db *Database) Set(key, value string) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.data[key] = value
}

func (db *Database) Get(key string) (string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	val, ok := db.data[key]

	return val, ok
}
