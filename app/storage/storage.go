package storage

import (
	"sync"
	"time"
)

type Expiry struct {
	Time    time.Time
	Expires bool
}

var NeverExpires = Expiry{Expires: false}

type Entry struct {
	value  string
	expiry Expiry
}

// Database is a simple, thread-safe, in-memory key-value store
type Database struct {
	data map[string]Entry
	mu   *sync.Mutex
}

func NewDatabase() *Database {
	return &Database{
		data: make(map[string]Entry),
		mu:   &sync.Mutex{},
	}
}

func (db *Database) Set(key, value string, expiry Expiry) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.data[key] = Entry{value: value, expiry: expiry}
}

func (db *Database) Get(key string) (string, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()

	entry, ok := db.data[key]

	if entry.expiry.Expires && time.Now().After(entry.expiry.Time) {
		delete(db.data, key)
		return "", false
	}

	return entry.value, ok
}
