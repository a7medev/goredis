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

type SetMode int64

const (
	SetDefault SetMode = 0
	// Only set the key if it does not already exist
	SetNX SetMode = 1
	// Only set the key if it already exists
	SetXX SetMode = 2
)

func (db *Database) Set(key, value string, expiry Expiry, mode SetMode, keepTTL, get bool) (string, bool, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()

	entry, ok := db.data[key]

	if keepTTL {
		expiry = entry.expiry
	}

	shouldSet := !(mode == SetNX && ok) && !(mode == SetXX && !ok)

	if shouldSet {
		db.data[key] = Entry{value: value, expiry: expiry}
	}

	if get {
		current, exists := db.data[key]
		return current.value, exists, shouldSet
	}

	return "", true, shouldSet
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
