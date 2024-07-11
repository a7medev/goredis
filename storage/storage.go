package storage

import (
	"sync"
	"time"
)

type Expiry struct {
	Time    time.Time
	Expires bool
}

func NewExpiry(t int64, mode string) Expiry {
	switch mode {
	case "EX":
		return NewSecondsExpiry(t)
	case "PX":
		return NewMillisExpiry(t)
	case "EXAT":
		return NewUnixSecondExpiry(t)
	case "PXAT":
		return NewUnixMilliExpiry(t)
	default:
		return NeverExpires
	}
}

var NeverExpires = Expiry{Expires: false}

func NewSecondsExpiry(seconds int64) Expiry {
	t := time.Now().Add(time.Duration(seconds) * time.Second)
	return Expiry{Time: t, Expires: true}
}

func NewMillisExpiry(millis int64) Expiry {
	t := time.Now().Add(time.Duration(millis) * time.Millisecond)
	return Expiry{Time: t, Expires: true}
}

func NewUnixSecondExpiry(seconds int64) Expiry {
	t := time.Unix(seconds, 0)
	return Expiry{Time: t, Expires: true}
}

func NewUnixMilliExpiry(millis int64) Expiry {
	t := time.UnixMilli(millis)
	return Expiry{Time: t, Expires: true}
}

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

func (db *Database) Set(key, value string, expiry Expiry, mode SetMode, keepTTL, get bool) (previous string, exists, isSet bool) {
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
		return entry.value, ok, shouldSet
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

func (db *Database) Delete(key string) bool {
	db.mu.Lock()
	defer db.mu.Unlock()

	_, ok := db.data[key]

	if ok {
		delete(db.data, key)
		return true
	}

	return false
}
