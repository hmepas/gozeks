// Package journal defines the bucket layout keys and the journal entry format.
package journal

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	// DataPrefix is where vault files live, 1:1 with the local tree.
	DataPrefix = "data/"
	// Prefix is the journal directory; keys under it sort chronologically.
	Prefix = "_sync/journal/"
	// CursorKey is the marker object whose ETag changes after every batch.
	CursorKey = "_sync/cursor"
	// LockKey is the relay leader lease.
	LockKey = "_sync/relay.lock"
)

// Op is one file operation inside a journal entry.
type Op struct {
	Op    string `json:"op"` // "put" or "delete"
	Path  string `json:"path"`
	Size  int64  `json:"size,omitempty"`
	Mtime int64  `json:"mtime,omitempty"` // unix seconds
	Hash  string `json:"hash,omitempty"`  // "sha256:<hex>"
	Prev  string `json:"prev,omitempty"`  // hash the uploader had before this change ("" for a new file)
}

// Entry is one upload batch.
type Entry struct {
	Node string `json:"node"`
	Ts   int64  `json:"ts"` // unix milliseconds
	Seq  int64  `json:"seq"`
	Ops  []Op   `json:"ops"`
}

// Key returns the object key for an entry.
func Key(ts int64, node string, seq int64) string {
	return fmt.Sprintf("%s%013d-%s-%d.json", Prefix, ts, node, seq)
}

// Cursor is the marker body; only its ETag matters.
type Cursor struct {
	Node string `json:"node"`
	Ts   int64  `json:"ts"`
	Seq  int64  `json:"seq"`
}

// ParseKey extracts the timestamp and node from a journal key.
func ParseKey(key string) (ts int64, node string, seq int64, err error) {
	name := strings.TrimPrefix(key, Prefix)
	name = strings.TrimSuffix(name, ".json")
	parts := strings.SplitN(name, "-", 2)
	if len(parts) != 2 {
		return 0, "", 0, fmt.Errorf("bad journal key %q", key)
	}
	ts, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, "", 0, fmt.Errorf("bad journal key %q: %w", key, err)
	}
	i := strings.LastIndex(parts[1], "-")
	if i < 0 {
		return 0, "", 0, fmt.Errorf("bad journal key %q", key)
	}
	node = parts[1][:i]
	seq, err = strconv.ParseInt(parts[1][i+1:], 10, 64)
	if err != nil {
		return 0, "", 0, fmt.Errorf("bad journal key %q: %w", key, err)
	}
	return ts, node, seq, nil
}

// StartAfter returns the LIST StartAfter marker for entries newer than
// lastTs minus rewind. Every real key with ts >= that sorts after it.
func StartAfter(lastTs int64, rewind time.Duration) string {
	ts := lastTs - rewind.Milliseconds()
	if ts < 0 {
		ts = 0
	}
	return fmt.Sprintf("%s%013d", Prefix, ts)
}
