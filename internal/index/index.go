// Package index is the local state database (bbolt): per-path file state,
// processed journal keys and small metadata values.
package index

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	bFiles     = []byte("files")
	bProcessed = []byte("processed")
	bMeta      = []byte("meta")
)

// Entry is the last synced state of one path.
type Entry struct {
	Size    int64  `json:"size"`
	Mtime   int64  `json:"mtime"` // unix nanoseconds of the local file
	Hash    string `json:"hash"`  // "sha256:<hex>"
	ETag    string `json:"etag"`  // remote ETag as last seen; "" if unknown
	Journal string `json:"journal,omitempty"`
}

// DB is an open index.
type DB struct{ db *bolt.DB }

// Open opens or creates the database file.
func Open(path string) (*DB, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open index %s: %w (is another gozeks running?)", path, err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bFiles, bProcessed, bMeta} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &DB{db: db}, nil
}

// Close closes the database.
func (d *DB) Close() error { return d.db.Close() }

// Get returns the entry for a path.
func (d *DB) Get(path string) (Entry, bool, error) {
	var e Entry
	found := false
	err := d.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bFiles).Get([]byte(path))
		if v == nil {
			return nil
		}
		found = true
		return json.Unmarshal(v, &e)
	})
	return e, found, err
}

// Put stores an entry.
func (d *DB) Put(path string, e Entry) error {
	v, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return d.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bFiles).Put([]byte(path), v)
	})
}

// Delete removes an entry.
func (d *DB) Delete(path string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bFiles).Delete([]byte(path))
	})
}

// All iterates over every entry in key order.
func (d *DB) All(fn func(path string, e Entry) error) error {
	return d.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bFiles).ForEach(func(k, v []byte) error {
			var e Entry
			if err := json.Unmarshal(v, &e); err != nil {
				return err
			}
			return fn(string(k), e)
		})
	})
}

// Under returns the paths that are inside directory dir.
func (d *DB) Under(dir string) ([]string, error) {
	prefix := []byte(strings.TrimSuffix(dir, "/") + "/")
	var out []string
	err := d.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bFiles).Cursor()
		for k, _ := c.Seek(prefix); k != nil && strings.HasPrefix(string(k), string(prefix)); k, _ = c.Next() {
			out = append(out, string(k))
		}
		return nil
	})
	return out, err
}

// Count returns the number of indexed paths.
func (d *DB) Count() (int, error) {
	n := 0
	err := d.db.View(func(tx *bolt.Tx) error {
		n = tx.Bucket(bFiles).Stats().KeyN
		return nil
	})
	return n, err
}

// MarkProcessed remembers a journal key.
func (d *DB) MarkProcessed(key string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bProcessed).Put([]byte(key), []byte(fmt.Sprint(time.Now().Unix())))
	})
}

// IsProcessed reports whether a journal key was already applied.
func (d *DB) IsProcessed(key string) (bool, error) {
	found := false
	err := d.db.View(func(tx *bolt.Tx) error {
		found = tx.Bucket(bProcessed).Get([]byte(key)) != nil
		return nil
	})
	return found, err
}

// PruneProcessed drops keys that sort before the given key (older journal entries).
func (d *DB) PruneProcessed(beforeKey string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bProcessed)
		c := b.Cursor()
		var del [][]byte
		for k, _ := c.First(); k != nil && string(k) < beforeKey; k, _ = c.Next() {
			del = append(del, append([]byte(nil), k...))
		}
		for _, k := range del {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetMeta returns a metadata value ("" if unset).
func (d *DB) GetMeta(key string) (string, error) {
	var v string
	err := d.db.View(func(tx *bolt.Tx) error {
		v = string(tx.Bucket(bMeta).Get([]byte(key)))
		return nil
	})
	return v, err
}

// SetMeta stores a metadata value.
func (d *DB) SetMeta(key, value string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bMeta).Put([]byte(key), []byte(value))
	})
}
