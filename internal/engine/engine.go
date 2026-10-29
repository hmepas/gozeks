// Package engine implements the sync logic: uploading local changes as
// journal batches, applying remote journal entries, reconciling drift, and
// the relay role state machine.
package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hmepas/gozeks/internal/config"
	"github.com/hmepas/gozeks/internal/exclude"
	"github.com/hmepas/gozeks/internal/index"
	"github.com/hmepas/gozeks/internal/store"
)

// Storage is the subset of the S3 store the engine uses (fakeable in tests).
type Storage interface {
	Put(ctx context.Context, key string, body []byte, meta map[string]string) (string, error)
	Get(ctx context.Context, key string) ([]byte, string, map[string]string, error)
	GetTo(ctx context.Context, key string, w io.Writer) (string, map[string]string, error)
	Head(ctx context.Context, key string) (string, map[string]string, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix, startAfter string, fn func(store.Object) error) error
}

const (
	metaCursorETag  = "cursor_etag"
	metaLastJournal = "last_journal_ts"
	metaCounters    = "counters"
	metaSince       = "counters_since"

	journalRewind  = 120 * time.Second
	journalKeep    = 7 * 24 * time.Hour
	processedKeep  = 24 * time.Hour
	opsPerEntry    = 1000
	transferWorker = 8
	tmpPrefix      = ".gozeks-"
)

// Engine is one node.
type Engine struct {
	cfg *config.Config
	log *slog.Logger
	st  Storage
	idx *index.DB
	ex  *exclude.Matcher

	// DryRun logs what would be done instead of doing it (push/pull --dry-run).
	DryRun bool

	mu         sync.Mutex // serializes every mutation of the tree and the index
	seq        atomic.Int64
	lastChange atomic.Int64 // unix nanos of the last local or remote change seen
	cursorETag string

	// OnKeys is called after every committed batch with its journal keys.
	OnKeys func(keys []string)

	relay relayState
	pend  func() int // pending batch size, set by Run
}

// New opens the index and prepares the engine.
func New(cfg *config.Config, st Storage, log *slog.Logger) (*Engine, error) {
	if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
		return nil, err
	}
	idx, err := index.Open(filepath.Join(cfg.StateDir, "index.db"))
	if err != nil {
		return nil, err
	}
	ex, err := exclude.New(cfg.Sync.Exclude)
	if err != nil {
		idx.Close()
		return nil, err
	}
	e := &Engine{cfg: cfg, log: log, st: st, idx: idx, ex: ex}
	e.cursorETag, _ = idx.GetMeta(metaCursorETag)
	e.relay.role.Store("none")
	return e, nil
}

// Close releases the index.
func (e *Engine) Close() error { return e.idx.Close() }

// Index exposes the state database (status command).
func (e *Engine) Index() *index.DB { return e.idx }

func (e *Engine) abs(rel string) string { return filepath.Join(e.cfg.Root, filepath.FromSlash(rel)) }

func (e *Engine) touch() { e.lastChange.Store(time.Now().UnixNano()) }

// localFile is the current state of a path on disk.
type localFile struct {
	size  int64
	mtime int64 // unix nanos
	hash  string
}

// localState stats rel and returns nil if it is absent or not a regular file.
// When size and mtime match the index entry the stored hash is reused.
func (e *Engine) localState(rel string, idx *index.Entry) (*localFile, error) {
	fi, err := os.Lstat(e.abs(rel))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, nil
	}
	lf := &localFile{size: fi.Size(), mtime: fi.ModTime().UnixNano()}
	if idx != nil && idx.Size == lf.size && idx.Mtime == lf.mtime && idx.Hash != "" {
		lf.hash = idx.Hash
		return lf, nil
	}
	h, err := hashFile(e.abs(rel))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	lf.hash = h
	return lf, nil
}

func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

func hashBytes(b []byte) string {
	s := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(s[:])
}

func metaGet(m map[string]string, key string) string {
	for k, v := range m {
		if strings.EqualFold(k, key) {
			return v
		}
	}
	return ""
}

func (e *Engine) setLastJournalTs(ts int64) {
	cur, _ := e.idx.GetMeta(metaLastJournal)
	if c, _ := strconv.ParseInt(cur, 10, 64); ts > c {
		_ = e.idx.SetMeta(metaLastJournal, strconv.FormatInt(ts, 10))
	}
}

func (e *Engine) lastJournalTs() int64 {
	cur, _ := e.idx.GetMeta(metaLastJournal)
	v, _ := strconv.ParseInt(cur, 10, 64)
	return v
}

// walkLocal lists every regular, non-excluded file under root.
func (e *Engine) walkLocal(dir string, fn func(rel string, fi fs.FileInfo) error) error {
	start := e.abs(dir)
	return filepath.WalkDir(start, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		rel, _ := filepath.Rel(e.cfg.Root, p)
		rel = filepath.ToSlash(rel)
		if rel == "." {
			return nil
		}
		if e.ex.Match(rel) || p == e.cfg.StateDir {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if d.Type()&fs.ModeSymlink != 0 {
			e.log.Debug("skipping symlink", "path", rel)
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return nil
		}
		return fn(rel, fi)
	})
}

// conflictName builds "<name> (conflict <node> <YYYY-MM-DD HHMM>).<ext>".
func conflictName(rel, node string, t time.Time) string {
	dir := ""
	base := rel
	if i := strings.LastIndex(rel, "/"); i >= 0 {
		dir, base = rel[:i+1], rel[i+1:]
	}
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)
	return fmt.Sprintf("%s%s (conflict %s %s)%s", dir, name, node, t.Format("2006-01-02 1504"), ext)
}

// trash moves rel out of the tree into <state>/trash/<stamp>/<rel>.
func (e *Engine) trash(rel, stamp string) error {
	src := e.abs(rel)
	dst := filepath.Join(e.cfg.StateDir, "trash", stamp, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	if err := os.Rename(src, dst); err != nil {
		// different volume: copy then remove
		if err2 := copyFile(src, dst); err2 != nil {
			return err
		}
		if err2 := os.Remove(src); err2 != nil {
			return err2
		}
	}
	e.removeEmptyParents(rel)
	return nil
}

func (e *Engine) removeEmptyParents(rel string) {
	for {
		i := strings.LastIndex(rel, "/")
		if i < 0 {
			return
		}
		rel = rel[:i]
		if os.Remove(e.abs(rel)) != nil {
			return
		}
	}
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
