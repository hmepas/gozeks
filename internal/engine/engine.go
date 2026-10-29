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
	"golang.org/x/text/unicode/norm"
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
	pollMu     sync.Mutex // single-flights CheckCursor (poll loop, relay connect, relay drop)
	seq        atomic.Int64
	lastChange atomic.Int64 // unix nanos of the last local or remote change seen
	cursorETag atomic.Value // string: ETag of _sync/cursor as of the last processed journal

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
	etag, _ := idx.GetMeta(metaCursorETag)
	e.cursorETag.Store(etag)
	e.relay.role.Store("none")
	if err := e.normalizeIndex(); err != nil {
		idx.Close()
		return nil, err
	}
	return e, nil
}

// nfc returns the path in Unicode NFC. All keys (bucket, index, journal) are
// NFC: macOS historically produced NFD names, Linux keeps whatever bytes it is
// given, and without one canonical form the same note becomes two files.
func nfc(rel string) string { return norm.NFC.String(rel) }

// normalizeIndex re-keys index entries written before NFC normalization.
func (e *Engine) normalizeIndex() error {
	type kv struct {
		k string
		v index.Entry
	}
	var bad []kv
	err := e.idx.All(func(p string, ent index.Entry) error {
		if !norm.NFC.IsNormalString(p) {
			bad = append(bad, kv{p, ent})
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, b := range bad {
		if _, has, _ := e.idx.Get(nfc(b.k)); !has {
			if err := e.idx.Put(nfc(b.k), b.v); err != nil {
				return err
			}
		}
		if err := e.idx.Delete(b.k); err != nil {
			return err
		}
	}
	if len(bad) > 0 {
		e.log.Info("index normalized to NFC", "entries", len(bad))
	}
	return nil
}

// normalizeName makes sure the on-disk name of rel is NFC. On a
// normalization-insensitive filesystem (APFS) nothing changes. On Linux an
// NFD-named file is renamed; if an NFC twin already exists, an identical
// duplicate goes to trash and a differing one becomes a conflict copy.
// It returns the NFC path to use, or "" if the file should be skipped.
func (e *Engine) normalizeName(rel string) (string, error) {
	n := nfc(rel)
	if n == rel {
		return rel, nil
	}
	src, dst := e.abs(rel), e.abs(n)
	dfi, err := os.Lstat(dst)
	if errors.Is(err, fs.ErrNotExist) {
		if err := os.Rename(src, dst); err != nil {
			return "", err
		}
		e.log.Info("renamed to NFC", "path", n)
		return n, nil
	}
	sfi, err := os.Lstat(src)
	if err != nil || !sfi.Mode().IsRegular() {
		return "", nil
	}
	if dfi != nil && os.SameFile(sfi, dfi) {
		return n, nil // normalization-insensitive filesystem (APFS): same file
	}
	// a distinct NFC twin exists (Linux): compare content
	hs, err1 := hashFile(src)
	hd, err2 := hashFile(dst)
	if err1 != nil || err2 != nil {
		return "", nil
	}
	if hs == hd {
		if err := e.trash(rel, time.Now().Format("20060102-150405")); err != nil {
			return "", err
		}
		e.log.Info("trashed NFD duplicate", "path", n)
		return "", nil
	}
	c := conflictName(n, e.cfg.Node, time.Now())
	if err := os.Rename(src, e.abs(c)); err != nil {
		return "", err
	}
	e.log.Warn("NFD twin differs, kept as conflict copy", "path", n, "kept_as", c)
	return c, nil
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

// throughSymlink reports whether any directory component of rel is a
// symlink. Such paths are never read or written: the same name would mean a
// different file on every node, and writing there would modify the link's
// target.
func (e *Engine) throughSymlink(rel string) bool {
	parts := strings.Split(rel, "/")
	p := e.cfg.Root
	for _, c := range parts[:len(parts)-1] {
		p = filepath.Join(p, c)
		fi, err := os.Lstat(p)
		if err != nil {
			return false // absent: nothing to traverse
		}
		if fi.Mode()&fs.ModeSymlink != 0 {
			return true
		}
	}
	return false
}

// caseConflict reports whether rel resolves to a file on disk whose on-disk
// spelling differs from rel (case-insensitive filesystem, e.g. APFS). Such a
// path belongs to another key and must not be read, written or deleted.
func (e *Engine) caseConflict(rel string) bool {
	if _, err := os.Lstat(e.abs(rel)); err != nil {
		return false
	}
	dir := e.cfg.Root
	for _, c := range strings.Split(rel, "/") {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return false
		}
		found := false
		for _, d := range entries {
			if d.Name() == c {
				found = true
				break
			}
		}
		if !found {
			return true
		}
		dir = filepath.Join(dir, c)
	}
	return false
}

// localState stats rel and returns nil if it is absent, not a regular file,
// or reached through a symlinked directory.
// When size and mtime match the index entry the stored hash is reused.
func (e *Engine) localState(rel string, idx *index.Entry) (*localFile, error) {
	if e.throughSymlink(rel) {
		return nil, nil
	}
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
	if idx != nil && idx.Size == lf.size && idx.Mtime == lf.mtime && idx.Hash != "" && !recent(fi.ModTime()) {
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

// recent reports whether a modification time is too fresh for the size+mtime
// fast path: two writes inside the filesystem's timestamp granularity would
// otherwise look identical (rsync's "modify window").
func recent(mtime time.Time) bool { return time.Since(mtime) < 2*time.Second }

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
			if n := nfc(rel); n != rel {
				if _, err := os.Lstat(e.abs(n)); errors.Is(err, fs.ErrNotExist) {
					if err := os.Rename(p, e.abs(n)); err == nil {
						e.log.Info("renamed directory to NFC", "path", n)
						return filepath.SkipDir // re-walked on the next pass
					}
				}
				e.log.Warn("directory name is not NFC and an NFC twin exists; skipping", "path", rel)
				return filepath.SkipDir
			}
			return nil
		}
		if d.Type()&fs.ModeSymlink != 0 {
			e.log.Debug("skipping symlink", "path", rel)
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		n, err := e.normalizeName(rel)
		if err != nil {
			e.log.Warn("normalize name", "path", rel, "err", err)
			return nil
		}
		if n == "" {
			return nil
		}
		fi, err := os.Lstat(e.abs(n))
		if err != nil {
			return nil
		}
		return fn(n, fi)
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
