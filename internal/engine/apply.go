package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/hmepas/gozeks/internal/index"
	"github.com/hmepas/gozeks/internal/journal"
	"github.com/hmepas/gozeks/internal/store"
)

// CheckCursor HEADs the marker and processes new journal entries when its
// ETag changed. It returns the journal keys that were newly processed.
func (e *Engine) CheckCursor(ctx context.Context) ([]string, error) {
	e.pollMu.Lock()
	defer e.pollMu.Unlock()
	etag, _, err := e.st.Head(ctx, journal.CursorKey)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if etag == e.cursorETag.Load().(string) {
		return nil, nil
	}
	keys, err := e.processJournal(ctx)
	if err != nil {
		return keys, err
	}
	e.cursorETag.Store(etag)
	_ = e.idx.SetMeta(metaCursorETag, etag)
	return keys, nil
}

// processJournal lists entries newer than the last processed timestamp minus
// the skew rewind and applies the ones not seen before.
func (e *Engine) processJournal(ctx context.Context) ([]string, error) {
	var keys []string
	err := e.st.List(ctx, journal.Prefix, journal.StartAfter(e.lastJournalTs(), journalRewind), func(o store.Object) error {
		keys = append(keys, o.Key)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return e.ApplyKeys(ctx, keys)
}

// ApplyKeys fetches and applies the given journal entries (in order),
// skipping ones already processed. Used by both the poll and the relay path.
func (e *Engine) ApplyKeys(ctx context.Context, keys []string) ([]string, error) {
	var done []string
	for _, key := range keys {
		if seen, _ := e.idx.IsProcessed(key); seen {
			continue
		}
		if err := e.applyKey(ctx, key); err != nil {
			return done, err
		}
		done = append(done, key)
	}
	return done, nil
}

func (e *Engine) applyKey(ctx context.Context, key string) error {
	ts, _, _, err := journal.ParseKey(key)
	if err != nil {
		e.log.Warn("ignoring journal key", "key", key, "err", err)
		return e.idx.MarkProcessed(key)
	}
	body, _, _, err := e.st.Get(ctx, key)
	if errors.Is(err, store.ErrNotFound) {
		return e.idx.MarkProcessed(key)
	}
	if err != nil {
		return err
	}
	var entry journal.Entry
	if err := json.Unmarshal(body, &entry); err != nil {
		e.log.Warn("bad journal entry", "key", key, "err", err)
		return e.idx.MarkProcessed(key)
	}
	if entry.Node != e.cfg.Node {
		pending, err := e.applyEntry(ctx, key, &entry)
		if err != nil {
			return err
		}
		if len(pending) > 0 {
			if err := e.UploadPaths(ctx, pending); err != nil {
				e.log.Error("upload after apply", "err", err)
			}
		}
	}
	if err := e.idx.MarkProcessed(key); err != nil {
		return err
	}
	e.setLastJournalTs(ts)
	return nil
}

// applyEntry applies one remote batch and returns local paths that must be
// uploaded afterwards (conflict copies, locally modified files kept on delete).
func (e *Engine) applyEntry(ctx context.Context, key string, entry *journal.Entry) ([]string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	stamp := time.Now().Format("20060102-150405")
	var pending []string
	applied := 0
	for _, op := range entry.Ops {
		if e.ex.Match(op.Path) {
			continue
		}
		var p []string
		var err error
		switch op.Op {
		case "put":
			p, err = e.applyPut(ctx, op.Path, key, op.Hash, op.Prev, op.ETag, op.Mtime)
		case "delete":
			p, err = e.applyDelete(ctx, op.Path, stamp)
		default:
			continue
		}
		if err != nil {
			e.log.Error("apply", "op", op.Op, "path", op.Path, "err", err)
			continue
		}
		pending = append(pending, p...)
		applied++
	}
	e.touch()
	e.log.Info("batch applied", "key", key, "from", entry.Node, "ops", len(entry.Ops), "changed", applied)
	return pending, nil
}

// applyPut makes the local path match the remote object described by the
// journal entry key (or "" from reconcile, where remoteHash/prevHash are
// unknown and the file is fetched and compared afterwards).
//
// Concurrent edits are resolved deterministically: an incoming version whose
// base (prev) is not what we hold is divergent. If our version is newer by
// journal timestamp we ignore the entry (the other node will take ours and
// keep its own as a conflict copy); otherwise theirs takes the path and ours
// becomes the conflict copy. Both nodes compare the same two timestamps, so
// clock skew cannot make them disagree.
func (e *Engine) applyPut(ctx context.Context, rel, key, remoteHash, prevHash, remoteETag string, remoteMtime int64) ([]string, error) {
	cur, has, err := e.idx.Get(rel)
	if err != nil {
		return nil, err
	}
	var idx *index.Entry
	if has {
		idx = &cur
	}
	if has && remoteHash != "" && cur.Hash == remoteHash {
		return nil, nil // already have this version
	}
	divergent := has && key != "" && prevHash != cur.Hash
	if divergent && newerThan(cur.Journal, key) {
		e.log.Info("ignoring older divergent version", "path", rel, "entry", key, "have", cur.Journal)
		return nil, nil
	}
	loc, err := e.localState(rel, idx)
	if err != nil {
		return nil, err
	}
	if loc != nil && remoteHash != "" && loc.hash == remoteHash {
		// identical content already on disk (e.g. written on both nodes);
		// record the remote ETag so reconcile does not re-fetch it
		etag := remoteETag
		if etag == "" {
			etag = cur.ETag
		}
		return nil, e.idx.Put(rel, index.Entry{Size: loc.size, Mtime: loc.mtime, Hash: loc.hash, ETag: etag, Journal: key})
	}
	return e.pull(ctx, rel, key, idx, loc, divergent, remoteMtime)
}

// newerThan reports whether journal key a has a later timestamp than key b.
func newerThan(a, b string) bool {
	ta, _, _, errA := journal.ParseKey(a)
	tb, _, _, errB := journal.ParseKey(b)
	return errA == nil && errB == nil && ta > tb
}

// pull downloads data/<rel> to a temp file, then decides: identical to the
// local file → just record; local file modified since last sync, or the
// incoming version is divergent → keep ours as a conflict copy; then the
// download takes the path.
func (e *Engine) pull(ctx context.Context, rel, key string, idx *index.Entry, loc *localFile, divergent bool, fallbackMtime int64) ([]string, error) {
	if e.DryRun {
		e.log.Info("would download", "path", rel)
		return nil, nil
	}
	abs := e.abs(rel)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(abs), tmpPrefix)
	if err != nil {
		return nil, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	h := sha256.New()
	etag, meta, err := e.st.GetTo(ctx, journal.DataPrefix+rel, io.MultiWriter(tmp, h))
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil // deleted again meanwhile; a delete entry will follow
	}
	if err != nil {
		return nil, err
	}
	hash := "sha256:" + hex.EncodeToString(h.Sum(nil))
	if loc != nil && loc.hash == hash {
		return nil, e.idx.Put(rel, index.Entry{Size: loc.size, Mtime: loc.mtime, Hash: hash, ETag: etag, Journal: key})
	}
	var pending []string
	if loc != nil && (idx == nil || loc.hash != idx.Hash || divergent) {
		cname := conflictName(rel, e.cfg.Node, time.Now())
		if err := os.Rename(abs, e.abs(cname)); err != nil {
			return nil, fmt.Errorf("conflict copy: %w", err)
		}
		e.log.Warn("conflict", "path", rel, "kept_as", cname)
		pending = append(pending, cname)
	}
	mtime := fallbackMtime
	if v, err := strconv.ParseInt(metaGet(meta, "mtime"), 10, 64); err == nil && v > 0 {
		mtime = v
	}
	if mtime > 0 {
		t := time.Unix(mtime, 0)
		_ = os.Chtimes(tmpName, t, t)
	}
	if err := os.Rename(tmpName, abs); err != nil {
		return nil, err
	}
	fi, err := os.Stat(abs)
	if err != nil {
		return nil, err
	}
	e.log.Debug("downloaded", "path", rel, "size", fi.Size())
	return pending, e.idx.Put(rel, index.Entry{Size: fi.Size(), Mtime: fi.ModTime().UnixNano(), Hash: hash, ETag: etag, Journal: key})
}

// applyDelete moves the local file to trash unless it was modified locally
// since the last sync, in which case it is kept and re-uploaded.
func (e *Engine) applyDelete(ctx context.Context, rel, stamp string) ([]string, error) {
	cur, has, err := e.idx.Get(rel)
	if err != nil {
		return nil, err
	}
	var idx *index.Entry
	if has {
		idx = &cur
	}
	loc, err := e.localState(rel, idx)
	if err != nil {
		return nil, err
	}
	if loc == nil {
		return nil, e.idx.Delete(rel)
	}
	if has && loc.hash == cur.Hash {
		if e.DryRun {
			e.log.Info("would trash", "path", rel)
			return nil, nil
		}
		if err := e.trash(rel, stamp); err != nil {
			return nil, err
		}
		e.log.Debug("trashed", "path", rel)
		return nil, e.idx.Delete(rel)
	}
	e.log.Warn("remote delete ignored, local file modified", "path", rel)
	return []string{rel}, e.idx.Delete(rel)
}
