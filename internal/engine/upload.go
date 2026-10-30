package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/hmepas/gozeks/internal/index"
	"github.com/hmepas/gozeks/internal/journal"
)

// UploadPaths brings the bucket in line with the local state of the given
// root-relative paths (files or directories; missing paths become deletes),
// then commits a journal entry and bumps the cursor. Unchanged files are
// skipped by hash, which is what swallows echoes of our own downloads.
func (e *Engine) UploadPaths(ctx context.Context, rels []string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.uploadLocked(ctx, rels)
}

type uploadResult struct {
	rel   string
	op    *journal.Op
	entry *index.Entry // nil for delete
	err   error
}

func (e *Engine) uploadLocked(ctx context.Context, rels []string) error {
	files, deletes, err := e.expand(rels)
	if err != nil {
		return err
	}
	if len(files) == 0 && len(deletes) == 0 {
		return nil
	}
	e.log.Debug("upload candidates", "files", len(files), "deletes", len(deletes))

	jobs := make(chan string)
	results := make(chan uploadResult)
	var wg sync.WaitGroup
	for i := 0; i < transferWorker; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rel := range jobs {
				results <- e.uploadOne(ctx, rel)
			}
		}()
	}
	go func() {
		for _, rel := range files {
			jobs <- rel
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	var ops []journal.Op
	puts := map[string]index.Entry{}
	var dels []string
	flush := func() error {
		if len(ops) == 0 {
			return nil
		}
		_, err := e.commit(ctx, ops, puts, dels)
		ops, puts, dels = nil, map[string]index.Entry{}, nil
		return err
	}
	var firstErr error
	for r := range results {
		if r.err != nil {
			e.log.Error("upload", "path", r.rel, "err", r.err)
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		if r.op == nil {
			continue
		}
		ops = append(ops, *r.op)
		puts[r.rel] = *r.entry
		if len(ops) >= opsPerEntry {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	for _, rel := range deletes {
		if e.DryRun {
			e.log.Info("would delete remote", "path", rel)
			continue
		}
		if err := e.st.Delete(ctx, journal.DataPrefix+rel); err != nil {
			e.log.Error("delete remote", "path", rel, "err", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		e.log.Debug("deleted remote", "path", rel)
		ops = append(ops, journal.Op{Op: "delete", Path: rel})
		dels = append(dels, rel)
		if len(ops) >= opsPerEntry {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	return firstErr
}

// expand turns watcher paths into a list of files to consider and a list of
// indexed paths that no longer exist locally.
func (e *Engine) expand(rels []string) (files, deletes []string, err error) {
	seenF := map[string]bool{}
	seenD := map[string]bool{}
	addDel := func(rel string) {
		if !seenD[rel] {
			seenD[rel] = true
			deletes = append(deletes, rel)
		}
	}
	addFile := func(rel string) {
		if !seenF[rel] {
			seenF[rel] = true
			files = append(files, rel)
		}
	}
	for _, rel := range rels {
		if rel == "" {
			continue
		}
		if n := nfc(rel); n != rel {
			// watcher reported a non-NFC name: normalize it on disk first
			if fi, err := os.Lstat(e.abs(rel)); err == nil && fi.Mode().IsRegular() {
				if m, err := e.normalizeName(rel); err == nil && m != "" {
					addFile(m)
				}
			}
			// whatever happened on disk, the NFC path is what we track
			rel = n
		}
		if e.caseConflict(rel) {
			e.log.Warn("skipping path that differs only by case from a local file", "path", rel)
			continue
		}
		excluded := e.ex.Match(rel) || e.throughSymlink(rel)
		fi, statErr := os.Lstat(e.abs(rel))
		switch {
		case statErr != nil || excluded:
			// gone (or no longer wanted): drop it and anything indexed below it
			if _, has, _ := e.idx.Get(rel); has {
				addDel(rel)
			}
			under, _ := e.idx.Under(rel)
			for _, u := range under {
				addDel(u)
			}
		case fi.IsDir():
			present := map[string]bool{}
			err = e.walkLocal(rel, func(r string, _ os.FileInfo) error {
				present[r] = true
				addFile(r)
				return nil
			})
			if err != nil {
				return nil, nil, err
			}
			under, _ := e.idx.Under(rel)
			for _, u := range under {
				if !present[u] {
					addDel(u)
				}
			}
		case fi.Mode().IsRegular():
			addFile(rel)
		}
	}
	sort.Strings(files)
	sort.Strings(deletes)
	return files, deletes, nil
}

// uploadOne hashes a file and PUTs it when it differs from the index.
func (e *Engine) uploadOne(ctx context.Context, rel string) uploadResult {
	res := uploadResult{rel: rel}
	cur, has, err := e.idx.Get(rel)
	if err != nil {
		res.err = err
		return res
	}
	fi, err := os.Lstat(e.abs(rel))
	if err != nil || !fi.Mode().IsRegular() || e.throughSymlink(rel) {
		return res // vanished between expand and now; reconcile will sort it out
	}
	if has && cur.Size == fi.Size() && cur.Mtime == fi.ModTime().UnixNano() && !recent(fi.ModTime()) {
		return res // unchanged by stat
	}
	data, err := os.ReadFile(e.abs(rel))
	if err != nil {
		if os.IsNotExist(err) {
			return res
		}
		res.err = err
		return res
	}
	h := hashBytes(data)
	if has && cur.Hash == h {
		// content unchanged (e.g. echo of our own download): refresh stat only
		cur.Size, cur.Mtime = fi.Size(), fi.ModTime().UnixNano()
		_ = e.idx.Put(rel, cur)
		return res
	}
	if e.DryRun {
		e.log.Info("would upload", "path", rel, "size", len(data))
		return res
	}
	mtime := fi.ModTime().Unix()
	etag, err := e.st.Put(ctx, journal.DataPrefix+rel, data, map[string]string{
		"mtime": strconv.FormatInt(mtime, 10),
		"hash":  h,
	})
	if err != nil {
		res.err = err
		return res
	}
	e.log.Debug("uploaded", "path", rel, "size", len(data))
	res.op = &journal.Op{Op: "put", Path: rel, Size: int64(len(data)), Mtime: mtime, Hash: h, Prev: cur.Hash, ETag: etag}
	res.entry = &index.Entry{Size: int64(len(data)), Mtime: fi.ModTime().UnixNano(), Hash: h, ETag: etag}
	return res
}

// commit writes the journal entry, then the cursor, then updates the index.
func (e *Engine) commit(ctx context.Context, ops []journal.Op, puts map[string]index.Entry, dels []string) (string, error) {
	ts := time.Now().UnixMilli()
	seq := e.seq.Add(1)
	key := journal.Key(ts, e.cfg.Node, seq)
	body, err := json.Marshal(journal.Entry{Node: e.cfg.Node, Ts: ts, Seq: seq, Ops: ops})
	if err != nil {
		return "", err
	}
	if _, err := e.st.Put(ctx, key, body, nil); err != nil {
		return "", fmt.Errorf("journal: %w", err)
	}
	cur, _ := json.Marshal(journal.Cursor{Node: e.cfg.Node, Ts: ts, Seq: seq})
	// Deliberately not recorded as the "seen" cursor ETag: another node may
	// have written a batch in the same window, and only a journal LIST on
	// the next poll will show it.
	if _, err := e.st.Put(ctx, journal.CursorKey, cur, nil); err != nil {
		return "", fmt.Errorf("cursor: %w", err)
	}
	var paths []string
	for rel, ent := range puts {
		ent.Journal = key
		if err := e.idx.Put(rel, ent); err != nil {
			return "", err
		}
		e.noteChange("put", rel, e.cfg.Node)
		paths = append(paths, rel)
	}
	for _, rel := range dels {
		if err := e.idx.Delete(rel); err != nil {
			return "", err
		}
		e.noteChange("delete", rel, e.cfg.Node)
		paths = append(paths, rel)
	}
	_ = e.idx.MarkProcessed(key)
	e.setLastJournalTs(ts)
	e.touch()
	args := []any{"key", key, "puts", len(puts), "deletes", len(dels)}
	if len(paths) <= logPathsMax {
		sort.Strings(paths)
		args = append(args, "paths", paths)
	}
	e.log.Info("batch uploaded", args...)
	if e.OnKeys != nil {
		go e.OnKeys([]string{key})
	}
	return key, nil
}
