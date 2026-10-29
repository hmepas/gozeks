package engine

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hmepas/gozeks/internal/index"
	"github.com/hmepas/gozeks/internal/journal"
	"github.com/hmepas/gozeks/internal/store"
)

// ReconcileOptions tune a reconcile pass.
type ReconcileOptions struct {
	// AllowEmptyLocal disables the guard that refuses to act when the local
	// tree is empty but the index is not (missing mount, wrong root).
	AllowEmptyLocal bool
}

type localInfo struct {
	size  int64
	mtime int64
}

// Reconcile compares bucket, local tree and index and repairs every
// difference using the same rules as the journal path.
func (e *Engine) Reconcile(ctx context.Context, opts ReconcileOptions) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	start := time.Now()

	remote := map[string]store.Object{}
	err := e.st.List(ctx, journal.DataPrefix, "", func(o store.Object) error {
		rel := strings.TrimPrefix(o.Key, journal.DataPrefix)
		if rel == "" || strings.HasSuffix(rel, "/") || e.ex.Match(rel) {
			return nil
		}
		remote[rel] = o
		return nil
	})
	if err != nil {
		return err
	}
	local := map[string]localInfo{}
	err = e.walkLocal("", func(rel string, fi fs.FileInfo) error {
		local[rel] = localInfo{size: fi.Size(), mtime: fi.ModTime().UnixNano()}
		return nil
	})
	if err != nil {
		return err
	}
	indexed := map[string]index.Entry{}
	err = e.idx.All(func(rel string, ent index.Entry) error {
		indexed[rel] = ent
		return nil
	})
	if err != nil {
		return err
	}
	if len(local) == 0 && len(indexed) > 0 && !opts.AllowEmptyLocal {
		return errors.New("reconcile: local tree is empty but the index is not; refusing to act (missing mount?)")
	}

	all := map[string]struct{}{}
	for k := range remote {
		all[k] = struct{}{}
	}
	for k := range local {
		all[k] = struct{}{}
	}
	for k := range indexed {
		all[k] = struct{}{}
	}
	rels := make([]string, 0, len(all))
	for k := range all {
		rels = append(rels, k)
	}
	sort.Strings(rels)

	var toUpload, toDownload, toTrash []string
	stamp := time.Now().Format("20060102-150405")
	for _, rel := range rels {
		L, hasL := local[rel]
		R, hasR := remote[rel]
		I, hasI := indexed[rel]
		localChanged := func() bool {
			if !hasI {
				return true
			}
			if L.size == I.Size && L.mtime == I.Mtime {
				return false
			}
			h, err := hashFile(e.abs(rel))
			if err != nil {
				return true
			}
			if h == I.Hash {
				I.Size, I.Mtime = L.size, L.mtime
				_ = e.idx.Put(rel, I)
				return false
			}
			return true
		}
		switch {
		case hasL && hasR:
			lc := localChanged()
			rc := !hasI || R.ETag != I.ETag
			switch {
			case lc && rc, rc:
				toDownload = append(toDownload, rel) // pull() detects conflicts and identical content
			case lc:
				toUpload = append(toUpload, rel)
			}
		case hasL && !hasR:
			if hasI && !localChanged() {
				toTrash = append(toTrash, rel) // deleted remotely while we were away
			} else {
				toUpload = append(toUpload, rel)
			}
		case !hasL && hasR:
			if hasI {
				toUpload = append(toUpload, rel) // uploadLocked turns a missing indexed path into a remote delete
			} else {
				toDownload = append(toDownload, rel)
			}
		default:
			if hasI && !e.DryRun {
				_ = e.idx.Delete(rel)
			}
		}
	}
	e.log.Info("reconcile plan", "remote", len(remote), "local", len(local), "indexed", len(indexed),
		"upload", len(toUpload), "download", len(toDownload), "trash", len(toTrash))

	for _, rel := range toTrash {
		if e.DryRun {
			e.log.Info("would trash", "path", rel)
			continue
		}
		if err := e.trash(rel, stamp); err != nil {
			e.log.Error("trash", "path", rel, "err", err)
			continue
		}
		_ = e.idx.Delete(rel)
	}
	pending := e.downloadMany(ctx, toDownload)
	toUpload = append(toUpload, pending...)
	if len(toUpload) > 0 {
		if err := e.uploadLocked(ctx, toUpload); err != nil {
			e.log.Error("reconcile upload", "err", err)
		}
	}
	if !e.DryRun {
		e.pruneJournal(ctx)
	}
	e.log.Info("reconcile done", "took", time.Since(start).Round(time.Millisecond))
	return nil
}

// downloadMany pulls paths concurrently and returns paths that need uploading.
func (e *Engine) downloadMany(ctx context.Context, rels []string) []string {
	var mu sync.Mutex
	var pending []string
	jobs := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < transferWorker; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rel := range jobs {
				p, err := e.applyPut(ctx, rel, "", "", "", "", 0)
				if err != nil {
					e.log.Error("download", "path", rel, "err", err)
					continue
				}
				mu.Lock()
				pending = append(pending, p...)
				mu.Unlock()
			}
		}()
	}
	for _, rel := range rels {
		jobs <- rel
	}
	close(jobs)
	wg.Wait()
	return pending
}

// pruneJournal deletes journal entries older than journalKeep and forgets
// processed keys older than processedKeep.
func (e *Engine) pruneJournal(ctx context.Context) {
	cutoff := time.Now().Add(-journalKeep).UnixMilli()
	limit := fmt.Sprintf("%s%013d", journal.Prefix, cutoff)
	var old []string
	err := e.st.List(ctx, journal.Prefix, "", func(o store.Object) error {
		if o.Key < limit {
			old = append(old, o.Key)
			return nil
		}
		return errStopList
	})
	if err != nil && !errors.Is(err, errStopList) {
		e.log.Warn("journal prune list", "err", err)
		return
	}
	for _, k := range old {
		if err := e.st.Delete(ctx, k); err != nil {
			e.log.Warn("journal prune", "key", k, "err", err)
		}
	}
	if len(old) > 0 {
		e.log.Info("journal pruned", "entries", len(old))
	}
	_ = e.idx.PruneProcessed(journal.StartAfter(time.Now().Add(-processedKeep).UnixMilli(), 0))
}

var errStopList = errors.New("stop")
