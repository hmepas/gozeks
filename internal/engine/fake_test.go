package engine

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hmepas/gozeks/internal/config"
	"github.com/hmepas/gozeks/internal/store"
)

type fakeObj struct {
	body []byte
	etag string
	meta map[string]string
}

// fakeStore is an in-memory S3 with request counters.
type fakeStore struct {
	mu   sync.Mutex
	objs map[string]fakeObj
	puts int
	gets int
}

func newFake() *fakeStore { return &fakeStore{objs: map[string]fakeObj{}} }

func (f *fakeStore) Put(_ context.Context, key string, body []byte, meta map[string]string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts++
	sum := md5.Sum(body)
	o := fakeObj{body: append([]byte(nil), body...), etag: hex.EncodeToString(sum[:]), meta: meta}
	f.objs[key] = o
	return o.etag, nil
}

func (f *fakeStore) Get(ctx context.Context, key string) ([]byte, string, map[string]string, error) {
	var buf bytes.Buffer
	etag, meta, err := f.GetTo(ctx, key, &buf)
	return buf.Bytes(), etag, meta, err
}

func (f *fakeStore) GetTo(_ context.Context, key string, w io.Writer) (string, map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	o, ok := f.objs[key]
	if !ok {
		return "", nil, store.ErrNotFound
	}
	if _, err := w.Write(o.body); err != nil {
		return "", nil, err
	}
	return o.etag, o.meta, nil
}

func (f *fakeStore) Head(_ context.Context, key string) (string, map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	o, ok := f.objs[key]
	if !ok {
		return "", nil, store.ErrNotFound
	}
	return o.etag, o.meta, nil
}

func (f *fakeStore) Delete(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objs, key)
	return nil
}

func (f *fakeStore) List(_ context.Context, prefix, startAfter string, fn func(store.Object) error) error {
	f.mu.Lock()
	var keys []string
	for k := range f.objs {
		if strings.HasPrefix(k, prefix) && k > startAfter {
			keys = append(keys, k)
		}
	}
	f.mu.Unlock()
	sort.Strings(keys)
	for _, k := range keys {
		f.mu.Lock()
		o := f.objs[k]
		f.mu.Unlock()
		if err := fn(store.Object{Key: k, ETag: o.etag, Size: int64(len(o.body))}); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeStore) has(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.objs[key]
	return ok
}

func (f *fakeStore) keys(prefix string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for k := range f.objs {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

type node struct {
	e    *Engine
	root string
	cfg  *config.Config
}

func newNode(t *testing.T, fs *fakeStore, name string) *node {
	t.Helper()
	root := filepath.Join(t.TempDir(), "vault")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Node: name, Root: root, StateDir: filepath.Join(t.TempDir(), "state")}
	cfg.Sync.Exclude = config.DefaultExcludes
	cfg.Relay.Lease.Duration = time.Minute
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if testing.Verbose() {
		log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})).With("node", name)
	}
	e, err := New(cfg, fs, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return &node{e: e, root: root, cfg: cfg}
}

func (n *node) write(t *testing.T, rel, content string) {
	t.Helper()
	p := filepath.Join(n.root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func (n *node) read(t *testing.T, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(n.root, rel))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return string(b)
}

func (n *node) exists(rel string) bool {
	_, err := os.Stat(filepath.Join(n.root, rel))
	return err == nil
}

func (n *node) upload(t *testing.T, rels ...string) {
	t.Helper()
	if err := n.e.UploadPaths(context.Background(), rels); err != nil {
		t.Fatal(err)
	}
}

func (n *node) poll(t *testing.T) []string {
	t.Helper()
	keys, err := n.e.CheckCursor(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func (n *node) tree(t *testing.T) map[string]string {
	t.Helper()
	out := map[string]string{}
	_ = filepath.Walk(n.root, func(p string, fi os.FileInfo, err error) error {
		if err != nil || fi.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(n.root, p)
		b, _ := os.ReadFile(p)
		out[filepath.ToSlash(rel)] = string(b)
		return nil
	})
	return out
}

func (n *node) trashed(t *testing.T) []string {
	var out []string
	dir := filepath.Join(n.cfg.StateDir, "trash")
	_ = filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
		if err != nil || fi.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(dir, p)
		out = append(out, filepath.ToSlash(rel))
		return nil
	})
	return out
}

// hookStore runs onData before every GET of a data object, so a test can fail
// the request or change the bucket at that exact moment.
type hookStore struct {
	*fakeStore
	onData func() error
}

func (h *hookStore) GetTo(ctx context.Context, key string, w io.Writer) (string, map[string]string, error) {
	if strings.HasPrefix(key, "data/") {
		if err := h.onData(); err != nil {
			return "", nil, err
		}
	}
	return h.fakeStore.GetTo(ctx, key, w)
}

func (n *node) conflictCopies(t *testing.T) []string {
	t.Helper()
	var out []string
	for p := range n.tree(t) {
		if strings.Contains(p, "(conflict ") {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}
