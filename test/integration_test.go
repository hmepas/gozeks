//go:build integration

// Package test runs several in-process gozeks daemons against a real S3 endpoint.
package test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/hmepas/gozeks/internal/config"
	"github.com/hmepas/gozeks/internal/engine"
	"github.com/hmepas/gozeks/internal/store"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

var (
	endpoint  = env("GOZEKS_TEST_S3_ENDPOINT", "http://127.0.0.1:9000")
	accessKey = env("GOZEKS_TEST_S3_ACCESS_KEY", "gozeks")
	secretKey = env("GOZEKS_TEST_S3_SECRET_KEY", "gozeks-secret")
)

var fastTimings = engine.Timings{LockPoll: time.Second, Skew: time.Second, JitterMax: time.Second, Verify: time.Second, Backoff: time.Second}

func rawClient() *s3.Client {
	return s3.New(s3.Options{
		Region: "us-east-1", BaseEndpoint: aws.String(endpoint), UsePathStyle: true,
		Credentials:                credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
}

// newBucket creates a fresh bucket and removes it when the test ends.
func newBucket(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("gozeks-%d", time.Now().UnixNano())
	c := rawClient()
	ctx := context.Background()
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &name}); err != nil {
		t.Skipf("cannot create bucket at %s: %v (is MinIO running? see test/README.md)", endpoint, err)
	}
	t.Cleanup(func() {
		p := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{Bucket: &name})
		for p.HasMorePages() {
			page, err := p.NextPage(ctx)
			if err != nil {
				return
			}
			for _, o := range page.Contents {
				_, _ = c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &name, Key: o.Key})
			}
		}
		_, _ = c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: &name})
	})
	return name
}

type daemon struct {
	name   string
	cfg    *config.Config
	st     *store.Store
	e      *engine.Engine
	cancel context.CancelFunc
	done   chan struct{}
	t      *testing.T
}

type opt func(*config.Config)

func withRelay(port int) opt {
	return func(c *config.Config) {
		c.Relay.Listen = fmt.Sprintf("127.0.0.1:%d", port)
		c.Relay.Advertise = fmt.Sprintf("https://127.0.0.1:%d", port)
	}
}

// noRelay disables the relay role: pure marker polling.
func noRelay() opt {
	return func(c *config.Config) { c.Relay.Token = "" }
}

func withPoll(poll, fallback time.Duration) opt {
	return func(c *config.Config) {
		c.Sync.Poll.Duration = poll
		c.Sync.FallbackPoll.Duration = fallback
	}
}

func newConfig(t *testing.T, bucket, name string, opts ...opt) *config.Config {
	t.Helper()
	base := t.TempDir()
	cfg := &config.Config{
		Node:     name,
		Root:     filepath.Join(base, "vault"),
		StateDir: filepath.Join(base, "state"),
		S3: config.S3{Endpoint: endpoint, Region: "us-east-1", Bucket: bucket, PathStyle: true,
			AccessKey: accessKey, SecretKey: secretKey},
	}
	cfg.Sync.Poll.Duration = time.Second
	cfg.Sync.IdlePoll.Duration = time.Second
	cfg.Sync.IdleAfter.Duration = time.Hour
	cfg.Sync.Debounce.Duration = 300 * time.Millisecond
	cfg.Sync.MaxBatchWait.Duration = time.Second
	cfg.Sync.Reconcile.Duration = time.Hour
	cfg.Sync.FallbackPoll.Duration = 5 * time.Second
	cfg.Sync.Exclude = config.DefaultExcludes
	cfg.Relay.Lease.Duration = 6 * time.Second
	cfg.Relay.Token = "test-token"
	for _, o := range opts {
		o(cfg)
	}
	if err := os.MkdirAll(cfg.Root, 0o755); err != nil {
		t.Fatal(err)
	}
	return cfg
}

// start runs a daemon on cfg (a restart reuses the same cfg and dirs).
func start(t *testing.T, cfg *config.Config) *daemon {
	t.Helper()
	st, err := store.New(cfg.S3)
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if testing.Verbose() {
		log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})).With("node", cfg.Node)
	}
	e, err := engine.New(cfg, st, log)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	d := &daemon{name: cfg.Node, cfg: cfg, st: st, e: e, cancel: cancel, done: make(chan struct{}), t: t}
	go func() {
		defer close(d.done)
		if err := e.Run(ctx, engine.RunOptions{Counters: &st.Counters, Timings: fastTimings}); err != nil {
			t.Errorf("%s: run: %v", cfg.Node, err)
		}
	}()
	t.Cleanup(d.stop)
	return d
}

func (d *daemon) stop() {
	d.cancel()
	select {
	case <-d.done:
	case <-time.After(15 * time.Second):
		d.t.Errorf("%s: daemon did not stop", d.name)
	}
	d.e.Close()
}

func (d *daemon) write(rel, content string) {
	d.t.Helper()
	p := filepath.Join(d.cfg.Root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		d.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		d.t.Fatal(err)
	}
}

func (d *daemon) read(rel string) (string, bool) {
	b, err := os.ReadFile(filepath.Join(d.cfg.Root, rel))
	return string(b), err == nil
}

func (d *daemon) tree() map[string]string {
	out := map[string]string{}
	_ = filepath.Walk(d.cfg.Root, func(p string, fi os.FileInfo, err error) error {
		if err != nil || fi.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(d.cfg.Root, p)
		b, _ := os.ReadFile(p)
		out[filepath.ToSlash(rel)] = string(b)
		return nil
	})
	return out
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) time.Duration {
	t.Helper()
	start := time.Now()
	for time.Since(start) < timeout {
		if cond() {
			return time.Since(start)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout after %s waiting for %s", timeout, what)
	return 0
}

func sameTrees(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func bucketKeys(t *testing.T, bucket, prefix string) []string {
	c := rawClient()
	var keys []string
	p := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{Bucket: &bucket, Prefix: &prefix})
	for p.HasMorePages() {
		page, err := p.NextPage(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range page.Contents {
			keys = append(keys, *o.Key)
		}
	}
	return keys
}

// ---- scenarios ----

func TestCreateModifyDeletePropagate(t *testing.T) {
	bucket := newBucket(t)
	a := start(t, newConfig(t, bucket, "a", noRelay()))
	b := start(t, newConfig(t, bucket, "b", noRelay()))

	a.write("Notes/one.md", "v1")
	d := eventually(t, 10*time.Second, "create", func() bool { v, ok := b.read("Notes/one.md"); return ok && v == "v1" })
	t.Logf("create propagated in %s", d)

	a.write("Notes/one.md", "v2")
	d = eventually(t, 10*time.Second, "modify", func() bool { v, _ := b.read("Notes/one.md"); return v == "v2" })
	t.Logf("modify propagated in %s", d)

	os.Remove(filepath.Join(a.cfg.Root, "Notes/one.md"))
	d = eventually(t, 10*time.Second, "delete", func() bool { _, ok := b.read("Notes/one.md"); return !ok })
	t.Logf("delete propagated in %s", d)

	// binary-ish content and nested dirs
	a.write("Files/deep/er/blob.bin", strings.Repeat("\x00\x01\x02", 50000))
	eventually(t, 10*time.Second, "nested", func() bool { v, ok := b.read("Files/deep/er/blob.bin"); return ok && len(v) == 150000 })
}

func TestSimultaneousEditOneConflict(t *testing.T) {
	bucket := newBucket(t)
	a := start(t, newConfig(t, bucket, "a"))
	b := start(t, newConfig(t, bucket, "b"))
	a.write("n.md", "base")
	eventually(t, 10*time.Second, "seed", func() bool { v, _ := b.read("n.md"); return v == "base" })

	a.write("n.md", "edit a")
	b.write("n.md", "edit b")
	eventually(t, 20*time.Second, "convergence", func() bool {
		ta, tb := a.tree(), b.tree()
		return len(ta) == 2 && sameTrees(ta, tb)
	})
	tr := a.tree()
	conflicts := 0
	for p := range tr {
		if strings.Contains(p, "(conflict ") {
			conflicts++
		}
	}
	if conflicts != 1 {
		t.Fatalf("expected exactly one conflict copy, tree: %v", tr)
	}
	vals := map[string]bool{}
	for _, v := range tr {
		vals[v] = true
	}
	if !vals["edit a"] || !vals["edit b"] {
		t.Fatalf("both edits must survive: %v", tr)
	}
	// stays stable: no conflict ping-pong
	time.Sleep(4 * time.Second)
	if !sameTrees(a.tree(), tr) || !sameTrees(b.tree(), tr) {
		t.Fatalf("trees changed after convergence: %v / %v", a.tree(), b.tree())
	}
}

func TestJournalRaceThirdNodeGetsBoth(t *testing.T) {
	bucket := newBucket(t)
	a := start(t, newConfig(t, bucket, "a", noRelay()))
	b := start(t, newConfig(t, bucket, "b", noRelay()))
	c := start(t, newConfig(t, bucket, "c", noRelay(), withPoll(3*time.Second, 3*time.Second)))
	for i := 0; i < 3; i++ {
		a.write(fmt.Sprintf("a%d.md", i), "a")
		b.write(fmt.Sprintf("b%d.md", i), "b")
	}
	eventually(t, 15*time.Second, "all six files on c", func() bool { return len(c.tree()) == 6 })
}

func TestCrashMidBatchHeals(t *testing.T) {
	bucket := newBucket(t)
	a := start(t, newConfig(t, bucket, "a"))
	cfgB := newConfig(t, bucket, "b")
	b := start(t, cfgB)
	// seed many files on B, then kill B while it is uploading
	for i := 0; i < 60; i++ {
		b.write(fmt.Sprintf("bulk/%02d.md", i), strings.Repeat("x", 2000+i))
	}
	eventually(t, 10*time.Second, "upload started", func() bool { return b.st.Counters.Put.Load() > 5 })
	b.stop()
	uploaded := len(bucketKeys(t, bucket, "data/bulk/"))
	t.Logf("B killed after %d of 60 files reached the bucket", uploaded)

	// B restarts; startup reconcile uploads the rest
	b = start(t, cfgB)
	eventually(t, 30*time.Second, "all 60 files in bucket", func() bool { return len(bucketKeys(t, bucket, "data/bulk/")) == 60 })
	// files that reached the bucket before the crash were never journaled;
	// A's periodic reconcile (forced here) is what picks them up
	time.Sleep(2 * time.Second)
	if err := a.e.Reconcile(context.Background(), engine.ReconcileOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, "a has all 60", func() bool { return len(a.tree()) == 60 })
	if !sameTrees(a.tree(), b.tree()) {
		t.Fatal("trees differ after heal")
	}
	for p := range a.tree() {
		if strings.Contains(p, "conflict") {
			t.Fatalf("spurious conflict copy %s", p)
		}
	}
}

func TestOwnDownloadsDoNotEcho(t *testing.T) {
	bucket := newBucket(t)
	a := start(t, newConfig(t, bucket, "a", noRelay()))
	b := start(t, newConfig(t, bucket, "b", noRelay()))
	for i := 0; i < 5; i++ {
		a.write(fmt.Sprintf("f%d.md", i), fmt.Sprint(i))
	}
	eventually(t, 10*time.Second, "b has files", func() bool { return len(b.tree()) == 5 })
	time.Sleep(3 * time.Second) // let B's debounce window pass
	if n := b.st.Counters.Put.Load(); n != 0 {
		t.Fatalf("B made %d PUT requests after only downloading", n)
	}
	journals := bucketKeys(t, bucket, "_sync/journal/")
	for _, k := range journals {
		if strings.Contains(k, "-b-") {
			t.Fatalf("B wrote a journal entry: %s", k)
		}
	}
}

func TestExcludedPathsNeverUploaded(t *testing.T) {
	bucket := newBucket(t)
	a := start(t, newConfig(t, bucket, "a"))
	a.write(".obsidian/workspace.json", "{}")
	a.write(".obsidian/app.json", "{}")
	a.write(".trash/old.md", "x")
	a.write("Notes/.DS_Store", "x")
	a.write("Notes/draft.tmp", "x")
	a.write("Notes/real.md", "x")
	eventually(t, 10*time.Second, "real file uploaded", func() bool { return len(bucketKeys(t, bucket, "data/Notes/real.md")) == 1 })
	time.Sleep(2 * time.Second)
	keys := bucketKeys(t, bucket, "data/")
	if len(keys) != 2 {
		t.Fatalf("bucket has %v, expected only data/.obsidian/app.json and data/Notes/real.md", keys)
	}
}

func TestRelayFastPathAndFailover(t *testing.T) {
	bucket := newBucket(t)
	// Slow polls everywhere so that only the relay can deliver in < 2 s.
	slow := withPoll(2*time.Second, 60*time.Second)
	portR, portC := freePort(t), freePort(t)
	cfgR := newConfig(t, bucket, "relay1", slow, withRelay(portR))
	r := start(t, cfgR)
	eventually(t, 10*time.Second, "relay1 leads", func() bool { return r.e.Role() == "leader" })

	a := start(t, newConfig(t, bucket, "a", slow))
	b := start(t, newConfig(t, bucket, "b", slow))
	eventually(t, 10*time.Second, "a and b connected", func() bool { return a.e.RelayConnected() && b.e.RelayConnected() })

	a.write("fast.md", "hello")
	d := eventually(t, 5*time.Second, "relay delivery", func() bool { v, _ := b.read("fast.md"); return v == "hello" })
	t.Logf("relay path delivered in %s", d)
	if d > 2*time.Second {
		t.Fatalf("relay path took %s, want < 2s", d)
	}

	// kill the relay: B falls back to polling and still receives changes
	r.stop()
	a.write("after-kill.md", "still works")
	d = eventually(t, 15*time.Second, "fallback delivery", func() bool { v, _ := b.read("after-kill.md"); return v == "still works" })
	t.Logf("fallback delivered in %s", d)

	// a second candidate takes over once the lease expires
	c := start(t, newConfig(t, bucket, "relay2", slow, withRelay(portC)))
	d = eventually(t, 30*time.Second, "relay2 leads", func() bool { return c.e.Role() == "leader" })
	t.Logf("failover took %s", d)
	eventually(t, 15*time.Second, "b reconnects", func() bool { return b.e.RelayConnected() })

	// the old leader comes back and must stay a follower
	r = start(t, cfgR)
	time.Sleep(6 * time.Second)
	if r.e.Role() == "leader" {
		t.Fatal("restarted old leader took the lease back")
	}
	if c.e.Role() != "leader" {
		t.Fatalf("relay2 lost leadership: %s", c.e.Role())
	}
	eventually(t, 15*time.Second, "old leader follows", func() bool { return r.e.RelayConnected() })
	a.write("after-failover.md", "x")
	eventually(t, 5*time.Second, "delivery via relay2", func() bool { _, ok := b.read("after-failover.md"); return ok })
}

func TestTwoCandidatesOneLeader(t *testing.T) {
	bucket := newBucket(t)
	var ds []*daemon
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, n := range []string{"c1", "c2"} {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			d := start(t, newConfig(t, bucket, name, withRelay(freePort(t))))
			mu.Lock()
			ds = append(ds, d)
			mu.Unlock()
		}(n)
	}
	wg.Wait()
	time.Sleep(10 * time.Second)
	leaders := 0
	for _, d := range ds {
		if d.e.Role() == "leader" {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("%d leaders after 10 s", leaders)
	}
}
