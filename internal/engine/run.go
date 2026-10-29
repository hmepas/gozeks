package engine

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hmepas/gozeks/internal/batch"
	"github.com/hmepas/gozeks/internal/journal"
	"github.com/hmepas/gozeks/internal/relay"
	"github.com/hmepas/gozeks/internal/store"
	"github.com/hmepas/gozeks/internal/watch"
)

// Timings are the relay election delays; shortened in tests.
type Timings struct {
	LockPoll  time.Duration // follower lock check while disconnected
	Skew      time.Duration // tolerated clock skew when judging a lease
	JitterMax time.Duration // random wait before claiming a free lock
	Verify    time.Duration // wait before re-reading a claimed lock
	Backoff   time.Duration // wait after losing a race or a connection
}

// DefaultTimings follow the spec.
var DefaultTimings = Timings{
	LockPoll:  30 * time.Second,
	Skew:      10 * time.Second,
	JitterMax: 3 * time.Second,
	Verify:    3 * time.Second,
	Backoff:   5 * time.Second,
}

type relayState struct {
	role      atomic.Value // string
	connected atomic.Bool
	mu        sync.Mutex
	srv       *relay.Server
	lock      *relay.Lock // the relay we follow
}

func (r *relayState) server() *relay.Server {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.srv
}

func (r *relayState) url() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lock != nil {
		return r.lock.URL
	}
	return ""
}

// RunOptions configure the daemon.
type RunOptions struct {
	Counters *store.Counters // request counters to persist and report
	Timings  Timings
	// SkipStartupReconcile is used by tests that seed state themselves.
	SkipStartupReconcile bool
}

// Run is the daemon: watch + poll + reconcile (+ relay role) until ctx ends.
func (e *Engine) Run(ctx context.Context, opts RunOptions) error {
	if opts.Timings == (Timings{}) {
		opts.Timings = DefaultTimings
	}
	if opts.Counters != nil {
		e.loadCounters(opts.Counters)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup

	// local changes → batches → uploads. Started before the startup reconcile
	// so edits made during it are not lost until the next reconcile.
	b := batch.New(e.cfg.Sync.Debounce.Duration, e.cfg.Sync.MaxBatchWait.Duration)
	e.pend = b.Pending
	events := make(chan string, 1024)
	if err := watch.Start(ctx, e.cfg.Root, events); err != nil {
		return err
	}
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case rel := <-events:
				if rel == "" || e.ex.Match(rel) {
					continue
				}
				b.Add(rel)
			}
		}
	}()
	go func() {
		defer wg.Done()
		b.Run(ctx, func(paths []string) {
			if err := e.UploadPaths(context.WithoutCancel(ctx), paths); err != nil {
				e.log.Error("upload batch", "err", err)
			}
		})
	}()

	if !opts.SkipStartupReconcile {
		if err := e.Reconcile(ctx, ReconcileOptions{}); err != nil {
			e.log.Error("startup reconcile", "err", err)
		}
	}

	// relay role
	e.OnKeys = e.announce
	if e.cfg.Relay.Token != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.relayLoop(ctx, opts.Timings)
		}()
	} else {
		e.log.Info("relay disabled (GOZEKS_RELAY_TOKEN not set); polling only")
	}

	// marker polling
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			iv := e.cfg.Sync.Poll.Duration
			switch {
			case e.relay.connected.Load():
				iv = e.cfg.Sync.FallbackPoll.Duration
			case time.Since(time.Unix(0, e.lastChange.Load())) > e.cfg.Sync.IdleAfter.Duration:
				iv = e.cfg.Sync.IdlePoll.Duration
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(iv):
			}
			e.pollOnce(ctx)
		}
	}()

	// periodic reconcile, status file, hourly counters
	wg.Add(1)
	go func() {
		defer wg.Done()
		rt := time.NewTicker(e.cfg.Sync.Reconcile.Duration)
		st := time.NewTicker(10 * time.Second)
		ht := time.NewTicker(time.Hour)
		defer rt.Stop()
		defer st.Stop()
		defer ht.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-rt.C:
				if err := e.Reconcile(ctx, ReconcileOptions{}); err != nil {
					e.log.Error("reconcile", "err", err)
				}
			case <-st.C:
				e.writeStatus(opts.Counters)
				if opts.Counters != nil {
					e.saveCounters(opts.Counters)
				}
			case <-ht.C:
				if opts.Counters != nil {
					s := opts.Counters.Snapshot()
					e.log.Info("request counters", "put", s.Put, "get", s.Get, "delete", s.Delete,
						"bytes_up", s.BytesUp, "bytes_down", s.BytesDown)
				}
			}
		}
	}()

	e.writeStatus(opts.Counters)
	<-ctx.Done()
	wg.Wait()
	if srv := e.relay.server(); srv != nil {
		sctx, c := context.WithTimeout(context.Background(), 3*time.Second)
		srv.Shutdown(sctx)
		c()
	}
	if opts.Counters != nil {
		e.saveCounters(opts.Counters)
	}
	e.writeStatus(opts.Counters)
	return nil
}

// pollOnce checks the marker; when we are the relay, newly discovered keys
// are broadcast to followers.
func (e *Engine) pollOnce(ctx context.Context) {
	keys, err := e.CheckCursor(ctx)
	if err != nil && ctx.Err() == nil {
		e.log.Warn("poll", "err", err)
	}
	if len(keys) > 0 {
		if srv := e.relay.server(); srv != nil {
			srv.Broadcast(keys)
		}
	}
}

// announce hands our own journal keys to the relay (or broadcasts them when
// we are the relay). Failure is not an error: polling covers it.
func (e *Engine) announce(keys []string) {
	if srv := e.relay.server(); srv != nil {
		srv.Broadcast(keys)
		return
	}
	e.relay.mu.Lock()
	lock := e.relay.lock
	connected := e.relay.connected.Load()
	e.relay.mu.Unlock()
	if lock == nil || !connected {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := relay.Notify(ctx, lock.URL, lock.CertSHA256, e.cfg.Relay.Token, keys); err != nil {
		e.log.Debug("relay notify failed", "err", err)
	}
}

func (e *Engine) getLock(ctx context.Context) (*relay.Lock, error) {
	body, _, _, err := e.st.Get(ctx, journal.LockKey)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var l relay.Lock
	if err := json.Unmarshal(body, &l); err != nil {
		return nil, nil // garbage lock: treat as absent
	}
	return &l, nil
}

func (e *Engine) putLock(ctx context.Context, fp string) error {
	l := relay.Lock{
		Node:       e.cfg.Node,
		URL:        e.cfg.Relay.Advertise,
		CertSHA256: fp,
		LeaseUntil: time.Now().Add(e.cfg.Relay.Lease.Duration).Unix(),
	}
	b, _ := json.Marshal(l)
	_, err := e.st.Put(ctx, journal.LockKey, b, nil)
	return err
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// relayLoop runs the candidate/follower state machine.
func (e *Engine) relayLoop(ctx context.Context, t Timings) {
	candidate := e.cfg.Relay.Listen != ""
	var cert tls.Certificate
	if candidate {
		c, err := relay.LoadOrCreateCert(e.cfg.StateDir)
		if err != nil {
			e.log.Error("relay certificate", "err", err)
			candidate = false
		}
		cert = c
	}
	e.relay.role.Store("follower")
	for ctx.Err() == nil {
		lock, err := e.getLock(ctx)
		if err != nil {
			e.log.Warn("relay lock", "err", err)
			sleepCtx(ctx, t.Backoff)
			continue
		}
		free := lock.Expired(time.Now(), t.Skew)
		if candidate && (free || lock.Node == e.cfg.Node) {
			e.relay.role.Store("candidate")
			if free && !sleepCtx(ctx, time.Duration(rand.Int64N(int64(t.JitterMax)+1))) {
				return
			}
			if l, _ := e.getLock(ctx); free && !l.Expired(time.Now(), t.Skew) && l.Node != e.cfg.Node {
				continue // someone claimed it during our jitter
			}
			if err := e.putLock(ctx, relay.Fingerprint(cert)); err != nil {
				e.log.Warn("relay lock put", "err", err)
				sleepCtx(ctx, t.Backoff)
				continue
			}
			if !sleepCtx(ctx, t.Verify) {
				return
			}
			l, err := e.getLock(ctx)
			if err != nil || l == nil || l.Node != e.cfg.Node {
				e.log.Info("lost relay election", "winner", nodeOf(l))
				e.relay.role.Store("follower")
				sleepCtx(ctx, t.Backoff)
				continue
			}
			e.lead(ctx, cert, t)
			e.relay.role.Store("follower")
			continue
		}
		if lock != nil && !free && lock.Node != e.cfg.Node && lock.URL != "" {
			if e.follow(ctx, lock) {
				sleepCtx(ctx, time.Duration(rand.Int64N(int64(t.Backoff)+1)))
			} else {
				// never connected (unreachable relay): re-read the lock at the
				// slow follower cadence instead of hammering GET lock + dial
				sleepCtx(ctx, t.LockPoll)
			}
			continue
		}
		sleepCtx(ctx, t.LockPoll)
	}
}

func nodeOf(l *relay.Lock) string {
	if l == nil {
		return ""
	}
	return l.Node
}

// lead runs the relay server and renews the lease until we lose it.
func (e *Engine) lead(ctx context.Context, cert tls.Certificate, t Timings) {
	srv := relay.NewServer(e.cfg.Relay.Listen, e.cfg.Relay.Token, cert, e.log)
	if err := srv.Start(); err != nil {
		e.log.Error("relay listen", "err", err)
		sleepCtx(ctx, t.Backoff)
		return
	}
	e.relay.mu.Lock()
	e.relay.srv = srv
	e.relay.mu.Unlock()
	e.relay.role.Store("leader")
	e.log.Info("became relay", "listen", e.cfg.Relay.Listen, "advertise", e.cfg.Relay.Advertise)
	defer func() {
		e.relay.mu.Lock()
		e.relay.srv = nil
		e.relay.mu.Unlock()
		sctx, c := context.WithTimeout(context.Background(), 3*time.Second)
		srv.Shutdown(sctx)
		c()
		e.log.Info("stopped relaying")
	}()
	renew := e.cfg.Relay.Lease.Duration / 2
	for {
		if !sleepCtx(ctx, renew) {
			return
		}
		l, err := e.getLock(ctx)
		if err != nil {
			e.log.Warn("relay lock", "err", err)
			continue
		}
		if l == nil || l.Node != e.cfg.Node {
			e.log.Warn("relay lock taken by another node, stepping down", "node", nodeOf(l))
			return
		}
		if err := e.putLock(ctx, relay.Fingerprint(cert)); err != nil {
			e.log.Warn("lease renew", "err", err)
		}
	}
}

// follow holds an SSE connection to the relay until it drops. It reports
// whether the stream was ever established.
func (e *Engine) follow(ctx context.Context, lock *relay.Lock) bool {
	e.relay.mu.Lock()
	e.relay.lock = lock
	e.relay.mu.Unlock()
	keys := make(chan string, 1024)
	applyCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		for {
			select {
			case <-applyCtx.Done():
				return
			case k := <-keys:
				if _, err := e.ApplyKeys(applyCtx, []string{k}); err != nil && applyCtx.Err() == nil {
					e.log.Warn("apply relayed key", "key", k, "err", err)
				}
			}
		}
	}()
	err := relay.Subscribe(ctx, lock.URL, lock.CertSHA256, e.cfg.Relay.Token,
		func() {
			e.relay.connected.Store(true)
			e.log.Info("connected to relay", "url", lock.URL, "node", lock.Node)
			go e.pollOnce(ctx) // catch up on anything missed while disconnected
		},
		func(k string) {
			select {
			case keys <- k:
			default:
			}
		})
	wasConnected := e.relay.connected.Swap(false)
	e.relay.mu.Lock()
	e.relay.lock = nil
	e.relay.mu.Unlock()
	if ctx.Err() != nil {
		return wasConnected
	}
	if errors.Is(err, relay.ErrLeaderGone) {
		e.log.Info("relay stepped down")
	} else if wasConnected {
		e.log.Warn("relay connection lost", "err", err)
	} else {
		e.log.Debug("relay unreachable", "url", lock.URL, "err", err)
		return false // the poll loop keeps checking the marker on its own
	}
	e.pollOnce(ctx) // catch up on keys pushed while the stream was closing
	return true
}
