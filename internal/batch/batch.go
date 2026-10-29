// Package batch debounces per-path change notifications and flushes them in
// batches: a path is flushed once it has been quiet for `debounce`, and any
// path is flushed at most `maxWait` after it was first seen.
package batch

import (
	"context"
	"sync"
	"time"
)

type pending struct{ first, last time.Time }

// Batcher collects paths.
type Batcher struct {
	mu       sync.Mutex
	paths    map[string]pending
	debounce time.Duration
	maxWait  time.Duration
	tick     time.Duration
	kick     chan struct{}
}

// New creates a batcher.
func New(debounce, maxWait time.Duration) *Batcher {
	tick := debounce / 4
	if tick < 10*time.Millisecond {
		tick = 10 * time.Millisecond
	}
	if tick > 250*time.Millisecond {
		tick = 250 * time.Millisecond
	}
	return &Batcher{paths: map[string]pending{}, debounce: debounce, maxWait: maxWait, tick: tick, kick: make(chan struct{}, 1)}
}

// Add records a change to path.
func (b *Batcher) Add(path string) {
	now := time.Now()
	b.mu.Lock()
	p, ok := b.paths[path]
	if !ok {
		p.first = now
	}
	p.last = now
	b.paths[path] = p
	b.mu.Unlock()
}

// Pending returns the number of paths waiting.
func (b *Batcher) Pending() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.paths)
}

// Flush forces everything pending out on the next tick.
func (b *Batcher) Flush() {
	select {
	case b.kick <- struct{}{}:
	default:
	}
}

// Run calls flush with ready paths until ctx is done. On exit any remaining
// paths are flushed once.
func (b *Batcher) Run(ctx context.Context, flush func([]string)) {
	t := time.NewTicker(b.tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			if ready := b.take(true); len(ready) > 0 {
				flush(ready)
			}
			return
		case <-b.kick:
			if ready := b.take(true); len(ready) > 0 {
				flush(ready)
			}
		case <-t.C:
			if ready := b.take(false); len(ready) > 0 {
				flush(ready)
			}
		}
	}
}

func (b *Batcher) take(all bool) []string {
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	for p, st := range b.paths {
		if all || now.Sub(st.last) >= b.debounce || now.Sub(st.first) >= b.maxWait {
			out = append(out, p)
			delete(b.paths, p)
		}
	}
	return out
}
