package batch

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"
)

func collect(ctx context.Context, b *Batcher) (*sync.Mutex, *[][]string) {
	var mu sync.Mutex
	var batches [][]string
	go b.Run(ctx, func(p []string) {
		sort.Strings(p)
		mu.Lock()
		batches = append(batches, p)
		mu.Unlock()
	})
	return &mu, &batches
}

func TestDebounceGroupsBurst(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := New(100*time.Millisecond, time.Second)
	mu, batches := collect(ctx, b)
	for i := 0; i < 5; i++ {
		b.Add("a")
		b.Add("b")
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(250 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(*batches) != 1 || len((*batches)[0]) != 2 {
		t.Fatalf("batches %v", *batches)
	}
}

func TestMaxWaitForcesFlush(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := New(100*time.Millisecond, 300*time.Millisecond)
	mu, batches := collect(ctx, b)
	stop := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(stop) {
		b.Add("hot") // never quiet
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	n := len(*batches)
	mu.Unlock()
	if n < 1 {
		t.Fatalf("expected a forced flush, got %d", n)
	}
}

func TestFlushOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	b := New(time.Hour, time.Hour)
	done := make(chan []string, 1)
	go b.Run(ctx, func(p []string) { done <- p })
	b.Add("x")
	cancel()
	select {
	case p := <-done:
		if len(p) != 1 {
			t.Fatal(p)
		}
	case <-time.After(time.Second):
		t.Fatal("no flush on cancel")
	}
}
