package relay

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

func TestNotifyAndSubscribe(t *testing.T) {
	cert, err := LoadOrCreateCert(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := NewServer("srv", "127.0.0.1:0", "tok", cert, slog.Default())
	notified := make(chan []string, 10)
	s.OnNotify = func(keys []string) { notified <- keys }
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	url := "https://" + s.Addr()
	fp := s.Fingerprint()

	got := make(chan string, 10)
	peers := make(chan []Peer, 10)
	connected := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() {
		errc <- Subscribe(ctx, url, fp, "tok", "cli", func() { close(connected) }, func(k string) { got <- k }, func(p []Peer) { peers <- p })
	}()
	<-connected
	select {
	case p := <-peers:
		if len(p) != 2 || !p[0].Relay || p[0].Node != "srv" || p[1].Node != "cli" {
			t.Fatalf("peers = %+v", p)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no peers event")
	}
	if p := s.Peers(); len(p) != 2 || p[1].Node != "cli" {
		t.Fatalf("server peers = %+v", p)
	}

	if err := Notify(ctx, url, fp, "tok", []string{"_sync/journal/a", "_sync/journal/b"}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"_sync/journal/a", "_sync/journal/b"} {
		select {
		case k := <-got:
			if k != want {
				t.Fatalf("got %q want %q", k, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout")
		}
	}
	// the relay node itself is told too
	select {
	case ks := <-notified:
		if len(ks) != 2 || ks[0] != "_sync/journal/a" {
			t.Fatalf("OnNotify got %v", ks)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnNotify not called")
	}
	if err := Notify(ctx, url, fp, "wrong", []string{"x"}); err == nil {
		t.Fatal("bad token accepted")
	}
	select {
	case ks := <-notified:
		t.Fatalf("OnNotify called for an unauthorized notify: %v", ks)
	default:
	}
	if err := Notify(ctx, url, "00deadbeef", "tok", []string{"x"}); err == nil {
		t.Fatal("bad fingerprint accepted")
	}
	s.Shutdown(context.Background())
	select {
	case err := <-errc:
		if err != ErrLeaderGone {
			t.Fatalf("expected ErrLeaderGone, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscribe did not end on shutdown")
	}
}

func TestCertPersists(t *testing.T) {
	dir := t.TempDir()
	a, _ := LoadOrCreateCert(dir)
	b, _ := LoadOrCreateCert(dir)
	if Fingerprint(a) != Fingerprint(b) {
		t.Fatal("fingerprint changed between loads")
	}
}
