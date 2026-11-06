// Package relay implements the sync relay: a tiny HTTPS server that fans out
// journal keys to connected nodes over Server-Sent Events, the matching client,
// and the lease record used for leader election.
package relay

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Lock is the body of _sync/relay.lock.
type Lock struct {
	Node       string `json:"node"`
	URL        string `json:"url"`
	CertSHA256 string `json:"cert_sha256"`
	LeaseUntil int64  `json:"lease_until"` // unix seconds
}

// Expired reports whether the lease ended more than skew ago.
func (l *Lock) Expired(now time.Time, skew time.Duration) bool {
	return l == nil || time.Unix(l.LeaseUntil, 0).Add(skew).Before(now)
}

type notifyBody struct {
	Keys []string `json:"keys"`
}

// Peer is one node connected to the relay (or the relay itself). The relay
// sends the full list to every client whenever it changes, so each node knows
// who else is online.
type Peer struct {
	Node  string    `json:"node"`
	Since time.Time `json:"since"`
	Relay bool      `json:"relay,omitempty"`
}

// Server is the relay HTTP server.
type Server struct {
	node    string
	token   string
	log     *slog.Logger
	cert    tls.Certificate
	srv     *http.Server
	ln      net.Listener
	started time.Time

	mu   sync.Mutex
	subs map[chan string]Peer

	// OnNotify, if set before Start, receives the keys of every accepted
	// /notify so the relay node can apply them itself (it has no SSE stream
	// of its own). It must not block.
	OnNotify func(keys []string)
}

// NewServer prepares a server; call Start to listen. node is the relay's own
// name, reported to clients in the peer list.
func NewServer(node, listen, token string, cert tls.Certificate, log *slog.Logger) *Server {
	s := &Server{node: node, token: token, log: log, cert: cert, subs: map[chan string]Peer{}, started: time.Now()}
	mux := http.NewServeMux()
	mux.HandleFunc("/notify", s.handleNotify)
	mux.HandleFunc("/events", s.handleEvents)
	s.srv = &http.Server{
		Addr:              listen,
		Handler:           mux,
		TLSConfig:         &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
		ReadHeaderTimeout: 10 * time.Second,
	}
	return s
}

// Fingerprint of the server certificate.
func (s *Server) Fingerprint() string { return Fingerprint(s.cert) }

// Start begins listening in the background.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.srv.Addr)
	if err != nil {
		return err
	}
	s.ln = ln
	go func() {
		if err := s.srv.ServeTLS(ln, "", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("relay server", "err", err)
		}
	}()
	return nil
}

// Addr returns the bound address (useful when listening on port 0).
func (s *Server) Addr() string {
	if s.ln == nil {
		return s.srv.Addr
	}
	return s.ln.Addr().String()
}

// Shutdown tells clients to re-elect and closes the server.
func (s *Server) Shutdown(ctx context.Context) {
	s.send("leader", "")
	s.mu.Lock()
	for ch := range s.subs {
		close(ch)
		delete(s.subs, ch)
	}
	s.mu.Unlock()
	_ = s.srv.Shutdown(ctx)
}

// Broadcast pushes journal keys to every connected client.
func (s *Server) Broadcast(keys []string) {
	for _, k := range keys {
		s.send("key", k)
	}
}

func (s *Server) send(event, data string) {
	msg := fmt.Sprintf("event: %s\ndata: %s\n\n", event, data)
	s.mu.Lock()
	defer s.mu.Unlock()
	for ch := range s.subs {
		select {
		case ch <- msg:
		default: // slow client; it will catch up through the marker poll
		}
	}
}

// Peers lists the relay itself followed by every connected client, oldest first.
func (s *Server) Peers() []Peer {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peersLocked()
}

func (s *Server) peersLocked() []Peer {
	out := []Peer{{Node: s.node, Since: s.started, Relay: true}}
	for _, p := range s.subs {
		out = append(out, p)
	}
	sort.Slice(out[1:], func(i, j int) bool { return out[i+1].Since.Before(out[j+1].Since) })
	return out
}

// sendPeers broadcasts the current peer list. Caller holds s.mu.
func (s *Server) sendPeersLocked() {
	b, _ := json.Marshal(s.peersLocked())
	msg := fmt.Sprintf("event: peers\ndata: %s\n\n", b)
	for ch := range s.subs {
		select {
		case ch <- msg:
		default:
		}
	}
}

func (s *Server) authorized(r *http.Request) bool {
	return r.Header.Get("Authorization") == "Bearer "+s.token && s.token != ""
}

func (s *Server) handleNotify(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	var body notifyBody
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body); err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	s.Broadcast(body.Keys)
	if s.OnNotify != nil {
		s.OnNotify(body.Keys)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "no streaming", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, ": connected\n\n")
	fl.Flush()

	node := r.Header.Get("X-Gozeks-Node")
	if node == "" {
		node = r.RemoteAddr
	}
	ch := make(chan string, 256)
	s.mu.Lock()
	s.subs[ch] = Peer{Node: node, Since: time.Now()}
	s.sendPeersLocked()
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if _, ok := s.subs[ch]; ok {
			delete(s.subs, ch)
			close(ch)
			s.sendPeersLocked()
		}
		s.mu.Unlock()
	}()

	ping := time.NewTicker(20 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			if _, err := io.WriteString(w, msg); err != nil {
				return
			}
			fl.Flush()
		case <-ping.C:
			if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
				return
			}
			fl.Flush()
		}
	}
}

// Notify posts journal keys to a relay. Failures are expected to be ignored
// by the caller: marker polling covers them.
func Notify(ctx context.Context, url, fingerprint, token string, keys []string) error {
	body, _ := json.Marshal(notifyBody{Keys: keys})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(url, "/")+"/notify", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	c := &http.Client{Transport: &http.Transport{TLSClientConfig: pinnedTLS(fingerprint)}, Timeout: 5 * time.Second}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("relay notify: %s", resp.Status)
	}
	return nil
}

// ErrLeaderGone is returned by Subscribe when the relay announces shutdown.
var ErrLeaderGone = errors.New("relay: leader stepped down")

// Subscribe connects to the relay's event stream as node and calls onKey for
// every journal key until the connection drops, the relay steps down, or ctx
// ends. onConnected is called once the stream is open; onPeers (optional)
// whenever the relay sends the list of connected nodes.
func Subscribe(ctx context.Context, url, fingerprint, token, node string, onConnected func(), onKey func(string), onPeers func([]Peer)) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(url, "/")+"/events", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("X-Gozeks-Node", node)
	tr := &http.Transport{
		TLSClientConfig:       pinnedTLS(fingerprint),
		ResponseHeaderTimeout: 10 * time.Second,
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
	}
	defer tr.CloseIdleConnections()
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("relay events: %s", resp.Status)
	}
	if onConnected != nil {
		onConnected()
	}
	// The server pings every 20 s; treat a silent minute as a dead connection.
	r := bufio.NewReader(resp.Body)
	event, data := "", ""
	for {
		lineCh := make(chan string, 1)
		errCh := make(chan error, 1)
		go func() {
			l, err := r.ReadString('\n')
			if err != nil {
				errCh <- err
				return
			}
			lineCh <- l
		}()
		var line string
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			return err
		case line = <-lineCh:
		case <-time.After(60 * time.Second):
			return errors.New("relay: stream timeout")
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case line == "":
			switch {
			case event == "key" && data != "":
				onKey(data)
			case event == "leader":
				return ErrLeaderGone
			case event == "peers" && onPeers != nil:
				var peers []Peer
				if json.Unmarshal([]byte(data), &peers) == nil {
					onPeers(peers)
				}
			}
			event, data = "", ""
		case strings.HasPrefix(line, "event: "):
			event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			data = strings.TrimPrefix(line, "data: ")
		}
	}
}
