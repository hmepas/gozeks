package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/hmepas/gozeks/internal/store"
)

// Status is what the daemon periodically writes to <state>/status.json.
type Status struct {
	Node           string         `json:"node"`
	Role           string         `json:"relay_role"` // none | follower | leader | candidate
	RelayConnected bool           `json:"relay_connected"`
	RelayURL       string         `json:"relay_url,omitempty"`
	Subscribers    int            `json:"relay_subscribers,omitempty"`
	Pending        int            `json:"pending_paths"`
	IndexCount     int            `json:"index_count"`
	CursorETag     string         `json:"cursor_etag"`
	LastJournalTs  int64          `json:"last_journal_ts"`
	LastChange     time.Time      `json:"last_change"`
	Counters       store.Snapshot `json:"counters"`
	CountersSince  time.Time      `json:"counters_since"`
	UpdatedAt      time.Time      `json:"updated_at"`
	PID            int            `json:"pid"`
}

// StatusPath is where the daemon writes its status.
func StatusPath(stateDir string) string { return filepath.Join(stateDir, "status.json") }

// status builds the current snapshot. counters are the live totals.
func (e *Engine) status(counters *store.Counters) Status {
	n, _ := e.idx.Count()
	since, _ := e.idx.GetMeta(metaSince)
	sinceT, _ := time.Parse(time.RFC3339, since)
	s := Status{
		Node:           e.cfg.Node,
		Role:           e.relay.role.Load().(string),
		RelayConnected: e.relay.connected.Load(),
		RelayURL:       e.relay.url(),
		IndexCount:     n,
		CursorETag:     e.cursorETag.Load().(string),
		LastJournalTs:  e.lastJournalTs(),
		LastChange:     time.Unix(0, e.lastChange.Load()),
		CountersSince:  sinceT,
		UpdatedAt:      time.Now(),
		PID:            os.Getpid(),
	}
	if counters != nil {
		s.Counters = counters.Snapshot()
	}
	if e.pend != nil {
		s.Pending = e.pend()
	}
	if srv := e.relay.server(); srv != nil {
		s.Subscribers = srv.Subscribers()
	}
	return s
}

func (e *Engine) writeStatus(counters *store.Counters) {
	s := e.status(counters)
	b, _ := json.MarshalIndent(s, "", "  ")
	tmp := StatusPath(e.cfg.StateDir) + ".tmp"
	if os.WriteFile(tmp, b, 0o600) == nil {
		_ = os.Rename(tmp, StatusPath(e.cfg.StateDir))
	}
}

// loadCounters restores persisted totals into c.
func (e *Engine) loadCounters(c *store.Counters) {
	if v, _ := e.idx.GetMeta(metaCounters); v != "" {
		var s store.Snapshot
		if json.Unmarshal([]byte(v), &s) == nil {
			c.Add(s)
		}
	}
	if v, _ := e.idx.GetMeta(metaSince); v == "" {
		_ = e.idx.SetMeta(metaSince, time.Now().Format(time.RFC3339))
	}
}

func (e *Engine) saveCounters(c *store.Counters) {
	b, _ := json.Marshal(c.Snapshot())
	_ = e.idx.SetMeta(metaCounters, string(b))
}

// ReadStatus loads the daemon's status file, if any.
func ReadStatus(stateDir string) (*Status, error) {
	b, err := os.ReadFile(StatusPath(stateDir))
	if err != nil {
		return nil, err
	}
	var s Status
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// Role returns the current relay role (none | follower | candidate | leader).
func (e *Engine) Role() string { return e.relay.role.Load().(string) }

// RelayConnected reports whether an SSE connection to a relay is open.
func (e *Engine) RelayConnected() bool { return e.relay.connected.Load() }
