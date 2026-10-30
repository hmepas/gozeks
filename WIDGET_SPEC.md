# gozeks status — data contract for widgets and monitors

This file is for anyone (human or agent) building a status-bar item, a desktop widget, a
cron health check or a dashboard on top of gozeks. It describes the only interface such a
tool should use: the daemon's status snapshot. Nothing here requires S3 credentials or
access to the bucket.

## Where the data comes from

The daemon rewrites `<state_dir>/status.json` atomically every 10 seconds, at startup and at
shutdown. Two ways to read it:

1. **Preferred:** `gozeks status --json`. Resolves the state directory from the config
   (`-config FILE`, default `~/.config/gozeks/config.toml`), prints the file and adds a
   `problems` array (see *Health*). Exit code 0 even when unhealthy; a non-zero exit means
   the file is missing (daemon never ran) or the config is unreadable. No credentials needed.
2. **Direct read** of the file, when the binary is not at hand. `state_dir` is the config
   key of that name, or `~/.local/state/gozeks/<basename of root>/` when unset. Read the
   whole file in one go; a half-written file never happens (rename-in-place).

For the health verdict without parsing anything: `gozeks status --check` prints `ok` and exits
0, or prints one problem per line and exits 1.

Other nodes: run the same command there (`ssh eowyn gozeks status --json`). The relay also
tells every node who is connected (`peers`), so a widget on one machine already knows which
nodes are online without contacting them.

## Schema

All times are RFC 3339 strings with a zone offset. Fields marked *absent* are omitted from the
JSON when they have no value; treat absence as "never".

| field | type | meaning |
|---|---|---|
| `node` | string | this node's name (`node` in the config) |
| `relay_role` | string | `none` (relay disabled, polling only), `follower`, `candidate`, `leader` (this node *is* the relay) |
| `relay_connected` | bool | an SSE stream to the relay is open right now. Always `false` on the leader itself |
| `relay_url` | string, absent | URL of the relay this node follows |
| `peers` | array, absent | the fleet as the relay reports it, relay first, then clients by connection time. Absent when not connected to a relay. Each item: `node` (string), `since` (time the connection was made), `relay` (bool, only on the relay's own entry) |
| `pending_paths` | int | local changes waiting in the debounce batch; non-zero briefly after you save a file |
| `index_count` | int | files this node believes are in sync |
| `cursor_etag` | string | ETag of the bucket marker as of the last processed journal; changes whenever any node uploads |
| `last_journal_ts` | int | unix **milliseconds** of the newest journal entry processed or written |
| `last_change` | time | last real change seen on this node, local or remote. Seeded from the index at startup, so it survives restarts |
| `last_poll_ok` | time, absent | last successful HEAD of the bucket marker. This is "the bucket is reachable". Absent only right after startup, before the first poll |
| `last_error` | string, absent | message of the last `level=ERROR` log record, with its `path` and `err` attributes appended |
| `last_error_at` | time, absent | when it happened |
| `errors` | int | Error-level records since `started_at` (resets on restart) |
| `recent` | array | last 50 changes, **oldest first, newest last**. Each item: `op` (`put`, `delete`, `conflict`), `path` (vault-relative, `/`-separated; for `conflict` it is the conflict copy's path), `node` (who made the edit: a node name, or `reconcile` when drift repair found it), `at` (time it was applied here). In-memory: empty right after a restart |
| `counters` | object | cumulative S3 requests since `counters_since`: `put`, `get` (GET/HEAD/LIST), `delete`, `bytes_up`, `bytes_down`. Persisted across restarts. Multiply by the provider's price list for a cost estimate |
| `counters_since` | time | when the counters started |
| `started_at` | time | when this daemon process started |
| `updated_at` | time | when this snapshot was written. **The liveness signal** |
| `pid` | int | daemon process id |
| `problems` | array of strings | only in `gozeks status --json`: the health findings below, empty when healthy |

## Health

Derive the state in this order; the first match wins. These are exactly the rules behind
`status --check`, so a widget and a cron job never disagree.

| state | rule | what it means |
|---|---|---|
| **down** | `now - updated_at > 30 s` | the daemon is not running (it writes every 10 s). Everything else in the file is stale history |
| **offline** | `last_poll_ok` older than 5 min, or absent while `started_at` is older than 5 min | the bucket has not answered. Local edits queue up; nothing is lost, nothing propagates |
| **error** | `last_error_at` within the last 10 min | something failed recently; show `last_error`. The daemon keeps running and retries on the next batch / reconcile |
| **degraded** (optional) | healthy, but `relay_role` is `follower` and `relay_connected` is `false` | working, on marker polling instead of the relay: propagation is `sync.poll`/`sync.fallback_poll` seconds instead of sub-second. Not a failure |
| **ok** | none of the above | |

Suggested glyphs for a one-character status item: `●` ok, `◐` degraded, `▲` error, `○` offline,
`✕` down. Add `↑N` while `pending_paths > 0`.

Useful secondary lines:

- "last sync": `recent[-1].at` if present, else `last_change`.
- "peers": `peers[*].node`, marking the one with `relay: true`. Expect the full fleet here;
  a node missing from the list is offline or disconnected (its own status would say).
- "recent": the tail of `recent`, newest first for display; group consecutive entries with
  the same `path` (agents append to the same file many times).
- conflicts: any `recent[*].op == "conflict"` deserves a highlight; the `path` is the copy
  that needs a human.

## Example

```json
{
  "node": "mac",
  "relay_role": "follower",
  "relay_connected": true,
  "relay_url": "https://eowyn.example:8712",
  "peers": [
    {"node": "eowyn", "since": "2026-10-30T07:59:05Z", "relay": true},
    {"node": "mac",   "since": "2026-10-30T07:59:06Z"},
    {"node": "arch",  "since": "2026-10-30T08:12:40Z"}
  ],
  "pending_paths": 0,
  "index_count": 4838,
  "cursor_etag": "36734dee7fcb87cb1c08507a1279e039",
  "last_journal_ts": 1793382871186,
  "last_change": "2026-10-30T20:54:31+03:00",
  "last_poll_ok": "2026-10-30T21:05:02+03:00",
  "errors": 0,
  "recent": [
    {"op": "put", "path": "Notes/DAR.md", "node": "mac", "at": "2026-10-30T20:51:10+03:00"},
    {"op": "put", "path": ".herald/wal/2026-10-30.jsonl", "node": "eowyn", "at": "2026-10-30T20:54:31+03:00"}
  ],
  "counters": {"put": 104, "get": 1421, "delete": 13573, "bytes_up": 51856880, "bytes_down": 66148048},
  "counters_since": "2026-10-29T22:33:13+03:00",
  "started_at": "2026-10-30T10:58:40+03:00",
  "updated_at": "2026-10-30T21:05:08+03:00",
  "pid": 16360,
  "problems": []
}
```

Shell one-liners:

```sh
gozeks status --json | jq -r '.problems[]'                     # empty = healthy
gozeks status --json | jq -r '.recent[-5:][] | "\(.at) \(.op) \(.node) \(.path)"'
gozeks status --json | jq -r '[.peers[].node] | join(", ")'
gozeks status --check && echo up || echo DOWN
```

## Polling and cost

Reading the file costs nothing and touches no network, so poll as often as the UI needs
(every 5–10 s is reasonable; the file changes every 10 s). Do **not** build monitoring on
`gozeks reconcile` or any command that opens the bucket: those count against the request
budget and `reconcile` cannot run while the daemon holds the state database.

## Stability

Fields may be added; existing ones keep their names and types. A widget should ignore unknown
fields. Anything shown by `gozeks status` (the human output) is derived from this file and
has no extra information.
