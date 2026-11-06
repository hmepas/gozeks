# gozeks — agent guide

Two-way sync of a local folder (an Obsidian vault) with an S3 bucket, with sub-10-second
propagation between nodes. Written in Go. This file is the spec: decisions already taken,
the design, and the acceptance criteria. Read it fully before touching code.

Owner: Pavel (hmepas). Talk to him in Russian. Code, comments, commits, README: English.

## Why this exists (context, decided 2026-10-29)

- The vault (`zk`, ~700 MB, ~1150 files, ~260 binaries, 47 dirs) is used by Obsidian on a Mac
  and by AI agents on several Linux servers. All nodes read and write.
- Today Yandex.Disk syncs it. Its sync engine is fine; the problem is the service itself
  (outages, slow sync that piles up changes and then produces conflict copies).
- Goal: own infrastructure that is trivially portable — any S3-compatible bucket, change
  provider by copying the bucket and editing a config. No VPS, no provider-specific services
  (no event notifications / queues), no Obsidian plugins, no git.
- Rejected: Syncthing (needs an always-on peer, Pavel does not want to run/pay for it),
  git (overkill), rclone bisync (scan-based, minute-level latency, expensive LIST polling),
  Remotely Save plugin (Obsidian-only, 1-minute minimum), FUSE mounts (slow for agents that
  grep the whole vault), bucket event notifications (provider lock-in).
- No mobile: Pavel uses the vault from the phone only through agents in Telegram.

## Core design

S3 is a dumb hub. Each node runs one `gozeks` daemon. Detection of remote changes must be
cheap: S3 has no "bucket last modified", so we use a **marker object + HEAD polling** and an
**append-only journal**.

### Bucket layout

```
data/<relative path>            vault files, 1:1 with the local tree
_sync/cursor                    tiny JSON {node, ts, seq}; its only job is to change ETag
_sync/journal/<ts>-<node>-<seq>.json   one entry per upload batch, append-only
```

- `<ts>` is unix milliseconds, zero-padded to 13 digits, so keys sort chronologically.
- Journal entry body: `{"node":"mac","ts":...,"seq":N,"ops":[{"op":"put|delete","path":"...","size":..,"mtime":..,"hash":"sha256:...","prev":"sha256:..."}]}`.
  `prev` (added 2026-10-29) is the hash the uploader held before the change ("" for a new
  file); readers use it to tell a sequential edit from a divergent one (see Conflicts).
- File objects carry metadata `mtime` (unix seconds) and `hash` (sha256) so a reader can skip
  downloads when the hash matches what it already has.

### Upload path (local → S3)

1. Watch the tree recursively with FS events (FSEvents on macOS, inotify on Linux).
   Must handle ~2k files / 50+ dirs without hitting fd limits; prefer `github.com/rjeczalik/notify`,
   or `fsnotify` if you verify it scales on macOS. Agent decides, documents the reason.
2. Debounce per path (default 2 s), batch everything pending into one journal entry
   (max batch wait default 5 s).
3. For each changed path: hash it; if hash equals the index entry → no-op (this also
   swallows echoes of our own downloads). Otherwise PUT `data/<path>` (or DELETE).
4. PUT the journal entry, then PUT `_sync/cursor`. Order matters: journal before cursor.
5. Update the local index.
6. If connected to a relay: POST the journal key to it (fire-and-forget, failure is not an
   error — the cursor poll covers it).

Expected latency to the bucket: debounce + one PUT ≈ 2–4 s.

### Download path (S3 → local)

Two sources of "something changed", same apply logic for both:

- **Relay push (primary).** A node connected to the relay receives the exact journal
  key(s) and GETs them directly, no LIST. See "Relay" below. The relay node itself applies
  the keys it receives on `POST /notify` the same way (2026-11-06; before that it waited for
  its own marker poll, so follower → relay took up to `poll`).
- **Marker polling (fallback and the relay's own source).** HEAD `_sync/cursor` every
  `poll` (default 5 s on the relay, `fallback_poll` 60 s on nodes that are connected to a
  relay, `poll` 5 s on nodes that cannot reach one).

1. On a changed cursor ETag: LIST `_sync/journal/` with `StartAfter = key(last processed ts − 120 s)`.
   The 120 s rewind covers clock skew between nodes; dedupe by a set of already-processed
   journal keys kept in local state.
3. Skip entries whose `node` is us. Apply the rest in key order:
   - `put`: if local index hash == entry hash → skip. If local file was modified since the
     last sync (hash ≠ index) → **conflict** (see below). Else GET to a temp file in the same
     dir, set mtime, rename atomically.
   - `delete`: move local file to `<state dir>/trash/<ts>/<path>` (never hard-delete), update index.
4. Record the processed journal keys and the cursor ETag.

Expected latency bucket → node: via relay ≈ one GET (sub-second); via polling ≤ poll + one GET.
End-to-end target ≤ 10 s in every mode, typical 3–5 s with the relay.

### Why marker + journal, not marker alone

The cursor is last-writer-wins. If nodes A and B upload within the same poll window, readers
see only B's cursor. That is fine *only because the cursor carries no data*: any ETag change
triggers a journal LIST, and the journal has both A's and B's entries (unique keys, never
overwritten). A cursor that embedded the change list would silently lose A's batch. Hence the
journal is mandatory; the cursor is just a doorbell.

### Relay (decided 2026-10-29)

The agent servers are always on, so one of them doubles as a **sync relay**: a stateless
relay that turns "I wrote journal key K" into an instant push to every other node. S3 stays the
only source of truth; the relay never carries file data and losing it only degrades latency
(nodes fall back to marker polling). Nothing may depend on the relay being up.

Roles are in the same binary and the same process; `relay` is an extra goroutine.

**Protocol (HTTPS, stdlib only):**
- `POST /notify` body `{"keys":["_sync/journal/..."]}` — a node announces its uploaded batch.
- `GET /events` — Server-Sent Events stream; each event is one journal key. Nodes hold this
  connection open, reconnect with backoff; on reconnect they do one marker check to catch what
  they missed.
- The relay also polls the marker itself at `poll` (5 s) and emits keys it discovers that
  way, so a node whose POST failed, or a node without relay access, still gets relayed.
  This makes the relay the single poller for the whole fleet.
- Auth: shared bearer token from config (`relay.token`, same on all nodes, via env).
- TLS: self-signed certificate generated by the relay on first start; its SHA-256
  fingerprint is published in the lock file (below). Clients pin that fingerprint. No CA, no
  reverse proxy, no provider involvement. The lock file can be trusted because reading it
  already requires the S3 credentials.

**Leader election via S3 lock file** `_sync/relay.lock`:
`{"node":"eowyn","url":"https://1.2.3.4:8711","cert_sha256":"...","lease_until":<unix>}`.

Only nodes with `[relay] listen` configured are candidates (servers with a reachable
address; the Mac is never a candidate). S3 has no portable compare-and-swap (AWS has
conditional PUTs, Selectel is not guaranteed to), so the election is a lease with
write-then-verify:
1. Candidate GETs the lock. If absent, or `lease_until` is in the past by more than 10 s →
   wait a random 0–3 s, PUT the lock with itself and `lease_until = now + 10 min`, wait 3 s,
   GET it back. If the lock still names us → we are the relay. Otherwise back off and
   watch.
2. The leader renews the lease by PUT every 5 min with `lease_until = now + 10 min` (a PUT
   every few seconds would cost more than the polling it replaces). Before every renewal it GETs the lock and
   steps down immediately if it names someone else (this resolves the rare double-leader
   from a simultaneous first PUT: the one that lost the race sees it on the next check).
3. Followers (all nodes, including the Mac) GET the lock every 30 s while not connected to
   a relay, and whenever the SSE connection drops. While connected they do not poll
   the lock; the relay sends a `leader` event on shutdown so clients re-elect fast.
4. A brief window with two leaders or no leader is harmless by construction: both push
   journal keys that nodes verify against S3, and marker polling continues underneath.

Clock skew between nodes is assumed < 10 s (servers run NTP). Lease timestamps are compared
with that margin.

**Why this is worth it:** latency drops from "poll interval" to sub-second, and polling cost
drops from one HEAD per node to one HEAD for the fleet (see cost model). Downloads were
already targeted (the journal names exactly which files changed, there is never a "sync
everything" pass except reconcile); the relay removes the *detection* cost, not the download
cost. With a relay in place `reconcile` can be relaxed to 1 h.

Name: "relay" was chosen over "dispatcher" because it describes exactly what it does — passes
journal keys along, owns nothing. ("herald" would fit but is already taken by Pavel's event
router in the vault.)

### Conflicts

Same semantics as Yandex.Disk, which Pavel is used to: the remote version takes the path
(so replicas converge), the local version is preserved as
`<name> (conflict <node> <YYYY-MM-DD HHMM>).<ext>` next to it. That copy is a new file and
gets uploaded like any other, so the conflict is visible on every node. Bucket versioning is
the second safety net (manual restore via provider console / aws cli).

Decision rule (2026-10-29). A conflict exists when an incoming `put` meets either
- a local file whose hash differs from the index (edited locally, not yet uploaded), or
- an index entry whose hash differs from the entry's `prev` (both nodes uploaded from the same
  base before seeing each other: *divergent*).

For the divergent case both nodes have already uploaded, so a tie-break decides who keeps the
path: the entry with the later journal timestamp wins. The node holding the older version
applies the newer one and keeps its own as the conflict copy; the node holding the newer
version ignores the older entry. Both compare the same two timestamps, so clock skew cannot
make them disagree, and exactly one conflict copy is produced. A sequential edit (entry `prev`
== our index hash) is always applied regardless of timestamps.

Only the author keeps the copy (2026-11-06). In the divergent case the displaced version
becomes a conflict copy only on the node that uploaded it (its index entry points at a
journal key written by this node). A node that merely downloaded that version replaces it
silently. Reasons: (1) a `prev` mismatch on a non-author is almost always lag, not divergence:
an op was dropped (failed GET), or a GET returned a newer object than the entry describes
while a series of edits was still running; both produced false conflict copies of DAR on
eowyn (2026-11-01, 2026-11-05), each time one entry after a 409 from Selectel; (2) with three
nodes the bystander used to make its own copy of the same content under a second name.
A locally edited, not yet uploaded file is still always kept (first bullet above).
Accepted cost: if the author crashes right after its upload and the version is overwritten
before it comes back, no node holds a conflict copy; the version is only in bucket versioning.
Known residual: an author that drops a sequential op (retries exhausted) still makes one
false copy on the next entry of that series.

### Reconcile (drift repair)

Full LIST of `data/` (2 requests for this vault) compared against the local index, at startup
and every `reconcile` interval (default 10 min). Fixes anything missed (lost journal entries,
crashes mid-batch, local edits made while the daemon was down). Reconcile applies the same
rules as the journal path (hash compare, conflict rule, trash instead of delete). Deletions
during reconcile: a path in the index but absent both locally and remotely → drop from index;
present remotely, absent locally, present in index → it was deleted locally while down → DELETE
remotely; present remotely, absent locally, absent in index → new remote file → download.

### Local state

Outside the vault, never synced: `~/.local/state/gozeks/<vault-id>/` (macOS: same path is
fine, keep it simple). Contains:
- `index` — per path: size, mtime, sha256, last synced journal key. Use bbolt (`go.etcd.io/bbolt`)
  or a single JSON/gob file rewritten atomically; agent decides, bbolt preferred for crash safety.
- `processed` — journal keys seen in the last 24 h.
- `trash/` — files removed by remote deletes.

### Excludes (defaults, configurable)

```
.obsidian/workspace*.json    # Obsidian rewrites it constantly → would ring the doorbell every 5 s
.obsidian/workspace           # same (legacy)
.obsidian/cache/
.trash/
.zk/                          # regenerated by vault scripts
.DS_Store
*.tmp, *~, .#*                # editor temp files
<state dir>, conflict-copy temp files, gozeks' own temp files
```

Everything else is synced, including `.obsidian/` (plugins, settings), `.herald/`, `.pi/`, `.agents/`.
`.venv` directories are synced too (decided 2026-10-29): each agent gets its own directory, so
platform mismatch is not a conflict source.

### Journal pruning

Delete journal entries older than 7 days during reconcile (DELETE is free at Selectel).
A node that was offline longer than that relies on reconcile, which is why reconcile runs at startup.

## Config

Single TOML (or YAML; agent decides, TOML preferred) file, default `~/.config/gozeks/config.toml`:

```toml
node = "mac"                       # unique per node, used in journal keys and conflict names
root = "/Users/hmepas/zk"          # local folder
state_dir = "~/.local/state/gozeks/zk"

[s3]
endpoint = "https://s3.ru-3.storage.selcloud.ru"
region = "ru-3"
bucket = "obs"
path_style = false                 # bucket created as vHosted
# keys come from env: GOZEKS_S3_ACCESS_KEY / GOZEKS_S3_SECRET_KEY (or AWS_* as fallback)

[sync]
poll = "5s"
idle_poll = "5s"                   # optional: longer interval when nothing changed for `idle_after`
idle_after = "10m"
debounce = "2s"
max_batch_wait = "5s"
reconcile = "10m"
fallback_poll = "60s"              # marker poll while connected to a relay
exclude = [ ...defaults above... ]

[relay]
# token from env GOZEKS_RELAY_TOKEN; same on every node
listen = "0.0.0.0:8711"            # set only on nodes that may become the relay (servers)
advertise = "https://1.2.3.4:8711" # URL other nodes can reach; written into the lock file
lease = "10m"                      # lease length; renewed at lease/2
```

Secrets only via environment or a mode-0600 env file referenced by the service unit. Never in
the config file, never logged.

## CLI

```
gozeks run                 # daemon: watch + poll + reconcile
gozeks push [--dry-run]    # one-shot: upload local → bucket (initial seeding)
gozeks pull [--dry-run]    # one-shot: download bucket → local (new node)
gozeks reconcile           # one full reconcile pass, then exit
gozeks status              # index stats, last cursor, pending batch, request counters,
                           # relay role (leader/follower/none) and connection state
gozeks trash list|restore  # files removed by remote deletes
```

`push`/`pull` refuse to run against a non-empty target unless `--force` (prevents accidents on
first setup). `push` into an empty bucket is the migration step from Yandex.Disk.

## Observability

- Structured logs (`log/slog`), one line per batch uploaded / applied, with paths at debug level.
- Request counters by class (PUT/POST, GET/HEAD/LIST, DELETE, bytes up/down), logged hourly and
  exposed via `gozeks status`. Pavel wants to see what the bucket costs; this is the source.

## Service units

- macOS: launchd plist (`KeepAlive`, runs as user).
- Linux: systemd user or system unit with `Restart=always` and an `EnvironmentFile` for keys.
Both shipped under `deploy/` with install instructions in README.

## Dependencies (keep minimal)

- `github.com/aws/aws-sdk-go-v2` (s3, config). Tested against Selectel; must work with any
  S3-compatible endpoint (MinIO in tests).
- FS watcher: `github.com/rjeczalik/notify` or `github.com/fsnotify/fsnotify` (see above).
- `go.etcd.io/bbolt` for state (if chosen).
- TOML: `github.com/BurntSushi/toml` or `github.com/pelletier/go-toml/v2`.
- Relay: stdlib `net/http`, `crypto/tls`, SSE by hand. No websocket library.
- No frameworks for CLI beyond stdlib `flag` unless it clearly saves code; `cobra` acceptable.

Go 1.26 is installed on the Mac. Target `darwin/arm64` and `linux/amd64`; CGO off.

## Testing / acceptance

1. Unit tests: index/hash logic, conflict decision table, reconcile decision table, exclude
   matching, journal key ordering and skew rewind, debounce/batching.
2. Integration tests against MinIO in Docker (`docker compose` under `test/`): two daemons on
   two temp dirs sharing one bucket. Assertions:
   - create/modify/delete on A appears on B within 10 s;
   - simultaneous edits of the same file on A and B produce exactly one conflict copy, both
     nodes end up with identical trees;
   - A and B upload within the same poll window → C receives both (the journal race);
   - kill B mid-batch, restart → reconcile heals, no duplicates, no lost files;
   - own downloads do not echo back as uploads (request counter stays flat);
   - excluded paths never reach the bucket;
   - three daemons, one with `relay.listen`: a change on A reaches B in < 2 s; kill the
     relay → B still receives changes within `fallback_poll`, and a second candidate takes
     the lease once it expires (use a short lease in the test); the old leader restarted sees the
     lock names someone else and stays a follower;
   - two candidates start simultaneously → exactly one holds the lease after 10 s.
3. Manual DoD on real infra: Mac + one server on the Selectel bucket, DAR file edited in
   Obsidian, agent on the server sees the change ≤ 10 s; agent appends a line, Obsidian shows it
   ≤ 10 s. Request counters after 24 h match the cost estimate in README within 2x.

## Cost model (Selectel ru-3, Standard, prices seen 2026-10-29)

Storage ₽2.5654/GB·month, PUT/POST/COPY ₽0.714/1000, GET/HEAD/LIST ₽0.0543/1000, DELETE free,
egress to internet ₽1.377/GB, ingress free.

- HEAD every 5 s, 24/7, by the relay only: 518k req/month ≈ ₽28.
- Followers: fallback HEAD every 60 s ≈ 43k/month ≈ ₽2.5 per node; lock GETs only while
  disconnected, negligible.
- Lease renewals: one PUT every 5 min ≈ 9k/month ≈ ₽6. (A PUT every 20 s would be ≈ ₽93,
  more than the polling it replaces — that is why the lease is 10 min and failover may take
  up to 10 min; followers keep working on `fallback_poll` meanwhile.)
- Uploads: ~200 batches/day × 3 PUT (file, journal, cursor) ≈ 18k/month ≈ ₽13.
- Storage 0.7 GB + versions ≈ ₽3–5. Reconcile LISTs and journal GETs: < ₽2.
- Rough total for Mac + 2 servers: **≈ ₽55/month**, with sub-second propagation. Without the
  relay (every node polling at 5 s) it would be ≈ ₽85 with 5–8 s propagation.

## Implementation notes (2026-10-29, first build)

- **FS watcher:** `github.com/rjeczalik/notify`. On macOS it uses FSEvents (one descriptor
  for the whole tree), which needs cgo; the macOS binary is therefore built with
  `CGO_ENABLED=1` (native build on the Mac, no external libraries). Linux (inotify) and the
  cross-compiled Linux binary stay `CGO_ENABLED=0`. Without cgo `notify` falls back to kqueue
  (one fd per file), which is why the spec's "CGO off" is relaxed for darwin only.
- **Symlinks** inside the vault are skipped (logged at debug). The vault has
  `agent-memory-v0 -> .agent-memory-v0` as an absolute link; only the real directory is synced.
- **Own cursor writes are not recorded as "seen":** after committing a batch the next poll
  always LISTs the journal once. Otherwise a node that wrote the cursor last would mask a batch
  another node wrote in the same window (the race described above, seen from the writer's side).
- **Reconcile guard:** if the local tree has zero files while the index is non-empty, reconcile
  refuses to run (a missing mount would otherwise turn into mass remote deletes). `pull` into an
  empty folder passes `AllowEmptyLocal`.
- **Files that reach the bucket without a journal entry** (crash between the data PUTs and the
  journal PUT) are not re-announced by the restarted node; other nodes pick them up on their
  next reconcile, as the spec says.
- **Index fast path:** size + mtime(ns) equal to the index entry → hash is assumed unchanged;
  otherwise the file is hashed. Index also stores the remote ETag so reconcile can detect
  remote changes from a LIST alone (no per-object HEAD).
- **Status:** the daemon writes `<state>/status.json` every 10 s; `gozeks status` reads it.
  Request counters are persisted in the index, so they survive restarts.
- **MinIO for tests:** Docker Hub and quay.io refused the `minio/minio` image from this
  network, so `test/minio.sh` runs a Homebrew-installed `minio` binary instead; the compose file
  is kept for environments where the image pulls.
- **Unicode NFC everywhere (2026-10-29, after the first live day).** Bucket keys, index keys and
  journal paths are NFC. macOS historically produced NFD names (`й` = `и` + U+0306) and APFS treats
  both spellings as one file, Linux treats them as two; without one canonical form the vault
  gained 81 duplicate notes within hours. On Linux a non-NFC file is renamed on disk; if an NFC
  twin already exists, an identical one goes to trash, a differing one becomes a conflict copy.
  Non-NFC keys left in the bucket are deleted during reconcile once an identical NFC twin exists.
  Dependency added: `golang.org/x/text/unicode/norm`.
- **Case twins are not synced on a case-insensitive disk.** If a key differs only by case from a
  file that exists locally (`Notes/attachments` vs `Notes/Attachments`), the Mac neither writes
  nor deletes it and logs a warning; the duplicate has to be resolved by hand on a Linux node.
  There is no canonical case to normalize to, so this stays manual.
- **Paths under a local symlink are never read or written.** Applying a remote `put` through a
  symlinked directory would modify the link's target, and a reconcile that skips symlinks would
  then "delete locally" what the apply had indexed. Both sides now skip such paths (warning in
  the log). The vault's `agent-memory-v0 -> .agent-memory-v0` link is the case that bit.
- **Stat fast path has a modify window:** size+mtime equality is trusted only for files older
  than 2 s, otherwise the file is rehashed (two writes inside the kernel's timestamp granularity
  looked identical on ext4).
- `gozeks index forget PREFIX` drops index entries so the next reconcile treats them as never
  synced; a repair tool, used only with the daemon stopped.
- **Not implemented** (nothing in the vault needs it): multipart upload, mobile, encryption.

Monitoring (2026-10-30):
- `status.json` carries `last_poll_ok` (last successful marker HEAD), `last_error` /
  `last_error_at` / `errors` (every Error-level log record is tapped by a slog.Handler wrapper,
  so the status never disagrees with the log), `started_at`, and `recent`: the last 50 changes
  (op, path, node, time) applied or uploaded. `gozeks status --check` exits 1 when the status
  is older than 30 s, the bucket has not answered for 5 min, or an error is < 10 min old.
- The relay tracks who is connected (clients send `X-Gozeks-Node`) and pushes a `peers` SSE
  event with the full list (relay first) on every join/leave, so every node, not only the
  leader, can show the fleet in `status`. The list is in-memory only; nothing is written to
  the bucket for it.
- Batch log lines include the paths when a batch has ≤ 10 ops.
- `gozeks status --json` prints the snapshot plus `problems`; `WIDGET_SPEC.md` is the contract
  for external widgets/monitors. Keep it in step with `engine.Status` and `healthProblems`.
- `<config dir>/env` is read by `config.Load` (process environment wins). Commands that do
  not touch the bucket (`status`, `trash`, `index`) no longer require credentials at all.

Transient S3 errors (2026-11-06):
- Selectel answers `409 OperationAborted` ("A conflicting conditional operation is currently
  in progress against this resource. Try again.") when a request collides with a concurrent
  write of the same key. The SDK does not retry that code by default; `store.New` adds it to
  the retryable set (4 attempts, SDK backoff), for every request class.
- An op that still fails is logged and dropped, and the journal key is marked processed; the
  file catches up with the next entry for that path or with reconcile. It never turns into a
  conflict copy on a node that did not author the version it holds (see Conflicts).

Review fixes (2026-10-29, after the first build):
- `CheckCursor` is single-flighted (`pollMu`) and the cursor ETag is an atomic value: the poll
  loop, the relay-connect catch-up and the relay-drop catch-up used to race on it.
- A follower that cannot reach the relay (valid lock, unreachable URL) re-reads the lock at
  `LockPoll` (30 s) instead of every few seconds; it relies on the normal marker poll meanwhile.
- The watcher starts before the startup reconcile, so edits made during it are not lost.
- Journal `put` ops carry the bucket ETag; the "identical content already on disk" path records
  it, so reconcile does not re-fetch such files once.
- Integration tests decide the relay token before starting a daemon (`noRelay()`), not after.

## Working rules for the building agent

- Simplest thing that meets the spec. No plugin systems, no multi-bucket, no encryption layer,
  no multipart upload unless a file > 100 MB actually shows up (then do it).
- Ask Pavel before changing any decision in this file; record changes here with a date.
- Commits and pushes are made by Pavel unless he says otherwise.
