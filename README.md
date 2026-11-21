# gozeks

Two-way sync of a local folder with any S3-compatible bucket, fast enough for a shared
Obsidian vault edited by a human on a Mac and by agents on several servers at the same time.
Changes typically propagate between nodes in 5–8 seconds. One static Go binary per node,
no server in the middle, no provider-specific services.

## How it works

Every node runs the `gozeks` daemon:

- **Local → bucket.** The daemon watches the folder with filesystem events, waits ~2 s for the
  editor to finish writing, uploads the changed files to `data/…`, appends a small journal
  entry listing what changed, and touches a marker object `_sync/cursor`.
- **Relay.** One of your always-on servers is elected (through a lock file in the same
  bucket) as a relay. A node that uploaded a batch tells the relay, which pushes the
  journal key to every other node over an open HTTPS stream. Propagation is sub-second. The
  relay carries no file data and keeps no state; if it goes away, another server takes
  over and in the meantime every node falls back to polling.
- **Bucket → local.** The relay (or, as a fallback, every node) does one cheap HEAD on
  the marker every few seconds. When its ETag changes, new journal entries are fetched, own
  ones skipped, and the files they mention are downloaded or removed. Downloads are written
  atomically (temp file + rename), so Obsidian never sees a half-written note.
- **Safety nets.** A full reconcile (one bucket listing compared with the local index) runs
  at startup and every 10 minutes and repairs anything missed. Remote deletes go to a local
  trash folder, never straight to `rm`. Turn on bucket versioning for a second layer of undo.
- **Conflicts.** If you edited a file locally and a newer version arrives from another node,
  the incoming version takes the file's path and your copy is kept next to it as
  `Name (conflict <node> <date>).md`, the same behaviour Yandex.Disk users know.

Idle cost is one HEAD request every 5 seconds for the whole fleet (the relay polls on
everyone's behalf) plus a slow fallback poll per node. Everything else is paid only when
something actually changes.

## Requirements

- An S3-compatible bucket (tested with a commercial S3 provider; MinIO used in tests) and an access key pair
  with read/write rights on that bucket.
- macOS or Linux. Go 1.26 to build.

## Quick start

```sh
brew install hmepas/formulae/gozeks   # macOS; run it with `brew services start gozeks`
# or build from source:
make build            # macOS: needs cgo for FSEvents (Xcode command line tools)
make build-linux      # static linux/amd64 binary in dist/

mkdir -p ~/.config/gozeks
cp deploy/config.example.toml ~/.config/gozeks/config.toml   # edit node name, root, endpoint, bucket
cp deploy/env.example ~/.config/gozeks/env && chmod 600 ~/.config/gozeks/env   # S3 keys, relay token

# first node: seed the empty bucket from your folder
gozeks push

# every other node: fetch the folder from the bucket
gozeks pull

# then keep it in sync
gozeks run
```

Service units for launchd (macOS) and systemd (Linux) are in `deploy/`.

## Commands

| command | what it does |
|---|---|
| `gozeks run` | daemon: watch, poll, reconcile |
| `gozeks push` | one-shot upload of the folder into an empty bucket |
| `gozeks pull` | one-shot download of the bucket into an empty folder |
| `gozeks reconcile` | one full consistency pass |
| `gozeks status` | health, connected peers, last 50 changes, request counters (for cost tracking) |
| `gozeks status --check` | exit 1 when the daemon is down, the bucket was unreachable for 5 min, or an error is less than 10 min old; for cron / health pings |
| `gozeks status --json` | the raw snapshot plus a `problems` array; the contract for widgets is in `WIDGET_SPEC.md` |
| `gozeks trash list / restore` | files removed by remote deletes |

## Configuration

See `deploy/config.example.toml`. The important knobs:

- `node` — unique name per machine; appears in journal keys and conflict file names.
- `sync.poll` — marker polling interval; 5 s by default. Longer = cheaper and slower.
- `sync.fallback_poll` — how often a node polls on its own while connected to a relay (60 s).
- `relay.listen` / `relay.advertise` — set on servers that may act as relay. The
  shared `GOZEKS_RELAY_TOKEN` env var authenticates nodes; TLS uses a self-signed cert whose
  fingerprint is published through the bucket, so no certificates to manage.
- `sync.exclude` — glob patterns never synced. Defaults cover Obsidian's `workspace*.json`,
  caches, `.trash/`, `.zk/`, `.DS_Store` and editor temp files. Everything else, including
  `.obsidian/` settings and plugins, is synced.

Credentials are read from `GOZEKS_S3_ACCESS_KEY` / `GOZEKS_S3_SECRET_KEY` (or the standard
`AWS_*` variables) and are never written to the config or logs. An `env` file next to the
config (`~/.config/gozeks/env`, `KEY=VALUE` lines, mode 0600) is read automatically; variables
already set in the environment take precedence. The service units point at the same file.

## Migrating from a cloud-drive folder

1. Create the bucket (private, versioning on), create an access key.
2. Stop the old sync client on every node. Copy the folder out of the cloud drive's directory.
3. On one node: `gozeks push`. On the others: `gozeks pull`.
4. Point Obsidian at the new folder location, start `gozeks run` everywhere.

## Cost

At 2026 prices of the provider in use a Mac plus two servers comes to roughly ₽55/month: the relay's
polling, lease renewals, a few hundred uploads a day and a 1 GB vault with versions.
`gozeks status` shows live request counters so you can check.

## Monitoring

The daemon writes `<state dir>/status.json` every 10 s; `gozeks status` prints it:
whether the daemon is alive, when the bucket was last reached, the last error, the nodes
currently connected to the relay (every node gets the full list from the relay), and the
last 50 changes with the node that made them. Logs (`log/slog`) go to stderr: one line per
batch with the paths when a batch has at most 10 of them, warnings for conflicts and skipped
paths, `level=ERROR` for anything that failed. A reconcile line with `upload=0 download=0
trash=0` and equal remote/local/indexed counts means the node is fully in sync.
`WIDGET_SPEC.md` documents the snapshot field by field, with the health rules, for building
status-bar items, widgets or health checks on top of it.

## Development

Design decisions, the bucket layout and the acceptance tests live in `AGENTS.md`.

    make test          # unit tests
    ./test/minio.sh    # or docker-compose -f test/docker-compose.yml up -d
    make integration   # multi-daemon scenarios against MinIO (see test/README.md)

## Releasing

The Homebrew formula lives in the `hmepas/homebrew-formulae` tap and builds from the tag's
source tarball.

    git tag vX.Y.Z && git push origin vX.Y.Z
    cd "$(brew --repository hmepas/formulae)"
    scripts/update-formula.sh gozeks X.Y.Z      # bumps url + sha256
    git commit -am "gozeks X.Y.Z" && git push
    brew update && brew upgrade gozeks          # brew services restarts the daemon

If `brew upgrade` reports a checksum mismatch, GitHub regenerated the tarball after the sha was
taken (seen once right after the repo went public): rerun `update-formula.sh` and push again.
