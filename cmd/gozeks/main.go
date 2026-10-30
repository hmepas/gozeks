// Command gozeks syncs a local folder with an S3 bucket.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/hmepas/gozeks/internal/config"
	"github.com/hmepas/gozeks/internal/engine"
	"github.com/hmepas/gozeks/internal/index"
	"github.com/hmepas/gozeks/internal/journal"
	"github.com/hmepas/gozeks/internal/store"
)

const usage = `usage: gozeks [-config FILE] [-v] COMMAND

commands:
  run                    daemon: watch + poll + reconcile
  push [--dry-run] [--force]   upload local folder into an (empty) bucket
  pull [--dry-run] [--force]   download bucket into an (empty) local folder
  reconcile [--dry-run]  one full reconcile pass
  status [--check]       health, peers, recent changes, request counters;
                         --check exits 1 when the daemon is stale, the bucket
                         was unreachable for 5 min, or an error is < 10 min old
  trash list             files removed by remote deletes
  trash restore ID/PATH  move a trashed file back into the folder
  index forget PREFIX    drop index entries under PREFIX (daemon must be stopped);
                         the next reconcile treats them as never synced
`

func main() {
	cfgPath := flag.String("config", config.DefaultPath(), "config file")
	verbose := flag.Bool("v", false, "debug logging")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()
	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(2)
	}
	lvl := slog.LevelInfo
	if *verbose {
		lvl = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fatal(err)
	}
	cmd, args := flag.Arg(0), flag.Args()[1:]
	switch cmd {
	case "status":
		err = cmdStatus(cfg, args)
	case "trash":
		err = cmdTrash(cfg, args)
	case "index":
		err = cmdIndex(cfg, args)
	case "run", "push", "pull", "reconcile":
		err = withEngine(cfg, log, func(ctx context.Context, e *engine.Engine, st *store.Store) error {
			switch cmd {
			case "run":
				return e.Run(ctx, engine.RunOptions{Counters: &st.Counters})
			case "push":
				return cmdPush(ctx, cfg, e, st, args)
			case "pull":
				return cmdPull(ctx, cfg, e, st, args)
			default:
				fs := flag.NewFlagSet(cmd, flag.ExitOnError)
				dry := fs.Bool("dry-run", false, "only report")
				_ = fs.Parse(args)
				e.DryRun = *dry
				return e.Reconcile(ctx, engine.ReconcileOptions{})
			}
		})
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "gozeks:", err)
	os.Exit(1)
}

func withEngine(cfg *config.Config, log *slog.Logger, fn func(context.Context, *engine.Engine, *store.Store) error) error {
	if err := cfg.CheckCredentials(); err != nil {
		return err
	}
	st, err := store.New(cfg.S3)
	if err != nil {
		return err
	}
	e, err := engine.New(cfg, st, log)
	if err != nil {
		return err
	}
	defer e.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err = fn(ctx, e, st)
	c := st.Counters.Snapshot()
	log.Info("requests", "put", c.Put, "get", c.Get, "delete", c.Delete, "bytes_up", c.BytesUp, "bytes_down", c.BytesDown)
	return err
}

func seedFlags(name string, args []string) (dry, force bool) {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	d := fs.Bool("dry-run", false, "only report")
	f := fs.Bool("force", false, "allow a non-empty target")
	_ = fs.Parse(args)
	return *d, *f
}

func cmdPush(ctx context.Context, cfg *config.Config, e *engine.Engine, st *store.Store, args []string) error {
	dry, force := seedFlags("push", args)
	if !force {
		empty := true
		err := st.List(ctx, journal.DataPrefix, "", func(store.Object) error { empty = false; return errStop })
		if err != nil && !errors.Is(err, errStop) {
			return err
		}
		if !empty {
			return errors.New("bucket already has data/; use --force to push anyway (or pull first)")
		}
	}
	e.DryRun = dry
	return e.Reconcile(ctx, engine.ReconcileOptions{})
}

var errStop = errors.New("stop")

func cmdPull(ctx context.Context, cfg *config.Config, e *engine.Engine, st *store.Store, args []string) error {
	dry, force := seedFlags("pull", args)
	if err := os.MkdirAll(cfg.Root, 0o755); err != nil {
		return err
	}
	if !force {
		entries, err := os.ReadDir(cfg.Root)
		if err != nil {
			return err
		}
		if len(entries) > 0 {
			return fmt.Errorf("%s is not empty; use --force to pull anyway", cfg.Root)
		}
	}
	e.DryRun = dry
	return e.Reconcile(ctx, engine.ReconcileOptions{AllowEmptyLocal: true})
}

func cmdStatus(cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	check := fs.Bool("check", false, "exit 1 when unhealthy (for cron / health pings)")
	_ = fs.Parse(args)
	s, err := engine.ReadStatus(cfg.StateDir)
	if err != nil {
		return fmt.Errorf("no status file in %s (daemon never ran?): %w", cfg.StateDir, err)
	}
	problems := healthProblems(s, time.Now())
	if *check {
		for _, p := range problems {
			fmt.Println(p)
		}
		if len(problems) > 0 {
			os.Exit(1)
		}
		fmt.Println("ok")
		return nil
	}
	age := time.Since(s.UpdatedAt).Round(time.Second)
	alive := "running"
	if age > staleAfter {
		alive = "stale, daemon probably stopped"
	}
	fmt.Printf("node:            %s (%s, status age %s, up since %s)\n", s.Node, alive, age, s.StartedAt.Format("2006-01-02 15:04"))
	fmt.Printf("root:            %s\n", cfg.Root)
	fmt.Printf("index:           %d files\n", s.IndexCount)
	fmt.Printf("pending batch:   %d paths\n", s.Pending)
	fmt.Printf("last poll ok:    %s\n", ago(s.LastPollOK))
	fmt.Printf("last change:     %s\n", ago(s.LastChange))
	if s.LastJournalTs > 0 {
		fmt.Printf("last journal:    %s\n", ago(time.UnixMilli(s.LastJournalTs)))
	}
	fmt.Printf("errors:          %d since start", s.Errors)
	if s.LastError != "" {
		fmt.Printf("; last %s: %s", ago(s.LastErrorAt), s.LastError)
	}
	fmt.Println()
	fmt.Printf("relay role:      %s", s.Role)
	if s.RelayConnected {
		fmt.Printf(", connected to %s", s.RelayURL)
	}
	fmt.Println()
	if len(s.Peers) > 0 {
		var parts []string
		for _, p := range s.Peers {
			tag := "since"
			if p.Relay {
				tag = "relay, since"
			}
			parts = append(parts, fmt.Sprintf("%s (%s %s)", p.Node, tag, p.Since.Local().Format("01-02 15:04")))
		}
		fmt.Printf("peers:           %s\n", strings.Join(parts, ", "))
	}
	for _, p := range problems {
		fmt.Printf("PROBLEM:         %s\n", p)
	}
	c := s.Counters
	fmt.Printf("requests since %s:\n", s.CountersSince.Format("2006-01-02 15:04"))
	fmt.Printf("  PUT/POST:      %d\n  GET/HEAD/LIST: %d\n  DELETE:        %d\n  bytes up:      %s\n  bytes down:    %s\n",
		c.Put, c.Get, c.Delete, human(c.BytesUp), human(c.BytesDown))
	if len(s.Recent) > 0 {
		fmt.Printf("recent changes (newest last):\n")
		for _, ch := range s.Recent {
			fmt.Printf("  %s %-8s %-10s %s\n", ch.At.Local().Format("01-02 15:04:05"), ch.Op, ch.Node, ch.Path)
		}
	}
	return nil
}

const (
	staleAfter    = 30 * time.Second // status.json is rewritten every 10 s
	pollGrace     = 5 * time.Minute
	recentErrorIn = 10 * time.Minute
)

// healthProblems is the rule set behind `status --check`.
func healthProblems(s *engine.Status, now time.Time) []string {
	var out []string
	if age := now.Sub(s.UpdatedAt); age > staleAfter {
		out = append(out, fmt.Sprintf("daemon not running: status is %s old", age.Round(time.Second)))
	}
	switch {
	case !s.LastPollOK.IsZero() && now.Sub(s.LastPollOK) > pollGrace:
		out = append(out, fmt.Sprintf("bucket unreachable: last successful poll %s", ago(s.LastPollOK)))
	case s.LastPollOK.IsZero() && now.Sub(s.StartedAt) > pollGrace:
		out = append(out, "bucket unreachable: no successful poll since start")
	}
	if s.LastError != "" && now.Sub(s.LastErrorAt) < recentErrorIn {
		out = append(out, fmt.Sprintf("error %s: %s", ago(s.LastErrorAt), s.LastError))
	}
	return out
}

func ago(t time.Time) string {
	if t.IsZero() || t.Unix() <= 0 {
		return "never"
	}
	return fmt.Sprintf("%s (%s ago)", t.Local().Format("2006-01-02 15:04:05"), time.Since(t).Round(time.Second))
}

func human(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func cmdTrash(cfg *config.Config, args []string) error {
	trashDir := filepath.Join(cfg.StateDir, "trash")
	if len(args) < 1 {
		return errors.New("usage: gozeks trash list | restore ID/PATH")
	}
	switch args[0] {
	case "list":
		var items []string
		err := filepath.WalkDir(trashDir, func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			rel, _ := filepath.Rel(trashDir, p)
			items = append(items, filepath.ToSlash(rel))
			return nil
		})
		if err != nil {
			return err
		}
		sort.Strings(items)
		for _, it := range items {
			fmt.Println(it)
		}
		return nil
	case "restore":
		if len(args) != 2 {
			return errors.New("usage: gozeks trash restore ID/PATH (as printed by trash list)")
		}
		item := filepath.FromSlash(args[1])
		src := filepath.Join(trashDir, item)
		parts := strings.SplitN(args[1], "/", 2)
		if len(parts) != 2 {
			return errors.New("expected ID/PATH")
		}
		dst := filepath.Join(cfg.Root, filepath.FromSlash(parts[1]))
		if _, err := os.Stat(dst); err == nil {
			return fmt.Errorf("%s already exists", dst)
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		if err := os.Rename(src, dst); err != nil {
			return err
		}
		fmt.Println("restored", dst)
		return nil
	default:
		return errors.New("usage: gozeks trash list | restore ID/PATH")
	}
}

func cmdIndex(cfg *config.Config, args []string) error {
	if len(args) != 2 || args[0] != "forget" || args[1] == "" {
		return errors.New("usage: gozeks index forget PREFIX")
	}
	db, err := index.Open(filepath.Join(cfg.StateDir, "index.db"))
	if err != nil {
		return err
	}
	defer db.Close()
	prefix := strings.TrimSuffix(args[1], "/")
	paths, err := db.Under(prefix)
	if err != nil {
		return err
	}
	if _, has, _ := db.Get(prefix); has {
		paths = append(paths, prefix)
	}
	for _, p := range paths {
		if err := db.Delete(p); err != nil {
			return err
		}
	}
	fmt.Printf("forgot %d index entries under %s\n", len(paths), prefix)
	return nil
}
