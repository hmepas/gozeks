package engine

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/hmepas/gozeks/internal/index"
	"github.com/hmepas/gozeks/internal/journal"
)

func TestUploadCreatesJournalAndCursor(t *testing.T) {
	fs := newFake()
	a := newNode(t, fs, "a")
	a.write(t, "Notes/x.md", "hello")
	a.write(t, ".obsidian/workspace.json", "{}")
	a.upload(t, "Notes/x.md", ".obsidian/workspace.json")

	if !fs.has("data/Notes/x.md") {
		t.Fatal("file not uploaded")
	}
	if fs.has("data/.obsidian/workspace.json") {
		t.Fatal("excluded path reached the bucket")
	}
	if !fs.has(journal.CursorKey) {
		t.Fatal("no cursor")
	}
	keys := fs.keys(journal.Prefix)
	if len(keys) != 1 {
		t.Fatalf("journal keys %v", keys)
	}
	body, _, _, _ := fs.Get(context.Background(), keys[0])
	var ent journal.Entry
	_ = json.Unmarshal(body, &ent)
	if ent.Node != "a" || len(ent.Ops) != 1 || ent.Ops[0].Op != "put" || ent.Ops[0].Path != "Notes/x.md" || !strings.HasPrefix(ent.Ops[0].Hash, "sha256:") {
		t.Fatalf("entry %+v", ent)
	}
	if n, _ := a.e.idx.Count(); n != 1 {
		t.Fatalf("index count %d", n)
	}
}

func TestUploadSkipsUnchangedAndEchoes(t *testing.T) {
	fs := newFake()
	a := newNode(t, fs, "a")
	a.write(t, "n.md", "v1")
	a.upload(t, "n.md")
	puts := fs.puts
	// touch with the same content: new mtime, same hash → no request at all
	time.Sleep(10 * time.Millisecond)
	a.write(t, "n.md", "v1")
	a.upload(t, "n.md")
	if fs.puts != puts {
		t.Fatalf("unchanged content caused %d PUTs", fs.puts-puts)
	}
	a.write(t, "n.md", "v2")
	a.upload(t, "n.md")
	if fs.puts != puts+3 { // data + journal + cursor
		t.Fatalf("expected 3 PUTs, got %d", fs.puts-puts)
	}
}

func TestTwoNodesPropagate(t *testing.T) {
	fs := newFake()
	a := newNode(t, fs, "a")
	b := newNode(t, fs, "b")
	a.write(t, "Notes/x.md", "from a")
	a.upload(t, "Notes/x.md")
	if keys := b.poll(t); len(keys) != 1 {
		t.Fatalf("b processed %v", keys)
	}
	if b.read(t, "Notes/x.md") != "from a" {
		t.Fatal("b did not receive the file")
	}
	// b's own echo: uploading what it just downloaded is a no-op
	puts := fs.puts
	b.upload(t, "Notes/x.md")
	if fs.puts != puts {
		t.Fatal("download echoed back as upload")
	}
	// a polls: its own entry is skipped, nothing else
	if keys := a.poll(t); len(keys) != 0 {
		t.Fatalf("a processed %v", keys)
	}
	// modify on b, delete on a afterwards
	b.write(t, "Notes/x.md", "from b")
	b.upload(t, "Notes/x.md")
	a.poll(t)
	if a.read(t, "Notes/x.md") != "from b" {
		t.Fatal("a did not get b's edit")
	}
	os.Remove(filepath.Join(a.root, "Notes/x.md"))
	a.upload(t, "Notes/x.md")
	if fs.has("data/Notes/x.md") {
		t.Fatal("remote not deleted")
	}
	b.poll(t)
	if b.exists("Notes/x.md") {
		t.Fatal("b still has the file")
	}
	if tr := b.trashed(t); len(tr) != 1 || !strings.HasSuffix(tr[0], "/Notes/x.md") {
		t.Fatalf("trash %v", tr)
	}
	if b.exists("Notes") {
		t.Fatal("empty dir left behind")
	}
}

func TestConflictKeepsBothVersions(t *testing.T) {
	fs := newFake()
	a := newNode(t, fs, "a")
	b := newNode(t, fs, "b")
	a.write(t, "n.md", "base")
	a.upload(t, "n.md")
	b.poll(t)

	a.write(t, "n.md", "edit a")
	b.write(t, "n.md", "edit b")
	a.upload(t, "n.md")
	b.poll(t) // b has a local edit and receives a's → conflict
	tb := b.tree(t)
	if tb["n.md"] != "edit a" {
		t.Fatalf("remote version should take the path: %v", tb)
	}
	var conflict string
	for p := range tb {
		if strings.Contains(p, "(conflict b ") {
			conflict = p
		}
	}
	if conflict == "" || tb[conflict] != "edit b" {
		t.Fatalf("conflict copy missing: %v", tb)
	}
	if !strings.HasSuffix(conflict, ").md") {
		t.Fatalf("conflict name %q", conflict)
	}
	// the conflict copy was uploaded automatically and reaches a
	a.poll(t)
	if !reflect.DeepEqual(a.tree(t), tb) {
		t.Fatalf("trees differ:\n a=%v\n b=%v", a.tree(t), tb)
	}
	// exactly one conflict copy
	n := 0
	for p := range tb {
		if strings.Contains(p, "conflict") {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d conflict copies", n)
	}
}

func TestSameContentBothSidesNoConflict(t *testing.T) {
	fs := newFake()
	a := newNode(t, fs, "a")
	b := newNode(t, fs, "b")
	a.write(t, "n.md", "same")
	b.write(t, "n.md", "same")
	a.upload(t, "n.md")
	b.poll(t)
	if len(b.tree(t)) != 1 {
		t.Fatalf("tree %v", b.tree(t))
	}
}

func TestRemoteDeleteVsLocalEdit(t *testing.T) {
	fs := newFake()
	a := newNode(t, fs, "a")
	b := newNode(t, fs, "b")
	a.write(t, "n.md", "base")
	a.upload(t, "n.md")
	b.poll(t)
	b.write(t, "n.md", "edited on b")
	os.Remove(filepath.Join(a.root, "n.md"))
	a.upload(t, "n.md")
	b.poll(t)
	if b.read(t, "n.md") != "edited on b" {
		t.Fatal("local edit lost to remote delete")
	}
	if !fs.has("data/n.md") {
		t.Fatal("kept file was not re-uploaded")
	}
	a.poll(t)
	if a.read(t, "n.md") != "edited on b" {
		t.Fatal("a did not get the resurrected file")
	}
}

func TestJournalRaceBothEntriesApplied(t *testing.T) {
	fs := newFake()
	a := newNode(t, fs, "a")
	b := newNode(t, fs, "b")
	c := newNode(t, fs, "c")
	a.write(t, "a.md", "a")
	b.write(t, "b.md", "b")
	a.upload(t, "a.md")
	b.upload(t, "b.md") // cursor now only names b
	keys := c.poll(t)
	if len(keys) != 2 {
		t.Fatalf("c processed %d entries", len(keys))
	}
	if c.read(t, "a.md") != "a" || c.read(t, "b.md") != "b" {
		t.Fatal("c missed a batch")
	}
	// second poll: cursor unchanged → no requests beyond the HEAD
	gets := fs.gets
	c.poll(t)
	if fs.gets != gets+1 {
		t.Fatalf("expected one HEAD, got %d requests", fs.gets-gets)
	}
}

func TestSkewRewindDoesNotReapply(t *testing.T) {
	fs := newFake()
	a := newNode(t, fs, "a")
	b := newNode(t, fs, "b")
	a.write(t, "n.md", "1")
	a.upload(t, "n.md")
	b.poll(t)
	// an entry with an older timestamp (clock skew) appears later
	old := time.Now().Add(-60 * time.Second).UnixMilli()
	ent := journal.Entry{Node: "a", Ts: old, Seq: 99, Ops: []journal.Op{{Op: "put", Path: "late.md", Hash: "sha256:x"}}}
	body, _ := json.Marshal(ent)
	_, _ = fs.Put(context.Background(), journal.Key(old, "a", 99), body, nil)
	_, _ = fs.Put(context.Background(), "data/late.md", []byte("late"), map[string]string{"hash": "sha256:x"})
	_, _ = fs.Put(context.Background(), journal.CursorKey, []byte("bump"), nil)
	keys := b.poll(t)
	if len(keys) != 1 || b.read(t, "late.md") != "late" {
		t.Fatalf("late entry not applied: %v", keys)
	}
	if keys := b.poll(t); len(keys) != 0 {
		t.Fatal("re-applied processed entry")
	}
}

func TestReconcileTable(t *testing.T) {
	fs := newFake()
	a := newNode(t, fs, "a")
	b := newNode(t, fs, "b")
	ctx := context.Background()
	// seed: a pushes three files, b pulls them
	a.write(t, "keep.md", "k")
	a.write(t, "gone-remote.md", "g")
	a.write(t, "gone-local.md", "g")
	a.write(t, "both.md", "b0")
	a.upload(t, "keep.md", "gone-remote.md", "gone-local.md", "both.md")
	if err := b.e.Reconcile(ctx, ReconcileOptions{AllowEmptyLocal: true}); err != nil {
		t.Fatal(err)
	}
	if len(b.tree(t)) != 4 {
		t.Fatalf("pull: %v", b.tree(t))
	}

	// while b is "down": a deletes gone-remote, edits both.md, adds new.md;
	// b deletes gone-local, edits both.md, adds local-new.md, edits keep.md
	os.Remove(filepath.Join(a.root, "gone-remote.md"))
	a.write(t, "both.md", "from a")
	a.write(t, "new.md", "new")
	a.upload(t, "gone-remote.md", "both.md", "new.md")
	os.Remove(filepath.Join(b.root, "gone-local.md"))
	b.write(t, "both.md", "from b")
	b.write(t, "local-new.md", "ln")
	b.write(t, "keep.md", "k2")

	if err := b.e.Reconcile(ctx, ReconcileOptions{}); err != nil {
		t.Fatal(err)
	}
	tb := b.tree(t)
	if _, ok := tb["gone-remote.md"]; ok {
		t.Fatal("remote delete not applied")
	}
	if fs.has("data/gone-local.md") {
		t.Fatal("local delete not propagated")
	}
	if tb["both.md"] != "from a" || tb["new.md"] != "new" || tb["keep.md"] != "k2" {
		t.Fatalf("tree %v", tb)
	}
	conflicts := 0
	for p, c := range tb {
		if strings.Contains(p, "(conflict b") && c == "from b" {
			conflicts++
		}
	}
	if conflicts != 1 {
		t.Fatalf("conflicts %d in %v", conflicts, tb)
	}
	if !fs.has("data/local-new.md") || !fs.has("data/keep.md") {
		t.Fatal("local additions not uploaded")
	}
	// a catches up through the journal and ends identical to b
	a.poll(t)
	if !reflect.DeepEqual(a.tree(t), tb) {
		t.Fatalf("trees differ:\n a=%v\n b=%v", a.tree(t), tb)
	}
	// a second reconcile is a no-op request-wise except the LIST
	gets, puts := fs.gets, fs.puts
	if err := b.e.Reconcile(ctx, ReconcileOptions{}); err != nil {
		t.Fatal(err)
	}
	if fs.puts != puts || fs.gets > gets+2 {
		t.Fatalf("steady-state reconcile made %d PUT, %d GET", fs.puts-puts, fs.gets-gets)
	}
}

func TestReconcileRefusesEmptyLocal(t *testing.T) {
	fs := newFake()
	a := newNode(t, fs, "a")
	a.write(t, "n.md", "1")
	a.upload(t, "n.md")
	os.RemoveAll(a.root)
	os.MkdirAll(a.root, 0o755)
	if err := a.e.Reconcile(context.Background(), ReconcileOptions{}); err == nil {
		t.Fatal("expected refusal")
	}
	if !fs.has("data/n.md") {
		t.Fatal("deleted remote file")
	}
}

func TestDryRunTouchesNothing(t *testing.T) {
	fs := newFake()
	a := newNode(t, fs, "a")
	a.write(t, "n.md", "1")
	a.e.DryRun = true
	if err := a.e.Reconcile(context.Background(), ReconcileOptions{}); err != nil {
		t.Fatal(err)
	}
	if fs.puts != 0 {
		t.Fatal("dry run uploaded")
	}
}

func TestDirectoryRemovalDeletesAllBelow(t *testing.T) {
	fs := newFake()
	a := newNode(t, fs, "a")
	a.write(t, "d/1.md", "1")
	a.write(t, "d/sub/2.md", "2")
	a.upload(t, "d")
	if len(fs.keys("data/d/")) != 2 {
		t.Fatal("dir upload")
	}
	os.RemoveAll(filepath.Join(a.root, "d"))
	a.upload(t, "d")
	if len(fs.keys("data/d/")) != 0 {
		t.Fatalf("remaining %v", fs.keys("data/d/"))
	}
}

func TestConflictName(t *testing.T) {
	ts := time.Date(2026, 10, 29, 21, 5, 0, 0, time.UTC)
	got := conflictName("Notes/Daily note.md", "mac", ts)
	if got != "Notes/Daily note (conflict mac 2026-10-29 2105).md" {
		t.Fatal(got)
	}
	if conflictName("README", "x", ts) != "README (conflict x 2026-10-29 2105)" {
		t.Fatal(conflictName("README", "x", ts))
	}
}

func TestConcurrentUploadsConvergeWithOneConflict(t *testing.T) {
	for _, order := range []string{"a-first", "b-first"} {
		t.Run(order, func(t *testing.T) {
			fs := newFake()
			a := newNode(t, fs, "a")
			b := newNode(t, fs, "b")
			a.write(t, "n.md", "base")
			a.upload(t, "n.md")
			b.poll(t)
			// both edit and both upload before either polls
			a.write(t, "n.md", "edit a")
			b.write(t, "n.md", "edit b")
			if order == "a-first" {
				a.upload(t, "n.md")
				time.Sleep(2 * time.Millisecond)
				b.upload(t, "n.md")
			} else {
				b.upload(t, "n.md")
				time.Sleep(2 * time.Millisecond)
				a.upload(t, "n.md")
			}
			for i := 0; i < 3; i++ { // a few rounds: conflict copy upload needs one more poll
				a.poll(t)
				b.poll(t)
			}
			ta, tb := a.tree(t), b.tree(t)
			if !reflect.DeepEqual(ta, tb) {
				t.Fatalf("trees differ:\n a=%v\n b=%v", ta, tb)
			}
			conflicts := 0
			vals := map[string]bool{}
			for p, v := range ta {
				vals[v] = true
				if strings.Contains(p, "(conflict ") {
					conflicts++
				}
			}
			if conflicts != 1 || !vals["edit a"] || !vals["edit b"] {
				t.Fatalf("want one conflict copy and both edits: %v", ta)
			}
			// the newer upload won the path
			want := "edit b"
			if order == "b-first" {
				want = "edit a"
			}
			if ta["n.md"] != want {
				t.Fatalf("n.md = %q, want %q", ta["n.md"], want)
			}
		})
	}
}

func TestOwnCursorDoesNotMaskOthers(t *testing.T) {
	fs := newFake()
	a := newNode(t, fs, "a")
	b := newNode(t, fs, "b")
	a.write(t, "a.md", "a")
	b.write(t, "b.md", "b")
	a.upload(t, "a.md")
	b.upload(t, "b.md") // b wrote the cursor last
	b.poll(t)
	if !b.exists("a.md") {
		t.Fatal("b missed a's batch because it wrote the cursor last")
	}
}

func TestSequentialEditWithSkewedClockIsNotStale(t *testing.T) {
	fs := newFake()
	a := newNode(t, fs, "a")
	b := newNode(t, fs, "b")
	a.write(t, "n.md", "v1")
	a.upload(t, "n.md")
	b.poll(t)
	// b edits on top of v1 but its clock is 30 s behind a's
	hv1, _ := hashFile(filepath.Join(b.root, "n.md"))
	b.write(t, "n.md", "v2")
	hv2, _ := hashFile(filepath.Join(b.root, "n.md"))
	old := time.Now().Add(-30 * time.Second).UnixMilli()
	ent := journal.Entry{Node: "b", Ts: old, Seq: 1, Ops: []journal.Op{{Op: "put", Path: "n.md", Hash: hv2, Prev: hv1}}}
	body, _ := json.Marshal(ent)
	ctx := context.Background()
	_, _ = fs.Put(ctx, "data/n.md", []byte("v2"), map[string]string{"hash": hv2})
	_, _ = fs.Put(ctx, journal.Key(old, "b", 1), body, nil)
	_, _ = fs.Put(ctx, journal.CursorKey, []byte("x"), nil)
	a.poll(t)
	if a.read(t, "n.md") != "v2" {
		t.Fatal("sequential edit treated as stale")
	}
}

func TestSymlinkedDirIsIgnoredBothWays(t *testing.T) {
	fs := newFake()
	a := newNode(t, fs, "a")
	b := newNode(t, fs, "b")
	// a has a real hidden dir and a symlink pointing at it; b has a real dir of the same name
	a.write(t, ".mem/README.md", "mac original")
	if err := os.Symlink(".mem", filepath.Join(a.root, "mem")); err != nil {
		t.Fatal(err)
	}
	a.upload(t, ".mem/README.md", "mem", "mem/README.md")
	if fs.has("data/mem/README.md") {
		t.Fatal("uploaded a path through a symlink")
	}
	b.poll(t)
	b.write(t, "mem/README.md", "b real dir")
	b.write(t, "mem/skills/x.md", "x")
	b.upload(t, "mem")
	a.poll(t) // a must not write through its symlink, nor index those paths
	if a.read(t, ".mem/README.md") != "mac original" {
		t.Fatal("remote put written through the symlink")
	}
	if _, has, _ := a.e.idx.Get("mem/README.md"); has {
		t.Fatal("indexed a path under a symlink")
	}
	// reconcile on a must not delete b's files remotely
	if err := a.e.Reconcile(context.Background(), ReconcileOptions{}); err != nil {
		t.Fatal(err)
	}
	if !fs.has("data/mem/README.md") || !fs.has("data/mem/skills/x.md") {
		t.Fatal("reconcile deleted remote files that live under a local symlink")
	}
	// after the symlink is replaced by a real directory, the files arrive
	os.Remove(filepath.Join(a.root, "mem"))
	if err := a.e.Reconcile(context.Background(), ReconcileOptions{}); err != nil {
		t.Fatal(err)
	}
	if a.read(t, "mem/README.md") != "b real dir" || a.read(t, "mem/skills/x.md") != "x" {
		t.Fatalf("tree after symlink removal: %v", a.tree(t))
	}
}

func TestDownloadedFileIsWorldReadable(t *testing.T) {
	fs := newFake()
	a := newNode(t, fs, "a")
	b := newNode(t, fs, "b")
	a.write(t, "n.md", "x")
	a.upload(t, "n.md")
	b.poll(t)
	fi, err := os.Stat(filepath.Join(b.root, "n.md"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o644 {
		t.Fatalf("mode %o", fi.Mode().Perm())
	}
}

func TestNFCNormalization(t *testing.T) {
	nfd := "Notes/Батарейки.md" // й decomposed
	nfcName := "Notes/Батарейки.md"
	if nfc(nfd) != nfcName {
		t.Fatal("test setup")
	}
	fs := newFake()
	a := newNode(t, fs, "a")
	b := newNode(t, fs, "b")
	a.write(t, nfd, "v1")
	a.upload(t, nfd) // watcher-style path in NFD
	if !fs.has("data/"+nfcName) || fs.has("data/"+nfd) {
		t.Fatalf("keys %v", fs.keys("data/"))
	}
	if _, has, _ := a.e.idx.Get(nfcName); !has {
		t.Fatal("index key not NFC")
	}
	b.poll(t)
	if b.read(t, nfcName) != "v1" {
		t.Fatal("b did not get the file under its NFC name")
	}
	// legacy state: an NFD key in the bucket next to its identical NFC twin
	_, _ = fs.Put(context.Background(), "data/"+nfd, []byte("v1"), nil)
	if err := b.e.Reconcile(context.Background(), ReconcileOptions{}); err != nil {
		t.Fatal(err)
	}
	if fs.has("data/" + nfd) {
		t.Fatal("legacy NFD key not removed")
	}
	if runtime.GOOS == "darwin" {
		return // APFS maps both spellings to one file; the twin cases below need Linux
	}
	// on Linux an NFD duplicate file on disk is a distinct file
	b.write(t, nfd, "v1")
	if err := b.e.Reconcile(context.Background(), ReconcileOptions{}); err != nil {
		t.Fatal(err)
	}
	tb := b.tree(t)
	if len(tb) != 1 || tb[nfcName] != "v1" {
		t.Fatalf("b tree %v", tb)
	}
	if tr := b.trashed(t); len(tr) != 1 {
		t.Fatalf("duplicate should be trashed, got %v", tr)
	}
	// a differing NFD twin becomes a conflict copy instead
	b.write(t, nfd, "other")
	if err := b.e.Reconcile(context.Background(), ReconcileOptions{}); err != nil {
		t.Fatal(err)
	}
	tb = b.tree(t)
	n := 0
	for p, v := range tb {
		if strings.Contains(p, "(conflict b") && v == "other" {
			n++
		}
	}
	if n != 1 || tb[nfcName] != "v1" {
		t.Fatalf("b tree %v", tb)
	}
}

func TestIndexMigratesToNFC(t *testing.T) {
	fs := newFake()
	a := newNode(t, fs, "a")
	nfd := "Ба" + "и\u0306" + ".md" // й decomposed
	_ = a.e.idx.Put(nfd, index.Entry{Hash: "sha256:x"})
	a.e.Close()
	e2, err := New(a.cfg, fs, a.e.log)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if _, has, _ := e2.idx.Get(nfd); has {
		t.Fatal("NFD key kept")
	}
	if _, has, _ := e2.idx.Get(nfc(nfd)); !has {
		t.Fatal("NFC key missing")
	}
}

func TestCaseTwinIsNotTouchedOnCaseInsensitiveFS(t *testing.T) {
	fs := newFake()
	a := newNode(t, fs, "a")
	b := newNode(t, fs, "b")
	a.write(t, "Notes/Attachments/x.jpg", "mac")
	if _, err := os.Lstat(filepath.Join(a.root, "Notes/attachments/x.jpg")); err != nil {
		t.Skip("case-sensitive filesystem")
	}
	a.upload(t, "Notes/Attachments/x.jpg")
	b.poll(t)
	b.write(t, "Notes/attachments/x.jpg", "linux twin") // on a Linux node this is a second file
	// simulate the Linux node's upload (its tree is case-sensitive; fake it via the store)
	_, _ = fs.Put(context.Background(), "data/Notes/attachments/x.jpg", []byte("linux twin"), map[string]string{"hash": hashBytes([]byte("linux twin"))})
	ent := journal.Entry{Node: "b", Ts: time.Now().UnixMilli(), Seq: 5, Ops: []journal.Op{{Op: "put", Path: "Notes/attachments/x.jpg", Hash: hashBytes([]byte("linux twin"))}}}
	body, _ := json.Marshal(ent)
	_, _ = fs.Put(context.Background(), journal.Key(ent.Ts, "b", 5), body, nil)
	_, _ = fs.Put(context.Background(), journal.CursorKey, []byte("x"), nil)
	a.poll(t)
	if a.read(t, "Notes/Attachments/x.jpg") != "mac" {
		t.Fatal("case twin overwrote the local file")
	}
	if _, has, _ := a.e.idx.Get("Notes/attachments/x.jpg"); has {
		t.Fatal("case twin indexed")
	}
	if err := a.e.Reconcile(context.Background(), ReconcileOptions{}); err != nil {
		t.Fatal(err)
	}
	if !fs.has("data/Notes/attachments/x.jpg") || !fs.has("data/Notes/Attachments/x.jpg") {
		t.Fatal("reconcile deleted a case twin remotely")
	}
	if a.read(t, "Notes/Attachments/x.jpg") != "mac" {
		t.Fatal("reconcile overwrote the local file with its case twin")
	}
}

func TestNFDOnDiskNameIsNotACaseConflict(t *testing.T) {
	fs := newFake()
	a := newNode(t, fs, "a")
	nfd := "Notes/Эльгуна и ка" + "й" + "т.md" // й decomposed, as APFS stores it
	a.write(t, nfd, "v1")
	if a.e.caseConflict(nfc(nfd)) {
		t.Fatal("NFD spelling on disk reported as case conflict")
	}
	a.upload(t, nfd)
	if !fs.has("data/" + nfc(nfd)) {
		t.Fatalf("not uploaded: %v", fs.keys("data/"))
	}
	edit := nfd // APFS: the NFD spelling still opens the same file
	if runtime.GOOS != "darwin" {
		// Linux: the upload renamed the file to NFC; writing the NFD spelling
		// now would create a distinct twin (covered by TestNFCNormalization)
		if _, err := os.Lstat(filepath.Join(a.root, nfd)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("NFD name not renamed on disk: %v", err)
		}
		edit = nfc(nfd)
	}
	a.write(t, edit, "v2")
	a.upload(t, nfc(nfd)) // reconcile-style NFC path
	body, _, _, _ := fs.Get(context.Background(), "data/"+nfc(nfd))
	if string(body) != "v2" {
		t.Fatal("edit of NFD-named file not uploaded under NFC key")
	}
}

func TestStatusTracksChangesAndErrors(t *testing.T) {
	fs := newFake()
	a := newNode(t, fs, "a")
	b := newNode(t, fs, "b")
	a.write(t, "Notes/x.md", "v1")
	a.upload(t, "Notes/x.md")
	b.poll(t)
	os.Remove(filepath.Join(a.root, "Notes/x.md"))
	a.upload(t, "Notes/x.md")
	b.poll(t)

	sa := a.e.status(nil)
	if len(sa.Recent) != 2 || sa.Recent[0].Op != "put" || sa.Recent[1].Op != "delete" || sa.Recent[0].Node != "a" {
		t.Fatalf("a recent = %+v", sa.Recent)
	}
	sb := b.e.status(nil)
	if len(sb.Recent) != 2 || sb.Recent[0].Op != "put" || sb.Recent[0].Node != "a" || sb.Recent[1].Op != "delete" || sb.Recent[1].Path != "Notes/x.md" {
		t.Fatalf("b recent = %+v", sb.Recent)
	}
	if sb.LastPollOK.IsZero() || sb.Errors != 0 || sb.LastError != "" {
		t.Fatalf("health = poll %v errors %d %q", sb.LastPollOK, sb.Errors, sb.LastError)
	}
	b.e.log.Error("download", "path", "Notes/y.md", "err", errors.New("boom"))
	sb = b.e.status(nil)
	if sb.Errors != 1 || !strings.Contains(sb.LastError, "boom") || sb.LastErrorAt.IsZero() {
		t.Fatalf("error not tapped: %+v", sb)
	}
	// ring is bounded
	for i := 0; i < recentMax+5; i++ {
		a.e.noteChange("put", "p", "a")
	}
	if n := len(a.e.status(nil).Recent); n != recentMax {
		t.Fatalf("recent len %d", n)
	}
}

// A put that could not be fetched leaves the reader one version behind; the
// next entry of the same series must replace the file, not fork it.
func TestLostPutIsNotAConflict(t *testing.T) {
	fs := newFake()
	a := newNode(t, fs, "a")
	b := newNode(t, fs, "b")
	a.write(t, "n.md", "v0")
	a.upload(t, "n.md")
	b.poll(t)

	fail := true
	b.e.st = &hookStore{fakeStore: fs, onData: func() error {
		if fail {
			fail = false
			return errors.New("409 OperationAborted")
		}
		return nil
	}}
	a.write(t, "n.md", "v0 v1")
	a.upload(t, "n.md")
	b.poll(t) // the data GET fails, the op is dropped
	a.write(t, "n.md", "v0 v1 v2")
	a.upload(t, "n.md")
	b.poll(t)
	if c := b.conflictCopies(t); len(c) > 0 {
		t.Fatalf("conflict copies on a node that never edited the file: %v", c)
	}
	if got := b.read(t, "n.md"); got != "v0 v1 v2" {
		t.Fatalf("n.md = %q", got)
	}
}

// A reader catching up on a series while the writer keeps going downloads
// content newer than the entry it is applying; that is lag, not a conflict.
func TestReaderBehindSeriesIsNotAConflict(t *testing.T) {
	fs := newFake()
	a := newNode(t, fs, "a")
	b := newNode(t, fs, "b")
	a.write(t, "n.md", "v0")
	a.upload(t, "n.md")
	b.poll(t)
	for _, v := range []string{"v0 v1", "v0 v1 v2", "v0 v1 v2 v3"} {
		a.write(t, "n.md", v)
		a.upload(t, "n.md")
	}
	gets := 0
	b.e.st = &hookStore{fakeStore: fs, onData: func() error {
		gets++
		if gets == 2 { // the writer lands one more version mid catch-up
			a.write(t, "n.md", "v0 v1 v2 v3 v4")
			a.upload(t, "n.md")
		}
		return nil
	}}
	b.poll(t)
	b.poll(t)
	if c := b.conflictCopies(t); len(c) > 0 {
		t.Fatalf("conflict copies on a node that never edited the file: %v", c)
	}
	if got := b.read(t, "n.md"); got != "v0 v1 v2 v3 v4" {
		t.Fatalf("n.md = %q", got)
	}
}

// Divergent edits on a and b: only the author of the displaced version keeps
// it as a conflict copy; a third node just follows.
func TestDivergentEditsOneCopyAcrossThreeNodes(t *testing.T) {
	fs := newFake()
	a := newNode(t, fs, "a")
	b := newNode(t, fs, "b")
	c := newNode(t, fs, "c")
	a.write(t, "n.md", "base")
	a.upload(t, "n.md")
	b.poll(t)
	c.poll(t)

	a.write(t, "n.md", "edit a")
	b.write(t, "n.md", "edit b")
	a.upload(t, "n.md")
	c.poll(t) // c holds a's version when b's arrives
	time.Sleep(2 * time.Millisecond)
	b.upload(t, "n.md")
	for i := 0; i < 3; i++ {
		a.poll(t)
		b.poll(t)
		c.poll(t)
	}
	ta := a.tree(t)
	if !reflect.DeepEqual(ta, b.tree(t)) || !reflect.DeepEqual(ta, c.tree(t)) {
		t.Fatalf("trees differ:\n a=%v\n b=%v\n c=%v", ta, b.tree(t), c.tree(t))
	}
	copies := c.conflictCopies(t)
	if len(copies) != 1 || !strings.Contains(copies[0], "(conflict a ") || ta[copies[0]] != "edit a" {
		t.Fatalf("want exactly a's conflict copy: %v", ta)
	}
	if ta["n.md"] != "edit b" {
		t.Fatalf("n.md = %q, want the newer upload", ta["n.md"])
	}
}
