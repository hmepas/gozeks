package index

import (
	"path/filepath"
	"testing"
)

func TestIndex(t *testing.T) {
	d, err := Open(filepath.Join(t.TempDir(), "index.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.Put("a/b.md", Entry{Size: 1, Hash: "sha256:x"}); err != nil {
		t.Fatal(err)
	}
	_ = d.Put("a/c.md", Entry{Size: 2})
	_ = d.Put("ab.md", Entry{Size: 3})
	e, ok, _ := d.Get("a/b.md")
	if !ok || e.Hash != "sha256:x" {
		t.Fatalf("get: %v %v", ok, e)
	}
	under, _ := d.Under("a")
	if len(under) != 2 {
		t.Fatalf("under: %v", under)
	}
	n, _ := d.Count()
	if n != 3 {
		t.Fatalf("count %d", n)
	}
	_ = d.MarkProcessed("_sync/journal/0000000000100-a-1.json")
	_ = d.MarkProcessed("_sync/journal/0000000000200-a-1.json")
	_ = d.PruneProcessed("_sync/journal/0000000000150")
	if ok, _ := d.IsProcessed("_sync/journal/0000000000100-a-1.json"); ok {
		t.Fatal("should be pruned")
	}
	if ok, _ := d.IsProcessed("_sync/journal/0000000000200-a-1.json"); !ok {
		t.Fatal("should remain")
	}
	_ = d.SetMeta("k", "v")
	if v, _ := d.GetMeta("k"); v != "v" {
		t.Fatal("meta")
	}
}
