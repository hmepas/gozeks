package journal

import (
	"sort"
	"testing"
	"time"
)

func TestKeyRoundTrip(t *testing.T) {
	k := Key(1761700000123, "my-node", 7)
	if k != "_sync/journal/1761700000123-my-node-7.json" {
		t.Fatalf("key %q", k)
	}
	ts, node, seq, err := ParseKey(k)
	if err != nil || ts != 1761700000123 || node != "my-node" || seq != 7 {
		t.Fatalf("parse: %d %q %d %v", ts, node, seq, err)
	}
}

func TestKeysSortChronologically(t *testing.T) {
	keys := []string{Key(1761700000500, "b", 1), Key(1761700000100, "z", 9), Key(99, "a", 1)}
	sort.Strings(keys)
	if keys[0] != Key(99, "a", 1) || keys[1] != Key(1761700000100, "z", 9) {
		t.Fatalf("order %v", keys)
	}
}

func TestStartAfterRewind(t *testing.T) {
	last := int64(1761700000000)
	sa := StartAfter(last, 120*time.Second)
	// an entry 100 s before the last processed one must still be listed
	old := Key(last-100_000, "x", 1)
	if !(old > sa) {
		t.Fatalf("%q should sort after %q", old, sa)
	}
	older := Key(last-130_000, "x", 1)
	if older > sa {
		t.Fatalf("%q should sort before %q", older, sa)
	}
	if StartAfter(5, time.Minute) != Prefix+"0000000000000" {
		t.Fatal("negative clamp")
	}
}
