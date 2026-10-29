package exclude

import "testing"

func TestMatch(t *testing.T) {
	m, err := New([]string{
		".obsidian/workspace*.json", ".obsidian/workspace", ".obsidian/cache/",
		".trash/", ".zk/", ".DS_Store", "*.tmp", "*~", ".#*", ".gozeks-*",
	})
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]bool{
		".obsidian/workspace.json":          true,
		".obsidian/workspace-mobile.json":   true,
		".obsidian/workspace":               true,
		".obsidian/app.json":                false,
		".obsidian/plugins/x/main.js":       false,
		".obsidian/cache/foo":               true,
		".trash/note.md":                    true,
		"Notes/.trash/note.md":              true,
		".zk/index.db":                      true,
		"Notes/.DS_Store":                   true,
		".DS_Store":                         true,
		"Notes/a.tmp":                       true,
		"Notes/a.md~":                       true,
		"Notes/.#a.md":                      true,
		"Notes/.gozeks-tmp-123":             true,
		"Notes/a.md":                        false,
		"Tasks Inbox.md":                    false,
		".agents/skills/x/.venv/bin/python": false,
		"workspace.json":                    false,
	}
	for rel, want := range cases {
		if got := m.Match(rel); got != want {
			t.Errorf("%q: got %v want %v", rel, got, want)
		}
	}
}

func TestBadPattern(t *testing.T) {
	if _, err := New([]string{"["}); err == nil {
		t.Fatal("expected error")
	}
}
