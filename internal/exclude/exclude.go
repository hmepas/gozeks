// Package exclude matches vault-relative paths against exclude patterns.
//
// Pattern rules:
//   - "name/"      directory named "name" at any depth (and everything below it)
//   - "a/b/"       directory at that exact root-relative path (and below)
//   - "a/b*.json"  glob against the full root-relative path ("*" does not cross "/")
//   - "*.tmp"      glob against the final path component, at any depth
package exclude

import (
	"fmt"
	"path"
	"strings"
)

type rule struct {
	pattern string
	kind    int // 0 basename glob, 1 full-path glob, 2 dir name, 3 dir path
}

// Matcher holds compiled patterns.
type Matcher struct{ rules []rule }

// New validates the patterns.
func New(patterns []string) (*Matcher, error) {
	m := &Matcher{}
	for _, p := range patterns {
		p = strings.TrimPrefix(p, "./")
		if p == "" {
			continue
		}
		r := rule{pattern: p}
		switch {
		case strings.HasSuffix(p, "/") && strings.Contains(strings.TrimSuffix(p, "/"), "/"):
			r.kind, r.pattern = 3, strings.TrimSuffix(p, "/")
		case strings.HasSuffix(p, "/"):
			r.kind, r.pattern = 2, strings.TrimSuffix(p, "/")
		case strings.Contains(p, "/"):
			r.kind = 1
		default:
			r.kind = 0
		}
		if _, err := path.Match(r.pattern, ""); err != nil {
			return nil, fmt.Errorf("exclude pattern %q: %w", p, err)
		}
		m.rules = append(m.rules, r)
	}
	return m, nil
}

// Match reports whether rel (forward slashes, no leading "/") is excluded.
func (m *Matcher) Match(rel string) bool {
	rel = strings.TrimPrefix(rel, "/")
	parts := strings.Split(rel, "/")
	base := parts[len(parts)-1]
	for _, r := range m.rules {
		switch r.kind {
		case 0:
			if ok, _ := path.Match(r.pattern, base); ok {
				return true
			}
		case 1:
			if ok, _ := path.Match(r.pattern, rel); ok {
				return true
			}
			// a pattern naming a directory also covers its content
			if strings.HasPrefix(rel, r.pattern+"/") {
				return true
			}
		case 2:
			for _, p := range parts {
				if ok, _ := path.Match(r.pattern, p); ok {
					return true
				}
			}
		case 3:
			if rel == r.pattern || strings.HasPrefix(rel, r.pattern+"/") {
				return true
			}
		}
	}
	return false
}
