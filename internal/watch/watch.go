// Package watch emits root-relative paths of filesystem changes.
//
// It uses github.com/rjeczalik/notify: on macOS it binds FSEvents (one
// descriptor for the whole tree, no per-file watches; this needs cgo, so the
// darwin binary is built with CGO_ENABLED=1), on Linux inotify with recursive
// watch management. fsnotify on macOS would use kqueue, which costs one file
// descriptor per watched file and does not scale to a vault of ~20k files.
package watch

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/rjeczalik/notify"
)

// Start watches root recursively and sends root-relative paths (forward
// slashes) to out until ctx is done. The root itself is reported as "".
func Start(ctx context.Context, root string, out chan<- string) error {
	// FSEvents reports real paths (/private/var/... for /var/...), so compare
	// against the resolved root as well.
	real, err := filepath.EvalSymlinks(root)
	if err != nil {
		real = root
	}
	c := make(chan notify.EventInfo, 4096)
	if err := notify.Watch(filepath.Join(root, "..."), c, notify.All); err != nil {
		return fmt.Errorf("watch %s: %w", root, err)
	}
	go func() {
		defer notify.Stop(c)
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-c:
				rel, err := filepath.Rel(real, ev.Path())
				if err != nil || strings.HasPrefix(rel, "..") {
					rel, err = filepath.Rel(root, ev.Path())
					if err != nil || strings.HasPrefix(rel, "..") {
						continue
					}
				}
				if rel == "." {
					rel = ""
				}
				select {
				case out <- filepath.ToSlash(rel):
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return nil
}
