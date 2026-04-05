// cli/dev_watch.go
// Polling-based file watcher for Go source files. No external dependencies.
// Skips hidden directories, vendor/, .git/, and _test.go files.
package cli

import (
	"os"
	"path/filepath"
	"strings"
	"time"
)

// fileWatcher polls a directory tree for .go file changes.
// Compares modification times between snapshots to detect edits.
type fileWatcher struct {
	dir      string
	delay    time.Duration
	modTimes map[string]time.Time
}

// newFileWatcher creates a watcher for the given directory.
// Panics if dir is empty or delay is less than 100ms.
func newFileWatcher(dir string, delay time.Duration) *fileWatcher {
	if dir == "" {
		panic("newFileWatcher: dir must not be empty")
	}
	if delay < 100*time.Millisecond {
		panic(
			"newFileWatcher: delay must be >= 100ms",
		)
	}
	return &fileWatcher{
		dir:      dir,
		delay:    delay,
		modTimes: make(map[string]time.Time),
	}
}

// snapshot records the current modification times of all .go files.
// Must be called before the first poll.
func (w *fileWatcher) snapshot() error {
	if w.dir == "" {
		panic("snapshot: dir must not be empty")
	}
	if w.modTimes == nil {
		panic("snapshot: modTimes must not be nil")
	}
	files, err := w.listGoFiles()
	if err != nil {
		return err
	}
	w.modTimes = files
	return nil
}

// poll compares the current file state against the last snapshot.
// Returns true if any .go file was added, removed, or modified.
// Updates the internal snapshot on change detection.
func (w *fileWatcher) poll() (bool, error) {
	if w.modTimes == nil {
		panic("poll: modTimes must not be nil")
	}
	if w.dir == "" {
		panic("poll: dir must not be empty")
	}
	current, err := w.listGoFiles()
	if err != nil {
		return false, err
	}
	changed := detectChanges(w.modTimes, current)
	if changed {
		w.modTimes = current
	}
	return changed, nil
}

// fileCount returns the number of tracked .go files.
func (w *fileWatcher) fileCount() int {
	return len(w.modTimes)
}

// maxGoFiles is the upper bound on tracked files to prevent
// unbounded memory growth in very large repositories.
const maxGoFiles = 10_000

// listGoFiles walks dir recursively and returns .go files
// with their modification times. Skips hidden dirs, vendor/,
// .git/, and _test.go files. Panics if file count exceeds
// maxGoFiles.
func (w *fileWatcher) listGoFiles() (
	map[string]time.Time, error,
) {
	if w.dir == "" {
		panic("listGoFiles: dir must not be empty")
	}
	files := make(map[string]time.Time)
	err := filepath.Walk(
		w.dir,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil // skip unreadable entries
			}
			if info.IsDir() {
				return skipDir(w.dir, path, info.Name())
			}
			if !isWatchableGoFile(info.Name()) {
				return nil
			}
			if len(files) >= maxGoFiles {
				panic("listGoFiles: exceeded 10000 file limit")
			}
			files[path] = info.ModTime()
			return nil
		},
	)
	return files, err
}

// skipDir returns filepath.SkipDir for directories that should
// not be watched: hidden dirs, vendor, .git.
func skipDir(
	root, path, name string,
) error {
	if path == root {
		return nil
	}
	if name == ".git" || name == "vendor" {
		return filepath.SkipDir
	}
	if strings.HasPrefix(name, ".") {
		return filepath.SkipDir
	}
	return nil
}

// isWatchableGoFile returns true for .go files that are not
// test files.
func isWatchableGoFile(name string) bool {
	if !strings.HasSuffix(name, ".go") {
		return false
	}
	return !strings.HasSuffix(name, "_test.go")
}

// detectChanges returns true if current differs from previous
// (added, removed, or modified files).
func detectChanges(
	previous, current map[string]time.Time,
) bool {
	if len(previous) != len(current) {
		return true
	}
	for path, modTime := range current {
		prevTime, exists := previous[path]
		if !exists || !modTime.Equal(prevTime) {
			return true
		}
	}
	return false
}
