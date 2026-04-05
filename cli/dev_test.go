// cli/dev_test.go
// Tests for the polling file watcher used by `dagnats dev`.
// Methodology: create temp directories with .go files, exercise
// snapshot + poll, and verify change detection logic.
package cli

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileWatcher_DetectsChange(t *testing.T) {
	dir := t.TempDir()

	// Create a .go file so the watcher has something to track.
	goFile := filepath.Join(dir, "main.go")
	if err := os.WriteFile(
		goFile, []byte("package main"), 0644,
	); err != nil {
		t.Fatalf("write file: %v", err)
	}

	w := newFileWatcher(dir, 100*time.Millisecond)
	if err := w.snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Positive: watcher tracks at least one file.
	if w.fileCount() < 1 {
		t.Fatalf(
			"expected >= 1 tracked file, got %d",
			w.fileCount(),
		)
	}

	// Before modification, poll should report no changes.
	changed, err := w.poll()
	if err != nil {
		t.Fatalf("poll before change: %v", err)
	}
	if changed {
		t.Fatal("expected no change before modification")
	}

	// Advance mod time so the watcher sees a difference.
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(goFile, future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	// Positive: poll detects the modification.
	changed, err = w.poll()
	if err != nil {
		t.Fatalf("poll after change: %v", err)
	}
	if !changed {
		t.Fatal("expected change after modification")
	}
}

func TestFileWatcher_IgnoresTestFiles(t *testing.T) {
	dir := t.TempDir()

	// Create only a _test.go file — watcher should ignore it.
	testFile := filepath.Join(dir, "main_test.go")
	if err := os.WriteFile(
		testFile, []byte("package main"), 0644,
	); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	w := newFileWatcher(dir, 100*time.Millisecond)
	if err := w.snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Negative: test files should not be tracked.
	if w.fileCount() != 0 {
		t.Fatalf(
			"expected 0 tracked files, got %d",
			w.fileCount(),
		)
	}

	// Modify the test file and verify no change detected.
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(
		testFile, future, future,
	); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	changed, err := w.poll()
	if err != nil {
		t.Fatalf("poll: %v", err)
	}

	// Negative: modifying _test.go should not trigger change.
	if changed {
		t.Fatal("expected no change for _test.go modification")
	}
}
