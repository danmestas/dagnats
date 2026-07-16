// natsutil/testserver_test.go
// Tests for StartTestServer's store-directory teardown. The embedded
// JetStream server's filestore and consumer-state writers can flush to
// StoreDir asynchronously — some writes land after Server.Shutdown() and
// even after WaitForShutdown() returns. When StoreDir was a t.TempDir(),
// testing's own RemoveAll cleanup raced those late writes and failed
// unrelated tests with "TempDir RemoveAll cleanup: unlinkat ... directory
// not empty". removeDirWithRetry owns the removal so a late write is
// absorbed by a bounded retry instead of failing the test.
// Methodology: exercise removeDirWithRetry directly against a populated
// directory (positive) and against a directory whose write bit is cleared
// so removal cannot succeed (negative), asserting it neither hangs nor
// panics and reports the terminal error.
package natsutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRemoveDirWithRetryRemovesPopulatedDir(t *testing.T) {
	dir, err := os.MkdirTemp("", "dagnats-rmretry-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	// Nested content so removal must recurse, mirroring a JetStream
	// StoreDir with stream/consumer subdirectories and message blocks.
	nested := filepath.Join(dir, "jetstream", "STREAM", "1")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(
		filepath.Join(nested, "1.blk"), []byte("x"), 0o644,
	); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if err := removeDirWithRetry(dir); err != nil {
		t.Fatalf("removeDirWithRetry returned error: %v", err)
	}
	// Positive: the directory is gone.
	if _, statErr := os.Stat(dir); !os.IsNotExist(statErr) {
		t.Fatalf("dir still present after removeDirWithRetry: stat=%v", statErr)
	}
}

func TestRemoveDirWithRetryReportsTerminalFailure(t *testing.T) {
	parent, err := os.MkdirTemp("", "dagnats-rmretry-fail-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() {
		os.Chmod(parent, 0o755)
		os.RemoveAll(parent)
	})
	target := filepath.Join(parent, "child")
	if err := os.MkdirAll(filepath.Join(target, "sub"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	// Clearing the parent's write bit makes the child undeletable, so
	// every RemoveAll attempt fails — a deterministic stand-in for a
	// removal that never succeeds. removeDirWithRetry must return the
	// terminal error after its bounded retries rather than loop forever.
	if err := os.Chmod(parent, 0o500); err != nil {
		t.Fatalf("Chmod: %v", err)
	}

	err = removeDirWithRetry(target)
	// Negative: a removal that cannot succeed surfaces an error.
	if err == nil {
		t.Fatal("removeDirWithRetry returned nil for an undeletable dir")
	}
	// Positive: it did not hang — reaching this assertion proves it
	// returned within the bounded retry budget.
	if _, statErr := os.Stat(target); os.IsNotExist(statErr) {
		t.Fatal("target unexpectedly removed; test cannot prove retry bound")
	}
}
