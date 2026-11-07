// engine/prune_size_test.go
// Tests for size-based run pruning: the workflow_runs bucket gets no
// MaxBytes (a full KV rejects writes), so the run pruner keeps it under its
// share of the store budget by deleting the OLDEST terminal runs, by
// completion time, until the bucket is under a low-water mark.
// Methodology: real embedded NATS server, one per test. Each test seeds
// terminal runs with a fixed-size payload through SaveInitial (so the
// runidx/runactive indexes exist exactly as production writes them), sets
// the share from the bucket's measured size, runs one pass, and asserts the
// deletions, the survivors, the bucket size, and index consistency.
package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/nats-io/nats.go/jetstream"
)

// sizedTerminalRun is terminalRun with a payload large enough that the
// run dominates its key's bytes.
func sizedTerminalRun(runID string, age time.Duration) dag.WorkflowRun {
	run := terminalRun(runID, age)
	run.Steps = map[string]dag.StepState{"a": {
		Status: dag.StepStatusCompleted, Output: make([]byte, 4096),
	}}
	return run
}

// seedSizedRuns saves count terminal runs, run-00 the oldest, through the
// creation path so their indexes exist.
func seedSizedRuns(t *testing.T, store *SnapshotStore, count int) []string {
	t.Helper()
	ids := make([]string, 0, count)
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("run-%02d", i)
		age := time.Duration(count-i) * time.Hour
		if err := store.SaveInitial(context.Background(),
			sizedTerminalRun(id, age)); err != nil {
			t.Fatalf("SaveInitial(%s): %v", id, err)
		}
		ids = append(ids, id)
	}
	return ids
}

// bucketBytes reads the workflow_runs bucket's stored bytes.
func bucketBytes(t *testing.T, kv jetstream.KeyValue) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	status, err := kv.Status(ctx)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	return int64(status.Bytes())
}

// keyPresent reports whether key exists in kv.
func keyPresent(t *testing.T, kv jetstream.KeyValue, key string) bool {
	t.Helper()
	_, err := kv.Get(context.Background(), key)
	if err == nil {
		return true
	}
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return false
	}
	t.Fatalf("Get(%s): %v", key, err)
	return false
}

func TestPruneTerminalBySize_DropsOldestUntilUnderLowWater(t *testing.T) {
	store, kv := newPruneStoreWithKV(t)
	ids := seedSizedRuns(t, store, 10)
	live := runningRun("live-running", 1000*time.Hour)
	if err := store.SaveInitial(context.Background(), live); err != nil {
		t.Fatalf("SaveInitial(live): %v", err)
	}
	share := bucketBytes(t, kv) / 2

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	deleted, err := store.PruneTerminalBySize(ctx, share, 100)
	if err != nil {
		t.Fatalf("PruneTerminalBySize: %v", err)
	}
	if deleted < 5 || deleted >= len(ids) {
		t.Fatalf("deleted = %d, want at least half of %d but not all", deleted, len(ids))
	}
	if got := bucketBytes(t, kv); got > share {
		t.Fatalf("bucket bytes after pass = %d, want <= share %d", got, share)
	}
	for i, id := range ids {
		gone := i < deleted
		if exists(t, store, id) == gone {
			t.Errorf("%s present=%v, want present=%v (oldest go first)", id, !gone, !gone)
		}
		if keyPresent(t, kv, runIndexPrefix+id) == gone {
			t.Errorf("runidx for %s present=%v, want %v", id, !gone, !gone)
		}
	}
	if !exists(t, store, live.RunID) {
		t.Fatal("a non-terminal run was deleted")
	}
	if !keyPresent(t, kv, runActivePrefix+live.RunID) {
		t.Fatal("the live run's runactive entry was deleted")
	}
}

func TestPruneTerminalBySize_UnderShareDeletesNothing(t *testing.T) {
	store, kv := newPruneStoreWithKV(t)
	ids := seedSizedRuns(t, store, 4)
	share := bucketBytes(t, kv) + 1

	deleted, err := store.PruneTerminalBySize(context.Background(), share, 100)
	if err != nil {
		t.Fatalf("PruneTerminalBySize: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("deleted = %d under the share, want 0", deleted)
	}
	for _, id := range ids {
		if !exists(t, store, id) {
			t.Fatalf("%s deleted while the bucket fits its share", id)
		}
	}
}

func TestPruneTerminalBySize_BoundedPerPass(t *testing.T) {
	store, kv := newPruneStoreWithKV(t)
	ids := seedSizedRuns(t, store, 6)
	share := bucketBytes(t, kv) / 10

	deleted, err := store.PruneTerminalBySize(context.Background(), share, 2)
	if err != nil {
		t.Fatalf("PruneTerminalBySize: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want the per-pass bound 2", deleted)
	}
	if exists(t, store, ids[0]) || exists(t, store, ids[1]) {
		t.Fatal("the bounded pass must take the two oldest runs")
	}
	if !exists(t, store, ids[2]) {
		t.Fatal("the bounded pass deleted past its bound")
	}
}
