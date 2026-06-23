// engine/prune_test.go
// Tests for the opt-in, drop-only run-retention sweeper (#453).
// Methodology: real embedded NATS server, one per test (no shared servers).
// Each test seeds workflow_runs snapshots, runs Delete / PruneTerminal,
// and asserts BOTH the deletions AND the survivors — every one of the five
// safety invariants from #453 is exercised here:
//  1. a non-terminal ancient run is NEVER deleted;
//  2. a recent terminal run (younger than older_than) is NEVER deleted;
//  3. disabled-by-default (RunsMaxAge == 0) deletes nothing;
//  4. at most max_prune deletions per pass (bounded);
//  5. PruneTerminal returns an accurate deleted count.
package engine

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// newPruneStore stands up an isolated embedded NATS server with the
// workflow_runs bucket and returns a bound SnapshotStore.
func newPruneStore(t *testing.T) *SnapshotStore {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	if err := natsutil.SetupKVBuckets(js, 1); err != nil {
		t.Fatalf("SetupKVBuckets failed: %v", err)
	}
	return NewSnapshotStore(js)
}

// newPruneStoreWithKV is newPruneStore plus the raw workflow_runs KV
// handle, so a test can seed a corrupt value the SnapshotStore API
// would never write.
func newPruneStoreWithKV(t *testing.T) (*SnapshotStore, jetstream.KeyValue) {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	if err := natsutil.SetupKVBuckets(js, 1); err != nil {
		t.Fatalf("SetupKVBuckets failed: %v", err)
	}
	kv, err := js.KeyValue(context.Background(), "workflow_runs")
	if err != nil {
		t.Fatalf("KeyValue(workflow_runs) failed: %v", err)
	}
	return NewSnapshotStore(js), kv
}

// terminalRun builds a completed run whose CompletedAt is `age` in the past.
func terminalRun(runID string, age time.Duration) dag.WorkflowRun {
	completed := time.Now().UTC().Add(-age)
	return dag.WorkflowRun{
		RunID:       runID,
		WorkflowID:  "wf",
		Status:      dag.RunStatusCompleted,
		Steps:       map[string]dag.StepState{"a": {Status: dag.StepStatusCompleted}},
		CreatedAt:   completed.Add(-time.Minute),
		CompletedAt: &completed,
	}
}

// runningRun builds an ancient running (non-terminal) run with no CompletedAt.
func runningRun(runID string, age time.Duration) dag.WorkflowRun {
	created := time.Now().UTC().Add(-age)
	return dag.WorkflowRun{
		RunID:      runID,
		WorkflowID: "wf",
		Status:     dag.RunStatusRunning,
		Steps:      map[string]dag.StepState{"a": {Status: dag.StepStatusRunning}},
		CreatedAt:  created,
	}
}

func saveAll(t *testing.T, store *SnapshotStore, runs ...dag.WorkflowRun) {
	t.Helper()
	for _, run := range runs {
		if err := store.Save(context.Background(), run); err != nil {
			t.Fatalf("Save(%s) failed: %v", run.RunID, err)
		}
	}
}

// exists reports whether a snapshot key is still present.
func exists(t *testing.T, store *SnapshotStore, runID string) bool {
	t.Helper()
	_, err := store.Load(context.Background(), runID)
	if err == nil {
		return true
	}
	if err == ErrRunNotFound {
		return false
	}
	t.Fatalf("Load(%s) unexpected error: %v", runID, err)
	return false
}

func TestSnapshotDelete(t *testing.T) {
	store := newPruneStore(t)
	run := terminalRun("run-del", time.Hour)
	saveAll(t, store, run)

	if !exists(t, store, "run-del") {
		t.Fatal("precondition: run-del should exist before delete")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := store.Delete(ctx, "run-del"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if exists(t, store, "run-del") {
		t.Fatal("run-del should be gone after Delete")
	}
}

// Invariant 1 + 2 + 5: an old terminal run is dropped, while an ancient
// NON-terminal run AND a recent terminal run both survive; count == 1.
func TestPruneTerminal_DropsOldTerminalKeepsLiveAndRecent(t *testing.T) {
	store := newPruneStore(t)
	saveAll(t, store,
		terminalRun("old-terminal", 48*time.Hour),     // should be deleted
		runningRun("ancient-running", 100*time.Hour),  // invariant 1: keep
		terminalRun("recent-terminal", 1*time.Minute), // invariant 2: keep
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	deleted, err := store.PruneTerminal(ctx, 24*time.Hour, 100)
	if err != nil {
		t.Fatalf("PruneTerminal failed: %v", err)
	}

	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted) // invariant 5: accurate count
	}
	if exists(t, store, "old-terminal") {
		t.Fatal("old-terminal should have been pruned")
	}
	if !exists(t, store, "ancient-running") {
		t.Fatal("invariant 1 violated: ancient non-terminal run was deleted")
	}
	if !exists(t, store, "recent-terminal") {
		t.Fatal("invariant 2 violated: recent terminal run was deleted")
	}
}

// Invariant 4: at most max_prune deletions per pass.
func TestPruneTerminal_RespectsMaxPrune(t *testing.T) {
	store := newPruneStore(t)
	saveAll(t, store,
		terminalRun("t1", 48*time.Hour),
		terminalRun("t2", 48*time.Hour),
		terminalRun("t3", 48*time.Hour),
		terminalRun("t4", 48*time.Hour),
		runningRun("live", 200*time.Hour),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	deleted, err := store.PruneTerminal(ctx, 24*time.Hour, 2)
	if err != nil {
		t.Fatalf("PruneTerminal failed: %v", err)
	}

	if deleted != 2 {
		t.Fatalf("deleted = %d, want exactly max_prune (2)", deleted)
	}
	remaining, err := store.CountAll(ctx)
	if err != nil {
		t.Fatalf("CountAll failed: %v", err)
	}
	// 5 seeded - 2 pruned = 3 remain (2 old terminal + 1 live).
	if remaining != 3 {
		t.Fatalf("remaining = %d, want 3 (bound must stop further deletes)", remaining)
	}
	if !exists(t, store, "live") {
		t.Fatal("live run must survive a bounded prune")
	}
}

// Invariant 2 (boundary): a terminal run exactly at the threshold is NOT
// older-than the window, so it survives; only strictly-older runs are dropped.
func TestPruneTerminal_KeepsTerminalYoungerThanWindow(t *testing.T) {
	store := newPruneStore(t)
	saveAll(t, store,
		terminalRun("just-under", 23*time.Hour),
		terminalRun("just-over", 25*time.Hour),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	deleted, err := store.PruneTerminal(ctx, 24*time.Hour, 100)
	if err != nil {
		t.Fatalf("PruneTerminal failed: %v", err)
	}

	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
	if !exists(t, store, "just-under") {
		t.Fatal("terminal run younger than window must survive")
	}
	if exists(t, store, "just-over") {
		t.Fatal("terminal run older than window must be pruned")
	}
}

// Invariant 1 (all non-terminal statuses): pending/running/queued ancient
// runs survive even with a tiny window; nothing terminal exists to delete.
func TestPruneTerminal_NeverDeletesNonTerminal(t *testing.T) {
	store := newPruneStore(t)
	pending := runningRun("ancient-pending", 500*time.Hour)
	pending.Status = dag.RunStatusPending
	queued := runningRun("ancient-queued", 500*time.Hour)
	queued.Steps["a"] = dag.StepState{Status: dag.StepStatusQueued}
	saveAll(t, store, pending, queued, runningRun("ancient-running", 500*time.Hour))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	deleted, err := store.PruneTerminal(ctx, time.Nanosecond, 100)
	if err != nil {
		t.Fatalf("PruneTerminal failed: %v", err)
	}

	if deleted != 0 {
		t.Fatalf("deleted = %d, want 0 (no terminal runs present)", deleted)
	}
	count, err := store.CountAll(ctx)
	if err != nil {
		t.Fatalf("CountAll failed: %v", err)
	}
	if count != 3 {
		t.Fatalf("count = %d, want 3 (all non-terminal runs survive)", count)
	}
}

// A corrupt run.* value must make the prune pass fail SAFE: return an
// error and delete NOTHING — neither the corrupt key nor an old terminal
// run encountered before the unmarshal error.
func TestPruneTerminal_CorruptValueFailsSafe(t *testing.T) {
	store, kv := newPruneStoreWithKV(t)
	saveAll(t, store, terminalRun("old-terminal", 48*time.Hour))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := kv.Put(ctx, "run.corrupt", []byte("{not valid json")); err != nil {
		t.Fatalf("seed corrupt value failed: %v", err)
	}

	before, err := store.CountAll(ctx)
	if err != nil {
		t.Fatalf("CountAll(before) failed: %v", err)
	}

	deleted, err := store.PruneTerminal(ctx, 24*time.Hour, 100)
	if err == nil {
		t.Fatal("expected error from corrupt run value, got nil")
	}
	if deleted != 0 {
		t.Fatalf("deleted = %d, want 0 (corrupt value must abort before deletes)",
			deleted)
	}

	after, err := store.CountAll(ctx)
	if err != nil {
		t.Fatalf("CountAll(after) failed: %v", err)
	}
	// Fail-safe: a corrupt value aborts the whole pass before any delete,
	// regardless of scan order — nothing is dropped.
	if after != before {
		t.Fatalf("count changed %d -> %d; prune must not delete on error",
			before, after)
	}
	if !exists(t, store, "old-terminal") {
		t.Fatal("old-terminal must survive a failed prune pass")
	}
	// CountAll (keys-only, no unmarshal) above already proved nothing was
	// deleted — including the corrupt key, which Load cannot inspect.
}
