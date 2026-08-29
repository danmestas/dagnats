// engine/snapshot_test.go
// Tests for KV snapshot operations: store and retrieve WorkflowRun state.
// Methodology: uses real embedded NATS server. Each test gets its own server.
// Tests write snapshots, read them back, verify field fidelity, and check missing key behavior.
package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestSnapshotWriteAndRead(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	err = natsutil.SetupKVBuckets(js, 1)
	if err != nil {
		t.Fatalf("SetupKVBuckets failed: %v", err)
	}
	store := NewSnapshotStore(js)
	run := dag.WorkflowRun{
		RunID: "run-123", WorkflowID: "test-wf", Status: dag.RunStatusRunning,
		Steps: map[string]dag.StepState{
			"step-a": {Status: dag.StepStatusCompleted, Attempts: 1, Output: []byte(`"ok"`)},
			"step-b": {Status: dag.StepStatusPending},
		},
		CreatedAt: time.Now().UTC().Truncate(time.Millisecond),
	}
	err = store.Save(context.Background(), run)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	got, err := store.Load(context.Background(), run.RunID)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if got.RunID != run.RunID {
		t.Fatalf("RunID = %q, want %q", got.RunID, run.RunID)
	}
	if got.Status != dag.RunStatusRunning {
		t.Fatalf("Status = %v, want Running", got.Status)
	}
	if len(got.Steps) != 2 {
		t.Fatalf("Steps count = %d, want 2", len(got.Steps))
	}
	if got.Steps["step-a"].Status != dag.StepStatusCompleted {
		t.Fatalf("step-a Status = %v, want Completed", got.Steps["step-a"].Status)
	}
}

func TestSnapshotLoadNotFound(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	err = natsutil.SetupKVBuckets(js, 1)
	if err != nil {
		t.Fatalf("SetupKVBuckets failed: %v", err)
	}
	store := NewSnapshotStore(js)
	_, err = store.Load(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent run, got nil")
	}
	if err != ErrRunNotFound {
		t.Fatalf("expected ErrRunNotFound, got: %v", err)
	}
}

func TestSnapshotUpdate(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	err = natsutil.SetupKVBuckets(js, 1)
	if err != nil {
		t.Fatalf("SetupKVBuckets failed: %v", err)
	}
	store := NewSnapshotStore(js)
	run := dag.WorkflowRun{
		RunID: "run-456", WorkflowID: "test-wf", Status: dag.RunStatusRunning,
		Steps:     map[string]dag.StepState{"a": {Status: dag.StepStatusPending}},
		CreatedAt: time.Now().UTC().Truncate(time.Millisecond),
	}
	err = store.Save(context.Background(), run)
	if err != nil {
		t.Fatalf("first Save failed: %v", err)
	}
	run.Steps["a"] = dag.StepState{Status: dag.StepStatusCompleted, Attempts: 1}
	run.Status = dag.RunStatusCompleted
	err = store.Save(context.Background(), run)
	if err != nil {
		t.Fatalf("second Save failed: %v", err)
	}
	got, err := store.Load(context.Background(), "run-456")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if got.Status != dag.RunStatusCompleted {
		t.Fatalf("Status = %v, want Completed", got.Status)
	}
	if got.Steps["a"].Status != dag.StepStatusCompleted {
		t.Fatalf("step-a Status = %v, want Completed", got.Steps["a"].Status)
	}
}

func TestSnapshotListActiveEmpty(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	err = natsutil.SetupKVBuckets(js, 1)
	if err != nil {
		t.Fatalf("SetupKVBuckets failed: %v", err)
	}
	store := NewSnapshotStore(js)

	// Positive: empty bucket returns empty slice, no error.
	runs, stats, err := store.ListActive(context.Background(), 100)
	if err != nil {
		t.Fatalf("ListActive on empty bucket failed: %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("expected 0 runs, got %d", len(runs))
	}
	// Negative: an empty scan is never reported truncated.
	if stats.Truncated {
		t.Fatalf("stats.Truncated = true on empty bucket, want false")
	}
}

func TestSnapshotListActiveOnlyNonTerminal(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	err = natsutil.SetupKVBuckets(js, 1)
	if err != nil {
		t.Fatalf("SetupKVBuckets failed: %v", err)
	}
	store := NewSnapshotStore(js)

	run1 := dag.WorkflowRun{
		RunID:      "run-001",
		WorkflowID: "wf-a",
		Status:     dag.RunStatusRunning,
		Steps:      map[string]dag.StepState{"a": {Status: dag.StepStatusPending}},
		CreatedAt:  time.Now().UTC().Truncate(time.Millisecond),
	}
	run2 := dag.WorkflowRun{
		RunID:      "run-002",
		WorkflowID: "wf-b",
		Status:     dag.RunStatusCompleted,
		Steps:      map[string]dag.StepState{"b": {Status: dag.StepStatusCompleted}},
		CreatedAt:  time.Now().UTC().Add(1 * time.Second).Truncate(time.Millisecond),
	}
	if err := store.SaveInitial(context.Background(), run1); err != nil {
		t.Fatalf("SaveInitial run1 failed: %v", err)
	}
	if err := store.SaveInitial(context.Background(), run2); err != nil {
		t.Fatalf("SaveInitial run2 failed: %v", err)
	}
	runs, _, err := store.ListActive(context.Background(), 100)
	if err != nil {
		t.Fatalf("ListActive failed: %v", err)
	}
	// Positive: the non-terminal run is returned.
	if len(runs) != 1 || runs[0].RunID != "run-001" {
		t.Fatalf("ListActive = %v, want exactly [run-001]", runIDs(runs))
	}
	// Negative: the terminal run must never appear.
	for _, r := range runs {
		if r.RunID == "run-002" {
			t.Fatal("ListActive returned a terminal run")
		}
	}
}

// newListStore spins a fresh embedded NATS server and returns a bound
// SnapshotStore. Each call gets its own server so tests never share KV
// state. Methodology: real embedded NATS, no mocks.
func newListStore(t *testing.T) *SnapshotStore {
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

// seedNumberedRuns writes `total` runs run-00..run-NN where run-NN is
// the newest (CreatedAt strictly increasing by index).
func seedNumberedRuns(
	t *testing.T, store *SnapshotStore, total int,
) {
	t.Helper()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < total; i++ {
		run := dag.WorkflowRun{
			RunID:      fmt.Sprintf("run-%02d", i),
			WorkflowID: "wf",
			Status:     dag.RunStatusCompleted,
			Steps:      map[string]dag.StepState{},
			CreatedAt:  base.Add(time.Duration(i) * time.Hour),
		}
		// SaveInitial, not Save (#664 review round 2): this is each
		// run's genuine first (and only) write, so it is what must
		// create the runidx entry every caller of this helper relies
		// on for ScanNewestFirst/ListRecent visibility.
		if err := store.SaveInitial(context.Background(), run); err != nil {
			t.Fatalf("SaveInitial %d: %v", i, err)
		}
	}
}

// TestListRecentReturnsGlobalLatestN proves the #452 ordering fix: when
// limit < population, ListRecent returns the genuinely most-recent
// limit runs (global DESC sort applied BEFORE truncation), not an
// arbitrary subset of the unordered key scan.
func TestListRecentReturnsGlobalLatestN(t *testing.T) {
	store := newListStore(t)
	seedNumberedRuns(t, store, 10)

	got, err := store.ListRecent(context.Background(), 3)
	if err != nil {
		t.Fatalf("ListRecent: %v", err)
	}
	// Positive: the latest 3 (run-09, run-08, run-07), newest first.
	if len(got) != 3 {
		t.Fatalf("len(got) = %d, want 3", len(got))
	}
	wantIDs := []string{"run-09", "run-08", "run-07"}
	for i, want := range wantIDs {
		if got[i].RunID != want {
			t.Fatalf("got[%d].RunID = %q, want %q (full=%v)",
				i, got[i].RunID, want, runIDs(got))
		}
	}
	// Negative: no older run may sneak into the truncated window.
	for _, r := range got {
		if r.RunID < "run-07" {
			t.Fatalf("older run %q leaked past cap: %v",
				r.RunID, runIDs(got))
		}
	}
}

// seedNumberedActiveRuns writes `total` NON-terminal (Running) runs
// run-00..run-NN, so ListActive can see them (seedNumberedRuns' runs
// are Completed/terminal and invisible to ListActive by design).
func seedNumberedActiveRuns(
	t *testing.T, store *SnapshotStore, total int,
) {
	t.Helper()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < total; i++ {
		run := dag.WorkflowRun{
			RunID:      fmt.Sprintf("run-%02d", i),
			WorkflowID: "wf",
			Status:     dag.RunStatusRunning,
			Steps:      map[string]dag.StepState{"a": {Status: dag.StepStatusPending}},
			CreatedAt:  base.Add(time.Duration(i) * time.Hour),
		}
		if err := store.SaveInitial(context.Background(), run); err != nil {
			t.Fatalf("SaveInitial %d: %v", i, err)
		}
	}
}

// TestListActiveHonorsFetchMaxAndReportsTruncated proves ListActive
// caps its result at fetchMax and honestly reports Truncated when the
// active population exceeds it, while returning every active run
// (unordered — order is not part of ListActive's contract) when
// fetchMax comfortably covers the population.
func TestListActiveHonorsFetchMaxAndReportsTruncated(t *testing.T) {
	store := newListStore(t)
	const total = 10
	seedNumberedActiveRuns(t, store, total)

	// Positive: the cap is honoured and Truncated reported.
	capped, stats, err := store.ListActive(context.Background(), 4)
	if err != nil {
		t.Fatalf("ListActive(4): %v", err)
	}
	if len(capped) != 4 {
		t.Fatalf("len(capped) = %d, want 4 (cap honoured)", len(capped))
	}
	if !stats.Truncated {
		t.Fatalf("stats.Truncated = false, want true (10 active > fetchMax 4)")
	}

	// Negative: a fetchMax covering the whole population is NOT
	// truncated and returns every active run.
	got, stats, err := store.ListActive(context.Background(), total)
	if err != nil {
		t.Fatalf("ListActive(all): %v", err)
	}
	if len(got) != total {
		t.Fatalf("len(got) = %d, want %d", len(got), total)
	}
	if stats.Truncated {
		t.Fatalf("stats.Truncated = true, want false (fetchMax covers population)")
	}
}

// runIDs is a tiny helper to render a run slice's IDs in failures.
func runIDs(runs []dag.WorkflowRun) []string {
	if len(runs) > 10000 {
		panic("runIDs: runs exceeds bound")
	}
	out := make([]string, 0, len(runs))
	for _, r := range runs {
		out = append(out, r.RunID)
	}
	return out
}

// TestCountAllCountsRunKeys proves CountAll returns the full run.*
// population without materializing values, and ignores non-run keys.
func TestCountAllCountsRunKeys(t *testing.T) {
	store := newListStore(t)
	const total = 7
	for i := 0; i < total; i++ {
		run := dag.WorkflowRun{
			RunID:      fmt.Sprintf("count-%02d", i),
			WorkflowID: "wf",
			Status:     dag.RunStatusCompleted,
			Steps:      map[string]dag.StepState{},
			CreatedAt:  time.Now().UTC(),
		}
		if err := store.Save(context.Background(), run); err != nil {
			t.Fatalf("Save %d: %v", i, err)
		}
	}

	count, err := store.CountAll(context.Background())
	if err != nil {
		t.Fatalf("CountAll: %v", err)
	}
	// Positive: counts every run we saved.
	if count != total {
		t.Fatalf("CountAll = %d, want %d", count, total)
	}
	// Negative: an empty store reports zero, not an error.
	emptyStore := newListStore(t)
	zero, err := emptyStore.CountAll(context.Background())
	if err != nil {
		t.Fatalf("CountAll(empty): %v", err)
	}
	if zero != 0 {
		t.Fatalf("CountAll(empty) = %d, want 0", zero)
	}
}
