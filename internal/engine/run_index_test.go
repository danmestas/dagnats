// engine/run_index_test.go
// Tests for the #659 creation-ordered run index: the runidx.<runID>
// marker Save/CreateSnapshot write, ScanNewestFirst, PruneTerminal's
// index cleanup, and RepairRunIndex's backfill/orphan-removal sweep.
// Methodology: real embedded NATS server, one per test (no mocks, no
// shared state between tests).
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
)

// TestSaveWritesRunIndexEntry proves Save (the initial-snapshot write
// path for most trigger types) also Create-writes the runidx.<runID>
// marker, so the index is populated without any caller having to know
// it exists.
func TestSaveWritesRunIndexEntry(t *testing.T) {
	store := newListStore(t)
	run := dag.WorkflowRun{
		RunID: "run-idx-1", WorkflowID: "wf",
		Status: dag.RunStatusRunning, Steps: map[string]dag.StepState{},
		CreatedAt: time.Now().UTC(),
	}
	if err := store.Save(context.Background(), run); err != nil {
		t.Fatalf("Save: %v", err)
	}

	keys, err := store.listRunIndexKeys(context.Background())
	if err != nil {
		t.Fatalf("listRunIndexKeys: %v", err)
	}
	// Positive: exactly one index entry, for this run.
	if len(keys) != 1 {
		t.Fatalf("index keys = %v, want 1 entry", keys)
	}
	if keys[0] != runIndexPrefix+"run-idx-1" {
		t.Fatalf("index key = %q, want %q", keys[0], runIndexPrefix+"run-idx-1")
	}
	// Negative: a second Save (an advance) must NOT add a second entry.
	run.Status = dag.RunStatusCompleted
	if err := store.Save(context.Background(), run); err != nil {
		t.Fatalf("Save (advance): %v", err)
	}
	keys, err = store.listRunIndexKeys(context.Background())
	if err != nil {
		t.Fatalf("listRunIndexKeys (after advance): %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("index keys after advance = %v, want still 1", keys)
	}
}

// TestRunIndexOrderSurvivesOutOfOrderUpdates proves the index's
// creation order is NOT disturbed by later Save calls that touch an
// older run last — i.e. the KV revision-order bias the #659 bug
// exploited (Keys() latest-write order) cannot reappear here, because
// runidx.<runID> is written exactly once and never updated.
func TestRunIndexOrderSurvivesOutOfOrderUpdates(t *testing.T) {
	store := newListStore(t)
	seedNumberedRuns(t, store, 3) // run-00 (oldest) .. run-02 (newest)

	// Touch the OLDEST run last -- the exact scenario that biased
	// Keys() toward stale runs pre-#659.
	oldest, err := store.Load(context.Background(), "run-00")
	if err != nil {
		t.Fatalf("Load run-00: %v", err)
	}
	oldest.Status = dag.RunStatusFailed
	if err := store.Save(context.Background(), oldest); err != nil {
		t.Fatalf("Save run-00 (advance): %v", err)
	}

	keys, err := store.listRunIndexKeys(context.Background())
	if err != nil {
		t.Fatalf("listRunIndexKeys: %v", err)
	}
	want := []string{
		runIndexPrefix + "run-00",
		runIndexPrefix + "run-01",
		runIndexPrefix + "run-02",
	}
	if len(keys) != len(want) {
		t.Fatalf("index keys = %v, want %v", keys, want)
	}
	for i, w := range want {
		if keys[i] != w {
			t.Fatalf("index order = %v, want %v (run-00's advance "+
				"must not move it)", keys, want)
		}
	}
}

// TestScanNewestFirstOrderAndLimit proves ScanNewestFirst returns
// matches newest-first and stops once limit matches are collected.
func TestScanNewestFirstOrderAndLimit(t *testing.T) {
	store := newListStore(t)
	// Larger than scanBatchSize so "stopped at limit" is observable:
	// a scan that satisfies limit within the first (newest) batch
	// must not go on to fetch the second, older batch at all.
	const total = scanBatchSize + 20
	seedNumberedRuns(t, store, total) // run-000 oldest .. newest last

	matches, stats, err := store.ScanNewestFirst(
		context.Background(), alwaysMatch, 5, ScanFetchMax,
	)
	if err != nil {
		t.Fatalf("ScanNewestFirst: %v", err)
	}
	// Positive: exactly limit matches, newest-first.
	if len(matches) != 5 {
		t.Fatalf("len(matches) = %d, want 5", len(matches))
	}
	want := []string{
		fmt.Sprintf("run-%02d", total-1),
		fmt.Sprintf("run-%02d", total-2),
		fmt.Sprintf("run-%02d", total-3),
		fmt.Sprintf("run-%02d", total-4),
		fmt.Sprintf("run-%02d", total-5),
	}
	for i, w := range want {
		if matches[i].RunID != w {
			t.Fatalf("matches = %v, want %v (newest-first)",
				runIDs(matches), want)
		}
	}
	// Negative: the scan must not fetch the whole population once the
	// limit is satisfied within the first (newest) batch.
	if stats.Fetched >= total {
		t.Fatalf("stats.Fetched = %d, want < %d (stopped at limit)",
			stats.Fetched, total)
	}
	if stats.Truncated {
		t.Fatal("stats.Truncated must be false: plenty of budget left")
	}
}

// TestScanNewestFirstHonorsFetchMax proves a small fetchMax caps the
// scan and reports Truncated=true when the index still has unscanned
// older entries the caller never got to see.
func TestScanNewestFirstHonorsFetchMax(t *testing.T) {
	store := newListStore(t)
	seedNumberedRuns(t, store, 30)

	// pred never matches -- forces the scan to walk until fetchMax,
	// never satisfying limit via matches.
	neverMatch := func(dag.WorkflowRun) bool { return false }
	matches, stats, err := store.ScanNewestFirst(
		context.Background(), neverMatch, 5, 10,
	)
	if err != nil {
		t.Fatalf("ScanNewestFirst: %v", err)
	}
	// Positive: fetch stopped at the fetchMax cap.
	if stats.Fetched != 10 {
		t.Fatalf("stats.Fetched = %d, want 10 (fetchMax cap)", stats.Fetched)
	}
	if !stats.Truncated {
		t.Fatal("stats.Truncated must be true: 30 runs, fetchMax=10")
	}
	// Negative: no matches (pred always false).
	if len(matches) != 0 {
		t.Fatalf("len(matches) = %d, want 0", len(matches))
	}
}

// TestScanNewestFirstSkipsPrunedRun proves a run.* key deleted after
// its index entry was written (e.g. a race with PruneTerminal, or a
// manual delete) is skipped and counted, not an error.
func TestScanNewestFirstSkipsPrunedRun(t *testing.T) {
	store := newListStore(t)
	seedNumberedRuns(t, store, 5)

	// Delete run-04's snapshot directly, leaving its index entry
	// dangling -- simulates a race, not the normal PruneTerminal path
	// (which removes both together).
	if err := store.Delete(context.Background(), "run-04"); err != nil {
		t.Fatalf("Delete run-04: %v", err)
	}

	matches, stats, err := store.ScanNewestFirst(
		context.Background(), alwaysMatch, 10, ScanFetchMax,
	)
	if err != nil {
		t.Fatalf("ScanNewestFirst: %v", err)
	}
	// Positive: the 4 remaining runs are all returned.
	if len(matches) != 4 {
		t.Fatalf("len(matches) = %d, want 4", len(matches))
	}
	// Negative: run-04 must not appear, and must be counted skipped.
	for _, m := range matches {
		if m.RunID == "run-04" {
			t.Fatal("pruned run-04 must not appear in matches")
		}
	}
	if stats.Skipped != 1 {
		t.Fatalf("stats.Skipped = %d, want 1", stats.Skipped)
	}
}

// TestPruneTerminalRemovesIndexKey proves PruneTerminal deletes the
// runidx.<runID> entry alongside the run.<runID> snapshot it deletes.
func TestPruneTerminalRemovesIndexKey(t *testing.T) {
	store := newListStore(t)
	past := time.Now().Add(-48 * time.Hour)
	run := dag.WorkflowRun{
		RunID: "run-doomed", WorkflowID: "wf",
		Status: dag.RunStatusCompleted, Steps: map[string]dag.StepState{},
		CreatedAt: past, CompletedAt: &past,
	}
	if err := store.Save(context.Background(), run); err != nil {
		t.Fatalf("Save: %v", err)
	}

	deleted, err := store.PruneTerminal(
		context.Background(), 24*time.Hour, 10,
	)
	if err != nil {
		t.Fatalf("PruneTerminal: %v", err)
	}
	// Positive: the run was pruned.
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
	// Negative: its index entry must be gone too.
	keys, err := store.listRunIndexKeys(context.Background())
	if err != nil {
		t.Fatalf("listRunIndexKeys: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("index keys after prune = %v, want empty", keys)
	}
}

// seedRunDirect writes ONLY the run.<runID> snapshot key, bypassing
// Save/CreateSnapshot entirely -- simulating a pre-#659 store (or a
// crash between the snapshot write and writeRunIndexEntry) where the
// run has no index entry.
func seedRunDirect(t *testing.T, store *SnapshotStore, run dag.WorkflowRun) {
	t.Helper()
	if store.kv == nil {
		panic("seedRunDirect: store.kv must not be nil")
	}
	data, err := json.Marshal(run)
	if err != nil {
		t.Fatalf("marshal run %s: %v", run.RunID, err)
	}
	if _, err := store.kv.Put(
		context.Background(), "run."+run.RunID, data,
	); err != nil {
		t.Fatalf("direct put run %s: %v", run.RunID, err)
	}
}

// TestRepairRunIndexBackfillsAndRemovesOrphans proves RepairRunIndex
// (a) backfills index entries for runs written directly (no index),
// in CreatedAt order, bounded by pageMax across multiple calls, and
// (b) removes an index entry with no matching run.
func TestRepairRunIndexBackfillsAndRemovesOrphans(t *testing.T) {
	store := newListStore(t)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	const total = 15
	for i := 0; i < total; i++ {
		seedRunDirect(t, store, dag.WorkflowRun{
			RunID: runIDFor(i), WorkflowID: "wf",
			Status: dag.RunStatusCompleted, Steps: map[string]dag.StepState{},
			CreatedAt: base.Add(time.Duration(i) * time.Hour),
		})
	}
	// An orphan: an index entry with no run.
	if _, err := store.kv.Create(
		context.Background(), runIndexPrefix+"ghost", []byte{},
	); err != nil {
		t.Fatalf("seed orphan index entry: %v", err)
	}

	// First call: bounded by pageMax=10, less than the 15 missing.
	const pageMax = 10
	stats1, err := store.RepairRunIndex(context.Background(), pageMax)
	if err != nil {
		t.Fatalf("RepairRunIndex (call 1): %v", err)
	}
	if stats1.Repaired != pageMax {
		t.Fatalf("call 1 Repaired = %d, want %d", stats1.Repaired, pageMax)
	}
	if stats1.OrphansRemoved != 1 {
		t.Fatalf("call 1 OrphansRemoved = %d, want 1", stats1.OrphansRemoved)
	}

	// Second call: finishes the remaining 5.
	stats2, err := store.RepairRunIndex(context.Background(), pageMax)
	if err != nil {
		t.Fatalf("RepairRunIndex (call 2): %v", err)
	}
	if stats2.Repaired != total-pageMax {
		t.Fatalf("call 2 Repaired = %d, want %d",
			stats2.Repaired, total-pageMax)
	}
	if stats2.OrphansRemoved != 0 {
		t.Fatalf("call 2 OrphansRemoved = %d, want 0 (already removed)",
			stats2.OrphansRemoved)
	}

	// Positive: the FULL index now matches creation (CreatedAt) order
	// across both bounded calls, not just within one.
	keys, err := store.listRunIndexKeys(context.Background())
	if err != nil {
		t.Fatalf("listRunIndexKeys: %v", err)
	}
	if len(keys) != total {
		t.Fatalf("index keys = %d, want %d", len(keys), total)
	}
	for i, key := range keys {
		want := runIndexPrefix + runIDFor(i)
		if key != want {
			t.Fatalf("index order[%d] = %q, want %q (CreatedAt order "+
				"across bounded calls): full = %v", i, key, want, keys)
		}
	}
}

// TestListRecentIsCheapOnLargeStore proves ListRecent(10) still
// returns the genuinely-newest 10 on a 2,000-run store (#452's
// contract, preserved), while costing O(limit) rather than
// O(population) to get there (#659's win): the underlying
// ScanNewestFirst call it's built on fetches at most one scan batch,
// nowhere near the full 2,000-run population.
func TestListRecentIsCheapOnLargeStore(t *testing.T) {
	store := newListStore(t)
	const total = 2000
	seedNumberedRuns(t, store, total)

	got, err := store.ListRecent(context.Background(), 10)
	if err != nil {
		t.Fatalf("ListRecent: %v", err)
	}
	// Positive: exactly the 10 newest, newest-first.
	if len(got) != 10 {
		t.Fatalf("len(got) = %d, want 10", len(got))
	}
	for i := 0; i < 10; i++ {
		want := fmt.Sprintf("run-%02d", total-1-i)
		if got[i].RunID != want {
			t.Fatalf("got[%d].RunID = %q, want %q (full=%v)",
				i, got[i].RunID, want, runIDs(got))
		}
	}

	// Cost check: the same scan, via the primitive ListRecent is built
	// on, must not fetch anywhere near the 2,000-run population.
	_, stats, err := store.ScanNewestFirst(
		context.Background(), alwaysMatch, 10, ScanFetchMax,
	)
	if err != nil {
		t.Fatalf("ScanNewestFirst: %v", err)
	}
	if stats.Fetched > scanBatchSize {
		t.Fatalf("stats.Fetched = %d, want <= %d (one scan batch, "+
			"not O(population)=%d)", stats.Fetched, scanBatchSize, total)
	}
}

// runIDFor names the i-th run in backfill-ordering tests. Zero-padded
// so string comparisons in failure messages sort the same as CreatedAt.
func runIDFor(i int) string {
	if i < 0 || i > 99 {
		panic("runIDFor: i out of range")
	}
	return fmt.Sprintf("direct-run-%02d", i)
}
