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
	"github.com/danmestas/dagnats/internal/natsutil"
)

// TestDedupeOrderedKeysRemovesDuplicatesPreservingOrder proves the
// pure dedup step listRunIndexKeys applies keeps first-occurrence
// order and drops repeats. nats.go documents that ListKeysFiltered's
// underlying watcher "may report duplicate keys" on a bucket with a
// large number of keys and frequent writes (kv.go ~1457) -- an
// undeduped repeat would double-count an index entry's position and
// corrupt ScanNewestFirst's batch boundaries.
func TestDedupeOrderedKeysRemovesDuplicatesPreservingOrder(t *testing.T) {
	in := []string{
		runIndexPrefix + "a", runIndexPrefix + "b",
		runIndexPrefix + "a", runIndexPrefix + "c",
		runIndexPrefix + "b", runIndexPrefix + "b",
	}
	got := dedupeOrderedKeys(in)
	want := []string{
		runIndexPrefix + "a", runIndexPrefix + "b", runIndexPrefix + "c",
	}
	// Positive: exactly the 3 distinct keys survive, in first-seen order.
	if len(got) != len(want) {
		t.Fatalf("dedupeOrderedKeys(%v) = %v, want %v", in, got, want)
	}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("dedupeOrderedKeys(%v)[%d] = %q, want %q", in, i, got[i], w)
		}
	}
	// Negative: a slice with no duplicates is returned unchanged.
	noDupes := []string{runIndexPrefix + "x", runIndexPrefix + "y"}
	gotNoDupes := dedupeOrderedKeys(noDupes)
	if len(gotNoDupes) != 2 {
		t.Fatalf("dedupeOrderedKeys(%v) = %v, want unchanged", noDupes, gotNoDupes)
	}
}

// TestRunKeyCountOfIgnoresIndexKeys proves runKeyCountOf counts only
// run.<id> snapshot keys, not runidx.<id> index markers. The bucket
// now holds two keys per run, so bounding raw len(keys) against
// runKeyScanMax (as collectPrunable/CountAll/loadRunAndIndexIDSets
// used to) would silently halve the run population the bound was
// meant to cover.
func TestRunKeyCountOfIgnoresIndexKeys(t *testing.T) {
	keys := []string{
		"run.a", "run.b", "run.c",
		runIndexPrefix + "a", runIndexPrefix + "b", runIndexPrefix + "c",
	}
	// Positive: only the 3 run.* keys are counted.
	if got := runKeyCountOf(keys); got != 3 {
		t.Fatalf("runKeyCountOf(%v) = %d, want 3", keys, got)
	}
	// Negative: an all-index-keys slice counts zero run keys.
	onlyIndex := []string{runIndexPrefix + "x", runIndexPrefix + "y"}
	if got := runKeyCountOf(onlyIndex); got != 0 {
		t.Fatalf("runKeyCountOf(%v) = %d, want 0", onlyIndex, got)
	}
}

// TestListKeysFilteredReplaysWriteOrderNotLexicalOrder is a canary
// pinning the exact nats.go behavior listRunIndexKeys' correctness
// depends on: ListKeysFiltered replays keys in WRITE order, not
// Keys()'s sorted lexical order. Index keys are written deliberately
// OUT of lexical order ("zzz" first, then "aaa", then "mmm") so a
// regression to sorted/lexical replay would fail this immediately,
// independent of any of ScanNewestFirst's own logic.
func TestListKeysFilteredReplaysWriteOrderNotLexicalOrder(t *testing.T) {
	store := newListStore(t)
	writeOrder := []string{"zzz", "aaa", "mmm"}
	for _, id := range writeOrder {
		if _, err := store.kv.Create(
			context.Background(), runIndexPrefix+id, []byte{},
		); err != nil {
			t.Fatalf("Create %s: %v", id, err)
		}
	}

	keys, err := store.listRunIndexKeys(context.Background())
	if err != nil {
		t.Fatalf("listRunIndexKeys: %v", err)
	}
	want := []string{
		runIndexPrefix + "zzz", runIndexPrefix + "aaa", runIndexPrefix + "mmm",
	}
	// Positive: replay order matches WRITE order.
	if len(keys) != len(want) {
		t.Fatalf("keys = %v, want %v", keys, want)
	}
	for i, w := range want {
		if keys[i] != w {
			t.Fatalf("keys = %v, want %v (write order, not lexical)", keys, want)
		}
	}
	// Negative: lexical order ("aaa","mmm","zzz") must NOT be what we got.
	lexical := []string{runIndexPrefix + "aaa", runIndexPrefix + "mmm", runIndexPrefix + "zzz"}
	allLexical := true
	for i := range lexical {
		if keys[i] != lexical[i] {
			allLexical = false
			break
		}
	}
	if allLexical {
		t.Fatal("listRunIndexKeys returned lexically-sorted order -- " +
			"ListKeysFiltered must replay write order, not sort")
	}
}

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
	if stats.Attempted >= total {
		t.Fatalf("stats.Attempted = %d, want < %d (stopped at limit)",
			stats.Attempted, total)
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
	if stats.Attempted != 10 {
		t.Fatalf("stats.Attempted = %d, want 10 (fetchMax cap)", stats.Attempted)
	}
	if !stats.Truncated {
		t.Fatal("stats.Truncated must be true: 30 runs, fetchMax=10")
	}
	// Negative: no matches (pred always false).
	if len(matches) != 0 {
		t.Fatalf("len(matches) = %d, want 0", len(matches))
	}
}

// TestScanNewestFirstTruncatedFalseWhenLimitSatisfiedAtFetchMax proves
// the review-round edge case: when the SAME batch that reaches
// fetchMax also satisfies limit, Truncated must be false. A naive
// "Truncated = pos>0 && Attempted>=fetchMax" check would say true
// here even though the caller got everything it asked for.
func TestScanNewestFirstTruncatedFalseWhenLimitSatisfiedAtFetchMax(t *testing.T) {
	store := newListStore(t)
	// More runs than fetchMax, so pos > 0 after the scan (index not
	// exhausted) -- the precondition for the ambiguity this tests.
	seedNumberedRuns(t, store, 300)

	const limit = 5
	const fetchMax = 5 // == limit: the batch that satisfies limit
	// necessarily also hits Attempted==fetchMax exactly.
	matches, stats, err := store.ScanNewestFirst(
		context.Background(), alwaysMatch, limit, fetchMax,
	)
	if err != nil {
		t.Fatalf("ScanNewestFirst: %v", err)
	}
	// Positive: limit was satisfied.
	if len(matches) != limit {
		t.Fatalf("len(matches) = %d, want %d", len(matches), limit)
	}
	// Negative: Truncated must be false -- the caller got everything
	// it asked for, even though Attempted==fetchMax at that instant.
	if stats.Truncated {
		t.Fatalf("stats.Truncated = true, want false: limit(%d) was "+
			"satisfied, so hitting fetchMax(%d) in the same batch is "+
			"not truncation (stats=%+v)", limit, fetchMax, stats)
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
	if stats.Attempted > scanBatchSize {
		t.Fatalf("stats.Attempted = %d, want <= %d (one scan batch, "+
			"not O(population)=%d)", stats.Attempted, scanBatchSize, total)
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

// TestOrchestratorStartupRepairsRunIndexToConvergence reproduces the
// review-round bug: Start() previously ran exactly ONE bounded
// RepairRunIndex pass, so a store with more than repairPageMax
// unindexed runs (a pre-#659 upgrade) left the remainder unindexed
// until a LATER reconciler tick. Any run Saved between startup and
// that later tick was indexed newer than the still-unbackfilled old
// runs, so ScanNewestFirst returned the stale backlog as "newest" —
// permanently wrong for any old run repaired after the fact, since
// backfill order is CreatedAt-ascending but interleaves after
// whatever was already indexed.
//
// Start() must loop RepairRunIndex to convergence (Repaired==0 AND
// OrphansRemoved==0) BEFORE returning, so by the time a caller can
// Save a new run, the index is fully caught up and the new run is
// unambiguously newest.
func TestOrchestratorStartupRepairsRunIndexToConvergence(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	orch := NewOrchestrator(nc)

	// Seed MORE than one repairPageMax batch of unindexed runs --
	// simulates a pre-#659 store upgraded in place. Direct KV Put,
	// bypassing Save/writeRunIndexEntry entirely.
	const total = 1200
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < total; i++ {
		run := dag.WorkflowRun{
			RunID: fmt.Sprintf("preupgrade-%04d", i), WorkflowID: "wf",
			Status: dag.RunStatusCompleted, Steps: map[string]dag.StepState{},
			CreatedAt: base.Add(time.Duration(i) * time.Hour),
		}
		data, err := json.Marshal(run)
		if err != nil {
			t.Fatalf("marshal preupgrade run %d: %v", i, err)
		}
		if _, err := orch.store.kv.Put(
			context.Background(), "run."+run.RunID, data,
		); err != nil {
			t.Fatalf("seed preupgrade run %d: %v", i, err)
		}
	}

	orch.Start()
	t.Cleanup(orch.Stop)

	// A run Saved right after Start() returns -- before any later
	// reconciler tick -- must be found as the unambiguous newest run.
	newRun := dag.WorkflowRun{
		RunID: "post-start", WorkflowID: "wf",
		Status: dag.RunStatusRunning, Steps: map[string]dag.StepState{},
		CreatedAt: time.Now().UTC(),
	}
	if err := orch.store.Save(context.Background(), newRun); err != nil {
		t.Fatalf("Save post-start run: %v", err)
	}

	matches, _, err := orch.store.ScanNewestFirst(
		context.Background(), alwaysMatch, 5, ScanFetchMax,
	)
	if err != nil {
		t.Fatalf("ScanNewestFirst: %v", err)
	}
	// Positive: the post-start run is first.
	if len(matches) == 0 || matches[0].RunID != "post-start" {
		t.Fatalf("newest scan = %v, want post-start first "+
			"(startup repair must converge before Start() returns)",
			runIDs(matches))
	}
	// Negative: the index must be fully converged -- confirm via
	// RepairRunIndex reporting nothing left to do, not just that one
	// new run happened to land first by luck.
	stats, err := orch.store.RepairRunIndex(context.Background(), repairPageMax)
	if err != nil {
		t.Fatalf("RepairRunIndex (post-check): %v", err)
	}
	if stats.Repaired != 0 || stats.OrphansRemoved != 0 {
		t.Fatalf("RepairRunIndex still found work after Start(): %+v "+
			"(startup did not converge)", stats)
	}
}
