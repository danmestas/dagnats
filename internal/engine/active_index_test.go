// engine/active_index_test.go
// Tests for the #664 active-run liveness index, redesigned in review
// round 2 to fix a real bug: the original repair pass scanned "runs
// missing an active marker" out of the FULL run population, so a
// terminal-heavy store put every terminal run in the candidate pool
// forever (correctly absent, never repaired, no progress) -- 30,000
// terminal runs meant 30,000 pointless GETs every reconciler tick.
//
// The redesign moves index writes OFF the hot per-save path entirely
// (SaveInitial/CreateSnapshot write both indexes once, at creation;
// Save touches neither; finalizeRun explicitly deletes the liveness
// marker at the one place a run becomes terminal) and reshapes repair
// so its cost is proportional to the CRASH-GAP set (runs missing
// runidx, which can only happen if the process crashed between the
// snapshot write and index creation) plus the current ACTIVE marker
// count (validated directly, never enumerated from the full
// population) -- never proportional to the total run count. A
// one-time full-population pass (buildActiveIndexOnce, gated on the
// index.meta key) handles the pre-existing-store/large-backlog case
// exactly once, at startup.
//
// Methodology: real embedded NATS server, one per test (no mocks, no
// shared state between tests). Several tests use countingKV, a thin
// jetstream.KeyValue wrapper (below) to assert exact KV operation
// counts -- the whole point of this redesign is a specific cost
// bound, so the tests prove the bound numerically, not just that the
// end state is correct.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// countingKV wraps a real jetstream.KeyValue, counting Get/Put/Create/
// Delete calls so a test can assert an exact KV operation budget
// instead of only the end state. Everything else (Keys,
// ListKeysFiltered, Watch, etc.) is promoted unchanged via embedding.
//
// Per-key Create/Delete calls are ALSO recorded by key (createKeys,
// deleteKeys), not just totaled: the workflow_runs bucket's default
// History (1) means jetstream.KeyValue.History only ever returns the
// LATEST revision per key, so it cannot answer "how many times was
// this exact key Created/Deleted over its life" -- the wrapper's own
// call log is the only reliable source for that.
type countingKV struct {
	jetstream.KeyValue
	gets    atomic.Int64
	puts    atomic.Int64
	creates atomic.Int64
	deletes atomic.Int64

	mu         sync.Mutex
	getKeys    []string
	createKeys []string
	deleteKeys []string
}

func (c *countingKV) Get(
	ctx context.Context, key string,
) (jetstream.KeyValueEntry, error) {
	c.gets.Add(1)
	c.mu.Lock()
	c.getKeys = append(c.getKeys, key)
	c.mu.Unlock()
	return c.KeyValue.Get(ctx, key)
}

// runValueGetCount returns how many Get calls this wrapper has
// recorded against a "run.*" key -- as opposed to derived-index
// housekeeping keys like indexMetaKey -- so a test can assert "no run
// values were fetched" without being tripped up by the cheap,
// expected meta-key check.
func (c *countingKV) runValueGetCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, k := range c.getKeys {
		if isRunKey(k) {
			n++
		}
	}
	return n
}

func (c *countingKV) Put(
	ctx context.Context, key string, value []byte,
) (uint64, error) {
	c.puts.Add(1)
	return c.KeyValue.Put(ctx, key, value)
}

func (c *countingKV) Create(
	ctx context.Context, key string, value []byte, opts ...jetstream.KVCreateOpt,
) (uint64, error) {
	c.creates.Add(1)
	rev, err := c.KeyValue.Create(ctx, key, value, opts...)
	if err == nil {
		c.mu.Lock()
		c.createKeys = append(c.createKeys, key)
		c.mu.Unlock()
	}
	return rev, err
}

func (c *countingKV) Delete(
	ctx context.Context, key string, opts ...jetstream.KVDeleteOpt,
) error {
	c.deletes.Add(1)
	err := c.KeyValue.Delete(ctx, key, opts...)
	if err == nil {
		c.mu.Lock()
		c.deleteKeys = append(c.deleteKeys, key)
		c.mu.Unlock()
	}
	return err
}

// createCountFor returns how many successful Create calls this
// wrapper has recorded for the exact key, under the mutex.
func (c *countingKV) createCountFor(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, k := range c.createKeys {
		if k == key {
			n++
		}
	}
	return n
}

// deleteCountFor returns how many successful Delete calls this
// wrapper has recorded for the exact key, under the mutex.
func (c *countingKV) deleteCountFor(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, k := range c.deleteKeys {
		if k == key {
			n++
		}
	}
	return n
}

// wrapCounting installs a countingKV in front of store's underlying kv
// and returns it. Must be called before the operations under test --
// counts start at zero from the point of installation, not from
// store's construction.
func wrapCounting(store *SnapshotStore) *countingKV {
	cc := &countingKV{KeyValue: store.kv}
	store.kv = cc
	return cc
}

// createFailingKV wraps a real jetstream.KeyValue and fails every
// Create call whose key has failKeyPrefix, injecting err instead of
// delegating -- used to simulate a crash/transient failure on ONE of
// createEntryIndexes' two writes without touching the other (#664
// review round 3).
type createFailingKV struct {
	jetstream.KeyValue
	failKeyPrefix string
	err           error
}

func (f *createFailingKV) Create(
	ctx context.Context, key string, value []byte, opts ...jetstream.KVCreateOpt,
) (uint64, error) {
	if strings.HasPrefix(key, f.failKeyPrefix) {
		return 0, f.err
	}
	return f.KeyValue.Create(ctx, key, value, opts...)
}

// assertNoNonTerminalRunMissingActive scans every run.* entry that
// currently has a runidx marker and fails if any of them is
// non-terminal yet lacks a runactive marker (#664 review round 3).
// This is the exact invariant runactive-before-runidx ordering plus
// error propagation is meant to guarantee: the only way runidx can
// exist for a run is if runactive was ALREADY durably written for it
// (when non-terminal) -- so after one crash-gap repair pass, no such
// gap may remain.
func assertNoNonTerminalRunMissingActive(t *testing.T, store *SnapshotStore) {
	t.Helper()
	ctx := context.Background()
	runIDs, indexIDs, err := store.loadRunAndIndexIDSets(ctx)
	if err != nil {
		t.Fatalf("loadRunAndIndexIDSets: %v", err)
	}
	activeIDs, err := store.listActiveRunIDs(ctx)
	if err != nil {
		t.Fatalf("listActiveRunIDs: %v", err)
	}
	activeSet := make(map[string]bool, len(activeIDs))
	for _, id := range activeIDs {
		activeSet[id] = true
	}
	for id := range runIDs {
		if !indexIDs[id] {
			continue // no runidx yet -- outside this invariant's scope
		}
		run, err := store.Load(ctx, id)
		if err != nil {
			t.Fatalf("Load(%s): %v", id, err)
		}
		if !run.Status.IsTerminal() && !activeSet[id] {
			t.Fatalf("run %q has runidx but is non-terminal (%v) and "+
				"missing runactive -- invisible to ListActive forever",
				id, run.Status)
		}
	}
}

// TestCreateEntryIndexes_RunidxFailsAfterRunactiveSucceeds_RepairHeals
// is the review round 3 failing-test-first case: createEntryIndexes
// writes runactive FIRST, so a failure on the SECOND write (runidx)
// must leave the run fully visible to ListActive (runactive already
// durable) with only runidx missing -- exactly the crash-gap shape
// backfillMissingIndex is sized for. SaveInitial still returns nil
// (writeRunIndexEntry's failure is logged and swallowed BY DESIGN, the
// same as before this review round -- only createActiveEntry's error
// propagates), and one RepairRunIndex pass must recreate runidx
// without disturbing the already-correct runactive marker.
func TestCreateEntryIndexes_RunidxFailsAfterRunactiveSucceeds_RepairHeals(t *testing.T) {
	store := newListStore(t)
	injectedErr := errors.New("injected runidx create failure")
	failing := &createFailingKV{
		KeyValue: store.kv, failKeyPrefix: runIndexPrefix, err: injectedErr,
	}
	store.kv = failing

	run := dag.WorkflowRun{
		RunID: "crash-gap-runidx", WorkflowID: "wf",
		Status: dag.RunStatusRunning, Steps: map[string]dag.StepState{},
		CreatedAt: time.Now().UTC(),
	}
	err := store.SaveInitial(context.Background(), run)
	// Positive: SaveInitial still succeeds -- writeRunIndexEntry's
	// failure is logged and swallowed by design (the run snapshot is
	// the source of truth; RepairRunIndex backfills runidx). Only
	// createActiveEntry's error propagates (see createEntryIndexes'
	// doc comment) -- this test exercises the OTHER write failing.
	if err != nil {
		t.Fatalf("SaveInitial: want nil (runidx failure is swallowed "+
			"by design), got %v", err)
	}

	// Restore the real kv for verification and repair.
	store.kv = failing.KeyValue

	// Positive: runactive WAS created despite the runidx failure --
	// the run is already visible to ListActive.
	runs, _, err := store.ListActive(context.Background(), 100)
	if err != nil {
		t.Fatalf("ListActive: %v", err)
	}
	if len(runs) != 1 || runs[0].RunID != run.RunID {
		t.Fatalf("ListActive = %v, want exactly [%s]", runIDs(runs), run.RunID)
	}
	// Negative: runidx is missing before repair.
	idxKeys, err := store.listRunIndexKeys(context.Background())
	if err != nil {
		t.Fatalf("listRunIndexKeys: %v", err)
	}
	if len(idxKeys) != 0 {
		t.Fatalf("runidx keys before repair = %v, want empty", idxKeys)
	}

	stats, err := store.RepairRunIndex(context.Background(), repairPageMax)
	if err != nil {
		t.Fatalf("RepairRunIndex: %v", err)
	}
	if stats.Repaired != 1 {
		t.Fatalf("Repaired = %d, want 1", stats.Repaired)
	}

	idxKeys, err = store.listRunIndexKeys(context.Background())
	if err != nil {
		t.Fatalf("listRunIndexKeys (after repair): %v", err)
	}
	if len(idxKeys) != 1 || idxKeys[0] != runIndexPrefix+run.RunID {
		t.Fatalf("runidx keys after repair = %v, want [%s]",
			idxKeys, runIndexPrefix+run.RunID)
	}
	// Positive: ListActive still sees the run -- repair did not
	// disturb the already-correct runactive marker.
	runs, _, err = store.ListActive(context.Background(), 100)
	if err != nil {
		t.Fatalf("ListActive (after repair): %v", err)
	}
	if len(runs) != 1 || runs[0].RunID != run.RunID {
		t.Fatalf("ListActive (after repair) = %v, want exactly [%s]",
			runIDs(runs), run.RunID)
	}
	assertNoNonTerminalRunMissingActive(t, store)
}

// TestCreateEntryIndexes_CrashAfterOnlyRunactiveWritten_RepairHeals
// simulates a crash strictly between the two writes createEntryIndexes
// makes -- runactive landed, runidx never attempted -- by writing
// directly (bypassing SaveInitial entirely, since SaveInitial cannot
// itself produce this specific interleaving deterministically). This
// is the crash-gap state the reviewer's prior repro (marker deleted,
// runidx present) is now the mirror image of: by construction, that
// state -- runidx present, runactive missing, for a non-terminal run
// -- must be UNREACHABLE, so this test instead proves the reachable
// gap (runactive present, runidx missing) heals correctly and asserts
// the general invariant holds afterward.
func TestCreateEntryIndexes_CrashAfterOnlyRunactiveWritten_RepairHeals(t *testing.T) {
	store := newListStore(t)
	ctx := context.Background()
	run := dag.WorkflowRun{
		RunID: "crash-gap-active-only", WorkflowID: "wf",
		Status: dag.RunStatusRunning, Steps: map[string]dag.StepState{},
		CreatedAt: time.Now().UTC(),
	}
	data, err := json.Marshal(run)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := store.kv.Put(ctx, "run."+run.RunID, data); err != nil {
		t.Fatalf("Put run: %v", err)
	}
	if _, err := store.kv.Create(ctx, runActivePrefix+run.RunID, []byte{}); err != nil {
		t.Fatalf("Create runactive: %v", err)
	}

	// Positive: already visible to ListActive before any repair.
	runs, _, err := store.ListActive(ctx, 100)
	if err != nil {
		t.Fatalf("ListActive: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("ListActive = %v, want 1 run", runIDs(runs))
	}
	// Negative: runidx missing.
	idxKeys, err := store.listRunIndexKeys(ctx)
	if err != nil {
		t.Fatalf("listRunIndexKeys: %v", err)
	}
	if len(idxKeys) != 0 {
		t.Fatalf("runidx keys before repair = %v, want empty", idxKeys)
	}

	stats, err := store.RepairRunIndex(ctx, repairPageMax)
	if err != nil {
		t.Fatalf("RepairRunIndex: %v", err)
	}
	if stats.Repaired != 1 {
		t.Fatalf("Repaired = %d, want 1", stats.Repaired)
	}

	// The general invariant: after one repair pass, no non-terminal
	// run has runidx without also having runactive.
	assertNoNonTerminalRunMissingActive(t, store)
}

// TestActiveEntry_CreatedOnFirstSnapshot proves SaveInitial -- the
// genuine first-persistence call for the Put-based admission path --
// creates runactive.<runID>. Plain Save must NOT (see the next test).
func TestActiveEntry_CreatedOnFirstSnapshot(t *testing.T) {
	store := newListStore(t)
	run := dag.WorkflowRun{
		RunID: "active-1", WorkflowID: "wf",
		Status: dag.RunStatusRunning, Steps: map[string]dag.StepState{},
		CreatedAt: time.Now().UTC(),
	}
	if err := store.SaveInitial(context.Background(), run); err != nil {
		t.Fatalf("SaveInitial: %v", err)
	}

	ids, err := store.listActiveRunIDs(context.Background())
	if err != nil {
		t.Fatalf("listActiveRunIDs: %v", err)
	}
	// Positive: exactly one active entry, for this run.
	if len(ids) != 1 || ids[0] != "active-1" {
		t.Fatalf("active ids = %v, want [active-1]", ids)
	}
}

// TestSave_NeverWritesEitherIndex proves plain Save -- the per-advance
// path every mid-run status change and terminal transition ultimately
// routes through -- touches NEITHER runidx NOR runactive, even for a
// run that has never been indexed. This is the core of the #664 review
// round 2 redesign: index writes must not ride the hot save path.
func TestSave_NeverWritesEitherIndex(t *testing.T) {
	store := newListStore(t)
	run := dag.WorkflowRun{
		RunID: "save-only", WorkflowID: "wf",
		Status: dag.RunStatusRunning, Steps: map[string]dag.StepState{},
		CreatedAt: time.Now().UTC(),
	}
	if err := store.Save(context.Background(), run); err != nil {
		t.Fatalf("Save: %v", err)
	}

	indexIDs, err := store.listRunIndexKeys(context.Background())
	if err != nil {
		t.Fatalf("listRunIndexKeys: %v", err)
	}
	// Negative: no runidx entry from a plain Save.
	if len(indexIDs) != 0 {
		t.Fatalf("runidx keys after Save = %v, want empty", indexIDs)
	}
	activeIDs, err := store.listActiveRunIDs(context.Background())
	if err != nil {
		t.Fatalf("listActiveRunIDs: %v", err)
	}
	// Negative: no runactive entry from a plain Save either.
	if len(activeIDs) != 0 {
		t.Fatalf("runactive keys after Save = %v, want empty", activeIDs)
	}
	// Positive: the snapshot itself is still written correctly --
	// Save's actual job.
	got, err := store.Load(context.Background(), run.RunID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.RunID != run.RunID {
		t.Fatalf("Load.RunID = %q, want %q", got.RunID, run.RunID)
	}
}

// TestActiveEntry_DeletedAtFinalization_AllThreeTerminalStatuses proves
// finalizeRun -- not Save -- deletes runactive.<runID> the instant a
// run is persisted terminal, exercised across all three terminal
// statuses finalizeRun's callers can route through (Completed, Failed,
// Cancelled), proving the funnel is hooked once rather than per-status.
func TestActiveEntry_DeletedAtFinalization_AllThreeTerminalStatuses(t *testing.T) {
	for _, status := range []dag.RunStatus{
		dag.RunStatusCompleted,
		dag.RunStatusFailed,
		dag.RunStatusCancelled,
	} {
		t.Run(status.String(), func(t *testing.T) {
			_, nc := natsutil.StartTestServer(t)
			if err := natsutil.SetupAll(nc); err != nil {
				t.Fatalf("SetupAll: %v", err)
			}
			orch := NewOrchestrator(nc)
			ctx := context.Background()

			runID := "final-" + status.String()
			run := dag.WorkflowRun{
				RunID: runID, WorkflowID: "wf",
				Status: dag.RunStatusRunning, Steps: map[string]dag.StepState{},
				CreatedAt: time.Now().UTC(),
			}
			if err := orch.store.SaveInitial(ctx, run); err != nil {
				t.Fatalf("SaveInitial: %v", err)
			}
			ids, err := orch.store.listActiveRunIDs(ctx)
			if err != nil {
				t.Fatalf("listActiveRunIDs (before finalize): %v", err)
			}
			// Positive: the run is active before finalize.
			if len(ids) != 1 {
				t.Fatalf("active ids before finalize = %v, want 1 entry", ids)
			}

			if _, err := finalizeRun(
				ctx, orch.tp, orch.store, orch.saveSnapshot,
				run, status, "", nil,
			); err != nil {
				t.Fatalf("finalizeRun: %v", err)
			}
			ids, err = orch.store.listActiveRunIDs(ctx)
			if err != nil {
				t.Fatalf("listActiveRunIDs (after finalize): %v", err)
			}
			// Negative: the active marker is gone once terminal.
			if len(ids) != 0 {
				t.Fatalf("active ids after finalize (%v) = %v, want empty",
					status, ids)
			}
		})
	}
}

// TestDeleteActiveEntryHasNoOtherTerminalTransitionCaller is a
// source-level regression guard (#664 review round 2, allowed set
// widened round 4), mirroring metrics_test.go's snapshot-save-emit-site
// scan: deleteActiveEntry's doc comment asserts its ONLY callers are
// finalizeRun (the no-debt terminal-transition funnel),
// reconcileReleasePending/reconcileReleaseFailed (the debt-recovery/
// abandon funnels, reconciler.go -- the reconciler no longer owns the
// run once either clears it), and PruneTerminal (a separate, defensive
// cleanup for a run that reached terminal without ever going through
// any of the above). This scans every non-test .go file in the
// package for `deleteActiveEntry(` and fails if it appears anywhere
// other than snapshot.go (the definition and PruneTerminal's call),
// run_event.go (finalizeRun's call), and reconciler.go (the two
// release-pending funnel calls).
func TestDeleteActiveEntryHasNoOtherTerminalTransitionCaller(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	allowed := map[string]bool{
		"snapshot.go":   true,
		"run_event.go":  true,
		"reconciler.go": true,
	}
	totalFound := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") ||
			strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		count := strings.Count(string(data), "deleteActiveEntry(")
		if count == 0 {
			continue
		}
		totalFound += count
		if !allowed[name] {
			t.Errorf(
				"unexpected deleteActiveEntry reference in %s (%d "+
					"occurrence(s)) -- terminal-transition deletes must "+
					"route through finalizeRun or the reconciler's "+
					"release-pending funnels only", name, count,
			)
		}
	}
	// Positive: the guard itself is live -- at least the definition
	// plus finalizeRun's, PruneTerminal's, and the two reconciler
	// calls were found. If this drops the guard above is vacuously
	// passing.
	if totalFound < 5 {
		t.Fatalf("deleteActiveEntry occurrences found = %d, want >= 5 "+
			"(definition + finalizeRun + PruneTerminal + 2 reconciler "+
			"calls) -- guard may be vacuous", totalFound)
	}
}

// TestActiveEntry_KVOpCountOverRunLifecycle proves the #664 review
// round 2 cost claim directly: a run's ENTIRE lifecycle -- admission
// through a multi-step advance to completion -- performs EXACTLY one
// runactive Create (at admission) and EXACTLY one runactive Delete (at
// finalize), never more, regardless of how many intermediate saves
// happen in between (each of those routes through plain Save, which
// touches neither index).
func TestActiveEntry_KVOpCountOverRunLifecycle(t *testing.T) {
	store := newListStore(t)
	cc := wrapCounting(store)
	ctx := context.Background()

	run := dag.WorkflowRun{
		RunID: "lifecycle-1", WorkflowID: "wf",
		Status: dag.RunStatusRunning,
		Steps: map[string]dag.StepState{
			"a": {Status: dag.StepStatusPending},
			"b": {Status: dag.StepStatusPending},
			"c": {Status: dag.StepStatusPending},
		},
		CreatedAt: time.Now().UTC(),
	}
	if err := store.SaveInitial(ctx, run); err != nil {
		t.Fatalf("SaveInitial: %v", err)
	}
	// Three intermediate advances -- each a plain Save, exactly the
	// per-step-completion shape the real orchestrator uses.
	run.Steps["a"] = dag.StepState{Status: dag.StepStatusCompleted}
	if err := store.Save(ctx, run); err != nil {
		t.Fatalf("Save (advance a): %v", err)
	}
	run.Steps["b"] = dag.StepState{Status: dag.StepStatusCompleted}
	if err := store.Save(ctx, run); err != nil {
		t.Fatalf("Save (advance b): %v", err)
	}
	run.Steps["c"] = dag.StepState{Status: dag.StepStatusCompleted}
	if err := store.Save(ctx, run); err != nil {
		t.Fatalf("Save (advance c): %v", err)
	}
	// Finalize -- the explicit terminal delete.
	run.Status = dag.RunStatusCompleted
	if err := store.Save(ctx, run); err != nil {
		t.Fatalf("Save (terminal snapshot): %v", err)
	}
	store.deleteActiveEntry(ctx, run.RunID)

	// The workflow_runs bucket's default History (1) means
	// jetstream.KeyValue.History only ever returns the LATEST revision
	// per key, so it cannot answer "how many times was this key
	// Created/Deleted over its life" -- countingKV's own call log
	// (recorded at the wrapper, not read back from NATS) is the
	// authoritative, unambiguous source for that instead.
	activeKey := runActivePrefix + run.RunID
	// Positive: exactly one create, one delete -- the whole point.
	if got := cc.createCountFor(activeKey); got != 1 {
		t.Fatalf("runactive Create calls = %d, want 1", got)
	}
	if got := cc.deleteCountFor(activeKey); got != 1 {
		t.Fatalf("runactive Delete calls = %d, want 1", got)
	}
	// Negative: the three intermediate Save calls must not have
	// touched Create at all beyond the one at SaveInitial -- total
	// Create calls across BOTH indexes is exactly 2 (runidx + runactive,
	// both at SaveInitial).
	if got := cc.creates.Load(); got != 2 {
		t.Fatalf("total Create calls = %d, want 2 (runidx + runactive, "+
			"both at SaveInitial only)", got)
	}
}

// seedTerminalNoise seeds `total` terminal runs via SaveInitial --
// matching how a real store's history actually looks under the new
// write model: EVERY run gets a runidx entry at genuine creation
// (whether it started non-terminal and finalized later, or -- as
// here, for test economy -- was already terminal at its one and only
// write), and no runactive entry (createEntryIndexes never creates
// one for an already-terminal run). This is "converged, pure
// population noise": runidx present, runactive correctly absent.
func seedTerminalNoise(t *testing.T, store *SnapshotStore, total int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < total; i++ {
		run := dag.WorkflowRun{
			RunID:      fmt.Sprintf("noise-%05d", i),
			WorkflowID: "wf",
			Status:     dag.RunStatusCompleted,
			Steps:      map[string]dag.StepState{},
			CreatedAt:  time.Now().UTC(),
		}
		if err := store.SaveInitial(ctx, run); err != nil {
			t.Fatalf("seed noise %d: %v", i, err)
		}
	}
}

// TestRepairRunIndex_ConvergedStore_ZeroGETsBeyondActiveMarkers proves
// the #664 review round 2 fix directly: on a store that is FULLY
// converged (every run correctly indexed, no crash gap), a
// RepairRunIndex call performs ZERO run.* GETs beyond the current
// active-marker set -- regardless of how many TERMINAL runs the store
// holds. This is the exact scenario the reviewer's repro (30,000
// terminal + 3 running, 32 passes, 1-of-3 restored) exposed as broken
// in the prior design.
func TestRepairRunIndex_ConvergedStore_ZeroGETsBeyondActiveMarkers(t *testing.T) {
	store := newListStore(t)
	const (
		terminalCount = 20_000
		activeCount   = 5
	)
	seedTerminalNoise(t, store, terminalCount)
	ctx := context.Background()
	for i := 0; i < activeCount; i++ {
		run := dag.WorkflowRun{
			RunID: fmt.Sprintf("active-%02d", i), WorkflowID: "wf",
			Status: dag.RunStatusRunning, Steps: map[string]dag.StepState{},
			CreatedAt: time.Now().UTC(),
		}
		if err := store.SaveInitial(ctx, run); err != nil {
			t.Fatalf("seed active %d: %v", i, err)
		}
	}
	// The store is now fully converged: every run has whatever index
	// entries it should have (terminal noise has none; active runs
	// have both). Confirm a repair pass agrees there is nothing to do
	// BEFORE measuring cost, so a false "converged" doesn't make the
	// GET-count assertion meaningless.
	preStats, err := store.RepairRunIndex(context.Background(), repairPageMax)
	if err != nil {
		t.Fatalf("RepairRunIndex (pre-check): %v", err)
	}
	if !repairStatsAllZero(preStats) {
		t.Fatalf("store not converged before measurement: %+v", preStats)
	}

	cc := wrapCounting(store)
	stats, err := store.RepairRunIndex(context.Background(), repairPageMax)
	if err != nil {
		t.Fatalf("RepairRunIndex: %v", err)
	}
	// Positive: still nothing to do.
	if !repairStatsAllZero(stats) {
		t.Fatalf("RepairRunIndex found work on a converged store: %+v", stats)
	}
	// The core assertion: GETs are bounded by the ACTIVE population
	// (repairActiveOrphans validates each of the 5 active markers),
	// not by the 20,000-run total -- the terminal noise contributes
	// ZERO GETs.
	if got := cc.gets.Load(); got > activeCount {
		t.Fatalf("run.* GETs = %d, want <= %d (active population) -- "+
			"terminal runs must never be scanned by a converged repair pass",
			got, activeCount)
	}
}

// TestBuildActiveIndexOnce_ReviewerRepro_ConvergesAndGatesOnMetaKey
// reproduces the exact scenario from the #664 review round 2 report:
// 30,000 terminal runs plus 3 running runs, seeded DIRECTLY (no
// indexes at all, no index.meta key) -- simulating a pre-#664 store.
// The FIRST orchestrator Start() must run the one-time full build,
// correctly marking all 3 running runs active and writing
// index.meta -- and a SECOND Start() must skip the full build
// entirely (0 GETs against the terminal population) because the meta
// key is already present.
func TestBuildActiveIndexOnce_ReviewerRepro_ConvergesAndGatesOnMetaKey(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	orch := NewOrchestrator(nc)
	ctx := context.Background()

	const terminalCount = 30_000
	base := time.Now().Add(-24 * time.Hour)
	for i := 0; i < terminalCount; i++ {
		run := dag.WorkflowRun{
			RunID: fmt.Sprintf("repro-terminal-%05d", i), WorkflowID: "wf",
			Status: dag.RunStatusCompleted, Steps: map[string]dag.StepState{},
			CreatedAt: base.Add(time.Duration(i) * time.Millisecond),
		}
		data, err := json.Marshal(run)
		if err != nil {
			t.Fatalf("marshal terminal %d: %v", i, err)
		}
		if _, err := orch.store.kv.Put(ctx, "run."+run.RunID, data); err != nil {
			t.Fatalf("seed terminal %d: %v", i, err)
		}
	}
	runningIDs := []string{"repro-run-a", "repro-run-b", "repro-run-c"}
	for _, id := range runningIDs {
		run := dag.WorkflowRun{
			RunID: id, WorkflowID: "wf",
			Status: dag.RunStatusRunning, Steps: map[string]dag.StepState{},
			CreatedAt: base,
		}
		data, err := json.Marshal(run)
		if err != nil {
			t.Fatalf("marshal running %s: %v", id, err)
		}
		if _, err := orch.store.kv.Put(ctx, "run."+run.RunID, data); err != nil {
			t.Fatalf("seed running %s: %v", id, err)
		}
	}

	if err := orch.Start(); err != nil {
		t.Fatalf("Start (first, builds index): %v", err)
	}
	t.Cleanup(orch.Stop)

	// Positive: all 3 running runs are marked active.
	activeIDs, err := orch.store.listActiveRunIDs(ctx)
	if err != nil {
		t.Fatalf("listActiveRunIDs: %v", err)
	}
	if len(activeIDs) != len(runningIDs) {
		t.Fatalf("active ids = %v, want exactly %v", activeIDs, runningIDs)
	}
	for _, id := range runningIDs {
		found := false
		for _, got := range activeIDs {
			if got == id {
				found = true
			}
		}
		if !found {
			t.Fatalf("running run %q missing from active index: %v", id, activeIDs)
		}
	}
	// Positive: the meta key was written.
	if _, err := orch.store.kv.Get(ctx, indexMetaKey); err != nil {
		t.Fatalf("index.meta not written after first Start: %v", err)
	}

	// Second "startup" -- call buildActiveIndexOnce directly (Stop/
	// Start again would tear down and rebuild the whole orchestrator
	// unnecessarily for what this asserts): with the meta key present,
	// it must do exactly one Get (the meta-key check itself) and ZERO
	// run.* GETs -- never re-scanning the 30,000 terminal runs.
	cc := wrapCounting(orch.store)
	built, err := orch.store.buildActiveIndexOnce(ctx)
	if err != nil {
		t.Fatalf("buildActiveIndexOnce (second call): %v", err)
	}
	if built {
		t.Fatal("buildActiveIndexOnce ran the full pass again on a store " +
			"with index.meta already present")
	}
	if got := cc.gets.Load(); got != 1 {
		t.Fatalf("total GETs on second buildActiveIndexOnce call = %d, "+
			"want 1 (just the index.meta check)", got)
	}
	if got := cc.runValueGetCount(); got != 0 {
		t.Fatalf("run.* GETs on second buildActiveIndexOnce call = %d, "+
			"want 0 (meta key present -- must skip the full pass entirely)", got)
	}
}

// TestRepairStatsAddCoversEveryField is a reflection-based regression
// guard (#664 review round 4 MEDIUM): RepairStats.Add must incorporate
// EVERY field of the struct. For each field, this builds two
// RepairStats values differing ONLY in that field (distinctive
// nonzero ints, or false/true for a bool) and asserts Add's result
// reflects it -- summed for int fields, OR'd for bool fields -- while
// every OTHER field stays zero/false. A new RepairStats field added
// without a matching line in Add fails this test immediately, instead
// of silently accumulating a wrong (zero) total the way
// PendingOrphansRemoved and both truncation flags were dropped by an
// earlier version of repairRunIndexToConvergenceWith's accumulation.
func TestRepairStatsAddCoversEveryField(t *testing.T) {
	typ := reflect.TypeOf(RepairStats{})
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		var a, b RepairStats
		av := reflect.ValueOf(&a).Elem().Field(i)
		bv := reflect.ValueOf(&b).Elem().Field(i)
		switch field.Type.Kind() {
		case reflect.Int:
			av.SetInt(3)
			bv.SetInt(4)
		case reflect.Bool:
			av.SetBool(false)
			bv.SetBool(true)
		default:
			t.Fatalf("field %s has unhandled kind %s -- extend this test",
				field.Name, field.Type.Kind())
		}

		got := a.Add(b)
		gotVal := reflect.ValueOf(got)
		switch field.Type.Kind() {
		case reflect.Int:
			if gotVal.Field(i).Int() != 7 {
				t.Fatalf("field %s: Add result = %d, want 7 (3+4) -- "+
					"Add is not accumulating this field",
					field.Name, gotVal.Field(i).Int())
			}
		case reflect.Bool:
			if !gotVal.Field(i).Bool() {
				t.Fatalf("field %s: Add result = false, want true "+
					"(false || true) -- Add is not accumulating this field",
					field.Name)
			}
		}
		// Negative: every OTHER field stays untouched -- proves this
		// iteration's positive assertion isn't accidentally passing
		// because some unrelated field carries the signal.
		for j := 0; j < typ.NumField(); j++ {
			if j == i {
				continue
			}
			otherField := typ.Field(j)
			otherGot := gotVal.Field(j)
			switch otherField.Type.Kind() {
			case reflect.Int:
				if otherGot.Int() != 0 {
					t.Fatalf("field %s unexpectedly nonzero (%d) while "+
						"testing field %s", otherField.Name, otherGot.Int(),
						field.Name)
				}
			case reflect.Bool:
				if otherGot.Bool() {
					t.Fatalf("field %s unexpectedly true while testing "+
						"field %s", otherField.Name, field.Name)
				}
			}
		}
	}
}
