// engine/def_reaper_test.go
// Tests for the opt-in, two-phase ephemeral-def reaper (#377, ADR-021
// Phase A). The reaper garbage-collects "agent.<root>.<name>" workflow_defs
// once their tree-root run has been terminal longer than a grace window.
// Methodology: real embedded NATS server, one per test (no shared servers),
// reduced store budget for the disk-constrained sandbox. Each test asserts
// BOTH the deletion AND a SURVIVOR — every destructive-safety invariant is
// exercised here:
//  1. only agent.-prefixed keys are touched (promoted./bare survive);
//  2. only a terminal ROOT past grace is reaped (live/within-grace survive);
//  3. bounded scan + bounded delete per pass;
//  4. idempotent (a second pass deletes zero);
//  5. orphan-sweep is safe ONLY under the runsMaxAge >= defReaperGrace
//     Start-time invariant (a missing root means both windows elapsed);
//  6. fail-safe: any collect-phase read error aborts the pass with zero
//     deletions.
package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// --- pure helpers (no NATS) ---

func TestRootRunIDOf_SetWins(t *testing.T) {
	run := dag.WorkflowRun{RunID: "child", RootRunID: "root"}
	if got := RootRunIDOf(run); got != "root" {
		t.Fatalf("RootRunIDOf with RootRunID set = %q, want %q", got, "root")
	}
	// Negative space: an unset RootRunID self-roots to RunID.
	bare := dag.WorkflowRun{RunID: "solo"}
	if got := RootRunIDOf(bare); got != "solo" {
		t.Fatalf("RootRunIDOf self-root = %q, want %q", got, "solo")
	}
}

func TestRootFromDefKey(t *testing.T) {
	cases := []struct {
		key      string
		wantRoot string
		wantOK   bool
	}{
		{"agent.r1.foo", "r1", true},
		{"agent.r1.foo.bar", "r1", true}, // name may contain dots
		{"promoted.foo", "", false},      // no agent. prefix
		{"foo", "", false},               // bare
		{"agent..foo", "", false},        // empty root
		{"agent.r1.", "", false},         // empty name
		{"agent.", "", false},            // nothing after prefix
	}
	for _, c := range cases {
		root, ok := rootFromDefKey(c.key)
		if ok != c.wantOK || root != c.wantRoot {
			t.Fatalf("rootFromDefKey(%q) = (%q,%v), want (%q,%v)",
				c.key, root, ok, c.wantRoot, c.wantOK)
		}
	}
}

// --- reaper harness ---

// reaperHarness bundles the orchestrator under test with raw KV handles so
// a test can seed ephemeral def keys (and corrupt run values) the public
// API would never write.
type reaperHarness struct {
	orch   *Orchestrator
	defKV  jetstream.KeyValue
	runsKV jetstream.KeyValue
}

// newReaperOrch stands up an embedded server with a reduced store budget
// and returns the orchestrator (NOT started) plus raw KV handles.
func newReaperOrch(t *testing.T, grace time.Duration) reaperHarness {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(
		nc, natsutil.WithStoreBudget(256<<20),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	defKV, err := js.KeyValue(context.Background(), "workflow_defs")
	if err != nil {
		t.Fatalf("KeyValue(workflow_defs): %v", err)
	}
	runsKV, err := js.KeyValue(context.Background(), "workflow_runs")
	if err != nil {
		t.Fatalf("KeyValue(workflow_runs): %v", err)
	}
	orch := NewOrchestrator(nc, WithDefReaperGrace(grace))
	return reaperHarness{orch: orch, defKV: defKV, runsKV: runsKV}
}

func seedDef(t *testing.T, kv jetstream.KeyValue, key string) {
	t.Helper()
	def := dag.WorkflowDef{
		Name: key, Version: "1",
		Steps: []dag.StepDef{{ID: "s", Task: "t", Type: dag.StepTypeNormal}},
	}
	data, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal def %q: %v", key, err)
	}
	if _, err := kv.Put(context.Background(), key, data); err != nil {
		t.Fatalf("put def %q: %v", key, err)
	}
}

// corruptRunSnapshot writes a non-JSON value under the run.<runID> key so
// SnapshotStore.Load returns an unmarshal error (not ErrRunNotFound).
func corruptRunSnapshot(t *testing.T, kv jetstream.KeyValue, runID string) {
	t.Helper()
	if _, err := kv.Put(
		context.Background(), "run."+runID, []byte("{not-json"),
	); err != nil {
		t.Fatalf("corrupt run %q: %v", runID, err)
	}
}

func seedRootRunIn(
	t *testing.T, store *SnapshotStore, runID string,
	status dag.RunStatus, completedAgo time.Duration,
) {
	t.Helper()
	run := dag.WorkflowRun{
		RunID:      runID,
		RootRunID:  runID,
		WorkflowID: "wf",
		Status:     status,
		Steps:      map[string]dag.StepState{"s": {Status: dag.StepStatusCompleted}},
		CreatedAt:  time.Now().UTC().Add(-completedAgo - time.Minute),
	}
	if status.IsTerminal() && completedAgo >= 0 {
		c := time.Now().UTC().Add(-completedAgo)
		run.CompletedAt = &c
	}
	if err := store.Save(context.Background(), run); err != nil {
		t.Fatalf("save root run %q: %v", runID, err)
	}
}

func defExists(t *testing.T, kv jetstream.KeyValue, key string) bool {
	t.Helper()
	_, err := kv.Get(context.Background(), key)
	return err == nil
}

// Test 2: an ephemeral def vanishes after its root is terminal+grace, and
// the run snapshot itself is untouched (the reaper only touches defs).
func TestDefReaper_EphemeralDefVanishesRunSnapshotUntouched(t *testing.T) {
	h := newReaperOrch(t, time.Hour)
	orch, kv := h.orch, h.defKV
	seedRootRunIn(t, orch.store, "r1", dag.RunStatusCompleted, 48*time.Hour)
	seedDef(t, kv, "agent.r1.tool")

	orch.reapEphemeralDefs(context.Background(), defReaperMaxDelete)

	if defExists(t, kv, "agent.r1.tool") {
		t.Fatal("ephemeral def survived reaper pass, want deleted")
	}
	// Survivor: the run snapshot is NOT a def-reaper concern.
	if !exists(t, orch.store, "r1") {
		t.Fatal("run snapshot was deleted by def-reaper, want untouched")
	}
}

// Test 3 + 4: promoted. and bare defs survive a reaper pass (prefix gate).
func TestDefReaper_PromotedAndBareSurvive(t *testing.T) {
	h := newReaperOrch(t, time.Hour)
	orch, kv := h.orch, h.defKV
	seedRootRunIn(t, orch.store, "r1", dag.RunStatusCompleted, 48*time.Hour)
	seedDef(t, kv, "agent.r1.tool") // reapable control
	seedDef(t, kv, "promoted.tool") // reaper-immune namespace
	seedDef(t, kv, "normal-def")    // ordinary author def

	orch.reapEphemeralDefs(context.Background(), defReaperMaxDelete)

	if defExists(t, kv, "agent.r1.tool") {
		t.Fatal("reapable agent. def survived, want deleted")
	}
	if !defExists(t, kv, "promoted.tool") {
		t.Fatal("promoted. def was reaped, want immune")
	}
	if !defExists(t, kv, "normal-def") {
		t.Fatal("bare def was reaped, want untouched")
	}
}

// Test 5: a live (non-terminal) root's defs survive; a terminal-but-within-
// grace root's defs survive. A THIRD root that IS terminal+past-grace acts
// as a control deletion in the SAME pass — without it a disabled/broken
// reaper would pass this test by simply touching nothing.
func TestDefReaper_LiveAndWithinGraceSurvive(t *testing.T) {
	h := newReaperOrch(t, time.Hour)
	orch, kv := h.orch, h.defKV
	seedRootRunIn(t, orch.store, "live", dag.RunStatusRunning, -1)
	seedRootRunIn(t, orch.store, "fresh", dag.RunStatusCompleted, time.Minute)
	seedRootRunIn(t, orch.store, "aged", dag.RunStatusCompleted, 48*time.Hour)
	seedDef(t, kv, "agent.live.tool")
	seedDef(t, kv, "agent.fresh.tool")
	seedDef(t, kv, "agent.aged.tool")

	orch.reapEphemeralDefs(context.Background(), defReaperMaxDelete)

	if !defExists(t, kv, "agent.live.tool") {
		t.Fatal("def of a live root was reaped, want kept")
	}
	if !defExists(t, kv, "agent.fresh.tool") {
		t.Fatal("def of a within-grace root was reaped, want kept")
	}
	// Control: the past-grace root's def MUST be gone in the same pass —
	// proves the reaper actually ran while sparing live + within-grace.
	if defExists(t, kv, "agent.aged.tool") {
		t.Fatal("def of a past-grace root survived, want reaped (control)")
	}
}

// Test 6: idempotent — a second pass deletes zero and returns no error.
func TestDefReaper_Idempotent(t *testing.T) {
	h := newReaperOrch(t, time.Hour)
	orch, kv := h.orch, h.defKV
	seedRootRunIn(t, orch.store, "r1", dag.RunStatusCompleted, 48*time.Hour)
	seedDef(t, kv, "agent.r1.tool")

	orch.reapEphemeralDefs(context.Background(), defReaperMaxDelete)
	if defExists(t, kv, "agent.r1.tool") {
		t.Fatal("first pass did not delete reapable def")
	}
	// Second pass: nothing reapable remains; collectReapable returns empty.
	doomed, err := orch.collectReapable(context.Background(), defReaperMaxDelete)
	if err != nil {
		t.Fatalf("second collect errored: %v", err)
	}
	if len(doomed) != 0 {
		t.Fatalf("second pass collected %d, want 0", len(doomed))
	}
}

// Test 7: bounded — maxDelete=1 over two reapable trees collects exactly 1
// per pass.
func TestDefReaper_BoundedMaxDelete(t *testing.T) {
	h := newReaperOrch(t, time.Hour)
	orch, kv := h.orch, h.defKV
	seedRootRunIn(t, orch.store, "r1", dag.RunStatusCompleted, 48*time.Hour)
	seedRootRunIn(t, orch.store, "r2", dag.RunStatusCompleted, 48*time.Hour)
	seedDef(t, kv, "agent.r1.tool")
	seedDef(t, kv, "agent.r2.tool")

	orch.reapEphemeralDefs(context.Background(), 1)

	r1Gone := !defExists(t, kv, "agent.r1.tool")
	r2Gone := !defExists(t, kv, "agent.r2.tool")
	if r1Gone == r2Gone {
		t.Fatalf("expected exactly one deletion, got r1Gone=%v r2Gone=%v",
			r1Gone, r2Gone)
	}
	// Survivor proves the bound held; the next pass deletes the other.
	orch.reapEphemeralDefs(context.Background(), 1)
	if defExists(t, kv, "agent.r1.tool") || defExists(t, kv, "agent.r2.tool") {
		t.Fatal("second bounded pass did not finish the remaining tree")
	}
}

// Test 8: orphan sweep — agent.r3.tool with no run.r3 snapshot is swept
// (safe under the Start-time runsMaxAge >= defReaperGrace invariant).
func TestDefReaper_OrphanSwept(t *testing.T) {
	h := newReaperOrch(t, time.Hour)
	orch, kv := h.orch, h.defKV
	// Deliberately seed NO run for r3.
	seedDef(t, kv, "agent.r3.tool")
	// Survivor control: a def whose root exists and is live must be kept,
	// proving the orphan rule is the only reason r3 went.
	seedRootRunIn(t, orch.store, "alive", dag.RunStatusRunning, -1)
	seedDef(t, kv, "agent.alive.tool")

	orch.reapEphemeralDefs(context.Background(), defReaperMaxDelete)

	if defExists(t, kv, "agent.r3.tool") {
		t.Fatal("orphan def survived, want swept")
	}
	if !defExists(t, kv, "agent.alive.tool") {
		t.Fatal("live-root def was swept, want kept")
	}
}

// Test 9: the fix-1 Start-time invariant — runsMaxAge>0 && < defReaperGrace
// panics, guaranteeing the run snapshot outlives the def-grace window.
func TestDefReaper_StartInvariantPanics(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(
		nc, natsutil.WithStoreBudget(256<<20),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	orch := NewOrchestrator(nc,
		WithRunsMaxAge(time.Minute),
		WithDefReaperGrace(time.Hour),
	)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on runsMaxAge < defReaperGrace")
		}
		orch.Stop()
	}()
	orch.Start()
}

// Test 9b (positive space): runsMaxAge == 0 (pruner off) or >= grace starts
// cleanly — the invariant permits the safe configurations.
func TestDefReaper_StartInvariantAllowsSafe(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(
		nc, natsutil.WithStoreBudget(256<<20),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	orch := NewOrchestrator(nc,
		WithRunsMaxAge(2*time.Hour),
		WithDefReaperGrace(time.Hour),
	)
	orch.Start()
	orch.Stop()
}

// Test 10: fail-safe — a collect-phase read error aborts the pass with ZERO
// deletions. We seed a corrupt def value the public API would never write;
// json.Unmarshal of the def is fine (rootFromDefKey is pure on the KEY), so
// the read error must come from the run-store load path. We force that by
// seeding a corrupt RUN snapshot under the root the def points to, which
// makes defShouldBeReaped's store.Load fail on unmarshal — aborting collect.
func TestDefReaper_CollectFailSafeZeroDeletions(t *testing.T) {
	h := newReaperOrch(t, time.Hour)
	orch, kv := h.orch, h.defKV
	// A clearly-reapable def whose root we will corrupt.
	seedDef(t, kv, "agent.bad.tool")
	// Another reapable def with a healthy terminal root — it must NOT be
	// deleted because the pass aborts atomically on the first read error.
	seedRootRunIn(t, orch.store, "good", dag.RunStatusCompleted, 48*time.Hour)
	seedDef(t, kv, "agent.good.tool")

	// Corrupt the run snapshot for "bad" so store.Load returns an unmarshal
	// error (not ErrRunNotFound) → defShouldBeReaped propagates it.
	corruptRunSnapshot(t, h.runsKV, "bad")

	doomed, err := orch.collectReapable(context.Background(), defReaperMaxDelete)
	if err == nil {
		t.Fatal("expected collect to abort with an error, got nil")
	}
	if len(doomed) != 0 {
		t.Fatalf("fail-safe violated: collected %d keys, want 0", len(doomed))
	}
	// Survivors: a full pass must delete NOTHING when collect aborted.
	orch.reapEphemeralDefs(context.Background(), defReaperMaxDelete)
	if !defExists(t, kv, "agent.good.tool") {
		t.Fatal("healthy def was deleted despite collect abort, want kept")
	}
	if !defExists(t, kv, "agent.bad.tool") {
		t.Fatal("corrupt-root def was deleted despite collect abort, want kept")
	}
}
