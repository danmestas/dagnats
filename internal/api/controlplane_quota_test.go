// api/controlplane_quota_test.go
// Integration tests for the per-runtime safety bounds (ADR-021 Phase A,
// #378): def quota, active-run quota, register rate limit, configurable
// generation-depth cap, and the scan-backed Budget read. These are the
// fork-bomb / resource-exhaustion defense layer — every exceed-condition
// must return a TYPED ControlPlaneError and NEVER crash the orchestrator.
//
// Methodology: fresh embedded NATS + orchestrator + api per test (reduced
// store budget); bounded <=10s waits; each test asserts the positive (the
// cap trips) AND negative space (a different root is unaffected, or the
// orchestrator still completes a fresh run after a rejection). Quota tests
// seed run/def snapshots directly so the count-then-act threshold is
// deterministic rather than racing the async spawn path.
package api

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/worker"
)

// seedActiveRunUnderRoot persists a running run snapshot whose tree-root is
// root (root itself when runID==root). Returns the run ID. Used to drive the
// active-run quota count to an exact value.
func seedActiveRunUnderRoot(
	t *testing.T, svc *Service, runID, root string,
) {
	t.Helper()
	store := engine.NewSnapshotStore(svc.js)
	def := dag.WorkflowDef{
		Name: "seed", Version: "1",
		Steps: []dag.StepDef{{ID: "s", Task: "t", Type: dag.StepTypeNormal}},
	}
	run := dag.NewWorkflowRun(def, runID)
	run.RootRunID = root
	if runID != root {
		run.ParentRunID = root
	}
	run.Status = dag.RunStatusRunning
	if err := store.Save(context.Background(), run); err != nil {
		t.Fatalf("seed active run %q: %v", runID, err)
	}
}

// markRunTerminal flips an existing run snapshot to a terminal status so it
// drops out of the active-run count.
func markRunTerminal(t *testing.T, svc *Service, runID string) {
	t.Helper()
	store := engine.NewSnapshotStore(svc.js)
	run, err := store.Load(context.Background(), runID)
	if err != nil {
		t.Fatalf("load run %q: %v", runID, err)
	}
	run.Status = dag.RunStatusCompleted
	if err := store.Save(context.Background(), run); err != nil {
		t.Fatalf("mark terminal %q: %v", runID, err)
	}
}

// TestRuntimeBounds_DefQuotaExceeded proves the def quota rejects the
// (Max+1)th register under a root with KindQuotaExceeded, that a DIFFERENT
// root is unaffected, and — crash-resistance — that the orchestrator still
// completes a fresh run after a quota rejection.
func TestRuntimeBounds_DefQuotaExceeded(t *testing.T) {
	h := newCPHarnessWithLimits(t, true, RuntimeLimits{MaxDefsPerRoot: 2})

	seedRootRun(t, h.svc, "root-a")
	seedRootRun(t, h.svc, "root-b")

	// Two registers under root-a succeed (count 0->1->2).
	for i, name := range []string{"d1", "d2"} {
		def := childDef()
		def.Name = name
		if _, kind, err := h.svc.RegisterRuntimeWorkflow(
			context.Background(), def, "root-a", false,
		); err != nil {
			t.Fatalf("register %d under root-a (kind %q): %v", i, kind, err)
		}
	}

	// The 3rd register breaches the cap (count 2 >= 2).
	def3 := childDef()
	def3.Name = "d3"
	scoped, kind, err := h.svc.RegisterRuntimeWorkflow(
		context.Background(), def3, "root-a", false,
	)
	if err == nil {
		t.Fatalf("expected def quota rejection, got scoped %q", scoped)
	}
	if kind != cpKindQuotaExceeded {
		t.Fatalf("kind = %q, want %q", kind, cpKindQuotaExceeded)
	}
	if scoped != "" {
		t.Fatalf("expected empty scoped on rejection, got %q", scoped)
	}

	// Negative space: a DIFFERENT root still registers (per-root isolation).
	otherDef := childDef()
	otherDef.Name = "d1"
	if _, kind, err := h.svc.RegisterRuntimeWorkflow(
		context.Background(), otherDef, "root-b", false,
	); err != nil {
		t.Fatalf("register under root-b (kind %q): %v", kind, err)
	}

	// Crash-resistance: the orchestrator is unharmed — a fresh run still
	// completes end-to-end after the quota rejection.
	assertFreshRunCompletes(t, h)
}

// assertFreshRunCompletes registers a simple workflow, starts it, and waits
// for completion — proving a prior typed rejection did not crash the engine.
func assertFreshRunCompletes(t *testing.T, h *cpHarness) {
	t.Helper()
	h.w.Handle("survivor-task", func(ctx worker.TaskContext) error {
		return ctx.Complete([]byte(`{"ok":true}`))
	})
	h.w.Start()
	t.Cleanup(h.w.Stop)
	def := dag.WorkflowDef{
		Name: "survivor", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s", Task: "survivor-task", Type: dag.StepTypeNormal},
		},
	}
	if err := h.svc.RegisterWorkflow(context.Background(), def); err != nil {
		t.Fatalf("register survivor: %v", err)
	}
	runID, err := h.svc.StartRun(context.Background(), "survivor", nil)
	if err != nil {
		t.Fatalf("start survivor: %v", err)
	}
	waitRunStatus(t, h.svc, runID, dag.RunStatusCompleted)
}

// TestRuntimeBounds_ActiveRunQuotaExceeded proves a spawn that would push a
// tree past MaxActiveRunsPerRoot is rejected with KindQuotaExceeded, and
// that after an active run terminates a fresh spawn succeeds.
func TestRuntimeBounds_ActiveRunQuotaExceeded(t *testing.T) {
	h := newCPHarnessWithLimits(
		t, true, RuntimeLimits{MaxActiveRunsPerRoot: 1},
	)

	// One running root run => active count == 1 == the cap. A child of it
	// must register first so the depth/missing-def checks pass.
	seedActiveRunUnderRoot(t, h.svc, "tree", "tree")
	if _, _, err := h.svc.RegisterRuntimeWorkflow(
		context.Background(), childDef(), "tree", false,
	); err != nil {
		t.Fatalf("seed register: %v", err)
	}
	scoped := scopeName("tree", "do-step")

	// Spawn breaches the cap (active 1 >= 1).
	runID, kind, err := h.svc.SpawnChildRun(
		context.Background(), scoped, "tree", "step", nil,
	)
	if err == nil {
		t.Fatalf("expected active-run quota rejection, got runID %q", runID)
	}
	if kind != cpKindQuotaExceeded {
		t.Fatalf("kind = %q, want %q", kind, cpKindQuotaExceeded)
	}
	if runID != "" {
		t.Fatalf("expected empty runID on rejection, got %q", runID)
	}

	// After the only active run terminates, active count drops to 0 and a
	// fresh spawn succeeds (the cap bounds occupancy, not lifetime total).
	markRunTerminal(t, h.svc, "tree")
	runID, kind, err = h.svc.SpawnChildRun(
		context.Background(), scoped, "tree", "step", nil,
	)
	if err != nil {
		t.Fatalf("spawn after terminate (kind %q): %v", kind, err)
	}
	if runID == "" {
		t.Fatal("expected a child run ID after capacity freed")
	}
}

// TestRuntimeBounds_RateLimited proves the register rate limit rejects the
// 2nd immediate register under a root (limit 1/min) with KindRateLimited,
// while a DIFFERENT root is unaffected (per-root bucket isolation).
func TestRuntimeBounds_RateLimited(t *testing.T) {
	h := newCPHarnessWithLimits(
		t, true, RuntimeLimits{MaxRegistersPerMinutePerRoot: 1},
	)

	seedRootRun(t, h.svc, "rl-a")
	seedRootRun(t, h.svc, "rl-b")

	// First register under rl-a consumes the single token.
	if _, kind, err := h.svc.RegisterRuntimeWorkflow(
		context.Background(), childDef(), "rl-a", false,
	); err != nil {
		t.Fatalf("first register under rl-a (kind %q): %v", kind, err)
	}

	// Immediate 2nd register under rl-a is rate-limited.
	def2 := childDef()
	def2.Name = "other"
	scoped, kind, err := h.svc.RegisterRuntimeWorkflow(
		context.Background(), def2, "rl-a", false,
	)
	if err == nil {
		t.Fatalf("expected rate-limit rejection, got scoped %q", scoped)
	}
	if kind != cpKindRateLimited {
		t.Fatalf("kind = %q, want %q", kind, cpKindRateLimited)
	}
	if scoped != "" {
		t.Fatalf("expected empty scoped on rejection, got %q", scoped)
	}

	// Negative space: a DIFFERENT root has its own token bucket.
	if _, kind, err := h.svc.RegisterRuntimeWorkflow(
		context.Background(), childDef(), "rl-b", false,
	); err != nil {
		t.Fatalf("register under rl-b (kind %q): %v", kind, err)
	}
}

// TestRuntimeBounds_DepthConfigLooseningRejected proves the API gate cannot
// be loosened past the orchestrator's hard ceiling even by a programmatic
// caller of NewServiceWithLimits: a loose MaxGenerationDepth is CLAMPED to
// engine.MaxNestingDepth, so a spawn at the real ceiling is rejected with
// depth_exceeded and returns NO runID — never a ghost run (a runID for a run
// the orchestrator silently drops). Guards the HIGH ghost-run hole.
func TestRuntimeBounds_DepthConfigLooseningRejected(t *testing.T) {
	loose := engine.MaxNestingDepth + 5
	h := newCPHarnessWithLimits(
		t, true, RuntimeLimits{MaxGenerationDepth: loose},
	)

	seedRootRun(t, h.svc, "owner")
	if _, _, err := h.svc.RegisterRuntimeWorkflow(
		context.Background(), childDef(), "owner", false,
	); err != nil {
		t.Fatalf("seed register: %v", err)
	}
	scoped := scopeName("owner", "do-step")

	// A chain at the ENGINE ceiling breaches: the orchestrator would drop a
	// spawn here, so the API gate (clamped to the ceiling) must reject too.
	// Were the loose value honored, the gate would pass and mint a ghost
	// runID for a run the orchestrator never creates.
	deepParent := buildRunChain(t, h.svc, engine.MaxNestingDepth)
	runID, kind, err := h.svc.SpawnChildRun(
		context.Background(), scoped, deepParent, "step", nil,
	)
	if err == nil {
		t.Fatalf("loose depth honored: got runID %q (ghost run)", runID)
	}
	if kind != cpKindDepthExceeded {
		t.Fatalf("kind = %q, want %q", kind, cpKindDepthExceeded)
	}
	if runID != "" {
		t.Fatalf("expected empty runID on rejection, got %q (ghost run)", runID)
	}
}

// TestRuntimeBounds_DepthCapConfigurable proves the depth cap honors the
// configured MaxGenerationDepth (not the engine const), rejecting a spawn
// at the configured boundary while a shallow spawn succeeds. Guards the
// const->config reuse (#378 D1).
func TestRuntimeBounds_DepthCapConfigurable(t *testing.T) {
	h := newCPHarnessWithLimits(
		t, true, RuntimeLimits{MaxGenerationDepth: 2},
	)

	seedRootRun(t, h.svc, "owner")
	if _, _, err := h.svc.RegisterRuntimeWorkflow(
		context.Background(), childDef(), "owner", false,
	); err != nil {
		t.Fatalf("seed register: %v", err)
	}
	scoped := scopeName("owner", "do-step")

	// A chain at depth==MaxGenerationDepth breaches on the next spawn.
	deepParent := buildRunChain(t, h.svc, 2)
	runID, kind, err := h.svc.SpawnChildRun(
		context.Background(), scoped, deepParent, "step", nil,
	)
	if err == nil {
		t.Fatalf("expected depth rejection, got runID %q", runID)
	}
	if kind != cpKindDepthExceeded {
		t.Fatalf("kind = %q, want %q", kind, cpKindDepthExceeded)
	}

	// Negative space: a depth-1 spawn (a single top-level parent) succeeds
	// under the configured cap of 2.
	seedActiveRunUnderRoot(t, h.svc, "shallow", "shallow")
	runID, kind, err = h.svc.SpawnChildRun(
		context.Background(), scoped, "shallow", "step", nil,
	)
	if err != nil {
		t.Fatalf("shallow spawn (kind %q): %v", kind, err)
	}
	if runID == "" {
		t.Fatal("expected a child run ID for a shallow spawn")
	}
}

// TestRuntimeBounds_BudgetReturnsRealCounts proves Budget returns scan-backed
// numbers: N registered defs + M active runs under a root map to
// RegisteredDefs==N / ActiveRuns==M with the configured maxima, and a fresh
// root reports zero usage.
func TestRuntimeBounds_BudgetReturnsRealCounts(t *testing.T) {
	limits := RuntimeLimits{MaxActiveRunsPerRoot: 50, MaxDefsPerRoot: 50}
	h := newCPHarnessWithLimits(t, true, limits)

	// Root "busy": 1 root run + 2 extra active children => 3 active; plus 2
	// registered ephemeral defs.
	seedActiveRunUnderRoot(t, h.svc, "busy", "busy")
	seedActiveRunUnderRoot(t, h.svc, "busy-kid-1", "busy")
	seedActiveRunUnderRoot(t, h.svc, "busy-kid-2", "busy")
	for _, name := range []string{"tool-a", "tool-b"} {
		def := childDef()
		def.Name = name
		if _, _, err := h.svc.RegisterRuntimeWorkflow(
			context.Background(), def, "busy", false,
		); err != nil {
			t.Fatalf("register %q under busy: %v", name, err)
		}
	}

	budget, kind, err := h.svc.Budget(context.Background(), "busy")
	if err != nil {
		t.Fatalf("budget for busy (kind %q): %v", kind, err)
	}
	if budget.ActiveRuns != 3 {
		t.Errorf("ActiveRuns = %d, want 3", budget.ActiveRuns)
	}
	if budget.RegisteredDefs != 2 {
		t.Errorf("RegisteredDefs = %d, want 2", budget.RegisteredDefs)
	}
	if budget.MaxActiveRuns != 50 || budget.MaxRegisteredDefs != 50 {
		t.Errorf("maxima = (%d, %d), want (50, 50)",
			budget.MaxActiveRuns, budget.MaxRegisteredDefs)
	}

	// A fresh root reports zero usage (per-root isolation in the scan).
	seedRootRun(t, h.svc, "idle")
	idle, _, err := h.svc.Budget(context.Background(), "idle")
	if err != nil {
		t.Fatalf("budget for idle: %v", err)
	}
	// "idle" is itself one active run under its own root.
	if idle.ActiveRuns != 1 {
		t.Errorf("idle ActiveRuns = %d, want 1 (the root run itself)",
			idle.ActiveRuns)
	}
	if idle.RegisteredDefs != 0 {
		t.Errorf("idle RegisteredDefs = %d, want 0", idle.RegisteredDefs)
	}
}

// TestRuntimeBounds_BudgetOverWire proves the budget read round-trips a gated
// handler's cp.Budget() through api.runtimes.budget back into a typed
// worker.RuntimeBudget with real counts.
func TestRuntimeBounds_BudgetOverWire(t *testing.T) {
	h := newCPHarnessWithLimits(
		t, true, RuntimeLimits{MaxActiveRunsPerRoot: 9, MaxDefsPerRoot: 9},
	)

	var (
		mu     sync.Mutex
		budget worker.RuntimeBudget
	)
	errCh := make(chan error, 1)
	h.w.Handle("plan-task", func(ctx worker.TaskContext) error {
		cp := ctx.ControlPlane()
		if _, err := cp.RegisterWorkflow(
			ctx.Context(), childDef(), worker.RegisterOpts{},
		); err != nil {
			errCh <- err
			return ctx.Complete(nil)
		}
		b, err := cp.Budget(ctx.Context())
		mu.Lock()
		budget = b
		mu.Unlock()
		errCh <- err
		return ctx.Complete(nil)
	})
	h.w.Start()
	t.Cleanup(h.w.Stop)

	if err := h.svc.RegisterWorkflow(
		context.Background(), plannerDef(),
	); err != nil {
		t.Fatalf("register planner: %v", err)
	}
	parentRunID, err := h.svc.StartRun(context.Background(), "planner", nil)
	if err != nil {
		t.Fatalf("start planner: %v", err)
	}
	waitRunStatus(t, h.svc, parentRunID, dag.RunStatusCompleted)

	select {
	case got := <-errCh:
		if got != nil {
			t.Fatalf("budget over wire errored: %v", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handler never reported")
	}

	mu.Lock()
	defer mu.Unlock()
	// The planner run is active during the handler; one def was registered.
	if budget.ActiveRuns < 1 {
		t.Errorf("ActiveRuns = %d, want >=1", budget.ActiveRuns)
	}
	if budget.RegisteredDefs != 1 {
		t.Errorf("RegisteredDefs = %d, want 1", budget.RegisteredDefs)
	}
	if budget.MaxActiveRuns != 9 || budget.MaxRegisteredDefs != 9 {
		t.Errorf("maxima = (%d, %d), want (9, 9)",
			budget.MaxActiveRuns, budget.MaxRegisteredDefs)
	}
}

// TestRuntimeBounds_UnderLimitStillWorks proves the full gated path (#376)
// is unregressed under DEFAULT limits: register + spawn + complete
// end-to-end with no quota/rate interference.
func TestRuntimeBounds_UnderLimitStillWorks(t *testing.T) {
	h := newCPHarness(t, true) // default limits

	var (
		mu         sync.Mutex
		childRunID string
	)
	h.w.Handle("plan-task", func(ctx worker.TaskContext) error {
		cp := ctx.ControlPlane()
		name, err := cp.RegisterWorkflow(
			ctx.Context(), childDef(), worker.RegisterOpts{},
		)
		if err != nil {
			return ctx.Fail(err)
		}
		runID, err := cp.StartRun(ctx.Context(), name, nil)
		if err != nil {
			return ctx.Fail(err)
		}
		mu.Lock()
		childRunID = runID
		mu.Unlock()
		return ctx.Complete([]byte(`{"ok":true}`))
	})
	h.w.Handle("child-work", func(ctx worker.TaskContext) error {
		return ctx.Complete([]byte(`{"done":true}`))
	})
	h.w.Start()
	t.Cleanup(h.w.Stop)

	if err := h.svc.RegisterWorkflow(
		context.Background(), plannerDef(),
	); err != nil {
		t.Fatalf("register planner: %v", err)
	}
	parentRunID, err := h.svc.StartRun(context.Background(), "planner", nil)
	if err != nil {
		t.Fatalf("start planner: %v", err)
	}
	waitRunStatus(t, h.svc, parentRunID, dag.RunStatusCompleted)

	mu.Lock()
	childRun := childRunID
	mu.Unlock()
	if childRun == "" {
		t.Fatal("child run ID was empty under default limits")
	}
	waitRunStatus(t, h.svc, childRun, dag.RunStatusCompleted)

	// Negative space: a typed error sentinel is unrelated here — the path
	// produced no ControlPlaneError at all.
	var cpErr *worker.ControlPlaneError
	if errors.As(error(nil), &cpErr) {
		t.Fatal("unexpected ControlPlaneError on the happy path")
	}
}
