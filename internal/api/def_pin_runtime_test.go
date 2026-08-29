// api/def_pin_runtime_test.go
//
// Review fix for #637 blocker 2: ephemeral/scoped runtime defs
// (registerRuntimeWorkflow, internal/api/runtimes.go) used to write
// the workflow_defs pointer directly with defKV.Put, bypassing
// persistDef entirely. Since dag.NewWorkflowRun stamps DefHash
// unconditionally, every run spawned from a runtime def carried a pin
// to a version key that was NEVER WRITTEN -- with blocker 1's
// fail-loud fix, that made every agent-loop run fail its first
// advance. persistDef now backs both RegisterWorkflow and
// registerRuntimeWorkflow, and the def-quota scanners
// (countDefsForRoot, defCountsByRoot) must not double-count the
// version key it adds.
// Methodology: real embedded NATS server + orchestrator + worker via
// the existing cpHarness (controlplane_integration_test.go). Bounded
// <=10s waits.
package api

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/worker"
)

func TestRuntimeDefRunAdvancesWithResolvablePin(t *testing.T) {
	h := newCPHarness(t, true)

	var (
		mu         sync.Mutex
		scopedName string
		childRunID string
	)
	h.w.Handle("plan-task", func(ctx worker.TaskContext) error {
		cp := ctx.ControlPlane()
		if cp == nil {
			return ctx.Fail(errors.New("expected non-nil control plane"))
		}
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
		scopedName, childRunID = name, runID
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
	parentRunID, err := h.svc.StartRun(
		context.Background(), "planner", nil,
	)
	if err != nil {
		t.Fatalf("start planner: %v", err)
	}
	waitRunStatus(t, h.svc, parentRunID, dag.RunStatusCompleted)

	mu.Lock()
	scoped, childRun := scopedName, childRunID
	mu.Unlock()
	if scoped == "" || childRun == "" {
		t.Fatalf("gated handler never ran: scoped=%q childRun=%q",
			scoped, childRun)
	}

	// Positive: the child run, spawned from a runtime def, ADVANCES
	// AND COMPLETES -- before this fix it would get stuck on its
	// first advance with a "missing pinned version" error.
	childSnap := waitRunStatus(t, h.svc, childRun, dag.RunStatusCompleted)

	// Positive: the pin is resolvable -- the run carries a DefHash,
	// and the exact version key it names actually exists in
	// workflow_defs (persistDef wrote it, not a raw defKV.Put).
	if childSnap.DefHash == "" {
		t.Fatalf("child run.DefHash is empty, want a resolvable pin")
	}
	versionKey := dag.DefVersionKey(scoped, childSnap.DefHash)
	if _, err := h.svc.defKV.Get(
		context.Background(), versionKey,
	); err != nil {
		t.Fatalf("pinned version key %q not found: %v", versionKey, err)
	}

	// Negative: the def-quota scan for the parent root counts exactly
	// ONE def (the scoped pointer), not two (pointer + version key) --
	// the countDefsForRoot fix from the same review round.
	count, err := h.svc.countDefsForRoot(context.Background(), parentRunID)
	if err != nil {
		t.Fatalf("countDefsForRoot: %v", err)
	}
	if count != 1 {
		t.Fatalf("countDefsForRoot(%q) = %d, want 1 (version key must "+
			"not be double-counted)", parentRunID, count)
	}

	// Same fix, the batch-counting sibling: defCountsByRoot must also
	// exclude the version key.
	counts, err := h.svc.defCountsByRoot(
		context.Background(), map[string]int{parentRunID: 0},
	)
	if err != nil {
		t.Fatalf("defCountsByRoot: %v", err)
	}
	if counts[parentRunID] != 1 {
		t.Fatalf("defCountsByRoot[%q] = %d, want 1 (version key must "+
			"not be double-counted)", parentRunID, counts[parentRunID])
	}
}
