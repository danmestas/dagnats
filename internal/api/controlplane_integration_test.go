// api/controlplane_integration_test.go
// Integration tests for the agent-runtime control plane (#376, ADR-021
// Phase A). These wire a real embedded NATS server, orchestrator, api
// service + NATS endpoints, and a real worker holding a worker.ControlPlane
// handle — proving the full gated path: a granted handler registers an
// ephemeral def and spawns a child run that completes with correct
// lineage. Methodology: fresh server per test; bounded <=10s waits; every
// test asserts positive AND negative space. Living in internal/api lets
// the test wire engine + worker + api together (the worker package alone
// cannot import them).
package api

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

// cpHarness boots orchestrator + service + NATS API and returns the conn.
// Caller registers handlers on the returned worker and calls Start.
type cpHarness struct {
	nc  *nats.Conn
	svc *Service
	w   *worker.Worker
}

func newCPHarness(t *testing.T, gated bool) *cpHarness {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	// Size the per-stream MaxBytes ceilings off a small budget so the sum
	// fits a disk-constrained sandbox; the default 10 GiB budget can make
	// JetStream refuse stream creation (err_code=10047) when free disk is
	// tight. Functionally identical — these tests move kilobytes.
	if err := natsutil.SetupAll(
		nc, natsutil.WithStoreBudget(256<<20),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(orch.Stop)

	svc := NewService(nc)
	natsAPI := NewNATSAPI(svc, nc, "1.0.0")
	natsAPI.Start()
	t.Cleanup(natsAPI.Stop)

	var opts []worker.WorkerOption
	if gated {
		opts = append(opts, worker.WithControlPlane(
			worker.NewControlPlane(nc),
		))
	}
	w := worker.NewWorker(nc, opts...)
	return &cpHarness{nc: nc, svc: svc, w: w}
}

// childDef is the ephemeral def a planner registers at runtime.
func childDef() dag.WorkflowDef {
	return dag.WorkflowDef{
		Name:    "do-step",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "work", Task: "child-work", Type: dag.StepTypeNormal},
		},
	}
}

// plannerDef is the parent def whose "plan" step is gated.
func plannerDef() dag.WorkflowDef {
	return dag.WorkflowDef{
		Name:    "planner",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:                   "plan",
				Task:                 "plan-task",
				Type:                 dag.StepTypeNormal,
				RequiredCapabilities: []string{"control-plane"},
			},
		},
	}
}

// waitRunStatus polls GetRun until status matches or the deadline passes.
func waitRunStatus(
	t *testing.T, svc *Service, runID string, want dag.RunStatus,
) dag.WorkflowRun {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		run, err := svc.GetRun(context.Background(), runID)
		if err == nil && run.Status == want {
			return run
		}
		select {
		case <-deadline:
			t.Fatalf("run %s did not reach %v within 10s (last err %v)",
				runID, want, err)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestControlPlane_GatedHandlerRegistersAndStartsChild(t *testing.T) {
	h := newCPHarness(t, true)

	var (
		mu          sync.Mutex
		scopedSeen  string
		childRunSee string
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
		scopedSeen, childRunSee = name, runID
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

	// Parent completes only after the gated handler succeeds.
	waitRunStatus(t, h.svc, parentRunID, dag.RunStatusCompleted)

	mu.Lock()
	scoped, childRun := scopedSeen, childRunSee
	mu.Unlock()

	wantScoped := "agent." + parentRunID + ".do-step"
	if scoped != wantScoped {
		t.Fatalf("scopedName = %q, want %q", scoped, wantScoped)
	}
	if childRun == "" {
		t.Fatal("child run ID was empty")
	}

	// Child run completes and carries lineage back to the parent.
	childSnap := waitRunStatus(
		t, h.svc, childRun, dag.RunStatusCompleted,
	)
	if childSnap.ParentRunID != parentRunID {
		t.Fatalf("child ParentRunID = %q, want %q",
			childSnap.ParentRunID, parentRunID)
	}

	// The scoped def exists in workflow_defs under the scoped key.
	if _, err := h.svc.GetWorkflow(wantScoped); err != nil {
		t.Fatalf("scoped def not found in KV: %v", err)
	}
}

func TestControlPlane_UngatedHandlerGetsNil(t *testing.T) {
	// Worker built WITHOUT WithControlPlane; even a step declaring the
	// capability gets a nil handle (deny-by-default at the deployment
	// grant), and no agent:* def is ever written.
	h := newCPHarness(t, false)

	gotNil := make(chan bool, 1)
	h.w.Handle("plan-task", func(ctx worker.TaskContext) error {
		gotNil <- ctx.ControlPlane() == nil
		return ctx.Complete(nil)
	})
	h.w.Start()
	t.Cleanup(h.w.Stop)

	if err := h.svc.RegisterWorkflow(
		context.Background(), plannerDef(),
	); err != nil {
		t.Fatalf("register planner: %v", err)
	}
	runID, err := h.svc.StartRun(context.Background(), "planner", nil)
	if err != nil {
		t.Fatalf("start planner: %v", err)
	}
	waitRunStatus(t, h.svc, runID, dag.RunStatusCompleted)

	select {
	case isNil := <-gotNil:
		if !isNil {
			t.Fatal("ungated handler got a non-nil control plane")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handler never ran")
	}

	// No scoped def should have been written.
	defs, err := h.svc.ListWorkflows(context.Background())
	if err != nil {
		t.Fatalf("list workflows: %v", err)
	}
	for _, d := range defs {
		if len(d.Name) >= 6 && d.Name[:6] == "agent." {
			t.Fatalf("unexpected agent-scoped def written: %q", d.Name)
		}
	}
}

func TestControlPlane_InvalidDefReturnsTypedErrorNoPanic(t *testing.T) {
	h := newCPHarness(t, true)

	errCh := make(chan error, 1)
	h.w.Handle("plan-task", func(ctx worker.TaskContext) error {
		cp := ctx.ControlPlane()
		// Zero-step def is rejected by dag.Validate server-side.
		zero := dag.WorkflowDef{Name: "empty", Version: "1"}
		_, err := cp.RegisterWorkflow(
			ctx.Context(), zero, worker.RegisterOpts{},
		)
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
	parentRunID, err := h.svc.StartRun(
		context.Background(), "planner", nil,
	)
	if err != nil {
		t.Fatalf("start planner: %v", err)
	}

	select {
	case got := <-errCh:
		var cpErr *worker.ControlPlaneError
		if !errors.As(got, &cpErr) ||
			cpErr.Kind != worker.KindInvalidDef {
			t.Fatalf("expected KindInvalidDef, got %v", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handler never reported")
	}

	// Orchestrator + API still answer GetRun afterward — proves no panic
	// propagated out of the endpoint.
	waitRunStatus(t, h.svc, parentRunID, dag.RunStatusCompleted)
}

func TestControlPlane_NamespaceNameRejected(t *testing.T) {
	h := newCPHarness(t, true)

	errCh := make(chan error, 1)
	h.w.Handle("plan-task", func(ctx worker.TaskContext) error {
		cp := ctx.ControlPlane()
		def := childDef()
		def.Name = "bad:name"
		_, err := cp.RegisterWorkflow(
			ctx.Context(), def, worker.RegisterOpts{},
		)
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
	if _, err := h.svc.StartRun(
		context.Background(), "planner", nil,
	); err != nil {
		t.Fatalf("start planner: %v", err)
	}

	select {
	case got := <-errCh:
		var cpErr *worker.ControlPlaneError
		if !errors.As(got, &cpErr) ||
			cpErr.Kind != worker.KindNamespace {
			t.Fatalf("expected KindNamespace, got %v", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handler never reported")
	}

	// No KV write for the rejected name (no agent:* key at all).
	defs, err := h.svc.ListWorkflows(context.Background())
	if err != nil {
		t.Fatalf("list workflows: %v", err)
	}
	for _, d := range defs {
		if len(d.Name) >= 6 && d.Name[:6] == "agent." {
			t.Fatalf("unexpected scoped def written: %q", d.Name)
		}
	}
}

func TestControlPlane_DepthCapEnforced(t *testing.T) {
	// Build a parent chain at the cap, then call SpawnChildRun directly
	// at the boundary and assert it rejects with cpKindDepthExceeded —
	// proving the spawn-path reuse inherits the orchestrator's cap. This
	// is the key safety property: there is no depth-unchecked spawn path.
	h := newCPHarness(t, true)

	// Register the def the spawn will target so the depth check (not the
	// missing-def check) is what rejects.
	if _, _, err := h.svc.RegisterRuntimeWorkflow(
		context.Background(), childDef(), "owner-seed",
	); err != nil {
		t.Fatalf("seed register: %v", err)
	}
	scoped := scopeName("owner-seed", "do-step")

	// Manually persist a chain so the parent sits at the depth where one
	// more child breaches MaxNestingDepth. Depth is the count of
	// ParentRunID hops; we need depth+1 >= MaxNestingDepth at the parent.
	parentRunID := buildRunChain(t, h.svc, engine.MaxNestingDepth)

	runID, kind, err := h.svc.SpawnChildRun(
		context.Background(), scoped, parentRunID, "some-step", nil,
	)
	if err == nil {
		t.Fatalf("expected depth rejection, got runID %q", runID)
	}
	if kind != cpKindDepthExceeded {
		t.Fatalf("kind = %q, want %q", kind, cpKindDepthExceeded)
	}
	if runID != "" {
		t.Fatalf("expected empty runID on rejection, got %q", runID)
	}
}

func TestControlPlane_DepthCapEnforcedFullWirePath(t *testing.T) {
	// Full path: a gated worker handler calls cp.StartRun, which sends
	// api.runs.spawn, which rejects at the depth cap. This proves the
	// rejection round-trips through the NATS reply envelope back into a
	// *worker.ControlPlaneError{Kind: depth_exceeded} — not just that the
	// service-layer logic rejects (covered above). We place the planner
	// run itself at depth MaxNestingDepth-1 so the FIRST spawn its handler
	// attempts breaches the cap.
	h := newCPHarness(t, true)

	errCh := make(chan error, 1)
	h.w.Handle("plan-task", func(ctx worker.TaskContext) error {
		cp := ctx.ControlPlane()
		// Register a valid child first so the depth check (not a missing
		// def) is what rejects the spawn.
		name, err := cp.RegisterWorkflow(
			ctx.Context(), childDef(), worker.RegisterOpts{},
		)
		if err != nil {
			errCh <- err
			return ctx.Complete(nil)
		}
		_, err = cp.StartRun(ctx.Context(), name, nil)
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

	// Build ancestors so the planner run lands at depth MaxNestingDepth-1.
	// A chain of length MaxNestingDepth-1 has its deepest run at depth
	// MaxNestingDepth-2; spawning the planner under it puts the planner at
	// depth MaxNestingDepth-1, where one more spawn breaches the cap.
	deepestAncestor := buildRunChain(t, h.svc, engine.MaxNestingDepth-1)
	plannerRunID, kind, err := h.svc.SpawnChildRun(
		context.Background(), "planner", deepestAncestor, "anchor", nil,
	)
	if err != nil {
		t.Fatalf("spawn planner as child failed (kind %q): %v", kind, err)
	}
	if plannerRunID == "" {
		t.Fatal("planner child run ID was empty")
	}

	select {
	case got := <-errCh:
		var cpErr *worker.ControlPlaneError
		if !errors.As(got, &cpErr) ||
			cpErr.Kind != worker.KindDepthExceeded {
			t.Fatalf("expected KindDepthExceeded over the wire, got %v", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("gated handler never reported")
	}
}

// buildRunChain persists a linked chain of run snapshots of the given
// length and returns the deepest run's ID. Each run's ParentRunID points
// at the previous, so nestingDepth(deepest) == length-? — we build enough
// that a child of the returned run breaches the cap.
func buildRunChain(t *testing.T, svc *Service, length int) string {
	t.Helper()
	store := engine.NewSnapshotStore(svc.js)
	def := dag.WorkflowDef{
		Name: "chain", Version: "1",
		Steps: []dag.StepDef{{ID: "s", Task: "t", Type: dag.StepTypeNormal}},
	}
	prev := ""
	var deepest string
	for i := 0; i < length; i++ {
		runID := "chain-run-" + string(rune('a'+i))
		run := dag.NewWorkflowRun(def, runID)
		run.Status = dag.RunStatusRunning
		run.ParentRunID = prev
		if err := store.Save(context.Background(), run); err != nil {
			t.Fatalf("save chain run %d: %v", i, err)
		}
		prev = runID
		deepest = runID
	}
	return deepest
}
