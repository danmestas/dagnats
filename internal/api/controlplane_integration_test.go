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
	"github.com/danmestas/dagnats/internal/auditkv"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// cpHarness boots orchestrator + service + NATS API and returns the conn.
// Caller registers handlers on the returned worker and calls Start.
type cpHarness struct {
	nc    *nats.Conn
	svc   *Service
	w     *worker.Worker
	grant *engine.GrantPolicyHolder
}

func newCPHarness(t *testing.T, gated bool) *cpHarness {
	t.Helper()
	// Default limits ({}) resolve to the production defaults — the existing
	// tests assert today's behavior, which the defaults preserve.
	return newCPHarnessWithLimits(t, gated, RuntimeLimits{})
}

// newCPHarnessWithLimits is newCPHarness with explicit per-runtime bounds
// (#378). The quota/rate/depth tests pass reduced limits to trip the caps
// with a handful of registers/spawns instead of hundreds. It seeds a
// PERMISSIVE grant policy granting the test workflows ("planner") so the
// legacy gated handlers still receive a handle — deny-by-default is the
// production default, but these pre-#380 tests assume a grant (#380 test 7).
func newCPHarnessWithLimits(
	t *testing.T, gated bool, limits RuntimeLimits,
) *cpHarness {
	t.Helper()
	// Grant the workflow names the legacy tests use, and authorize promote
	// for the names they promote ("tool"), so deny-by-default (the #380
	// production default) does not break the pre-#380 gated paths.
	return newCPHarnessFull(t, gated, limits,
		engine.NewGrantPolicy(
			[]string{"planner", "tool"}, []string{"tool"}))
}

// newCPHarnessWithGrant boots the harness with an EXPLICIT grant policy
// (grant + promote lists) so the #380 grant/promote/audit tests can assert
// deny-by-default, stripping, and promotion authorization directly.
func newCPHarnessWithGrant(
	t *testing.T, grant, promote []string,
) *cpHarness {
	t.Helper()
	return newCPHarnessFull(t, true, RuntimeLimits{},
		engine.NewGrantPolicy(grant, promote))
}

// newCPHarnessFull is the shared boot path: it wires a grant-policy holder
// into BOTH the orchestrator (which shares it with the TaskPublisher for the
// enqueue-time capability strip) AND the api service (for promotion
// authorization), plus the console_audit KV for the audit-trail assertions.
func newCPHarnessFull(
	t *testing.T, gated bool, limits RuntimeLimits, policy *engine.GrantPolicy,
) *cpHarness {
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
	holder := &engine.GrantPolicyHolder{}
	holder.Store(policy)

	orch := engine.NewOrchestrator(nc, engine.WithGrantPolicyHolder(holder))
	orch.Start()
	t.Cleanup(orch.Stop)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	auditKV, err := auditkv.NewKV(context.Background(), js)
	if err != nil {
		t.Fatalf("audit KV: %v", err)
	}
	svc := NewServiceWithLimits(nc, limits,
		WithGrantPolicyHolder(holder), WithAuditKV(auditKV))
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
	return &cpHarness{nc: nc, svc: svc, w: w, grant: holder}
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
	// missing-def check) is what rejects. resolveRootRunID now loads the
	// owner run, so seed a top-level "owner-seed" run first (#377 fix-4:
	// a load miss is a real error, not a silent self-root).
	seedRootRun(t, h.svc, "owner-seed")
	if _, _, err := h.svc.RegisterRuntimeWorkflow(
		context.Background(), childDef(), "owner-seed", false,
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

// seedRootRun persists a top-level (self-rooting) running run snapshot so
// resolveRootRunID can load it. Used where a test calls
// RegisterRuntimeWorkflow directly without first starting a real run.
func seedRootRun(t *testing.T, svc *Service, runID string) {
	t.Helper()
	store := engine.NewSnapshotStore(svc.js)
	def := dag.WorkflowDef{
		Name: "seed", Version: "1",
		Steps: []dag.StepDef{{ID: "s", Task: "t", Type: dag.StepTypeNormal}},
	}
	run := dag.NewWorkflowRun(def, runID)
	run.RootRunID = runID
	run.Status = dag.RunStatusRunning
	if err := store.Save(context.Background(), run); err != nil {
		t.Fatalf("seed root run %q: %v", runID, err)
	}
}

// TestControlPlane_SharedRootAcrossLineage proves #377's tree-root
// namespacing: a top-level run and a child run that each register an
// ephemeral def both scope under the SAME root (the top-level run ID), and
// the depth cap still rejects beyond the bound (regression).
func TestControlPlane_SharedRootAcrossLineage(t *testing.T) {
	h := newCPHarness(t, true)

	// Top-level run "top" (self-rooting) and a child run "kid" whose parent
	// is "top". createChildRun would stamp kid.RootRunID = root("top") =
	// "top"; we seed that lineage directly to drive the resolver.
	store := engine.NewSnapshotStore(h.svc.js)
	def := dag.WorkflowDef{
		Name: "lin", Version: "1",
		Steps: []dag.StepDef{{ID: "s", Task: "t", Type: dag.StepTypeNormal}},
	}
	top := dag.NewWorkflowRun(def, "top")
	top.RootRunID = "top"
	top.Status = dag.RunStatusRunning
	if err := store.Save(context.Background(), top); err != nil {
		t.Fatalf("save top: %v", err)
	}
	kid := dag.NewWorkflowRun(def, "kid")
	kid.ParentRunID = "top"
	kid.RootRunID = "top" // inherited from parent (#377)
	kid.Status = dag.RunStatusRunning
	if err := store.Save(context.Background(), kid); err != nil {
		t.Fatalf("save kid: %v", err)
	}

	topScoped, _, err := h.svc.RegisterRuntimeWorkflow(
		context.Background(), childDef(), "top", false,
	)
	if err != nil {
		t.Fatalf("register from top: %v", err)
	}
	kidDef := childDef()
	kidDef.Name = "other-step"
	kidScoped, _, err := h.svc.RegisterRuntimeWorkflow(
		context.Background(), kidDef, "kid", false,
	)
	if err != nil {
		t.Fatalf("register from kid: %v", err)
	}

	if topScoped != "agent.top.do-step" {
		t.Fatalf("top scoped = %q, want agent.top.do-step", topScoped)
	}
	// The child's def scopes under the SAME root "top", not under "kid".
	if kidScoped != "agent.top.other-step" {
		t.Fatalf("kid scoped = %q, want agent.top.other-step "+
			"(shared root)", kidScoped)
	}

	// Regression: the depth cap still rejects a spawn beyond the bound.
	deepest := buildRunChain(t, h.svc, engine.MaxNestingDepth)
	_, kind, err := h.svc.SpawnChildRun(
		context.Background(), topScoped, deepest, "anchor", nil,
	)
	if err == nil || kind != cpKindDepthExceeded {
		t.Fatalf("expected depth rejection, got kind=%q err=%v", kind, err)
	}
}

// TestControlPlane_PromotionSurvivesReaper proves #377 Part C: a gated
// handler's RegisterWorkflow(opts{Promote:true}) returns a "promoted."-
// prefixed name over the full wire path (no ErrPromotionUnsupported), and
// the resulting def is reaper-invisible because its key carries no agent.
// prefix — the only keys the reaper's prefix gate selects.
func TestControlPlane_PromotionSurvivesReaper(t *testing.T) {
	h := newCPHarness(t, true)

	nameCh := make(chan string, 1)
	errCh := make(chan error, 1)
	h.w.Handle("plan-task", func(ctx worker.TaskContext) error {
		cp := ctx.ControlPlane()
		name, err := cp.RegisterWorkflow(
			ctx.Context(), promoteDef(),
			worker.RegisterOpts{Promote: true},
		)
		nameCh <- name
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
			t.Fatalf("promote register errored: %v", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handler never reported")
	}
	name := <-nameCh
	if name != "promoted.tool" {
		t.Fatalf("promoted name = %q, want promoted.tool", name)
	}

	// Survivor: the def exists, and rootFromDefKey rejects its key (no
	// agent. prefix) so a reaper pass would never select it.
	if _, err := h.svc.GetWorkflow("promoted.tool"); err != nil {
		t.Fatalf("promoted def missing from KV: %v", err)
	}
}

// promoteDef is the def a gated handler promotes to the reaper-immune
// "promoted." namespace.
func promoteDef() dag.WorkflowDef {
	return dag.WorkflowDef{
		Name: "tool", Version: "1",
		Steps: []dag.StepDef{
			{ID: "work", Task: "child-work", Type: dag.StepTypeNormal},
		},
	}
}

// TestControlPlane_ResolveRootMissingReturnsTypedError proves #377 fix-4: a
// resolveRootRunID load-miss returns a typed cpKindNamespace error rather
// than silently self-rooting.
func TestControlPlane_ResolveRootMissingReturnsTypedError(t *testing.T) {
	h := newCPHarness(t, true)

	// No run snapshot exists for "ghost".
	scoped, kind, err := h.svc.RegisterRuntimeWorkflow(
		context.Background(), childDef(), "ghost", false,
	)
	if err == nil {
		t.Fatalf("expected error on missing owner run, got scoped %q", scoped)
	}
	if kind != cpKindNamespace {
		t.Fatalf("kind = %q, want %q", kind, cpKindNamespace)
	}
	// Negative space: nothing was written under any namespace.
	if scoped != "" {
		t.Fatalf("expected empty scoped on miss, got %q", scoped)
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
	root := ""
	var deepest string
	for i := 0; i < length; i++ {
		runID := "chain-run-" + string(rune('a'+i))
		run := dag.NewWorkflowRun(def, runID)
		run.Status = dag.RunStatusRunning
		run.ParentRunID = prev
		if root == "" {
			root = runID // the head of the chain is its own tree-root
		}
		run.RootRunID = root // descendants share the root (#377 prod shape)
		if err := store.Save(context.Background(), run); err != nil {
			t.Fatalf("save chain run %d: %v", i, err)
		}
		prev = runID
		deepest = runID
	}
	return deepest
}
