// api/controlplane_grant_test.go
// Integration tests for the capability-grant policy, per-dispatch nonce
// run-binding, promotion authorization, and audit (#380, ADR-021 Phase A).
// These wire the SAME real stack as controlplane_integration_test.go — an
// embedded NATS server, orchestrator (carrying a grant-policy holder),
// api service + NATS endpoints, and a real worker — proving the full
// security-hardened path end to end. Methodology: fresh server per test;
// bounded <=10s waits; every test asserts positive AND negative space.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/auditkv"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go/jetstream"
)

// TestGrant_GrantedWorkflowGetsHandle proves a workflow on the grant list
// receives a non-nil control-plane handle and writes its scoped def. This
// is the positive control: deny-by-default does NOT strip a granted step.
func TestGrant_GrantedWorkflowGetsHandle(t *testing.T) {
	// planner is granted; the holder seeds it.
	h := newCPHarnessWithGrant(t, []string{"planner"}, nil)

	var (
		mu         sync.Mutex
		scopedSeen string
	)
	h.w.Handle("plan-task", func(ctx worker.TaskContext) error {
		cp := ctx.ControlPlane()
		if cp == nil {
			return ctx.Fail(errors.New("granted workflow must get a handle"))
		}
		name, err := cp.RegisterWorkflow(
			ctx.Context(), childDef(), worker.RegisterOpts{},
		)
		if err != nil {
			return ctx.Fail(err)
		}
		mu.Lock()
		scopedSeen = name
		mu.Unlock()
		return ctx.Complete([]byte(`{"ok":true}`))
	})
	h.w.Start()
	t.Cleanup(h.w.Stop)

	parentRunID := startPlanner(t, h)
	waitRunStatus(t, h.svc, parentRunID, dag.RunStatusCompleted)

	mu.Lock()
	scoped := scopedSeen
	mu.Unlock()
	wantScoped := "agent." + parentRunID + ".do-step"
	if scoped != wantScoped {
		t.Fatalf("scopedName = %q, want %q", scoped, wantScoped)
	}
	if _, err := h.svc.GetWorkflow(wantScoped); err != nil {
		t.Fatalf("granted def must be written: %v", err)
	}
}

// TestGrant_UngrantedDeclaringWorkflowStripped proves a workflow that
// DECLARES the control-plane capability but is NOT on the grant list gets a
// nil handle (the capability is stripped at the enqueue payload source) and
// writes no agent.* def.
func TestGrant_UngrantedDeclaringWorkflowStripped(t *testing.T) {
	// Empty grant list: planner declares control-plane but is denied.
	h := newCPHarnessWithGrant(t, nil, nil)

	gotNil := make(chan bool, 1)
	h.w.Handle("plan-task", func(ctx worker.TaskContext) error {
		gotNil <- ctx.ControlPlane() == nil
		return ctx.Complete(nil)
	})
	h.w.Start()
	t.Cleanup(h.w.Stop)

	runID := startPlanner(t, h)
	waitRunStatus(t, h.svc, runID, dag.RunStatusCompleted)

	select {
	case isNil := <-gotNil:
		if !isNil {
			t.Fatal("ungranted declaring workflow must get a nil handle")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handler never ran")
	}
	assertNoAgentDef(t, h)
}

// TestGrant_HotReloadFlipsGrant proves the grant flips live: with an empty
// policy the handle is nil; after holder.Store grants the workflow and the
// run is re-enqueued, the handle is present. No watcher machinery — the
// holder swap is the whole mechanism.
func TestGrant_HotReloadFlipsGrant(t *testing.T) {
	h := newCPHarnessWithGrant(t, nil, nil) // start denied

	handleState := make(chan bool, 2) // true == non-nil handle
	h.w.Handle("plan-task", func(ctx worker.TaskContext) error {
		handleState <- ctx.ControlPlane() != nil
		return ctx.Complete(nil)
	})
	h.w.Start()
	t.Cleanup(h.w.Stop)

	runID := startPlanner(t, h)
	waitRunStatus(t, h.svc, runID, dag.RunStatusCompleted)
	select {
	case got := <-handleState:
		if got {
			t.Fatal("before grant: handle must be nil")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handler never ran (pre-grant)")
	}

	// Flip the grant live, then run a fresh planner.
	h.grant.Store(engine.NewGrantPolicy([]string{"planner"}, nil))
	runID2 := startPlanner(t, h)
	waitRunStatus(t, h.svc, runID2, dag.RunStatusCompleted)
	select {
	case got := <-handleState:
		if !got {
			t.Fatal("after grant: handle must be present")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handler never ran (post-grant)")
	}
}

// TestGrant_NonceBindingRejectsForgedRun proves the per-dispatch nonce
// binds the caller to its exact dispatch: a register request echoing the
// WRONG nonce (a sibling-run forge) is rejected cpKindNamespace and writes
// no def, while the correct dispatch nonce succeeds.
func TestGrant_NonceBindingRejectsForgedRun(t *testing.T) {
	h := newCPHarnessWithGrant(t, []string{"planner"}, nil)

	// Seed a real, non-terminal owner run whose step carries a known
	// dispatch nonce — exactly the state a live gated dispatch is in.
	const realNonce = "real-dispatch-nonce"
	ownerRun := dag.WorkflowRun{
		RunID:      "owner-1",
		WorkflowID: "planner",
		Status:     dag.RunStatusRunning,
		Steps: map[string]dag.StepState{
			"plan": {
				Status:        dag.StepStatusRunning,
				DispatchNonce: realNonce,
			},
		},
	}
	if err := h.svc.store.Save(context.Background(), ownerRun); err != nil {
		t.Fatalf("seed owner run: %v", err)
	}

	// Forge: a sibling-run worker echoing a WRONG nonce → namespace denial.
	// It cannot know "owner-1"'s nonce — the nonce never left that dispatch.
	if kind, err := h.svc.VerifyDispatch(
		context.Background(), "owner-1", "plan", "forged-nonce",
	); err == nil || kind != cpKindNamespace {
		t.Fatalf("wrong nonce: kind=%q err=%v, want namespace denial", kind, err)
	}
	// Empty nonce → namespace denial (a request must carry the proof).
	if kind, err := h.svc.VerifyDispatch(
		context.Background(), "owner-1", "plan", "",
	); err == nil || kind != cpKindNamespace {
		t.Fatalf("empty nonce: kind=%q err=%v, want namespace denial", kind, err)
	}
	// Claiming a foreign run id that does not exist → namespace denial.
	if kind, err := h.svc.VerifyDispatch(
		context.Background(), "no-such-run", "plan", realNonce,
	); err == nil || kind != cpKindNamespace {
		t.Fatalf("foreign run: kind=%q err=%v, want namespace denial", kind, err)
	}
	// The correct dispatch nonce on the real owner run/step → success.
	if kind, err := h.svc.VerifyDispatch(
		context.Background(), "owner-1", "plan", realNonce,
	); err != nil || kind != "" {
		t.Fatalf("correct nonce: kind=%q err=%v, want success", kind, err)
	}
}

// TestGrant_WireForgeRejected proves the handler→VerifyDispatch threading
// under ADVERSARIAL wire input: a register request marshalled and sent over
// the real NATS subject with a FOREIGN nonce is rejected cpKindNamespace,
// writes no def, and lands a denied audit row — exercising the production
// path a forging worker would actually take (not just the direct unit call).
func TestGrant_WireForgeRejected(t *testing.T) {
	h := newCPHarnessWithGrant(t, []string{"planner"}, nil)

	// A real, non-terminal owner run with a known nonce.
	const realNonce = "wire-real-nonce"
	if err := h.svc.store.Save(context.Background(), dag.WorkflowRun{
		RunID: "wire-owner", WorkflowID: "planner",
		Status: dag.RunStatusRunning,
		Steps: map[string]dag.StepState{
			"plan": {Status: dag.StepStatusRunning, DispatchNonce: realNonce},
		},
	}); err != nil {
		t.Fatalf("seed owner run: %v", err)
	}

	// Forge over the wire: a foreign nonce on the real subject.
	req := map[string]any{
		"def": dag.WorkflowDef{
			Name: "forged", Version: "1",
			Steps: []dag.StepDef{
				{ID: "s", Task: "t", Type: dag.StepTypeNormal},
			},
		},
		"owner_run_id":  "wire-owner",
		"owner_step_id": "plan",
		"nonce":         "FORGED-not-the-real-nonce",
	}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	reply, err := h.nc.Request("api.runtimes.register", data, 5*time.Second)
	if err != nil {
		t.Fatalf("wire request: %v", err)
	}
	var resp struct {
		ScopedName string `json:"scoped_name"`
		Error      string `json:"error"`
		Kind       string `json:"kind"`
	}
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		t.Fatalf("unmarshal reply: %v", err)
	}
	if resp.Kind != cpKindNamespace {
		t.Fatalf("wire forge kind = %q, want %q (err=%q)",
			resp.Kind, cpKindNamespace, resp.Error)
	}
	if resp.ScopedName != "" {
		t.Fatalf("wire forge must not register a def, got %q", resp.ScopedName)
	}
	// No def written under the forged author name.
	if _, gerr := h.svc.GetWorkflow("agent.wire-owner.forged"); gerr == nil {
		t.Fatal("forged register must not write a def")
	}
	// A denied audit row landed for the register action.
	rows := readAudit(t, h)
	if !auditHas(rows, "runtime.register", "denied") {
		t.Fatalf("wire forge must emit a denied register audit row; got %v", rows)
	}
}

// TestGrant_AgentLoopContinueKeepsControlPlane is the BLOCKING-bug
// regression (#380): a GRANTED agent-loop step that Continues (re-enqueued
// via PublishIteration) must STILL receive a non-nil control-plane handle
// AND its RegisterWorkflow must succeed — i.e. the re-enqueue stamps a fresh
// nonce that matches the persisted step state, so VerifyDispatch passes
// rather than over-denying the durable agent loop.
func TestGrant_AgentLoopContinueKeepsControlPlane(t *testing.T) {
	h := newCPHarnessWithGrant(t, []string{"looper"}, nil)

	type result struct {
		handleNonNil bool
		registerErr  error
	}
	done := make(chan result, 1)
	var iteration atomic.Int32
	h.w.Handle("loop-task", func(ctx worker.TaskContext) error {
		if iteration.Add(1) == 1 {
			// First pass: Continue to force a PublishIteration re-enqueue.
			return ctx.Continue([]byte(`{"again":true}`))
		}
		// Second pass (the re-enqueued iteration): the handle must survive
		// and a control-plane call must succeed.
		cp := ctx.ControlPlane()
		if cp == nil {
			done <- result{handleNonNil: false}
			return ctx.Complete(nil)
		}
		_, err := cp.RegisterWorkflow(
			ctx.Context(), childDef(), worker.RegisterOpts{},
		)
		done <- result{handleNonNil: true, registerErr: err}
		return ctx.Complete([]byte(`{"ok":true}`))
	})
	h.w.Start()
	t.Cleanup(h.w.Stop)

	loopDef := dag.WorkflowDef{
		Name: "looper", Version: "1",
		Steps: []dag.StepDef{{
			ID: "loop", Task: "loop-task", Type: dag.StepTypeAgentLoop,
			Config:               dag.MarshalConfig(&dag.AgentLoopConfig{MaxIterations: 5}),
			RequiredCapabilities: []string{"control-plane"},
		}},
	}
	if err := h.svc.RegisterWorkflow(context.Background(), loopDef); err != nil {
		t.Fatalf("register looper: %v", err)
	}
	if _, err := h.svc.StartRun(context.Background(), "looper", nil); err != nil {
		t.Fatalf("start looper: %v", err)
	}

	select {
	case got := <-done:
		if !got.handleNonNil {
			t.Fatal("agent-loop Continue dropped the control-plane handle")
		}
		if got.registerErr != nil {
			t.Fatalf("re-enqueued iteration register failed (nonce "+
				"over-denial regression): %v", got.registerErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("agent-loop handler never reached the second iteration")
	}
}

// TestPromote_UnauthorizedDenied proves promotion is governed by the same
// policy: a granted-but-not-promote-listed workflow is denied cpKindDenied
// with no promoted.* key; an authorized workflow succeeds.
func TestPromote_UnauthorizedDenied(t *testing.T) {
	// "promoter" is in the promote list; "planner" is granted but NOT.
	// The promote check keys on the AUTHOR name = def.Name.
	h := newCPHarnessWithGrant(t, []string{"planner", "promoter"}, []string{"promoter"})

	// Unauthorized author (def.Name "planner") → denied, no promoted key.
	denied := childDef()
	denied.Name = "planner"
	_, kind, err := h.svc.RegisterRuntimeWorkflow(
		context.Background(), denied, "owner-run", true,
	)
	if err == nil || kind != cpKindDenied {
		t.Fatalf("unauthorized promote: kind=%q err=%v, want denied", kind, err)
	}
	if _, gerr := h.svc.GetWorkflow("promoted.planner"); gerr == nil {
		t.Fatal("denied promote must not write a promoted.* key")
	}

	// Authorized author (def.Name "promoter") → success, promoted key.
	allowed := childDef()
	allowed.Name = "promoter"
	scoped, kind, err := h.svc.RegisterRuntimeWorkflow(
		context.Background(), allowed, "owner-run", true,
	)
	if err != nil || kind != "" {
		t.Fatalf("authorized promote: kind=%q err=%v, want success", kind, err)
	}
	if scoped != "promoted.promoter" {
		t.Fatalf("scoped = %q, want promoted.promoter", scoped)
	}
	if _, gerr := h.svc.GetWorkflow("promoted.promoter"); gerr != nil {
		t.Fatalf("authorized promote must write promoted.* key: %v", gerr)
	}
}

// TestAudit_RowsWritten proves the audit trail records both a denied
// promote and an authorized promote into the console_audit KV.
func TestAudit_RowsWritten(t *testing.T) {
	h := newCPHarnessWithGrant(t, []string{"promoter"}, []string{"promoter"})

	// One denied (author "planner" not in promote list) + one success
	// (author "promoter" authorized).
	deniedDef := childDef()
	deniedDef.Name = "planner"
	_, _, _ = h.svc.RegisterRuntimeWorkflow(
		context.Background(), deniedDef, "owner-run", true,
	)
	okDef := childDef()
	okDef.Name = "promoter"
	_, _, _ = h.svc.RegisterRuntimeWorkflow(
		context.Background(), okDef, "owner-run", true,
	)

	rows := readAudit(t, h)
	if !auditHas(rows, "runtime.promote", "success") {
		t.Fatalf("missing runtime.promote/success audit row; got %v", rows)
	}
	if !auditHas(rows, "runtime.promote", "denied") {
		t.Fatalf("missing runtime.promote/denied audit row; got %v", rows)
	}
}

// --- helpers ---

func startPlanner(t *testing.T, h *cpHarness) string {
	t.Helper()
	if err := h.svc.RegisterWorkflow(
		context.Background(), plannerDef(),
	); err != nil {
		t.Fatalf("register planner: %v", err)
	}
	runID, err := h.svc.StartRun(context.Background(), "planner", nil)
	if err != nil {
		t.Fatalf("start planner: %v", err)
	}
	return runID
}

func assertNoAgentDef(t *testing.T, h *cpHarness) {
	t.Helper()
	defs, err := h.svc.ListWorkflows(context.Background())
	if err != nil {
		t.Fatalf("list workflows: %v", err)
	}
	for _, d := range defs {
		if strings.HasPrefix(d.Name, "agent.") {
			t.Fatalf("unexpected agent-scoped def written: %q", d.Name)
		}
	}
}

func readAudit(t *testing.T, h *cpHarness) []auditkv.AuditEvent {
	t.Helper()
	js, err := jetstream.New(h.nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	kv, err := js.KeyValue(context.Background(), auditkv.Bucket)
	if err != nil {
		t.Fatalf("audit bucket: %v", err)
	}
	keys, err := kv.Keys(context.Background())
	if err != nil {
		t.Fatalf("audit keys: %v", err)
	}
	out := make([]auditkv.AuditEvent, 0, len(keys))
	for _, k := range keys {
		entry, err := kv.Get(context.Background(), k)
		if err != nil {
			continue
		}
		var evt auditkv.AuditEvent
		if err := json.Unmarshal(entry.Value(), &evt); err != nil {
			continue
		}
		out = append(out, evt)
	}
	return out
}

func auditHas(rows []auditkv.AuditEvent, action, outcome string) bool {
	for _, r := range rows {
		if r.Action == action && r.Outcome == outcome {
			return true
		}
	}
	return false
}
