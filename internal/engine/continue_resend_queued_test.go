// internal/engine/continue_resend_queued_test.go
// Resend of a continue saved by an engine that died before publishing
// it while the step was still Queued (ADR-026, finding 3). In a
// cluster the engines share one history consumer, so a step.continue
// can be accepted (by nonce, #814) before the delivery's step.started
// is applied: the step stays Queued at the next iteration. A resend
// planned for a Queued step used to be a first dispatch, whose msg-id
// {run}.{step}.queued TASK_QUEUES had already seen: the iteration was
// never sent.
//
// Methodology: an orchestrator on an embedded NATS server that crashes
// at the save checkpoint, the test standing in for the worker; the
// first dispatch's message is published and consumed first so its
// msg-id is inside the stream's dedup window, as in production. Every
// wait is bounded.
package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestStepContinue_CrashBetweenSaveAndPublishWhileQueued_Resends(t *testing.T) {
	wfDef := loopDef("queued-crash-wf", "queued-crash-task", dag.AgentLoopConfig{}, nil)
	fx := newLoopFixture(t, wfDef)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const runID = "queued-crash-run"
	run := dag.NewWorkflowRun(wfDef, runID)
	run.Status = dag.RunStatusRunning
	run.DefHash = dag.DefHash(wfDef)
	// Queued on attempt 1: the continue below is accepted by nonce
	// before any step.started lands, as the second engine sees it.
	run.Steps["agent"] = dag.StepState{
		Status: dag.StepStatusQueued, Attempts: 1, DispatchNonce: "nonce-0",
	}
	if err := NewOrchestrator(fx.nc).store.SaveInitial(ctx, run); err != nil {
		t.Fatalf("SaveInitial: %v", err)
	}
	// The first dispatch landed and a worker took it: its msg-id is
	// in TASK_QUEUES' dedup window.
	first := protocol.TaskPayload{
		RunID: runID, StepID: "agent", Attempt: 1, DispatchNonce: "nonce-0",
	}
	mustPublish(t, fx.js, "task.queued-crash-task."+runID, mustMarshal(t, first),
		nats.MsgId(runID+".agent.queued"))
	if got := fx.takeTask(t, 5*time.Second); got.DispatchNonce != "nonce-0" {
		t.Fatalf("first dispatch nonce = %q, want nonce-0", got.DispatchNonce)
	}

	committed := []byte(`{"turn":1}`)
	evt := identityEvent(protocol.EventStepContinue, first, 1, continuePayload(t, committed))
	err := crashAt(fx.nc, checkpointSaved).handleEventWithConflictRetry(ctx, evt)
	if !errors.Is(err, errSimulatedCrash) {
		t.Fatalf("first handle: err = %v, want the simulated crash", err)
	}
	saved, err := fx.store.Load(ctx, runID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	s := saved.Steps["agent"]
	if s.Status != dag.StepStatusQueued || s.Iterations != 1 || !s.DispatchUnsent {
		t.Fatalf("after crash: status %v iterations=%d unsent=%v, want queued, 1, true",
			s.Status, s.Iterations, s.DispatchUnsent)
	}
	fx.noTask(t, 300*time.Millisecond)

	// The restarted engine gets the unacked continue again: the fence
	// drops it (iteration 0 is done) and the unsent mark must send
	// iteration 1 as a continue, under its own msg-id.
	if err := NewOrchestrator(fx.nc).handleEventWithConflictRetry(ctx, evt); err != nil {
		t.Fatalf("redelivered handle: %v", err)
	}
	next := fx.takeTask(t, 5*time.Second)
	if next.Iteration != 1 || next.Attempt != 1 || string(next.State) != string(committed) {
		t.Fatalf("resent task = iteration %d attempt %d state %s, "+
			"want iteration 1 attempt 1 with the committed state",
			next.Iteration, next.Attempt, next.State)
	}
	fx.noTask(t, 500*time.Millisecond)
	after, err := fx.store.Load(ctx, runID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if s := after.Steps["agent"]; s.Iterations != 1 || s.DispatchUnsent {
		t.Fatalf("after resend: iterations=%d unsent=%v, want 1 and sent", s.Iterations, s.DispatchUnsent)
	}
}
