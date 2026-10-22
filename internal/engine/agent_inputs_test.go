// internal/engine/agent_inputs_test.go
// Durable waits and input wakeups (ADR-027): a step.continue that asks
// for a delay is scheduled on SLEEP_TIMERS and survives an engine
// restart; one that waits for input holds no task until an input is
// queued, then gets its iteration; an input queued while the continue
// that commits the wait is still in flight wakes the step through the
// engine's re-check; a late wait.elapsed after an input wake does not
// send a second task; and the completion sweep settles queued inputs
// as unanswered, including one submitted after the step finished.
//
// Methodology: a real orchestrator on an embedded NATS server with the
// loop fixture of agent_loop_fencing_test.go; the test plays the worker
// (takes tasks, publishes step.started and step.continue with the
// dispatch identity) and the API (writes submissions through the inputs
// store and publishes step.input.queued). Every wait is bounded.
package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/inputs"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

const inputsTestTimeout = 10 * time.Second

func waitPayload(t *testing.T, wait protocol.ContinueWait) []byte {
	t.Helper()
	return mustMarshal(t, protocol.StepContinuePayload{Wait: &wait})
}

// submitInput plays the API: creates the submission and announces it.
func (fx loopFixture) submitInput(t *testing.T, runID, requestID string) *inputs.Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), inputsTestTimeout)
	defer cancel()
	store, err := inputs.Open(ctx, fx.jsNew)
	if err != nil {
		t.Fatalf("inputs.Open: %v", err)
	}
	if _, _, err := store.Submit(ctx, inputs.Submission{
		RunID: runID, StepID: "agent", RequestID: requestID, Mode: inputs.ModeSteer,
		Content: json.RawMessage(`"x"`),
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	fx.publishEvent(t, protocol.NewInputQueuedEvent(runID, "agent", requestID))
	return store
}

// shortenTaskDedup shrinks TASK_QUEUES' dedup window so a duplicate
// dispatch is not hidden by JetStream.
func (fx loopFixture) shortenTaskDedup(t *testing.T, window time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), inputsTestTimeout)
	defer cancel()
	stream, err := fx.jsNew.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	cfg := stream.CachedInfo().Config
	cfg.Duplicates = window
	if _, err := fx.jsNew.UpdateStream(ctx, cfg); err != nil {
		t.Fatalf("UpdateStream: %v", err)
	}
}

func (fx loopFixture) inputStatus(t *testing.T, store *inputs.Store, runID, requestID string) inputs.Status {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), inputsTestTimeout)
	defer cancel()
	entry, err := store.Get(ctx, runID, "agent", requestID)
	if err != nil {
		t.Fatalf("Get %s: %v", requestID, err)
	}
	return entry.Status
}

func TestContinueWait_DelaySurvivesEngineRestart(t *testing.T) {
	fx := newLoopFixture(t, loopDef("wait-delay-wf", "wait-delay-task", dag.AgentLoopConfig{}, nil))
	first := NewOrchestrator(fx.nc)
	first.Start()
	const runID = "wait-delay-run"
	fx.startRun(t, runID)
	task := fx.takeTask(t, 5*time.Second)
	attempt := fx.started(t, task)
	fx.publishEvent(t, identityEvent(protocol.EventStepContinue, task, attempt,
		waitPayload(t, protocol.ContinueWait{DelayMs: 1500})))

	run := fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return s.Iterations == 1 && s.WaitUntil != nil && !s.DispatchUnsent
	})
	if wake := run.Steps["agent"].WaitUntil; time.Until(*wake) > 1500*time.Millisecond {
		t.Fatalf("wake_at %s is further out than the delay", wake)
	}
	first.Stop()
	fx.noTask(t, 200*time.Millisecond)

	second := NewOrchestrator(fx.nc, WithDebounceHandler(func(TimerMessage, uint64) {}))
	second.Start()
	defer second.Stop()
	next := fx.takeTask(t, 5*time.Second)
	if next.Iteration != 1 || next.DispatchNonce == task.DispatchNonce {
		t.Fatalf("woken task = iteration %d nonce %q, want iteration 1 under a new nonce",
			next.Iteration, next.DispatchNonce)
	}
	fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return s.WaitUntil == nil && !s.DispatchUnsent
	})
	fx.noTask(t, time.Second)
}

func TestContinueWait_UntilInputWakesOnSubmission(t *testing.T) {
	fx := newLoopFixture(t, loopDef("wait-input-wf", "wait-input-task", dag.AgentLoopConfig{}, nil))
	orch := NewOrchestrator(fx.nc)
	orch.Start()
	defer orch.Stop()
	const runID = "wait-input-run"
	fx.startRun(t, runID)
	task := fx.takeTask(t, 5*time.Second)
	attempt := fx.started(t, task)
	fx.publishEvent(t, identityEvent(protocol.EventStepContinue, task, attempt,
		waitPayload(t, protocol.ContinueWait{Input: true})))
	fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return s.Iterations == 1 && s.WaitInput && !s.DispatchUnsent
	})
	fx.noTask(t, 500*time.Millisecond)

	store := fx.submitInput(t, runID, "req-1")
	next := fx.takeTask(t, 5*time.Second)
	if next.Iteration != 1 {
		t.Fatalf("woken task iteration = %d, want 1", next.Iteration)
	}
	run := fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return !s.WaitInput && !s.DispatchUnsent
	})
	if !run.Steps["agent"].HasInputs {
		t.Error("step does not remember it has inputs to settle")
	}
	if got := fx.inputStatus(t, store, runID, "req-1"); got != inputs.StatusQueued {
		t.Errorf("req-1 status = %s, want still queued for the worker to place", got)
	}
	// A late wait.elapsed for the dispatch that was woken is stale.
	// Outside TASK_QUEUES' dedup window, so that a second publish of
	// the iteration would be a real second task.
	fx.shortenTaskDedup(t, 200*time.Millisecond)
	time.Sleep(300 * time.Millisecond)
	stale := protocol.NewStepEvent(protocol.EventStepWaitElapsed, runID, "agent", nil)
	stale.SetIdentity(1, run.Steps["agent"].Attempts, run.Steps["agent"].DispatchNonce)
	fx.publishEvent(t, stale)
	fx.noTask(t, time.Second)
}

func TestContinueWait_InputQueuedDuringCommitWakesTheStep(t *testing.T) {
	fx := newLoopFixture(t, loopDef("wait-race-wf", "wait-race-task", dag.AgentLoopConfig{}, nil))
	orch := NewOrchestrator(fx.nc)
	orch.Start()
	defer orch.Stop()
	const runID = "wait-race-run"
	fx.startRun(t, runID)
	task := fx.takeTask(t, 5*time.Second)
	attempt := fx.started(t, task)

	// The input lands, and its event is handled, while the worker is
	// still running the turn that will commit UntilInput: the SDK's
	// own check ran before the input existed.
	fx.submitInput(t, runID, "early")
	fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool { return s.HasInputs })
	fx.publishEvent(t, identityEvent(protocol.EventStepContinue, task, attempt,
		waitPayload(t, protocol.ContinueWait{Input: true})))

	next := fx.takeTask(t, 5*time.Second)
	if next.Iteration != 1 {
		t.Fatalf("task iteration = %d, want 1 sent at once", next.Iteration)
	}
	run := fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool { return !s.DispatchUnsent })
	if run.Steps["agent"].WaitInput {
		t.Error("step saved as waiting for input it already has")
	}
	fx.noTask(t, 500*time.Millisecond)
}

func TestContinueWait_CompletionSweepSettlesQueuedInputs(t *testing.T) {
	fx := newLoopFixture(t, loopDef("sweep-wf", "sweep-task", dag.AgentLoopConfig{}, nil))
	orch := NewOrchestrator(fx.nc)
	orch.Start()
	defer orch.Stop()
	const runID = "sweep-run"
	fx.startRun(t, runID)
	task := fx.takeTask(t, 5*time.Second)
	attempt := fx.started(t, task)
	store := fx.submitInput(t, runID, "before")
	fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool { return s.HasInputs })
	fx.publishEvent(t, identityEvent(protocol.EventStepCompleted, task, attempt, []byte(`{}`)))
	fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return s.Status == dag.StepStatusCompleted && !s.HasInputs
	})
	if got := fx.inputStatus(t, store, runID, "before"); got != inputs.StatusUnanswered {
		t.Errorf("input queued before completion = %s, want unanswered", got)
	}
	// One submitted after the step finished is settled by its own event.
	fx.submitInput(t, runID, "after")
	deadline := time.Now().Add(5 * time.Second)
	for fx.inputStatus(t, store, runID, "after") != inputs.StatusUnanswered {
		if time.Now().After(deadline) {
			t.Fatal("input submitted after completion was never settled")
		}
		time.Sleep(25 * time.Millisecond)
	}
	msgs, err := fx.taskSub.Fetch(1, nats.MaxWait(300*time.Millisecond))
	if err == nil && len(msgs) > 0 {
		t.Fatalf("a finished step was dispatched again: %s", msgs[0].Data)
	}
}
