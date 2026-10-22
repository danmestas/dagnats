// internal/engine/first_dispatch_attempt_test.go
// The attempt stamped on a step's first dispatch (ADR-026, finding 1):
// the engine stamps attempt 1 on iteration 0 as it stamps every later
// dispatch, so a redelivery of the first task is a recovery of the
// same attempt, never a new one numbered by the delivery count.
//
// Methodology: one orchestrator on an embedded NATS server, with the
// test standing in for the worker and numbering its step.started the
// way the worker SDK does (the payload's attempt when stamped, else
// the delivery count). The redelivery case replays the order a second
// engine sharing the history consumer can see: the redelivery's
// step.continue before its step.started. Every wait is bounded.
package engine

import (
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
)

// workerAttempt is the worker SDK's numbering of a delivery
// (resolveAttemptNumber): the stamped attempt, else the delivery count.
func workerAttempt(task protocol.TaskPayload, delivered int) int {
	if delivered < 1 {
		panic("workerAttempt: delivered must be >= 1")
	}
	if task.Attempt < 0 {
		panic("workerAttempt: attempt must not be negative")
	}
	if task.Attempt > 0 {
		return task.Attempt
	}
	return delivered
}

// startedAs publishes the step.started a delivery numbered attempt
// sends for task.
func (fx loopFixture) startedAs(t *testing.T, task protocol.TaskPayload, attempt int) {
	t.Helper()
	if attempt < 1 {
		panic("startedAs: attempt must be >= 1")
	}
	if task.DispatchNonce == "" {
		panic("startedAs: task must carry its dispatch nonce")
	}
	evt := protocol.NewStepEvent(protocol.EventStepStarted, task.RunID, task.StepID, nil)
	evt.AttemptNumber = attempt
	evt.DispatchNonce = task.DispatchNonce
	fx.publishEvent(t, evt)
}

func TestFirstDispatch_CarriesAttemptOne(t *testing.T) {
	fx := newLoopFixture(t, loopDef("stamp-wf", "stamp-task", dag.AgentLoopConfig{}, nil))
	orch := NewOrchestrator(fx.nc)
	orch.Start()
	defer orch.Stop()
	fx.startRun(t, "stamp-run")
	task := fx.takeTask(t, 5*time.Second)
	if task.Attempt != 1 || task.Iteration != 0 {
		t.Fatalf("first dispatch = attempt %d iteration %d, want attempt 1 iteration 0",
			task.Attempt, task.Iteration)
	}
	// The stamp is what makes a redelivery keep its number: delivery
	// 3 of this task is still attempt 1.
	if got := workerAttempt(task, 3); got != 1 {
		t.Fatalf("redelivery 3 numbered %d, want attempt 1", got)
	}
}

// TestStepContinue_RedeliveryContinueBeforeItsStarted_Advances is
// finding 2 of ADR-026 reduced to one engine: a redelivery's
// step.continue reaches the engine before that redelivery's
// step.started (the order the second of two engines can see). With
// the attempt following the delivery count, the continue carried
// attempt 2 against a Running step on attempt 1 and was dropped, then
// the late step.started raised Attempts to 2 and nothing continued
// the loop. With the attempt stamped both deliveries are attempt 1.
func TestStepContinue_RedeliveryContinueBeforeItsStarted_Advances(t *testing.T) {
	fx := newLoopFixture(t, loopDef("redeliver-wf", "redeliver-task", dag.AgentLoopConfig{}, nil))
	orch := NewOrchestrator(fx.nc)
	orch.Start()
	defer orch.Stop()
	const runID = "redeliver-run"
	fx.startRun(t, runID)
	task := fx.takeTask(t, 5*time.Second)
	if task.Attempt != 1 {
		t.Errorf("first dispatch carries attempt %d, want 1", task.Attempt)
	}
	// Delivery 1 starts and stalls past AckWait.
	fx.startedAs(t, task, workerAttempt(task, 1))
	fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return s.Status == dag.StepStatusRunning
	})
	// Delivery 2 finishes the iteration; its continue overtakes its
	// step.started on the way to this engine.
	second := workerAttempt(task, 2)
	fx.publishEvent(t, identityEvent(
		protocol.EventStepContinue, task, second, continuePayload(t, []byte(`{"turn":1}`)),
	))
	fx.startedAs(t, task, second)
	run := fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return s.Iterations == 1
	})
	step := run.Steps["agent"]
	if step.Iterations != 1 || step.Attempts != 1 {
		t.Fatalf("step = iterations %d attempts %d, want iteration 1 on attempt 1",
			step.Iterations, step.Attempts)
	}
	next := fx.takeTask(t, 5*time.Second)
	if next.Iteration != 1 || next.Attempt != 1 {
		t.Fatalf("next task = iteration %d attempt %d, want iteration 1 attempt 1",
			next.Iteration, next.Attempt)
	}
}
