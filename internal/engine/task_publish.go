package engine

import (
	"context"
	"fmt"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

// publishWorkflowEvent publishes a workflow lifecycle event
// (completed or failed) to the WORKFLOW_HISTORY stream via the
// TracingPublisher so W3C trace context auto-attaches (#334).
func publishWorkflowEvent(
	ctx context.Context,
	tp *natsutil.TracingPublisher,
	eventType protocol.EventType,
	runID string,
) error {
	if tp == nil {
		panic("publishWorkflowEvent: tp must not be nil")
	}
	if runID == "" {
		panic("publishWorkflowEvent: runID must not be empty")
	}
	evt := protocol.NewWorkflowEvent(eventType, runID, nil)
	data, err := evt.Marshal()
	if err != nil {
		return fmt.Errorf("marshal %s event: %w", eventType, err)
	}
	_, err = tp.JSPublish(
		ctx, evt.NATSSubject(), data,
		jetstream.WithMsgID(evt.NATSMsgID()),
	)
	return err
}

// dispatchIdentityMode distinguishes the two dispatch shapes
// dispatchIdentity supports — see dispatchIdentity's doc comment for
// why one pure function cannot infer this from (run, stepID) alone.
type dispatchIdentityMode int

const (
	// dispatchNewAttempt is for every re-dispatch that starts a
	// genuinely NEW attempt: initial dispatch, retry (rate/concurrency/
	// backoff/retry-after), DLQ replay, on-fail compensation, a map
	// instance's own dispatch. Attempt is projected as Attempts + 1
	// (or 0 for a never-started step) — see dispatchIdentity.
	dispatchNewAttempt dispatchIdentityMode = iota
	// dispatchSameAttempt is for the agent-loop Continue re-enqueue
	// ONLY: it is NOT a new attempt and must not consume retry budget
	// the step never spent, so Attempt is Steps[stepID].Attempts
	// AS-IS — no +1.
	dispatchSameAttempt
)

// dispatchIdentity is the ONE builder for the (attempt, iteration)
// pair every protocol.TaskPayload construction and BUILD_LOGS subject
// resolution in this package must use — no call site computes Attempt
// or Iteration by hand (#624 review round 4). It replaces the round-3
// nextDispatchAttempt/currentAttempt pair, which each computed only
// Attempt: every re-dispatch path built Iteration by hand (i.e. left
// it unset, the Go zero value), so a loop step that failed mid-loop
// and retried dispatched its retry at iteration 0 while
// Steps[stepID].Iterations — never reset by a retry — still reported
// the loop's true position. GET .../logs's default (attempt,
// iteration) params, which read directly off Steps[stepID], then
// resolved to a subject the retry never wrote to: an empty page,
// eof never tripping, follow hanging to its 1h cap.
//
// iteration is ALWAYS Steps[stepID].Iterations, in EITHER mode: for
// dispatchSameAttempt (Continue), the pure Advance() core already
// incremented it as part of computing the Continue effect (advance.go)
// before the caller's run reaches this function, so it already holds
// the NEW iteration; for dispatchNewAttempt (retry included), nothing
// resets it, so carrying it forward unconditionally is exactly what
// makes a retry's dispatch identity land on the SAME (attempt,
// iteration) pair the reader's snapshot-derived default resolves to —
// by construction, not by two independent formulas staying in sync.
//
// attempt DOES depend on mode, and cannot be inferred from (run,
// stepID) alone: Steps[stepID].Attempts is bumped only by
// handleStepStarted processing the step.started event AFTER a
// dispatch's own payload was already built and published (an
// event-driven, asynchronous update — see step_handlers.go), so at
// call time the snapshot still reflects the PREVIOUS attempt count for
// a genuinely new dispatch, requiring +1; a Continue re-dispatch,
// by contrast, is happening WITHIN an attempt whose step.started
// already landed, so Attempts already equals the attempt this
// dispatch belongs to and must NOT be bumped again. The mode
// parameter carries that caller-known distinction explicitly rather
// than dispatchIdentity guessing at it.
func dispatchIdentity(
	run dag.WorkflowRun, stepID string, mode dispatchIdentityMode,
) (attempt, iteration int) {
	if stepID == "" {
		panic("dispatchIdentity: stepID must not be empty")
	}
	state := run.Steps[stepID]
	switch mode {
	case dispatchNewAttempt:
		attempt = state.Attempts
		if attempt > 0 {
			attempt++
		}
	case dispatchSameAttempt:
		attempt = state.Attempts
	default:
		panic("dispatchIdentity: unknown dispatchIdentityMode")
	}
	return attempt, state.Iterations
}
