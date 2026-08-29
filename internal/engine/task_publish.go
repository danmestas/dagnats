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

// nextDispatchAttempt returns the protocol.TaskPayload.Attempt value
// for stepID's NEXT dispatch, derived from run's CURRENT
// Steps[stepID].Attempts (#624 review round 2) rather than a value
// threaded through however many hops a re-dispatch path took to reach
// its publish call. Every dispatch/re-dispatch site in this package
// that starts a NEW attempt (initial dispatch, retry) must call this
// instead of building its own Attempt value. Continuing the SAME
// attempt (agent-loop Continue) must NOT call this — see
// currentAttempt below.
//
// Steps[stepID].Attempts is the max AttemptNumber of any attempt that
// has STARTED for this step (handleStepStarted's monotonic max-merge
// against step.started's AttemptNumber) — not a tally of completed
// attempts. A step with Attempts == 0 has never started: its first
// dispatch uses 0 directly, letting the worker SDK's/bridge's own
// AttemptNumber resolution (worker/context.go's resolveAttemptNumber,
// bridge/poll.go's taskAttemptNumber) fall back to NATS NumDelivered —
// this asymmetry is intentional and pre-dates this fix. A step with
// Attempts >= 1 has already started at least once — the next dispatch
// is Attempts + 1; using the bare value again would resolve to the
// SAME AttemptNumber as the already-started attempt and collide on
// BUILD_LOGS's attempt-scoped subject within its dedup window.
func nextDispatchAttempt(run dag.WorkflowRun, stepID string) int {
	if stepID == "" {
		panic("nextDispatchAttempt: stepID must not be empty")
	}
	attempts := run.Steps[stepID].Attempts
	if attempts == 0 {
		return 0
	}
	return attempts + 1
}

// currentAttempt returns run's Steps[stepID].Attempts AS-IS — for
// dispatch paths that continue the SAME attempt (the agent-loop
// Continue re-enqueue, internal/engine/task_publisher.go's
// PublishIteration) rather than starting a new one. Unlike
// nextDispatchAttempt, this must NOT add 1: a Continue iteration is
// not a retry and must not consume retry budget the step never
// actually spent (#624 review round 3). iteration, not attempt, is
// what changes across Continue calls — see protocol.LogChunk's doc
// comment for why both are part of the BUILD_LOGS subject.
func currentAttempt(run dag.WorkflowRun, stepID string) int {
	if stepID == "" {
		panic("currentAttempt: stepID must not be empty")
	}
	return run.Steps[stepID].Attempts
}
