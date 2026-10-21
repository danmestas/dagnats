// internal/engine/step_event_fence.go
// Fencing of step lifecycle events (#801). A step.continue,
// step.completed or step.failed names the dispatch it answers
// (iteration, attempt, dispatch nonce); the engine acts on it only
// while the step is still Running on exactly that dispatch. Anything
// else is a stale worker, a redelivery of an event already applied, or
// a duplicate, and is dropped and counted. An event without identity
// comes from a worker or bridge client that predates the fields; it is
// accepted only while legacy events are enabled, with the pre-#801
// behaviour (and the duplicate-continue bug that comes with it).
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Rejection reasons, the "reason" attribute of the rejected-events
// metric.
const (
	rejectNoIdentity = "no_identity"
	rejectNotRunning = "not_running"
	rejectIteration  = "iteration"
	rejectAttempt    = "attempt"
	rejectNonce      = "nonce"
)

// acceptStepEvent is the acceptance rule: it reports whether the
// engine may act on evt given the step's saved state, and the reason
// when not. legacyAccepted is whether an event without identity is
// accepted as before #801.
func acceptStepEvent(
	state dag.StepState, evt protocol.Event, legacyAccepted bool,
) (bool, string) {
	if evt.StepID == "" {
		panic("acceptStepEvent: StepID must not be empty")
	}
	if !fencedEventType(evt.Type) {
		panic("acceptStepEvent: only continue, completed and failed are fenced")
	}
	if !evt.HasIdentity() {
		if legacyAccepted {
			return true, ""
		}
		return false, rejectNoIdentity
	}
	if state.Status != dag.StepStatusRunning {
		return false, rejectNotRunning
	}
	if *evt.Iteration != state.Iterations {
		return false, rejectIteration
	}
	if evt.AttemptNumber != state.Attempts {
		return false, rejectAttempt
	}
	if evt.DispatchNonce != "" && evt.DispatchNonce != state.DispatchNonce {
		return false, rejectNonce
	}
	return true, ""
}

// fencedEventType reports whether t is one of the three worker-sent
// lifecycle events the rule applies to.
func fencedEventType(t protocol.EventType) bool {
	switch t {
	case protocol.EventStepContinue, protocol.EventStepCompleted,
		protocol.EventStepFailed:
		return true
	}
	return false
}

// fenceStepEvent applies acceptStepEvent to evt against run and
// reports whether the caller may proceed. A rejected event is logged
// and counted; the caller acks it by returning nil. The engine's own
// step.sleep.completed and step.wait.matched share handleStepCompleted
// and pass through: no worker holds a dispatch for them.
func (o *Orchestrator) fenceStepEvent(
	ctx context.Context, run dag.WorkflowRun, evt protocol.Event,
) bool {
	if run.RunID != evt.RunID {
		panic("fenceStepEvent: event and run must name the same run")
	}
	if evt.StepID == "" {
		panic("fenceStepEvent: StepID must not be empty")
	}
	if !fencedEventType(evt.Type) {
		return true
	}
	state := run.Steps[evt.StepID]
	accepted, reason := acceptStepEvent(state, evt, o.legacyStepEvents)
	if accepted {
		return true
	}
	o.metrics.stepEventsRejected.Add(ctx, 1, metric.WithAttributes(
		attribute.String("event", string(evt.Type)),
		attribute.String("reason", reason),
	))
	iteration := -1
	if evt.Iteration != nil {
		iteration = *evt.Iteration
	}
	slog.WarnContext(ctx, "dropping step event for another dispatch",
		"event", string(evt.Type), "reason", reason,
		"run_id", evt.RunID, "step_id", evt.StepID,
		"event_iteration", iteration, "event_attempt", evt.AttemptNumber,
		"step_status", state.Status.String(),
		"step_iteration", state.Iterations, "step_attempt", state.Attempts,
	)
	return false
}

// continueState returns the state a step.continue carries (#801) and
// whether it carries one at all. A legacy event (no identity) carries
// none: its worker checkpointed to KV, and the engine leaves the saved
// state alone. A set but empty state clears the saved state.
func continueState(evt protocol.Event) (state []byte, set bool, err error) {
	if evt.Type != protocol.EventStepContinue {
		panic("continueState: event must be a step.continue")
	}
	if evt.StepID == "" {
		panic("continueState: StepID must not be empty")
	}
	if !evt.HasIdentity() || len(evt.Payload) == 0 {
		return nil, false, nil
	}
	var payload protocol.StepContinuePayload
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return nil, false, fmt.Errorf("decode step.continue payload: %w", err)
	}
	return payload.State, payload.StateSet || len(payload.State) > 0, nil
}

// loopStateOverCap returns the error that fails a loop whose committed
// state exceeds the cap, naming both sizes; nil when it fits.
func loopStateOverCap(size, capBytes int) error {
	if size < 0 {
		panic("loopStateOverCap: size must not be negative")
	}
	if capBytes <= 0 {
		panic("loopStateOverCap: cap must be positive")
	}
	if size <= capBytes {
		return nil
	}
	return fmt.Errorf(
		"agent loop state is %d bytes, over the %d byte cap (loop_state_max_bytes)",
		size, capBytes,
	)
}
