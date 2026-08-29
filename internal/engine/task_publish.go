package engine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// taskSubject builds the NATS subject for a task. Agent steps
// use the "agent_task" prefix; normal steps use "task".
func taskSubject(step dag.StepDef, runID string) string {
	prefix := "task"
	if step.Type == dag.StepTypeAgent {
		prefix = "agent_task"
	}
	subject := prefix + "." + step.Task
	if step.WorkerGroup != "" {
		subject += "." + step.WorkerGroup
	}
	return subject + "." + runID
}

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
// must call this instead of building its own Attempt value.
//
// A step with Attempts == 0 has never started: its first dispatch uses
// 0 directly, letting the worker SDK's/bridge's own AttemptNumber
// resolution (worker/context.go's resolveAttemptNumber,
// bridge/poll.go's taskAttemptNumber) fall back to NATS NumDelivered —
// this asymmetry is intentional and pre-dates this fix. A step with
// Attempts >= 1 has completed at least one attempt already —
// Steps[stepID].Attempts TALLIES completed attempts, so the next
// dispatch is that count + 1; using the bare count again would resolve
// to the SAME AttemptNumber as a prior attempt and collide on
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

// collectReadyMessages builds NATS messages for ready steps
// without publishing. Returns messages grouped by step. The grant policy
// (#380) strips the control-plane capability from any step whose workflow
// is not granted, and each step's already-stamped DispatchNonce rides the
// payload for server-side run-binding. A nil policy denies (deny-by-default).
func collectReadyMessages(
	runID string,
	ready []dag.StepDef,
	run *dag.WorkflowRun,
	grant *GrantPolicy,
	workflowName string,
) ([]*nats.Msg, error) {
	if runID == "" {
		panic("collectReadyMessages: runID must not be empty")
	}
	if run == nil {
		panic("collectReadyMessages: run must not be nil")
	}
	msgs := make([]*nats.Msg, 0, len(ready))
	for _, step := range ready {
		input, err := dag.ResolveInput(step, run.Steps)
		if err != nil {
			return nil, fmt.Errorf(
				"resolve input for %q: %w", step.ID, err,
			)
		}
		attempt := run.Steps[step.ID].Attempts
		payload := protocol.TaskPayload{
			TaskID:       runID + "." + step.ID,
			RunID:        runID,
			StepID:       step.ID,
			Attempt:      attempt,
			Input:        input,
			Metadata:     step.Metadata,
			WorkflowName: workflowName,
			RequiredCapabilities: effectiveCapabilities(
				step.RequiredCapabilities, run.WorkflowID, grant,
			),
			DispatchNonce: run.Steps[step.ID].DispatchNonce,
		}
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf(
				"marshal TaskPayload: %w", err,
			)
		}
		msgID := runID + "." + step.ID + ".queued"
		subject := taskSubject(step, runID)
		msgs = append(msgs, buildTaskMsg(subject, data, msgID))
	}
	return msgs, nil
}
