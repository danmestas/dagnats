package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/jetstreamext"
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
// (completed or failed) to the WORKFLOW_HISTORY stream.
func publishWorkflowEvent(
	ctx context.Context,
	js jetstream.JetStream,
	eventType protocol.EventType,
	runID string,
) error {
	if js == nil {
		panic("publishWorkflowEvent: js must not be nil")
	}
	if runID == "" {
		panic("publishWorkflowEvent: runID must not be empty")
	}
	evt := protocol.NewWorkflowEvent(eventType, runID, nil)
	data, err := evt.Marshal()
	if err != nil {
		return fmt.Errorf("marshal %s event: %w", eventType, err)
	}
	_, err = js.Publish(
		ctx, evt.NATSSubject(), data,
		jetstream.WithMsgID(evt.NATSMsgID()),
	)
	return err
}

// collectReadyMessages builds NATS messages for ready steps
// without publishing. Returns messages grouped by step.
func collectReadyMessages(
	runID string,
	ready []dag.StepDef,
	run *dag.WorkflowRun,
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
			TaskID:  runID + "." + step.ID,
			RunID:   runID,
			StepID:  step.ID,
			Attempt: attempt,
			Input:   input,
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

// enqueueReadySteps resolves ready steps, publishes tasks, and
// checks for workflow completion. Returns updated run state.
func enqueueReadySteps(
	ctx context.Context,
	js jetstream.JetStream,
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
) error {
	if js == nil {
		panic("enqueueReadySteps: js must not be nil")
	}
	if run == nil {
		panic("enqueueReadySteps: run must not be nil")
	}
	completed := completedSet(*run)
	queued := queuedSet(*run)

	// Process skipped steps first
	skipped := dag.ResolveSkipped(
		wfDef, completed, queued, run.Steps,
	)
	for _, step := range skipped {
		state := run.Steps[step.ID]
		state.Status = dag.StepStatusSkipped
		run.Steps[step.ID] = state
	}
	if len(skipped) > 0 {
		completed = completedSet(*run)
		if dag.IsComplete(wfDef, completed) {
			run.Status = dag.RunStatusCompleted
			return publishWorkflowEvent(
				ctx, js, protocol.EventWorkflowCompleted,
				run.RunID,
			)
		}
	}

	// Check completion before looking for ready steps
	if dag.IsComplete(wfDef, completed) {
		run.Status = dag.RunStatusCompleted
		return publishWorkflowEvent(
			ctx, js, protocol.EventWorkflowCompleted,
			run.RunID,
		)
	}

	ready := dag.ResolveReady(wfDef, completed, queued)
	// Exclude steps already skipped
	filtered := make([]dag.StepDef, 0, len(ready))
	for _, step := range ready {
		if run.Steps[step.ID].Status != dag.StepStatusSkipped {
			filtered = append(filtered, step)
		}
	}
	ready = filtered

	if len(ready) == 0 {
		return nil
	}

	for _, step := range ready {
		state := run.Steps[step.ID]
		state.Status = dag.StepStatusQueued
		run.Steps[step.ID] = state
	}

	msgs, err := collectReadyMessages(run.RunID, ready, run)
	if err != nil {
		return err
	}
	if len(msgs) == 0 {
		return nil
	}

	return publishAtomicBatches(ctx, js, msgs)
}

// publishAtomicBatches splits messages by stream prefix and
// publishes each group as an atomic batch. Normal tasks go to
// TASK_QUEUES (task.>), agent tasks to AGENT_TASKS (agent_task.>).
func publishAtomicBatches(
	ctx context.Context,
	js jetstream.JetStream, msgs []*nats.Msg,
) error {
	if js == nil {
		panic("publishAtomicBatches: js must not be nil")
	}
	if len(msgs) == 0 {
		panic("publishAtomicBatches: msgs must not be empty")
	}
	var taskMsgs, agentMsgs []*nats.Msg
	for _, msg := range msgs {
		if strings.HasPrefix(msg.Subject, "agent_task.") {
			agentMsgs = append(agentMsgs, msg)
		} else {
			taskMsgs = append(taskMsgs, msg)
		}
	}
	if len(taskMsgs) > 0 {
		_, err := jetstreamext.PublishMsgBatch(
			ctx, js, taskMsgs,
		)
		if err != nil {
			return fmt.Errorf("atomic task publish: %w", err)
		}
	}
	if len(agentMsgs) > 0 {
		_, err := jetstreamext.PublishMsgBatch(
			ctx, js, agentMsgs,
		)
		if err != nil {
			return fmt.Errorf("atomic agent publish: %w", err)
		}
	}
	return nil
}
