package engine

import (
	"encoding/json"
	"fmt"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// publishTask publishes a TaskPayload for a ready step to the
// TASK_QUEUES stream. Used by both Orchestrator and WorkflowActor.
func publishTask(
	js nats.JetStreamContext,
	runID string,
	step dag.StepDef,
	input []byte,
	attempt int,
) error {
	if js == nil {
		panic("publishTask: js must not be nil")
	}
	if runID == "" {
		panic("publishTask: runID must not be empty")
	}
	if step.ID == "" {
		panic("publishTask: step.ID must not be empty")
	}
	payload := protocol.TaskPayload{
		RunID:   runID,
		StepID:  step.ID,
		Attempt: attempt,
		Input:   input,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal TaskPayload: %w", err)
	}
	msgID := runID + "." + step.ID + ".queued"
	subject := taskSubject(step, runID)
	msg := buildTaskMsg(subject, data, msgID)
	_, err = js.PublishMsg(msg)
	return err
}

// publishIterationTask publishes a TaskPayload for an agent-loop
// re-enqueue with a distinct MsgId per iteration.
func publishIterationTask(
	js nats.JetStreamContext,
	runID string,
	step dag.StepDef,
	input []byte,
	iteration int,
) error {
	if js == nil {
		panic("publishIterationTask: js must not be nil")
	}
	if runID == "" {
		panic("publishIterationTask: runID must not be empty")
	}
	if step.ID == "" {
		panic("publishIterationTask: step.ID must not be empty")
	}
	payload := protocol.TaskPayload{
		RunID:     runID,
		StepID:    step.ID,
		Iteration: iteration,
		Input:     input,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal TaskPayload: %w", err)
	}
	msgID := fmt.Sprintf(
		"%s.%s.iter.%d", runID, step.ID, iteration,
	)
	subject := taskSubject(step, runID)
	msg := buildTaskMsg(subject, data, msgID)
	_, err = js.PublishMsg(msg)
	return err
}

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
	js nats.JetStreamContext,
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
		evt.NATSSubject(), data,
		nats.MsgId(evt.NATSMsgID()),
	)
	return err
}

// enqueueReadySteps resolves ready steps, publishes tasks, and
// checks for workflow completion. Returns updated run state.
func enqueueReadySteps(
	js nats.JetStreamContext,
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
				js, protocol.EventWorkflowCompleted, run.RunID,
			)
		}
	}

	// Check completion before looking for ready steps
	if dag.IsComplete(wfDef, completed) {
		run.Status = dag.RunStatusCompleted
		return publishWorkflowEvent(
			js, protocol.EventWorkflowCompleted, run.RunID,
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

	for _, step := range ready {
		input, err := dag.ResolveInput(step, run.Steps)
		if err != nil {
			return fmt.Errorf(
				"resolve input for %q: %w", step.ID, err,
			)
		}
		attempt := run.Steps[step.ID].Attempts
		if err := publishTask(
			js, run.RunID, step, input, attempt,
		); err != nil {
			return err
		}
	}
	return nil
}
