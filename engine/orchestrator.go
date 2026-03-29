// engine/orchestrator.go
// The orchestrator is the thin I/O shell of DagNats. It subscribes to the
// history stream, resolves DAG dependencies via dag.ResolveReady, and publishes
// task messages. All delivery guarantees, retries, and timeouts are handled by
// NATS — this file contains no timers, no retry logic, no in-memory queues.
package engine

import (
	"encoding/json"
	"fmt"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// Orchestrator subscribes to the history stream and drives workflow execution.
// It is intentionally stateless between events — all run state lives in the
// snapshot store (NATS KV), so the orchestrator can crash and resume safely.
type Orchestrator struct {
	nc      *nats.Conn
	js      nats.JetStreamContext
	defKV   nats.KeyValue
	store   *SnapshotStore
	logger  observe.Logger
	metrics observe.Metrics
	sub     *nats.Subscription
}

// NewOrchestrator creates an Orchestrator bound to the given NATS connection.
// Panics if nc is nil or JetStream cannot be obtained — both are programmer errors.
// KV buckets must already exist (call natsutil.SetupAll before NewOrchestrator).
func NewOrchestrator(nc *nats.Conn, logger observe.Logger, metrics observe.Metrics) *Orchestrator {
	if nc == nil {
		panic("NewOrchestrator: nc must not be nil")
	}
	if logger == nil {
		panic("NewOrchestrator: logger must not be nil")
	}
	js, err := nc.JetStream()
	if err != nil {
		panic("NewOrchestrator: JetStream failed: " + err.Error())
	}
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		panic("NewOrchestrator: workflow_defs bucket not found: " + err.Error())
	}
	return &Orchestrator{
		nc:      nc,
		js:      js,
		defKV:   defKV,
		store:   NewSnapshotStore(js),
		logger:  logger,
		metrics: metrics,
	}
}

// Start subscribes to history.> on the WORKFLOW_HISTORY stream using push-subscribe.
// Messages are delivered asynchronously to handleEvent. Panics if already started.
func (o *Orchestrator) Start() {
	if o.sub != nil {
		panic("Orchestrator.Start: already started")
	}
	sub, err := o.js.Subscribe("history.>", o.handleEvent,
		nats.DeliverAll(),
		nats.AckExplicit(),
	)
	if err != nil {
		panic("Orchestrator.Start: subscribe failed: " + err.Error())
	}
	o.sub = sub
}

// Stop drains and unsubscribes from the history stream. Safe to call multiple times.
func (o *Orchestrator) Stop() {
	if o.sub == nil {
		return
	}
	if err := o.sub.Unsubscribe(); err != nil {
		o.logger.Error("Stop: unsubscribe error", err)
	}
	o.sub = nil
}

// handleEvent is the central dispatcher. It unmarshals the event and routes to
// the appropriate handler. Unknown event types are acked and logged — not errors.
func (o *Orchestrator) handleEvent(msg *nats.Msg) {
	if msg == nil {
		return
	}
	evt, err := protocol.UnmarshalEvent(msg.Data)
	if err != nil {
		o.logger.Error("handleEvent: unmarshal failed", err)
		msg.Nak()
		return
	}
	switch evt.Type {
	case protocol.EventWorkflowStarted:
		err = o.handleWorkflowStarted(evt)
	case protocol.EventStepCompleted:
		err = o.handleStepCompleted(evt)
	case protocol.EventStepContinue:
		err = o.handleStepContinue(evt)
	case protocol.EventStepFailed:
		err = o.handleStepFailed(evt)
	default:
		msg.Ack()
		return
	}
	if err != nil {
		o.logger.Error("handleEvent: handler error", err,
			observe.String("event_type", string(evt.Type)),
			observe.String("run_id", evt.RunID),
		)
		msg.Nak()
		return
	}
	msg.Ack()
}

// handleWorkflowStarted creates the initial WorkflowRun from the event payload,
// saves it, then enqueues all steps whose dependencies are already satisfied
// (entry-point steps with no DependsOn).
func (o *Orchestrator) handleWorkflowStarted(evt protocol.Event) error {
	if evt.RunID == "" {
		panic("handleWorkflowStarted: RunID must not be empty")
	}
	if evt.Payload == nil {
		panic("handleWorkflowStarted: Payload must not be nil")
	}
	var wfDef dag.WorkflowDef
	if err := json.Unmarshal(evt.Payload, &wfDef); err != nil {
		return fmt.Errorf("unmarshal WorkflowDef: %w", err)
	}
	run := dag.NewWorkflowRun(wfDef, evt.RunID)
	run.Status = dag.RunStatusRunning
	if err := o.store.Save(run); err != nil {
		return fmt.Errorf("save initial run: %w", err)
	}
	return o.enqueueReady(wfDef, run)
}

// handleStepCompleted marks the step output in the snapshot, then checks whether
// the workflow is fully complete or whether new steps have become unblocked.
func (o *Orchestrator) handleStepCompleted(evt protocol.Event) error {
	if evt.RunID == "" {
		panic("handleStepCompleted: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleStepCompleted: StepID must not be empty")
	}
	wfDef, run, err := o.loadRunAndDef(evt.RunID)
	if err != nil {
		return err
	}
	state := run.Steps[evt.StepID]
	state.Status = dag.StepStatusCompleted
	state.Output = evt.Payload
	run.Steps[evt.StepID] = state

	completed := completedSet(run)
	if dag.IsComplete(wfDef, completed) {
		run.Status = dag.RunStatusCompleted
		if err := o.store.Save(run); err != nil {
			return err
		}
		return o.publishWorkflowCompleted(run.RunID)
	}
	if err := o.store.Save(run); err != nil {
		return err
	}
	return o.enqueueReady(wfDef, run)
}

// handleStepContinue re-enqueues an agent-loop step for another iteration.
// Iterations is incremented in the snapshot before re-publishing so each dispatch
// carries a unique iteration index — preventing JetStream dedup from swallowing
// subsequent task messages for the same step.
func (o *Orchestrator) handleStepContinue(evt protocol.Event) error {
	if evt.RunID == "" {
		panic("handleStepContinue: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleStepContinue: StepID must not be empty")
	}
	wfDef, run, err := o.loadRunAndDef(evt.RunID)
	if err != nil {
		return err
	}
	var stepDef dag.StepDef
	found := false
	for _, s := range wfDef.Steps {
		if s.ID == evt.StepID {
			stepDef = s
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("step %q not found in workflow def", evt.StepID)
	}
	// Increment iteration count before re-publishing so the new task message
	// gets a distinct MsgId from all prior iterations.
	state := run.Steps[evt.StepID]
	state.Iterations++
	run.Steps[evt.StepID] = state
	if err := o.store.Save(run); err != nil {
		return err
	}
	input, err := dag.ResolveInput(stepDef, run.Steps)
	if err != nil {
		return fmt.Errorf("resolve input for step %q: %w", stepDef.ID, err)
	}
	return o.publishIterationTask(run.RunID, stepDef, input, state.Iterations)
}

// handleStepFailed increments attempt count and fails the workflow if retries
// are exhausted. The step's Retries field in the def controls how many attempts
// are allowed before the whole run is marked failed.
func (o *Orchestrator) handleStepFailed(evt protocol.Event) error {
	if evt.RunID == "" {
		panic("handleStepFailed: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleStepFailed: StepID must not be empty")
	}
	wfDef, run, err := o.loadRunAndDef(evt.RunID)
	if err != nil {
		return err
	}
	var stepDef dag.StepDef
	for _, s := range wfDef.Steps {
		if s.ID == evt.StepID {
			stepDef = s
			break
		}
	}
	state := run.Steps[evt.StepID]
	state.Attempts++
	if evt.Payload != nil {
		state.Error = string(evt.Payload)
	}
	if state.Attempts > stepDef.Retries {
		state.Status = dag.StepStatusFailed
		run.Steps[evt.StepID] = state
		run.Status = dag.RunStatusFailed
		return o.store.Save(run)
	}
	// Retries remain — re-queue the step. NakWithDelay on the worker side
	// handles backoff; orchestrator simply re-publishes.
	run.Steps[evt.StepID] = state
	if err := o.store.Save(run); err != nil {
		return err
	}
	input, err := dag.ResolveInput(stepDef, run.Steps)
	if err != nil {
		return fmt.Errorf("resolve input for step %q: %w", stepDef.ID, err)
	}
	return o.publishTask(run.RunID, stepDef, input)
}

// enqueueReady resolves all currently-ready steps and publishes one task message
// per step. Steps already queued are skipped via the queued set check inside
// dag.ResolveReady, preventing double dispatch.
func (o *Orchestrator) enqueueReady(wfDef dag.WorkflowDef, run dag.WorkflowRun) error {
	if run.RunID == "" {
		panic("enqueueReady: RunID must not be empty")
	}
	completed := completedSet(run)
	queued := queuedSet(run)
	ready := dag.ResolveReady(wfDef, completed, queued)
	for _, step := range ready {
		input, err := dag.ResolveInput(step, run.Steps)
		if err != nil {
			return fmt.Errorf("resolve input for step %q: %w", step.ID, err)
		}
		if err := o.publishTask(run.RunID, step, input); err != nil {
			return err
		}
		state := run.Steps[step.ID]
		state.Status = dag.StepStatusQueued
		run.Steps[step.ID] = state
	}
	if len(ready) == 0 {
		return nil
	}
	return o.store.Save(run)
}

// publishTask publishes a TaskPayload to task.{step.Task}.{runID} with a
// deduplication ID of {runID}.{stepID}.queued so replays are idempotent.
func (o *Orchestrator) publishTask(runID string, step dag.StepDef, input []byte) error {
	if runID == "" {
		panic("publishTask: runID must not be empty")
	}
	if step.ID == "" {
		panic("publishTask: step.ID must not be empty")
	}
	payload := protocol.TaskPayload{
		RunID:  runID,
		StepID: step.ID,
		Input:  input,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal TaskPayload: %w", err)
	}
	subject := "task." + step.Task + "." + runID
	msgID := runID + "." + step.ID + ".queued"
	_, err = o.js.Publish(subject, data, nats.MsgId(msgID))
	return err
}

// publishIterationTask publishes a TaskPayload for an agent-loop re-enqueue.
// iteration is the new cycle index and is embedded in both the payload and the
// MsgId, making each iteration's task message distinct for JetStream dedup.
func (o *Orchestrator) publishIterationTask(
	runID string, step dag.StepDef, input []byte, iteration int,
) error {
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
	subject := "task." + step.Task + "." + runID
	msgID := fmt.Sprintf("%s.%s.continue.%d", runID, step.ID, iteration)
	_, err = o.js.Publish(subject, data, nats.MsgId(msgID))
	return err
}

// loadRunAndDef loads the workflow definition from the defKV bucket and the
// current run snapshot from the snapshot store. Both must exist — missing either
// is an error, not a panic, since it could indicate a race or corrupt state.
func (o *Orchestrator) loadRunAndDef(runID string) (dag.WorkflowDef, dag.WorkflowRun, error) {
	if runID == "" {
		panic("loadRunAndDef: runID must not be empty")
	}
	run, err := o.store.Load(runID)
	if err != nil {
		return dag.WorkflowDef{}, dag.WorkflowRun{}, fmt.Errorf("load run %q: %w", runID, err)
	}
	entry, err := o.defKV.Get(run.WorkflowID)
	if err != nil {
		return dag.WorkflowDef{}, dag.WorkflowRun{},
			fmt.Errorf("load workflow def %q: %w", run.WorkflowID, err)
	}
	var wfDef dag.WorkflowDef
	if err := json.Unmarshal(entry.Value(), &wfDef); err != nil {
		return dag.WorkflowDef{}, dag.WorkflowRun{},
			fmt.Errorf("unmarshal workflow def %q: %w", run.WorkflowID, err)
	}
	return wfDef, run, nil
}

// publishWorkflowCompleted publishes a workflow.completed event to the history
// stream. This lets consumers (including tests) observe the terminal state
// transition via the event log rather than polling KV.
func (o *Orchestrator) publishWorkflowCompleted(runID string) error {
	if runID == "" {
		panic("publishWorkflowCompleted: runID must not be empty")
	}
	evt := protocol.NewWorkflowEvent(protocol.EventWorkflowCompleted, runID, nil)
	data, err := evt.Marshal()
	if err != nil {
		return fmt.Errorf("marshal workflow.completed event: %w", err)
	}
	_, err = o.js.Publish(evt.NATSSubject(), data, nats.MsgId(evt.NATSMsgID()))
	return err
}

// completedSet returns a set of step IDs whose status is Completed.
// Used to satisfy the dag.ResolveReady and dag.IsComplete contracts.
func completedSet(run dag.WorkflowRun) map[string]bool {
	if run.Steps == nil {
		panic("completedSet: run.Steps must not be nil")
	}
	result := make(map[string]bool, len(run.Steps))
	for id, state := range run.Steps {
		if state.Status == dag.StepStatusCompleted {
			result[id] = true
		}
	}
	return result
}

// queuedSet returns a set of step IDs whose status is Queued or beyond
// (Running, Completed, Failed, Skipped). This prevents re-dispatching steps
// that have already been sent to a worker.
func queuedSet(run dag.WorkflowRun) map[string]bool {
	if run.Steps == nil {
		panic("queuedSet: run.Steps must not be nil")
	}
	result := make(map[string]bool, len(run.Steps))
	for id, state := range run.Steps {
		switch state.Status {
		case dag.StepStatusQueued, dag.StepStatusRunning,
			dag.StepStatusCompleted, dag.StepStatusFailed, dag.StepStatusSkipped:
			result[id] = true
		}
	}
	return result
}
