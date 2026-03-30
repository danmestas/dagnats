// engine/orchestrator.go
// The orchestrator is the thin I/O shell of DagNats. It subscribes to the
// history stream, resolves DAG dependencies via dag.ResolveReady, publishes
// task messages, and owns retry decisions via StepDef.Retries.
// LoopDelay uses time.AfterFunc which does not survive orchestrator restart
// — pending delayed iterations are lost on crash and must be manually retried.
package engine

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// Orchestrator subscribes to the history stream and drives workflow
// execution. It is intentionally stateless between events — all run
// state lives in the snapshot store (NATS KV), so the orchestrator
// can crash and resume safely.
type Orchestrator struct {
	nc       *nats.Conn
	js       nats.JetStreamContext
	defKV    nats.KeyValue
	store    *SnapshotStore
	tel      *observe.Telemetry
	sub      *nats.Subscription
	runLocks sync.Map // map[string]*sync.Mutex
}

// NewOrchestrator creates an Orchestrator bound to the given NATS
// connection. Panics if nc is nil or JetStream cannot be obtained.
func NewOrchestrator(
	nc *nats.Conn, tel *observe.Telemetry,
) *Orchestrator {
	if nc == nil {
		panic("NewOrchestrator: nc must not be nil")
	}
	if tel == nil {
		panic("NewOrchestrator: tel must not be nil")
	}
	js, err := nc.JetStream()
	if err != nil {
		panic("NewOrchestrator: JetStream failed: " + err.Error())
	}
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		panic("NewOrchestrator: workflow_defs bucket: " +
			err.Error())
	}
	return &Orchestrator{
		nc:    nc,
		js:    js,
		defKV: defKV,
		store: NewSnapshotStore(js),
		tel:   tel,
	}
}

// Start subscribes to history.> on the WORKFLOW_HISTORY stream.
func (o *Orchestrator) Start() {
	if o.sub != nil {
		panic("Orchestrator.Start: already started")
	}
	sub, err := o.js.Subscribe("history.>", o.handleEvent,
		nats.DeliverAll(), nats.AckExplicit(),
	)
	if err != nil {
		panic("Orchestrator.Start: subscribe failed: " + err.Error())
	}
	o.sub = sub
}

// Stop drains and unsubscribes. Safe to call multiple times.
func (o *Orchestrator) Stop() {
	if o.sub == nil {
		return
	}
	if err := o.sub.Unsubscribe(); err != nil {
		o.tel.Logger.Error("Stop: unsubscribe error", err)
	}
	o.sub = nil
}

func (o *Orchestrator) getRunLock(runID string) *sync.Mutex {
	val, _ := o.runLocks.LoadOrStore(runID, &sync.Mutex{})
	return val.(*sync.Mutex)
}

// handleEvent is the central dispatcher.
func (o *Orchestrator) handleEvent(msg *nats.Msg) {
	if msg == nil {
		return
	}
	evt, err := protocol.UnmarshalEvent(msg.Data)
	if err != nil {
		o.tel.Logger.Error("handleEvent: unmarshal failed", err)
		msg.NakWithDelay(5 * time.Second)
		return
	}
	switch evt.Type {
	case protocol.EventWorkflowStarted,
		protocol.EventStepCompleted,
		protocol.EventStepContinue,
		protocol.EventStepFailed:
		// handled below
	default:
		msg.Ack()
		return
	}
	lock := o.getRunLock(evt.RunID)
	lock.Lock()
	defer lock.Unlock()
	switch evt.Type {
	case protocol.EventWorkflowStarted:
		err = o.handleWorkflowStarted(evt)
	case protocol.EventStepCompleted:
		err = o.handleStepCompleted(evt)
	case protocol.EventStepContinue:
		err = o.handleStepContinue(evt)
	case protocol.EventStepFailed:
		err = o.handleStepFailed(evt)
	}
	if err != nil {
		o.tel.Logger.Error("handleEvent: handler error", err,
			observe.String("event_type", string(evt.Type)),
			observe.String("run_id", evt.RunID),
		)
		msg.NakWithDelay(5 * time.Second)
		return
	}
	msg.Ack()
}

func (o *Orchestrator) handleWorkflowStarted(
	evt protocol.Event,
) error {
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

func (o *Orchestrator) handleStepCompleted(
	evt protocol.Event,
) error {
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

func (o *Orchestrator) handleStepContinue(
	evt protocol.Event,
) error {
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
	stepDef, found := findStepDef(wfDef, evt.StepID)
	if !found {
		return fmt.Errorf("step %q not found in def", evt.StepID)
	}
	state := run.Steps[evt.StepID]
	state.Iterations++
	if state.Iterations == 1 {
		state.LoopStartedAt = time.Now().UTC()
	}
	if exceeded, reason := checkLoopBounds(stepDef, state); exceeded {
		return o.failLoopStep(run, evt.StepID, state, reason)
	}
	run.Steps[evt.StepID] = state
	if err := o.store.Save(run); err != nil {
		return err
	}
	input, err := dag.ResolveInput(stepDef, run.Steps)
	if err != nil {
		return fmt.Errorf("resolve input for %q: %w", stepDef.ID, err)
	}
	return o.publishLoopIteration(
		run.RunID, stepDef, input, state.Iterations,
	)
}

// publishLoopIteration re-enqueues an agent-loop step, respecting
// LoopDelay if configured. NOTE: time.AfterFunc does not survive
// orchestrator restart — see package comment.
func (o *Orchestrator) publishLoopIteration(
	runID string, step dag.StepDef, input []byte, iteration int,
) error {
	if step.Loop != nil && step.Loop.LoopDelay > 0 {
		delay := step.Loop.LoopDelay
		time.AfterFunc(delay, func() {
			err := o.publishIterationTask(
				runID, step, input, iteration,
			)
			if err != nil {
				o.tel.Logger.Error("delayed iteration failed", err,
					observe.String("run_id", runID),
					observe.String("step_id", step.ID),
				)
			}
		})
		return nil
	}
	return o.publishIterationTask(runID, step, input, iteration)
}

func checkLoopBounds(
	stepDef dag.StepDef, state dag.StepState,
) (bool, string) {
	if stepDef.Loop == nil {
		return false, ""
	}
	if stepDef.Loop.MaxIterations > 0 &&
		state.Iterations >= stepDef.Loop.MaxIterations {
		return true, fmt.Sprintf(
			"agent loop exceeded max iterations (%d)",
			stepDef.Loop.MaxIterations,
		)
	}
	if stepDef.Loop.MaxDuration > 0 &&
		!state.LoopStartedAt.IsZero() &&
		time.Since(state.LoopStartedAt) >= stepDef.Loop.MaxDuration {
		return true, fmt.Sprintf(
			"agent loop exceeded max duration (%s)",
			stepDef.Loop.MaxDuration,
		)
	}
	return false, ""
}

func (o *Orchestrator) failLoopStep(
	run dag.WorkflowRun, stepID string,
	state dag.StepState, reason string,
) error {
	if stepID == "" {
		panic("failLoopStep: stepID must not be empty")
	}
	if reason == "" {
		panic("failLoopStep: reason must not be empty")
	}
	state.Status = dag.StepStatusFailed
	state.Error = reason
	run.Steps[stepID] = state
	run.Status = dag.RunStatusFailed
	if err := o.store.Save(run); err != nil {
		return err
	}
	return o.publishWorkflowFailed(run.RunID)
}

// handleStepFailed processes a failure event published by a worker.
// If the step has retries remaining (StepDef.Retries), the
// orchestrator re-publishes the task with an incremented attempt.
// Otherwise the step and workflow are marked permanently failed.
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
	state := run.Steps[evt.StepID]
	state.Attempts++
	if evt.Payload != nil {
		state.Error = string(evt.Payload)
	}
	stepDef, _ := findStepDef(wfDef, evt.StepID)
	if state.Attempts <= stepDef.Retries {
		run.Steps[evt.StepID] = state
		if err := o.store.Save(run); err != nil {
			return err
		}
		input, err := dag.ResolveInput(stepDef, run.Steps)
		if err != nil {
			return fmt.Errorf("resolve input for retry: %w", err)
		}
		return o.publishTask(
			run.RunID, stepDef, input, state.Attempts,
		)
	}
	state.Status = dag.StepStatusFailed
	run.Steps[evt.StepID] = state
	run.Status = dag.RunStatusFailed
	if err := o.store.Save(run); err != nil {
		return err
	}
	return o.publishWorkflowFailed(run.RunID)
}

// enqueueReady resolves skipped and ready steps. Skips are processed
// in a bounded loop so cascading skip chains are handled eagerly.
func (o *Orchestrator) enqueueReady(
	wfDef dag.WorkflowDef, run dag.WorkflowRun,
) error {
	if run.RunID == "" {
		panic("enqueueReady: RunID must not be empty")
	}
	completed := completedSet(run)
	queued := queuedSet(run)

	// Bounded skip cascade — max iterations = number of steps.
	maxSkipRounds := len(wfDef.Steps)
	for range maxSkipRounds {
		skipped := dag.ResolveSkipped(
			wfDef, completed, queued, run.Steps,
		)
		if len(skipped) == 0 {
			break
		}
		for _, step := range skipped {
			state := run.Steps[step.ID]
			state.Status = dag.StepStatusSkipped
			run.Steps[step.ID] = state
			completed[step.ID] = true
		}
	}
	if dag.IsComplete(wfDef, completed) {
		run.Status = dag.RunStatusCompleted
		if err := o.store.Save(run); err != nil {
			return err
		}
		return o.publishWorkflowCompleted(run.RunID)
	}

	ready := dag.ResolveReady(wfDef, completed, queued)
	filtered := ready[:0]
	for _, step := range ready {
		if run.Steps[step.ID].Status != dag.StepStatusSkipped {
			filtered = append(filtered, step)
		}
	}
	ready = filtered
	if len(ready) == 0 {
		if err := o.store.Save(run); err != nil {
			return err
		}
		return nil
	}
	for _, step := range ready {
		state := run.Steps[step.ID]
		state.Status = dag.StepStatusQueued
		run.Steps[step.ID] = state
	}
	if err := o.store.Save(run); err != nil {
		return err
	}
	for _, step := range ready {
		input, err := dag.ResolveInput(step, run.Steps)
		if err != nil {
			return fmt.Errorf("resolve input for %q: %w", step.ID, err)
		}
		attempt := run.Steps[step.ID].Attempts
		if err := o.publishTask(run.RunID, step, input, attempt); err != nil {
			return err
		}
	}
	return nil
}

func (o *Orchestrator) publishTask(
	runID string, step dag.StepDef, input []byte, attempt int,
) error {
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
	subject := "task." + step.Task + "." + runID
	msgID := fmt.Sprintf("%s.%s.attempt.%d", runID, step.ID, attempt)
	_, err = o.js.Publish(subject, data, nats.MsgId(msgID))
	return err
}

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
	msgID := fmt.Sprintf(
		"%s.%s.continue.%d", runID, step.ID, iteration,
	)
	_, err = o.js.Publish(subject, data, nats.MsgId(msgID))
	return err
}

// findStepDef looks up a step by ID in the workflow definition.
func findStepDef(
	wfDef dag.WorkflowDef, stepID string,
) (dag.StepDef, bool) {
	for _, s := range wfDef.Steps {
		if s.ID == stepID {
			return s, true
		}
	}
	return dag.StepDef{}, false
}

func (o *Orchestrator) loadRunAndDef(
	runID string,
) (dag.WorkflowDef, dag.WorkflowRun, error) {
	if runID == "" {
		panic("loadRunAndDef: runID must not be empty")
	}
	run, err := o.store.Load(runID)
	if err != nil {
		return dag.WorkflowDef{}, dag.WorkflowRun{},
			fmt.Errorf("load run %q: %w", runID, err)
	}
	entry, err := o.defKV.Get(run.WorkflowID)
	if err != nil {
		return dag.WorkflowDef{}, dag.WorkflowRun{},
			fmt.Errorf("load def %q: %w", run.WorkflowID, err)
	}
	var wfDef dag.WorkflowDef
	if err := json.Unmarshal(entry.Value(), &wfDef); err != nil {
		return dag.WorkflowDef{}, dag.WorkflowRun{},
			fmt.Errorf("unmarshal def %q: %w", run.WorkflowID, err)
	}
	return wfDef, run, nil
}

func (o *Orchestrator) publishWorkflowCompleted(runID string) error {
	if runID == "" {
		panic("publishWorkflowCompleted: runID must not be empty")
	}
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowCompleted, runID, nil,
	)
	data, err := evt.Marshal()
	if err != nil {
		return fmt.Errorf("marshal workflow.completed: %w", err)
	}
	_, err = o.js.Publish(
		evt.NATSSubject(), data, nats.MsgId(evt.NATSMsgID()),
	)
	return err
}

func (o *Orchestrator) publishWorkflowFailed(runID string) error {
	if runID == "" {
		panic("publishWorkflowFailed: runID must not be empty")
	}
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowFailed, runID, nil,
	)
	data, err := evt.Marshal()
	if err != nil {
		return fmt.Errorf("marshal workflow.failed: %w", err)
	}
	_, err = o.js.Publish(
		evt.NATSSubject(), data, nats.MsgId(evt.NATSMsgID()),
	)
	return err
}

// completedSet returns step IDs whose status is Completed or Skipped.
func completedSet(run dag.WorkflowRun) map[string]bool {
	if run.Steps == nil {
		panic("completedSet: run.Steps must not be nil")
	}
	result := make(map[string]bool, len(run.Steps))
	for id, state := range run.Steps {
		if state.Status == dag.StepStatusCompleted ||
			state.Status == dag.StepStatusSkipped {
			result[id] = true
		}
	}
	return result
}

// queuedSet returns step IDs whose status is Queued or beyond.
func queuedSet(run dag.WorkflowRun) map[string]bool {
	if run.Steps == nil {
		panic("queuedSet: run.Steps must not be nil")
	}
	result := make(map[string]bool, len(run.Steps))
	for id, state := range run.Steps {
		switch state.Status {
		case dag.StepStatusQueued, dag.StepStatusRunning,
			dag.StepStatusCompleted, dag.StepStatusFailed,
			dag.StepStatusSkipped:
			result[id] = true
		}
	}
	return result
}
