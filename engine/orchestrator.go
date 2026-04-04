// engine/orchestrator.go
// The orchestrator is the thin I/O shell of DagNats. It subscribes to the
// history stream, resolves DAG dependencies via dag.ResolveReady, and publishes
// task messages. All delivery guarantees, retries, and timeouts are handled by
// NATS — this file contains no timers, no retry logic, no in-memory queues.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"golang.org/x/sync/errgroup"
)

// Orchestrator subscribes to the history stream and drives workflow execution.
// It is intentionally stateless between events — all run state lives in the
// snapshot store (NATS KV), so the orchestrator can crash and resume safely.
type Orchestrator struct {
	nc          *nats.Conn
	js          nats.JetStreamContext
	defKV       nats.KeyValue
	store       *SnapshotStore
	tel         *observe.Telemetry
	sub         *nats.Subscription
	runLocks    sync.Map                // map[string]*sync.Mutex — per-run serialization
	stepRoutes  map[dag.StepType]string // step type → subject prefix
	concurrency *ConcurrencyManager     // nil if bucket missing

	// Pre-allocated metric instruments — created once in constructor.
	runsActive       observe.Gauge
	runsCompleted    observe.Counter
	runsFailed       observe.Counter
	stepEnqueueCount observe.Counter
	snapshotDuration observe.Histogram
}

// OrchestratorOption configures optional orchestrator behavior.
type OrchestratorOption func(*Orchestrator)

// WithStepRoutes configures step type → subject prefix routing.
// Steps with types not in the map route to "task" (default).
func WithStepRoutes(
	routes map[dag.StepType]string,
) OrchestratorOption {
	return func(o *Orchestrator) { o.stepRoutes = routes }
}

// NewOrchestrator creates an Orchestrator bound to the given NATS connection.
// Panics if nc is nil or JetStream cannot be obtained — both are programmer
// errors. KV buckets must already exist (call natsutil.SetupAll first).
func NewOrchestrator(
	nc *nats.Conn, tel *observe.Telemetry,
	opts ...OrchestratorOption,
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
		panic(
			"NewOrchestrator: workflow_defs bucket not found: " +
				err.Error(),
		)
	}
	cm, _ := NewConcurrencyManagerSafe(js)
	o := &Orchestrator{
		nc:          nc,
		js:          js,
		defKV:       defKV,
		store:       NewSnapshotStore(js),
		tel:         tel,
		concurrency: cm,
		runsActive: tel.Metrics.Gauge(
			"workflow.runs.active", nil,
		),
		runsCompleted: tel.Metrics.Counter(
			"workflow.runs.completed", nil,
		),
		runsFailed: tel.Metrics.Counter(
			"workflow.runs.failed", nil,
		),
		stepEnqueueCount: tel.Metrics.Counter(
			"step.enqueue.count", nil,
		),
		snapshotDuration: tel.Metrics.Histogram(
			"snapshot.save.duration_ms", nil,
		),
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// Start subscribes to history.> on the WORKFLOW_HISTORY stream using
// push-subscribe. Messages are delivered asynchronously to handleEvent.
// Panics if already started.
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

// Stop drains and unsubscribes from the history stream.
// Safe to call multiple times.
func (o *Orchestrator) Stop() {
	if o.sub == nil {
		return
	}
	if err := o.sub.Unsubscribe(); err != nil {
		o.tel.Logger.Error("Stop: unsubscribe error", err)
	}
	o.sub = nil
}

// getRunLock returns a per-run mutex, creating one on first access.
// Serializes all event handling for a given run to prevent concurrent
// KV load-modify-save races between parallel step completions.
func (o *Orchestrator) getRunLock(runID string) *sync.Mutex {
	val, _ := o.runLocks.LoadOrStore(runID, &sync.Mutex{})
	return val.(*sync.Mutex)
}

// handleEvent is the central dispatcher. It unmarshals the event, extracts
// trace context, and routes to the appropriate handler. Unknown event types
// are acked and logged — not errors.
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
	if !isHandledEventType(evt.Type) {
		msg.Ack()
		return
	}
	ctx := extractTraceCtx(msg, &evt)
	ctx, span := o.tel.Tracer.Start(ctx,
		"orchestrator.handleEvent",
		observe.WithSpanKind(observe.SpanKindServer),
		observe.WithAttributes(
			observe.StringAttr("run_id", evt.RunID),
			observe.StringAttr("event_type", string(evt.Type)),
			observe.StringAttr("step_id", evt.StepID),
		),
	)
	defer span.End()
	err = o.dispatchEvent(ctx, evt)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(observe.StatusError, err.Error())
		o.tel.Logger.Error("handleEvent: handler error", err,
			observe.String("event_type", string(evt.Type)),
			observe.String("run_id", evt.RunID),
		)
		msg.NakWithDelay(5 * time.Second)
		return
	}
	msg.Ack()
}

// isHandledEventType returns true for event types the orchestrator processes.
func isHandledEventType(t protocol.EventType) bool {
	switch t {
	case protocol.EventWorkflowStarted,
		protocol.EventStepCompleted,
		protocol.EventStepContinue,
		protocol.EventStepFailed,
		protocol.EventWorkflowSpawn,
		protocol.EventWorkflowCancelled:
		return true
	}
	return false
}

// dispatchEvent routes an event to its handler under a per-run lock.
func (o *Orchestrator) dispatchEvent(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("dispatchEvent: RunID must not be empty")
	}
	lock := o.getRunLock(evt.RunID)
	lock.Lock()
	defer lock.Unlock()

	// Check workflow timeout before dispatching any event.
	run, loadErr := o.store.Load(evt.RunID)
	if loadErr == nil && run.Deadline != nil &&
		time.Now().After(*run.Deadline) &&
		run.Status == dag.RunStatusRunning {
		return o.handleWorkflowCancelled(ctx, evt)
	}

	switch evt.Type {
	case protocol.EventWorkflowStarted:
		return o.handleWorkflowStarted(ctx, evt)
	case protocol.EventStepCompleted:
		return o.handleStepCompleted(ctx, evt)
	case protocol.EventStepContinue:
		return o.handleStepContinue(ctx, evt)
	case protocol.EventStepFailed:
		return o.handleStepFailed(ctx, evt)
	case protocol.EventWorkflowSpawn:
		return o.handleWorkflowSpawn(ctx, evt)
	case protocol.EventWorkflowCancelled:
		return o.handleWorkflowCancelled(ctx, evt)
	default:
		return nil
	}
}

// handleWorkflowStarted creates the initial WorkflowRun from the event
// payload, saves it, then enqueues all entry-point steps. If concurrency
// limit is configured and reached, the run stays Pending.
func (o *Orchestrator) handleWorkflowStarted(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleWorkflowStarted: RunID must not be empty")
	}
	if evt.Payload == nil {
		panic("handleWorkflowStarted: Payload must not be nil")
	}

	// The payload can be either just the WorkflowDef (backward compat)
	// or a structure containing both def and input.
	var startPayload struct {
		WorkflowDef json.RawMessage `json:"workflow_def"`
		Input       json.RawMessage `json:"input"`
	}
	var wfDef dag.WorkflowDef
	var input json.RawMessage

	// Try to unmarshal as structured payload first
	if err := json.Unmarshal(evt.Payload, &startPayload); err == nil &&
		startPayload.WorkflowDef != nil {
		// New format with separate workflow_def and input
		if err := json.Unmarshal(
			startPayload.WorkflowDef, &wfDef,
		); err != nil {
			return fmt.Errorf("unmarshal WorkflowDef: %w", err)
		}
		input = startPayload.Input
	} else {
		// Backward compat: payload is just the WorkflowDef
		if err := json.Unmarshal(evt.Payload, &wfDef); err != nil {
			return fmt.Errorf("unmarshal WorkflowDef: %w", err)
		}
		input = nil
	}

	// Validate input against schema if configured.
	if wfDef.InputSchema != nil {
		if err := dag.ValidateSchema(wfDef.InputSchema, input); err != nil {
			// Create a failed run for visibility
			run := dag.NewWorkflowRun(wfDef, evt.RunID)
			run.Status = dag.RunStatusFailed
			o.saveSnapshot(ctx, run)
			return fmt.Errorf("input validation: %w", err)
		}
	}

	run := dag.NewWorkflowRun(wfDef, evt.RunID)
	run.Input = input

	// Check concurrency limit if configured.
	if wfDef.Concurrency != nil && o.concurrency != nil {
		acquired, err := o.concurrency.AcquireRun(
			wfDef.Name, wfDef.Concurrency.MaxRuns,
		)
		if err != nil {
			return fmt.Errorf("acquire run: %w", err)
		}
		if !acquired {
			// Limit reached — save as Pending and return.
			run.Status = dag.RunStatusPending
			if err := o.saveSnapshot(ctx, run); err != nil {
				return fmt.Errorf("save pending run: %w", err)
			}
			return nil
		}
	}

	run.Status = dag.RunStatusRunning
	if wfDef.Timeout > 0 {
		deadline := time.Now().Add(wfDef.Timeout)
		run.Deadline = &deadline
	}
	if err := o.saveSnapshot(ctx, run); err != nil {
		return fmt.Errorf("save initial run: %w", err)
	}
	o.runsActive.Inc()
	return o.enqueueReady(ctx, wfDef, run)
}

// handleStepCompleted marks the step output in the snapshot, then checks
// whether the workflow is fully complete or new steps have become unblocked.
func (o *Orchestrator) handleStepCompleted(
	ctx context.Context, evt protocol.Event,
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

	// Check if this completed step is an OnFailure handler.
	// If so, mark the original failed step as Recovered and
	// skip its dependents.
	o.recoverIfOnFailure(wfDef, &run, evt.StepID)

	// Check if this is a compensate step completing.
	if o.handleCompensateStepCompleted(
		ctx, wfDef, &run, evt.StepID,
	) {
		return nil
	}

	completed := completedSet(run)
	if dag.IsComplete(wfDef, completed) {
		return o.completeWorkflow(ctx, run)
	}
	if err := o.saveSnapshot(ctx, run); err != nil {
		return err
	}
	return o.enqueueReady(ctx, wfDef, run)
}

// recoverIfOnFailure checks if stepID is an OnFailure target for a
// failed step. If so, transitions the failed step to Recovered and
// marks dependents of the failed step as Skipped.
func (o *Orchestrator) recoverIfOnFailure(
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	completedStepID string,
) {
	for _, stepDef := range wfDef.Steps {
		if stepDef.OnFailure != completedStepID {
			continue
		}
		failedState := run.Steps[stepDef.ID]
		if failedState.Status != dag.StepStatusFailed {
			continue
		}
		// Mark the original failed step as recovered
		failedState.Status = dag.StepStatusRecovered
		run.Steps[stepDef.ID] = failedState

		// Skip dependents of the failed step — they can't run
		// without its output
		for _, s := range wfDef.Steps {
			for _, dep := range s.DependsOn {
				if dep == stepDef.ID {
					depState := run.Steps[s.ID]
					if depState.Status == dag.StepStatusPending {
						depState.Status = dag.StepStatusSkipped
						run.Steps[s.ID] = depState
					}
				}
			}
		}
		return
	}
}

// completeWorkflow marks the run complete, saves, publishes the event,
// adjusts metrics, and releases concurrency slot.
func (o *Orchestrator) completeWorkflow(
	ctx context.Context, run dag.WorkflowRun,
) error {
	if run.RunID == "" {
		panic("completeWorkflow: RunID must not be empty")
	}
	run.Status = dag.RunStatusCompleted
	if err := o.saveSnapshot(ctx, run); err != nil {
		return err
	}
	o.runsActive.Dec()
	o.runsCompleted.Inc()
	if o.concurrency != nil {
		if err := o.concurrency.ReleaseRun(run.WorkflowID); err != nil {
			return fmt.Errorf("release run: %w", err)
		}
		// Auto-start next pending run if available
		if err := o.startNextPendingRun(ctx, run.WorkflowID); err != nil {
			o.tel.Logger.Error(
				"failed to start next pending run", err,
				observe.String("workflow_id", run.WorkflowID),
			)
		}
	}
	if err := o.publishWorkflowCompleted(run.RunID); err != nil {
		return err
	}
	return o.notifyParentIfChild(run, nil)
}

// startNextPendingRun finds the oldest pending run for a workflow and
// transitions it to Running. Called after ReleaseRun to enable queue
// progression. No-op if no pending runs exist.
func (o *Orchestrator) startNextPendingRun(
	ctx context.Context, workflowID string,
) error {
	if workflowID == "" {
		panic("startNextPendingRun: workflowID must not be empty")
	}
	if o.store == nil {
		panic("startNextPendingRun: store must not be nil")
	}

	runID, found, err := o.findOldestPendingRun(workflowID)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	return o.transitionPendingToRunning(ctx, runID)
}

// findOldestPendingRun scans workflow_runs KV for the oldest pending
// run for the given workflow. Returns (runID, true, nil) when found.
func (o *Orchestrator) findOldestPendingRun(
	workflowID string,
) (string, bool, error) {
	if workflowID == "" {
		panic("findOldestPendingRun: workflowID must not be empty")
	}
	if o.store == nil {
		panic("findOldestPendingRun: store must not be nil")
	}
	keys, err := o.store.kv.Keys()
	if err != nil {
		return "", false, fmt.Errorf("list run keys: %w", err)
	}

	entries, err := natsutil.ParallelGet(
		o.store.kv, keys, natsutil.DefaultParallelism,
	)
	if err != nil {
		return "", false, fmt.Errorf(
			"parallel get runs: %w", err,
		)
	}

	var oldestRun dag.WorkflowRun
	var foundPending bool

	for _, entry := range entries {
		var run dag.WorkflowRun
		if err := json.Unmarshal(entry.Value(), &run); err != nil {
			continue
		}
		if run.WorkflowID != workflowID {
			continue
		}
		if run.Status != dag.RunStatusPending {
			continue
		}
		if !foundPending ||
			run.CreatedAt.Before(oldestRun.CreatedAt) {
			oldestRun = run
			foundPending = true
		}
	}

	if !foundPending {
		return "", false, nil
	}
	return oldestRun.RunID, true, nil
}

// transitionPendingToRunning loads a pending run, acquires concurrency,
// transitions to Running, and enqueues entry steps.
func (o *Orchestrator) transitionPendingToRunning(
	ctx context.Context, runID string,
) error {
	if runID == "" {
		panic("transitionPendingToRunning: runID must not be empty")
	}
	wfDef, run, err := o.loadRunAndDef(runID)
	if err != nil {
		return fmt.Errorf("load pending run %q: %w", runID, err)
	}

	if wfDef.Concurrency != nil {
		acquired, err := o.concurrency.AcquireRun(
			wfDef.Name, wfDef.Concurrency.MaxRuns,
		)
		if err != nil {
			return fmt.Errorf("acquire for pending run: %w", err)
		}
		if !acquired {
			return nil // Slot not available (shouldn't happen)
		}
	}

	run.Status = dag.RunStatusRunning
	if wfDef.Timeout > 0 {
		deadline := time.Now().Add(wfDef.Timeout)
		run.Deadline = &deadline
	}
	if err := o.saveSnapshot(ctx, run); err != nil {
		return fmt.Errorf("save running run: %w", err)
	}
	o.runsActive.Inc()
	return o.enqueueReady(ctx, wfDef, run)
}

// handleStepContinue re-enqueues an agent-loop step for another iteration.
// MaxIterations and MaxDuration are enforced; violations fail the run.
func (o *Orchestrator) handleStepContinue(
	ctx context.Context, evt protocol.Event,
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
		return fmt.Errorf(
			"step %q not found in workflow def", evt.StepID,
		)
	}
	state := run.Steps[evt.StepID]
	state.Iterations++
	if state.Iterations == 1 {
		state.LoopStartedAt = time.Now().UTC()
	}
	if exceeded, reason := checkLoopBounds(stepDef, state); exceeded {
		return o.failLoopStep(ctx, run, evt.StepID, state, reason)
	}
	run.Steps[evt.StepID] = state
	if err := o.saveSnapshot(ctx, run); err != nil {
		return err
	}
	input, err := dag.ResolveInput(stepDef, run.Steps)
	if err != nil {
		return fmt.Errorf(
			"resolve input for step %q: %w", stepDef.ID, err,
		)
	}
	// If LoopDelay is configured, delay re-enqueue via a context-aware
	// timer goroutine. Cancels cleanly if context expires before delay.
	if stepDef.Loop != nil && stepDef.Loop.LoopDelay > 0 {
		delay := stepDef.Loop.LoopDelay
		runID := run.RunID
		iter := state.Iterations
		loopCtx := ctx
		go func() {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-loopCtx.Done():
				return
			case <-timer.C:
				pubErr := o.publishIterationTask(
					loopCtx, runID, stepDef, input, iter,
				)
				if pubErr != nil {
					o.tel.Logger.Error(
						"delayed iteration publish failed", pubErr,
						observe.String("run_id", runID),
						observe.String("step_id", stepDef.ID),
					)
				}
			}
		}()
		return nil
	}
	return o.publishIterationTask(
		ctx, run.RunID, stepDef, input, state.Iterations,
	)
}

// findStepDef locates a step definition by ID within a workflow def.
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

// checkLoopBounds returns (true, reason) when the step has hit its
// MaxIterations or MaxDuration ceiling. Both limits are checked.
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

// failLoopStep marks the step and run as failed, saves state, publishes
// a workflow.failed event, and adjusts metrics.
func (o *Orchestrator) failLoopStep(
	ctx context.Context,
	run dag.WorkflowRun,
	stepID string,
	state dag.StepState,
	reason string,
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
	if err := o.saveSnapshot(ctx, run); err != nil {
		return err
	}
	o.runsActive.Dec()
	o.runsFailed.Inc()
	if o.concurrency != nil {
		if err := o.concurrency.ReleaseRun(run.WorkflowID); err != nil {
			return fmt.Errorf("release run: %w", err)
		}
		// Auto-start next pending run if available
		if err := o.startNextPendingRun(ctx, run.WorkflowID); err != nil {
			o.tel.Logger.Error(
				"failed to start next pending run", err,
				observe.String("workflow_id", run.WorkflowID),
			)
		}
	}
	if err := o.publishWorkflowFailed(run.RunID); err != nil {
		return err
	}
	return o.notifyParentIfChild(run, fmt.Errorf("%s", reason))
}

// handleStepFailed processes a step failure event. If the step has retries
// remaining, it stays queued for JetStream redelivery. If retries are
// exhausted (or the step has zero retries configured), the step and workflow
// are marked as permanently failed.
func (o *Orchestrator) handleStepFailed(
	ctx context.Context, evt protocol.Event,
) error {
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
	policy := dag.ResolveRetryPolicy(wfDef, stepDef)

	if policy != nil && state.Attempts <= policy.MaxAttempts {
		// Retries remaining — save state and let NATS redeliver.
		run.Steps[evt.StepID] = state
		return o.saveSnapshot(ctx, run)
	}

	// Retries exhausted — permanent failure.
	state.Status = dag.StepStatusFailed
	run.Steps[evt.StepID] = state

	// If this is an auxiliary step (compensate target) failing,
	// the compensation itself failed — critical state.
	if wfDef.AuxSteps[evt.StepID] {
		run.Status = dag.RunStatusCompensateFailed
		if err := o.saveSnapshot(ctx, run); err != nil {
			return err
		}
		o.runsActive.Dec()
		o.runsFailed.Inc()
		o.publishDeadLetter(run.RunID, stepDef, state)
		return o.notifyParentIfChild(
			run,
			fmt.Errorf("compensation failed: %s", state.Error),
		)
	}

	// Check for on-failure handler before failing the workflow.
	if stepDef.OnFailure != "" {
		onFailStep, found := findStepDef(wfDef, stepDef.OnFailure)
		if found {
			ofState := run.Steps[onFailStep.ID]
			ofState.Status = dag.StepStatusQueued
			run.Steps[onFailStep.ID] = ofState
			if err := o.saveSnapshot(ctx, run); err != nil {
				return err
			}
			errorInput := []byte(fmt.Sprintf(
				`{"failed_step":"%s","error":%s}`,
				evt.StepID, state.Error))
			return o.publishTask(ctx, run.RunID, onFailStep,
				errorInput, 0)
		}
	}

	// No on-failure handler — check for compensation.
	completed := completedSet(run)
	chain := dag.ResolveCompensateChain(
		wfDef, completed, evt.StepID,
	)
	if len(chain) > 0 {
		return o.startCompensation(
			ctx, wfDef, &run, evt.StepID, state.Error,
		)
	}

	// No compensation either — fail the workflow.
	return o.failWorkflow(ctx, run, stepDef, state)
}

// failWorkflow marks the workflow as permanently failed and releases
// resources. Extracted to avoid duplication between failure paths.
func (o *Orchestrator) failWorkflow(
	ctx context.Context,
	run dag.WorkflowRun,
	stepDef dag.StepDef,
	state dag.StepState,
) error {
	run.Status = dag.RunStatusFailed
	if err := o.saveSnapshot(ctx, run); err != nil {
		return err
	}
	o.runsActive.Dec()
	o.runsFailed.Inc()
	if o.concurrency != nil {
		if err := o.concurrency.ReleaseRun(run.WorkflowID); err != nil {
			return fmt.Errorf("release run: %w", err)
		}
		if err := o.startNextPendingRun(ctx, run.WorkflowID); err != nil {
			o.tel.Logger.Error(
				"failed to start next pending run", err,
				observe.String("workflow_id", run.WorkflowID),
			)
		}
	}
	if err := o.publishWorkflowFailed(run.RunID); err != nil {
		return err
	}
	o.publishDeadLetter(run.RunID, stepDef, state)
	return o.notifyParentIfChild(
		run, fmt.Errorf("%s", state.Error),
	)
}

// startCompensation begins the saga compensation chain. Resolves
// compensate steps in reverse topo order and enqueues the first one.
func (o *Orchestrator) startCompensation(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	failedStepID string,
	failedError string,
) error {
	completed := completedSet(*run)
	chain := dag.ResolveCompensateChain(
		wfDef, completed, failedStepID,
	)
	if len(chain) == 0 {
		return nil
	}

	// Mark compensate steps as queued
	for _, step := range chain {
		state := run.Steps[step.ID]
		state.Status = dag.StepStatusQueued
		run.Steps[step.ID] = state
	}
	if err := o.saveSnapshot(ctx, *run); err != nil {
		return err
	}

	// Build input for the first compensate step
	first := chain[0]
	originalID := findCompensateSource(wfDef, first.ID)
	input := buildCompensateInput(
		originalID, run.Steps[originalID].Output,
		failedStepID, failedError,
	)
	return o.publishTask(ctx, run.RunID, first, input, 0)
}

// handleCompensateStepCompleted checks if the completed step is part
// of a compensation chain. If the chain is done, marks the workflow
// as Compensated. If more steps remain, publishes the next one.
// Returns true if this was a compensate step (caller should return).
func (o *Orchestrator) handleCompensateStepCompleted(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	stepID string,
) bool {
	if !wfDef.AuxSteps[stepID] {
		return false
	}
	// Only handle steps that are Compensate targets, not OnFailure.
	if findCompensateSource(wfDef, stepID) == "" {
		return false
	}

	// Find the next queued compensate step
	for _, step := range wfDef.Steps {
		if step.Compensate == "" {
			continue
		}
		compState := run.Steps[step.Compensate]
		if compState.Status != dag.StepStatusQueued {
			continue
		}
		// Found a queued compensate step — publish it
		compDef, _ := findStepDef(wfDef, step.Compensate)

		// Find the original failure for error context
		var failedStepID, failedError string
		for _, s := range wfDef.Steps {
			st := run.Steps[s.ID]
			if st.Status == dag.StepStatusFailed {
				failedStepID = s.ID
				failedError = st.Error
				break
			}
		}

		input := buildCompensateInput(
			step.ID, run.Steps[step.ID].Output,
			failedStepID, failedError,
		)
		o.saveSnapshot(ctx, *run)
		o.publishTask(ctx, run.RunID, compDef, input, 0)
		return true
	}

	// All compensate steps done — mark workflow as Compensated
	run.Status = dag.RunStatusCompensated
	o.saveSnapshot(ctx, *run)
	o.runsActive.Dec()
	return true
}

// findCompensateSource returns the step ID whose Compensate field
// points to the given compensate step ID.
func findCompensateSource(
	wfDef dag.WorkflowDef, compStepID string,
) string {
	for _, step := range wfDef.Steps {
		if step.Compensate == compStepID {
			return step.ID
		}
	}
	return ""
}

// buildCompensateInput creates the JSON input for a compensate step.
func buildCompensateInput(
	originalID string, originalOutput []byte,
	failedStepID string, failedError string,
) []byte {
	return []byte(fmt.Sprintf(
		`{"original_step":%q,"original_output":%s,`+
			`"trigger_step":%q,"trigger_error":%s}`,
		originalID,
		jsonOrNull(originalOutput),
		failedStepID,
		jsonOrNull([]byte(failedError)),
	))
}

func jsonOrNull(b []byte) string {
	if len(b) == 0 {
		return "null"
	}
	return string(b)
}

// publishDeadLetter publishes failed step info to the dead-letter queue.
func (o *Orchestrator) publishDeadLetter(
	runID string, stepDef dag.StepDef, state dag.StepState,
) {
	if runID == "" {
		panic("publishDeadLetter: runID must not be empty")
	}
	if stepDef.ID == "" {
		panic("publishDeadLetter: stepDef.ID must not be empty")
	}
	payload, err := json.Marshal(map[string]interface{}{
		"run_id":   runID,
		"step_id":  stepDef.ID,
		"task":     stepDef.Task,
		"error":    state.Error,
		"attempts": state.Attempts,
	})
	if err != nil {
		return
	}
	subject := fmt.Sprintf("dead.%s.%s.%s",
		stepDef.Task, runID, stepDef.ID)
	o.js.Publish(subject, payload)
}

// handleWorkflowCancelled marks the run and all in-flight steps as
// cancelled, saves state, and adjusts metrics.
func (o *Orchestrator) handleWorkflowCancelled(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleWorkflowCancelled: RunID must not be empty")
	}
	_, run, err := o.loadRunAndDef(evt.RunID)
	if err != nil {
		return err
	}
	if run.Status != dag.RunStatusRunning {
		return nil
	}

	run.Status = dag.RunStatusCancelled
	for id, state := range run.Steps {
		if state.Status == dag.StepStatusQueued ||
			state.Status == dag.StepStatusRunning ||
			state.Status == dag.StepStatusPending {
			state.Status = dag.StepStatusCancelled
			run.Steps[id] = state
		}
	}

	if err := o.saveSnapshot(ctx, run); err != nil {
		return err
	}
	o.runsActive.Dec()
	if o.concurrency != nil {
		if err := o.concurrency.ReleaseRun(run.WorkflowID); err != nil {
			return fmt.Errorf("release run: %w", err)
		}
		// Auto-start next pending run if available
		if err := o.startNextPendingRun(ctx, run.WorkflowID); err != nil {
			o.tel.Logger.Error(
				"failed to start next pending run", err,
				observe.String("workflow_id", run.WorkflowID),
			)
		}
	}
	return o.notifyParentIfChild(run, fmt.Errorf("cancelled"))
}

const maxNestingDepth = 3

// nestingDepth walks the parent chain to compute current depth.
// Returns 0 for top-level runs, 1 for first child, etc.
func (o *Orchestrator) nestingDepth(runID string) int {
	depth := 0
	currentID := runID
	for i := 0; i < maxNestingDepth+1; i++ {
		run, err := o.store.Load(currentID)
		if err != nil || run.ParentRunID == "" {
			break
		}
		depth++
		currentID = run.ParentRunID
	}
	return depth
}

// handleWorkflowSpawn creates a child WorkflowRun from a spawn event.
// The child is linked to the parent via ParentRunID and ParentStepID.
func (o *Orchestrator) handleWorkflowSpawn(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleWorkflowSpawn: RunID must not be empty")
	}
	var payload struct {
		ChildRunID    string `json:"child_run_id"`
		ChildWorkflow string `json:"child_workflow"`
		ParentStepID  string `json:"parent_step_id"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal spawn payload: %w", err)
	}
	if payload.ChildRunID == "" {
		panic("handleWorkflowSpawn: child_run_id must not be empty")
	}

	// Enforce max nesting depth by walking the parent chain.
	// The child would be at depth+1, so reject when depth+1 > max.
	depth := o.nestingDepth(evt.RunID)
	if depth+1 >= maxNestingDepth {
		o.tel.Logger.Error(
			"spawn rejected: max nesting depth exceeded",
			fmt.Errorf("depth %d >= max %d", depth, maxNestingDepth),
		)
		return fmt.Errorf(
			"max nesting depth %d exceeded", maxNestingDepth,
		)
	}

	entry, err := o.defKV.Get(payload.ChildWorkflow)
	if err != nil {
		return fmt.Errorf(
			"load child workflow def %q: %w",
			payload.ChildWorkflow, err,
		)
	}
	var childDef dag.WorkflowDef
	if err := json.Unmarshal(entry.Value(), &childDef); err != nil {
		return fmt.Errorf("unmarshal child def: %w", err)
	}

	childRun := dag.NewWorkflowRun(childDef, payload.ChildRunID)
	childRun.ParentRunID = evt.RunID
	childRun.ParentStepID = payload.ParentStepID
	childRun.Status = dag.RunStatusRunning

	if err := o.saveSnapshot(ctx, childRun); err != nil {
		return err
	}

	o.runsActive.Inc()
	return o.enqueueReady(ctx, childDef, childRun)
}

// notifyParentIfChild publishes a child completion or failure event on the
// parent's history subject when this run has a parent. No-op for top-level.
func (o *Orchestrator) notifyParentIfChild(
	run dag.WorkflowRun, childErr error,
) error {
	if run.ParentRunID == "" {
		return nil
	}

	eventType := protocol.EventWorkflowChildCompleted
	if childErr != nil {
		eventType = protocol.EventWorkflowChildFailed
	}

	payload, err := json.Marshal(map[string]interface{}{
		"child_run_id":   run.RunID,
		"parent_step_id": run.ParentStepID,
		"error":          errString(childErr),
	})
	if err != nil {
		return fmt.Errorf("marshal child event payload: %w", err)
	}

	evt := protocol.NewWorkflowEvent(
		eventType, run.ParentRunID, payload)
	data, err := evt.Marshal()
	if err != nil {
		return fmt.Errorf("marshal child event: %w", err)
	}
	msg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {evt.NATSMsgID()}},
	}
	_, err = o.js.PublishMsg(msg)
	return err
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// enqueueReady resolves all currently-ready steps and publishes one task
// message per step. Steps with satisfied SkipIf conditions are marked Skipped
// instead of enqueued, potentially unblocking further downstream steps.
func (o *Orchestrator) enqueueReady(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
) error {
	if run.RunID == "" {
		panic("enqueueReady: RunID must not be empty")
	}
	ctx, span := o.tel.Tracer.Start(ctx,
		"orchestrator.enqueueReady",
		observe.WithAttributes(
			observe.StringAttr("run_id", run.RunID),
		),
	)
	defer span.End()
	completed := completedSet(run)
	queued := queuedSet(run)

	// Process skipped steps first — they may unblock downstream steps
	// that would otherwise not appear in ResolveReady.
	skipped := dag.ResolveSkipped(wfDef, completed, queued, run.Steps)
	for _, step := range skipped {
		state := run.Steps[step.ID]
		state.Status = dag.StepStatusSkipped
		run.Steps[step.ID] = state
	}
	if len(skipped) > 0 {
		// Recompute completed set after marking skips.
		completed = completedSet(run)
		if dag.IsComplete(wfDef, completed) {
			return o.completeWorkflow(ctx, run)
		}
	}

	ready := dag.ResolveReady(wfDef, completed, queued)
	// Exclude steps that were just marked as skipped.
	filtered := make([]dag.StepDef, 0, len(ready))
	for _, step := range ready {
		if run.Steps[step.ID].Status != dag.StepStatusSkipped {
			filtered = append(filtered, step)
		}
	}
	ready = filtered
	span.SetAttributes(
		observe.Int64Attr("ready_steps_count", int64(len(ready))),
	)
	if len(ready) == 0 && len(skipped) == 0 {
		return nil
	}
	if len(ready) == 0 {
		return nil
	}
	for _, step := range ready {
		state := run.Steps[step.ID]
		state.Status = dag.StepStatusQueued
		run.Steps[step.ID] = state
	}
	if err := o.saveSnapshot(ctx, run); err != nil {
		return err
	}
	return o.publishReadyTasks(ctx, run.RunID, wfDef, run, ready)
}

// publishReadyTasks publishes a task message for each ready step.
// Steps are published concurrently since they are independent.
func (o *Orchestrator) publishReadyTasks(
	ctx context.Context,
	runID string,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	ready []dag.StepDef,
) error {
	if runID == "" {
		panic("publishReadyTasks: runID must not be empty")
	}
	if len(ready) == 0 {
		panic("publishReadyTasks: ready must not be empty")
	}
	var g errgroup.Group
	for _, step := range ready {
		step := step
		input, err := dag.ResolveInput(step, run.Steps)
		if err != nil {
			return fmt.Errorf(
				"resolve input for step %q: %w", step.ID, err,
			)
		}
		attempt := run.Steps[step.ID].Attempts
		g.Go(func() error {
			return o.publishTask(ctx, runID, step, input, attempt)
		})
	}
	return g.Wait()
}

// publishTask publishes a TaskPayload to task.{step.Task}.{runID} with
// dedup ID and trace context headers on the outgoing NATS message.
func (o *Orchestrator) publishTask(
	ctx context.Context,
	runID string,
	step dag.StepDef,
	input []byte,
	attempt int,
) error {
	if runID == "" {
		panic("publishTask: runID must not be empty")
	}
	if step.ID == "" {
		panic("publishTask: step.ID must not be empty")
	}
	ctx, span := o.tel.Tracer.Start(ctx,
		"orchestrator.enqueueTask",
		observe.WithSpanKind(observe.SpanKindClient),
		observe.WithAttributes(
			observe.StringAttr("run_id", runID),
			observe.StringAttr("step_id", step.ID),
			observe.StringAttr("task_name", step.Task),
		),
	)
	defer span.End()
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
	subject := o.stepSubject(step, runID)
	msg := buildTaskMsg(subject, data, msgID)
	injectTraceCtx(ctx, span, msg)
	_, err = o.js.PublishMsg(msg)
	o.stepEnqueueCount.Inc()
	return err
}

// publishIterationTask publishes a TaskPayload for an agent-loop
// re-enqueue. Each iteration's MsgId is distinct for JetStream dedup.
func (o *Orchestrator) publishIterationTask(
	ctx context.Context,
	runID string,
	step dag.StepDef,
	input []byte,
	iteration int,
) error {
	if runID == "" {
		panic("publishIterationTask: runID must not be empty")
	}
	if step.ID == "" {
		panic("publishIterationTask: step.ID must not be empty")
	}
	ctx, span := o.tel.Tracer.Start(ctx,
		"orchestrator.enqueueTask",
		observe.WithSpanKind(observe.SpanKindClient),
		observe.WithAttributes(
			observe.StringAttr("run_id", runID),
			observe.StringAttr("step_id", step.ID),
			observe.StringAttr("task_name", step.Task),
		),
	)
	defer span.End()
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
		"%s.%s.continue.%d", runID, step.ID, iteration,
	)
	subject := o.stepSubject(step, runID)
	msg := buildTaskMsg(subject, data, msgID)
	injectTraceCtx(ctx, span, msg)
	_, err = o.js.PublishMsg(msg)
	o.stepEnqueueCount.Inc()
	return err
}

// stepSubject resolves the NATS subject for a step based on routing config.
// Defaults to "task.{task}.{runID}" if no custom route is configured.
// When WorkerGroup is set, routes to "task.{task}.{group}.{runID}".
func (o *Orchestrator) stepSubject(
	step dag.StepDef, runID string,
) string {
	prefix := "task"
	if o.stepRoutes != nil {
		if p, ok := o.stepRoutes[step.Type]; ok {
			prefix = p
		}
	}
	subject := prefix + "." + step.Task
	if step.WorkerGroup != "" {
		subject += "." + step.WorkerGroup
	}
	return subject + "." + runID
}

// buildTaskMsg constructs a *nats.Msg with headers for task publishing.
func buildTaskMsg(
	subject string, data []byte, msgID string,
) *nats.Msg {
	if subject == "" {
		panic("buildTaskMsg: subject must not be empty")
	}
	if msgID == "" {
		panic("buildTaskMsg: msgID must not be empty")
	}
	return &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {msgID}},
	}
}

// saveSnapshot saves the run state to KV and records the duration.
func (o *Orchestrator) saveSnapshot(
	ctx context.Context, run dag.WorkflowRun,
) error {
	if run.RunID == "" {
		panic("saveSnapshot: RunID must not be empty")
	}
	start := time.Now()
	err := o.store.Save(run)
	elapsed := float64(time.Since(start).Milliseconds())
	o.snapshotDuration.Observe(elapsed)
	return err
}

// loadRunAndDef loads the workflow definition and current run snapshot.
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
			fmt.Errorf("load workflow def %q: %w",
				run.WorkflowID, err)
	}
	var wfDef dag.WorkflowDef
	if err := json.Unmarshal(entry.Value(), &wfDef); err != nil {
		return dag.WorkflowDef{}, dag.WorkflowRun{},
			fmt.Errorf("unmarshal workflow def %q: %w",
				run.WorkflowID, err)
	}
	return wfDef, run, nil
}

// publishWorkflowCompleted publishes a workflow.completed event.
func (o *Orchestrator) publishWorkflowCompleted(runID string) error {
	if runID == "" {
		panic("publishWorkflowCompleted: runID must not be empty")
	}
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowCompleted, runID, nil,
	)
	data, err := evt.Marshal()
	if err != nil {
		return fmt.Errorf(
			"marshal workflow.completed event: %w", err,
		)
	}
	_, err = o.js.Publish(
		evt.NATSSubject(), data, nats.MsgId(evt.NATSMsgID()),
	)
	return err
}

// publishWorkflowFailed publishes a workflow.failed event.
func (o *Orchestrator) publishWorkflowFailed(runID string) error {
	if runID == "" {
		panic("publishWorkflowFailed: runID must not be empty")
	}
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowFailed, runID, nil,
	)
	data, err := evt.Marshal()
	if err != nil {
		return fmt.Errorf(
			"marshal workflow.failed event: %w", err,
		)
	}
	_, err = o.js.Publish(
		evt.NATSSubject(), data, nats.MsgId(evt.NATSMsgID()),
	)
	return err
}

// extractTraceCtx reads W3C traceparent from the NATS message header
// or event payload and returns a context with parent span info.
func extractTraceCtx(
	msg *nats.Msg, evt *protocol.Event,
) context.Context {
	if msg == nil {
		panic("extractTraceCtx: msg must not be nil")
	}
	if evt == nil {
		panic("extractTraceCtx: evt must not be nil")
	}
	traceID, spanID, ok := parseTraceparent(msg, evt)
	if !ok {
		return context.Background()
	}
	return observe.ContextWithParentInfo(
		context.Background(), traceID, spanID,
	)
}

// parseTraceparent reads traceparent from NATS header first, falling
// back to the event field. Returns ok=false when neither has a value.
func parseTraceparent(
	msg *nats.Msg, evt *protocol.Event,
) (traceID, spanID string, ok bool) {
	if msg.Header != nil {
		if h := msg.Header.Get("traceparent"); h != "" {
			return splitTraceparent(h)
		}
	}
	if evt.TraceParent != "" {
		return splitTraceparent(evt.TraceParent)
	}
	return "", "", false
}

// splitTraceparent parses "00-{traceID}-{spanID}-{flags}" into parts.
func splitTraceparent(
	tp string,
) (traceID, spanID string, ok bool) {
	parts := strings.Split(tp, "-")
	if len(parts) != 4 || parts[0] != "00" {
		return "", "", false
	}
	return parts[1], parts[2], true
}

// injectTraceCtx writes traceparent to outgoing NATS message headers.
// Uses SpanContext type assertion to extract IDs from the active span.
// No-op when the span does not implement SpanContext or has empty IDs.
func injectTraceCtx(
	ctx context.Context, span observe.Span, msg *nats.Msg,
) {
	if msg == nil {
		panic("injectTraceCtx: msg must not be nil")
	}
	sc, ok := span.(observe.SpanContext)
	if !ok {
		return
	}
	traceID := sc.TraceID()
	spanID := sc.SpanID()
	if traceID == "" || spanID == "" {
		return
	}
	tp := "00-" + traceID + "-" + spanID + "-01"
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	msg.Header.Set("traceparent", tp)
}

// completedSet returns a set of step IDs whose status is Completed,
// Skipped, or Recovered. All three count as "resolved" for downstream
// dependency resolution and workflow completion checks.
func completedSet(run dag.WorkflowRun) map[string]bool {
	if run.Steps == nil {
		panic("completedSet: run.Steps must not be nil")
	}
	result := make(map[string]bool, len(run.Steps))
	for id, state := range run.Steps {
		if state.Status == dag.StepStatusCompleted ||
			state.Status == dag.StepStatusSkipped ||
			state.Status == dag.StepStatusRecovered {
			result[id] = true
		}
	}
	return result
}

// queuedSet returns a set of step IDs whose status is Queued or beyond.
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
