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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
	"golang.org/x/sync/errgroup"
)

// Orchestrator subscribes to the history stream and drives workflow execution.
// It is intentionally stateless between events — all run state lives in the
// snapshot store (NATS KV), so the orchestrator can crash and resume safely.
type Orchestrator struct {
	nc          *nats.Conn
	js          jetstream.JetStream
	defKV       jetstream.KeyValue
	store       *SnapshotStore
	tel         *observe.Telemetry
	cc          jetstream.ConsumeContext
	runLocks    sync.Map                // map[string]*sync.Mutex — per-run serialization
	stepRoutes  map[dag.StepType]string // step type → subject prefix
	concurrency *ConcurrencyManager     // nil if bucket missing
	rateLimiter *RateLimiter            // nil if bucket missing
	sleepTimer  *SleepTimer             // durable sleep via NakWithDelay
	correlator  *Correlator             // event wait-for-event matching
	stickyKV    jetstream.KeyValue      // sticky_bindings — run-to-worker

	// Pre-allocated metric instruments — created once in constructor.
	runsActive              observe.Gauge
	runsCompleted           observe.Counter
	runsFailed              observe.Counter
	stepEnqueueCount        observe.Counter
	snapshotDuration        observe.Histogram
	failNonRetriable        observe.Counter
	failRetryAfter          observe.Counter
	taskConcurrencyAcquired observe.Counter
	taskConcurrencyRejected observe.Counter
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
	js, err := jetstream.New(nc)
	if err != nil {
		panic("NewOrchestrator: jetstream.New: " + err.Error())
	}
	defKV, err := js.KeyValue(
		context.Background(), "workflow_defs",
	)
	if err != nil {
		panic(
			"NewOrchestrator: workflow_defs bucket not found: " +
				err.Error(),
		)
	}
	cm, _ := NewConcurrencyManagerSafe(js)
	rl := NewRateLimiter(js)
	o := &Orchestrator{
		nc:          nc,
		js:          js,
		defKV:       defKV,
		store:       NewSnapshotStore(js),
		tel:         tel,
		concurrency: cm,
		rateLimiter: rl,
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
		failNonRetriable: tel.Metrics.Counter(
			"step.failure.non_retriable", nil,
		),
		failRetryAfter: tel.Metrics.Counter(
			"step.failure.retry_after", nil,
		),
		taskConcurrencyAcquired: tel.Metrics.Counter(
			"task.concurrency.acquired", nil,
		),
		taskConcurrencyRejected: tel.Metrics.Counter(
			"task.concurrency.rejected", nil,
		),
	}
	o.sleepTimer = NewSleepTimer(nc, js)
	o.correlator = NewCorrelator(nc, js)
	o.stickyKV, _ = js.KeyValue(
		context.Background(), "sticky_bindings",
	)
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// Start subscribes to history.> on the WORKFLOW_HISTORY stream using
// a pull consumer. Messages are delivered asynchronously to handleEvent.
// Panics if already started.
func (o *Orchestrator) Start() {
	if o.cc != nil {
		panic("Orchestrator.Start: already started")
	}
	if err := o.sleepTimer.Start(); err != nil {
		panic(
			"Orchestrator.Start: sleepTimer failed: " +
				err.Error(),
		)
	}
	if err := o.correlator.Start(); err != nil {
		panic(
			"Orchestrator.Start: correlator failed: " +
				err.Error(),
		)
	}
	stream, err := o.js.Stream(
		context.Background(), "WORKFLOW_HISTORY",
	)
	if err != nil {
		panic(
			"Orchestrator.Start: stream: " + err.Error(),
		)
	}
	cons, err := stream.CreateOrUpdateConsumer(
		context.Background(), jetstream.ConsumerConfig{
			FilterSubject: "history.>",
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverAllPolicy,
		},
	)
	if err != nil {
		panic(
			"Orchestrator.Start: consumer: " + err.Error(),
		)
	}
	cc, err := cons.Consume(o.handleEventJS)
	if err != nil {
		panic(
			"Orchestrator.Start: consume: " + err.Error(),
		)
	}
	o.cc = cc
}

// Stop drains and unsubscribes from the history stream.
// Safe to call multiple times.
func (o *Orchestrator) Stop() {
	if o.correlator != nil {
		o.correlator.Stop()
	}
	if o.sleepTimer != nil {
		o.sleepTimer.Stop()
	}
	if o.cc == nil {
		return
	}
	o.cc.Stop()
	o.cc = nil
}

// getRunLock returns a per-run mutex, creating one on first access.
// Serializes all event handling for a given run to prevent concurrent
// KV load-modify-save races between parallel step completions.
func (o *Orchestrator) getRunLock(runID string) *sync.Mutex {
	val, _ := o.runLocks.LoadOrStore(runID, &sync.Mutex{})
	return val.(*sync.Mutex)
}

// handleEventJS is the central dispatcher. It unmarshals the event,
// extracts trace context, and routes to the appropriate handler.
// Unknown event types are acked and logged — not errors.
func (o *Orchestrator) handleEventJS(msg jetstream.Msg) {
	if msg == nil {
		return
	}
	evt, err := protocol.UnmarshalEvent(msg.Data())
	if err != nil {
		o.tel.Logger.Error("handleEvent: unmarshal failed", err)
		msg.NakWithDelay(5 * time.Second)
		return
	}
	if !isHandledEventType(evt.Type) {
		msg.Ack()
		return
	}
	ctx := extractTraceCtxJS(msg, &evt)
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
		protocol.EventWorkflowChildCompleted,
		protocol.EventWorkflowChildFailed,
		protocol.EventWorkflowCancelled,
		protocol.EventStepSleepCompleted,
		protocol.EventStepWaitMatched,
		protocol.EventStepWaitTimeout,
		protocol.EventApprovalGranted,
		protocol.EventApprovalRejected,
		protocol.EventApprovalExpired:
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
	run, loadErr := o.store.Load(ctx, evt.RunID)
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
	case protocol.EventStepSleepCompleted:
		return o.handleStepCompleted(ctx, evt)
	case protocol.EventStepWaitMatched:
		return o.handleStepCompleted(ctx, evt)
	case protocol.EventStepWaitTimeout:
		return o.handleWaitTimeout(ctx, evt)
	case protocol.EventStepContinue:
		return o.handleStepContinue(ctx, evt)
	case protocol.EventStepFailed:
		return o.handleStepFailed(ctx, evt)
	case protocol.EventWorkflowSpawn:
		return o.handleWorkflowSpawn(ctx, evt)
	case protocol.EventWorkflowChildCompleted:
		return o.handleChildCompleted(ctx, evt)
	case protocol.EventWorkflowChildFailed:
		return o.handleChildFailed(ctx, evt)
	case protocol.EventWorkflowCancelled:
		return o.handleWorkflowCancelled(ctx, evt)
	case protocol.EventApprovalGranted:
		return o.handleApprovalGranted(ctx, evt)
	case protocol.EventApprovalRejected:
		return o.handleApprovalRejected(ctx, evt)
	case protocol.EventApprovalExpired:
		return o.handleApprovalExpired(ctx, evt)
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
			ctx, wfDef.Name, wfDef.Concurrency.MaxRuns,
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
	wfDef, run, err := o.loadRunAndDef(ctx, evt.RunID)
	if err != nil {
		return err
	}

	// Check if this is a map instance completion.
	if isMapInstanceID(evt.StepID) {
		return o.handleMapInstanceCompleted(
			ctx, wfDef, run, evt,
		)
	}

	state := run.Steps[evt.StepID]
	state.Status = dag.StepStatusCompleted
	state.Output = evt.Payload
	run.Steps[evt.StepID] = state

	// Release task concurrency slot if configured.
	o.releaseTaskSlot(ctx, wfDef, evt.StepID)

	// If the completed step is a planner, materialize its output
	// into the running DAG before checking completion or enqueueing.
	stepDef, foundStep := findStepDef(wfDef, evt.StepID)
	if foundStep && stepDef.Type == dag.StepTypePlanner {
		return o.materializePlannerOutput(
			ctx, wfDef, run, stepDef, evt.Payload,
		)
	}

	// Create sticky binding if this is the first step of a
	// sticky workflow and the worker included its ID.
	o.createStickyBinding(ctx, wfDef, run, evt)

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
	o.deleteStickyBinding(ctx, run.RunID)
	o.runsActive.Dec()
	o.runsCompleted.Inc()
	if o.concurrency != nil {
		if err := o.concurrency.ReleaseRun(ctx, run.WorkflowID); err != nil {
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
	if err := o.publishWorkflowCompleted(ctx, run.RunID); err != nil {
		return err
	}
	return o.notifyParentIfChild(ctx, run, nil)
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

	runID, found, err := o.findOldestPendingRun(ctx, workflowID)
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
	ctx context.Context, workflowID string,
) (string, bool, error) {
	if workflowID == "" {
		panic("findOldestPendingRun: workflowID must not be empty")
	}
	if o.store == nil {
		panic("findOldestPendingRun: store must not be nil")
	}
	keys, err := o.store.kv.Keys(ctx)
	if err != nil {
		return "", false, fmt.Errorf("list run keys: %w", err)
	}

	entries, err := natsutil.ParallelGetJS(
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
	wfDef, run, err := o.loadRunAndDef(ctx, runID)
	if err != nil {
		return fmt.Errorf("load pending run %q: %w", runID, err)
	}

	if wfDef.Concurrency != nil {
		acquired, err := o.concurrency.AcquireRun(
			ctx, wfDef.Name, wfDef.Concurrency.MaxRuns,
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
	wfDef, run, err := o.loadRunAndDef(ctx, evt.RunID)
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
	loopCfg, _ := dag.ParseAgentLoopConfig(stepDef)
	if loopCfg.LoopDelay > 0 {
		delay := loopCfg.LoopDelay
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
	cfg, err := dag.ParseAgentLoopConfig(stepDef)
	if err != nil {
		return false, ""
	}
	if cfg.MaxIterations > 0 &&
		state.Iterations >= cfg.MaxIterations {
		return true, fmt.Sprintf(
			"agent loop exceeded max iterations (%d)",
			cfg.MaxIterations,
		)
	}
	if cfg.MaxDuration > 0 &&
		!state.LoopStartedAt.IsZero() &&
		time.Since(state.LoopStartedAt) >= cfg.MaxDuration {
		return true, fmt.Sprintf(
			"agent loop exceeded max duration (%s)",
			cfg.MaxDuration,
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
		if err := o.concurrency.ReleaseRun(ctx, run.WorkflowID); err != nil {
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
	if err := o.publishWorkflowFailed(ctx, run.RunID); err != nil {
		return err
	}
	return o.notifyParentIfChild(ctx, run, fmt.Errorf("%s", reason))
}

// parseFailPayload parses a StepFailedPayload from event payload.
// Falls back to treating raw strings as retriable errors for
// backward compatibility with old workers that send plain strings.
func parseFailPayload(
	data json.RawMessage,
) protocol.StepFailedPayload {
	if len(data) == 0 {
		return protocol.StepFailedPayload{
			FailureType: protocol.FailureTypeRetriable,
		}
	}
	var payload protocol.StepFailedPayload
	if err := json.Unmarshal(data, &payload); err == nil &&
		payload.Error != "" {
		if payload.FailureType == "" {
			payload.FailureType = protocol.FailureTypeRetriable
		}
		return payload
	}
	// Backward compat: raw quoted string
	var rawErr string
	if err := json.Unmarshal(data, &rawErr); err == nil {
		return protocol.StepFailedPayload{
			Error:       rawErr,
			FailureType: protocol.FailureTypeRetriable,
		}
	}
	return protocol.StepFailedPayload{
		Error:       string(data),
		FailureType: protocol.FailureTypeRetriable,
	}
}

// handleStepFailed processes a step failure event. Parses the
// structured StepFailedPayload and branches on FailureType:
// non-retriable skips retries, retry-after schedules exact delay,
// retriable uses existing backoff behavior.
func (o *Orchestrator) handleStepFailed(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleStepFailed: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleStepFailed: StepID must not be empty")
	}
	wfDef, run, err := o.loadRunAndDef(ctx, evt.RunID)
	if err != nil {
		return err
	}

	// Check if this is a map instance failure.
	if isMapInstanceID(evt.StepID) {
		return o.handleMapInstanceFailed(
			ctx, wfDef, run, evt,
		)
	}

	state := run.Steps[evt.StepID]
	state.Attempts++

	failPayload := parseFailPayload(evt.Payload)
	state.Error = failPayload.Error

	stepDef, _ := findStepDef(wfDef, evt.StepID)
	policy := dag.ResolveRetryPolicy(wfDef, stepDef)

	// Non-retriable: skip all retries immediately.
	if failPayload.FailureType ==
		protocol.FailureTypeNonRetriable {
		o.failNonRetriable.Inc()
		o.tel.Logger.Info(
			"step failed permanently (non-retriable)",
			observe.String("run_id", evt.RunID),
			observe.String("step_id", evt.StepID),
		)
		state.Status = dag.StepStatusFailed
		run.Steps[evt.StepID] = state
		return o.handlePermanentFailure(
			ctx, wfDef, run, stepDef, state, evt.StepID,
		)
	}

	// Retry-after: schedule exact delay if retries remain.
	if failPayload.FailureType ==
		protocol.FailureTypeRetryAfter {
		o.failRetryAfter.Inc()
		return o.handleRetryAfter(
			ctx, wfDef, &run, stepDef, &state,
			evt.StepID, failPayload.RetryAfterMs, policy,
		)
	}

	// Retriable (default): existing backoff behavior.
	if policy != nil && state.Attempts <= policy.MaxAttempts {
		run.Steps[evt.StepID] = state
		return o.saveSnapshot(ctx, run)
	}

	state.Status = dag.StepStatusFailed
	run.Steps[evt.StepID] = state
	return o.handlePermanentFailure(
		ctx, wfDef, run, stepDef, state, evt.StepID,
	)
}

// handleRetryAfter handles a retry-after failure: schedules an
// exact delay if retries remain, otherwise permanent failure.
func (o *Orchestrator) handleRetryAfter(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	stepDef dag.StepDef,
	state *dag.StepState,
	stepID string,
	retryAfterMs int64,
	policy *dag.RetryPolicy,
) error {
	if stepID == "" {
		panic("handleRetryAfter: stepID must not be empty")
	}
	if run.RunID == "" {
		panic("handleRetryAfter: RunID must not be empty")
	}
	if policy != nil && state.Attempts <= policy.MaxAttempts {
		run.Steps[stepID] = *state
		if err := o.saveSnapshot(ctx, *run); err != nil {
			return err
		}
		return o.scheduleRetryAfter(
			ctx, run.RunID, stepID, stepDef,
			retryAfterMs, *run,
		)
	}
	state.Status = dag.StepStatusFailed
	run.Steps[stepID] = *state
	return o.handlePermanentFailure(
		ctx, wfDef, *run, stepDef, *state, stepID,
	)
}

// handlePermanentFailure handles a step whose retries are exhausted
// or that was marked non-retriable. Checks aux steps, on-failure
// handlers, compensation chains, then fails the workflow.
func (o *Orchestrator) handlePermanentFailure(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	stepDef dag.StepDef,
	state dag.StepState,
	stepID string,
) error {
	if stepID == "" {
		panic(
			"handlePermanentFailure: stepID must not be empty",
		)
	}
	if run.RunID == "" {
		panic(
			"handlePermanentFailure: RunID must not be empty",
		)
	}

	// Release task concurrency slot if configured.
	o.releaseTaskSlot(ctx, wfDef, stepID)

	// If this is an auxiliary step (compensate target) failing,
	// the compensation itself failed — critical state.
	if wfDef.AuxSteps[stepID] {
		return o.failAuxStep(ctx, run, stepDef, state)
	}

	// Check for on-failure handler before failing the workflow.
	if stepDef.OnFailure != "" {
		handled, err := o.tryOnFailureHandler(
			ctx, wfDef, run, stepDef, state, stepID,
		)
		if err != nil {
			return err
		}
		if handled {
			return nil
		}
	}

	// No on-failure handler — check for compensation.
	completed := completedSet(run)
	chain := dag.ResolveCompensateChain(
		wfDef, completed, stepID,
	)
	if len(chain) > 0 {
		return o.startCompensation(
			ctx, wfDef, &run, stepID, state.Error,
		)
	}

	// No compensation either — fail the workflow.
	return o.failWorkflow(ctx, run, stepDef, state)
}

// failAuxStep handles failure of an auxiliary (compensate) step.
// Marks the run as CompensateFailed and publishes a dead letter.
func (o *Orchestrator) failAuxStep(
	ctx context.Context,
	run dag.WorkflowRun,
	stepDef dag.StepDef,
	state dag.StepState,
) error {
	if run.RunID == "" {
		panic("failAuxStep: RunID must not be empty")
	}
	if stepDef.ID == "" {
		panic("failAuxStep: stepDef.ID must not be empty")
	}
	run.Status = dag.RunStatusCompensateFailed
	if err := o.saveSnapshot(ctx, run); err != nil {
		return err
	}
	o.runsActive.Dec()
	o.runsFailed.Inc()
	o.publishDeadLetter(ctx, run.RunID, stepDef, state)
	return o.notifyParentIfChild(
		ctx, run,
		fmt.Errorf("compensation failed: %s", state.Error),
	)
}

// tryOnFailureHandler attempts to run the on-failure handler for a
// failed step. Returns (true, nil) if the handler was enqueued.
func (o *Orchestrator) tryOnFailureHandler(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	stepDef dag.StepDef,
	state dag.StepState,
	stepID string,
) (bool, error) {
	if stepDef.OnFailure == "" {
		panic("tryOnFailureHandler: OnFailure must not be empty")
	}
	if stepID == "" {
		panic("tryOnFailureHandler: stepID must not be empty")
	}
	onFailStep, found := findStepDef(
		wfDef, stepDef.OnFailure,
	)
	if !found {
		return false, nil
	}
	ofState := run.Steps[onFailStep.ID]
	ofState.Status = dag.StepStatusQueued
	run.Steps[onFailStep.ID] = ofState
	if err := o.saveSnapshot(ctx, run); err != nil {
		return false, err
	}
	errorInput := []byte(fmt.Sprintf(
		`{"failed_step":"%s","error":%q}`,
		stepID, state.Error,
	))
	err := o.publishTask(
		ctx, run.RunID, onFailStep, errorInput, 0,
	)
	return err == nil, err
}

// scheduleRetryAfter schedules a timer to re-publish the task
// after the worker-requested delay via SLEEP_TIMERS.
func (o *Orchestrator) scheduleRetryAfter(
	ctx context.Context,
	runID string, stepID string,
	stepDef dag.StepDef,
	retryAfterMs int64,
	run dag.WorkflowRun,
) error {
	if runID == "" {
		panic("scheduleRetryAfter: runID must not be empty")
	}
	if stepID == "" {
		panic("scheduleRetryAfter: stepID must not be empty")
	}
	if retryAfterMs <= 0 {
		retryAfterMs = 100
	}
	if retryAfterMs > 3_600_000 {
		retryAfterMs = 3_600_000
	}
	input, err := dag.ResolveInput(stepDef, run.Steps)
	if err != nil {
		return fmt.Errorf(
			"resolve input for retry-after step %q: %w",
			stepID, err,
		)
	}
	return o.sleepTimer.Schedule(ctx, TimerMessage{
		Action:     TimerActionRetryAfter,
		RunID:      runID,
		StepID:     stepID,
		DurationMs: retryAfterMs,
		TaskType:   stepDef.Task,
		Input:      input,
		Attempt:    run.Steps[stepID].Attempts,
	})
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
	o.deleteStickyBinding(ctx, run.RunID)
	o.runsActive.Dec()
	o.runsFailed.Inc()
	if o.concurrency != nil {
		if err := o.concurrency.ReleaseRun(ctx, run.WorkflowID); err != nil {
			return fmt.Errorf("release run: %w", err)
		}
		if err := o.startNextPendingRun(ctx, run.WorkflowID); err != nil {
			o.tel.Logger.Error(
				"failed to start next pending run", err,
				observe.String("workflow_id", run.WorkflowID),
			)
		}
	}
	if err := o.publishWorkflowFailed(ctx, run.RunID); err != nil {
		return err
	}
	o.publishDeadLetter(ctx, run.RunID, stepDef, state)
	return o.notifyParentIfChild(
		ctx, run, fmt.Errorf("%s", state.Error),
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
			`"trigger_step":%q,"trigger_error":%q}`,
		originalID,
		jsonOrNull(originalOutput),
		failedStepID,
		failedError,
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
	ctx context.Context,
	runID string, stepDef dag.StepDef, state dag.StepState,
) {
	if runID == "" {
		panic("publishDeadLetter: runID must not be empty")
	}
	if stepDef.ID == "" {
		panic("publishDeadLetter: stepDef.ID must not be empty")
	}
	payload, err := json.Marshal(map[string]any{
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
	o.js.Publish(ctx, subject, payload)
}

// handleWorkflowCancelled marks the run and all in-flight steps as
// cancelled, saves state, and adjusts metrics.
func (o *Orchestrator) handleWorkflowCancelled(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleWorkflowCancelled: RunID must not be empty")
	}
	wfDef, run, err := o.loadRunAndDef(ctx, evt.RunID)
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

	// Release task concurrency slots for cancelled steps that
	// were queued or running (they held a slot).
	o.releaseCancelledTaskSlots(ctx, wfDef, run)

	// Clean up approval tokens for cancelled approval steps.
	o.cleanupApprovalTokens(ctx, wfDef, run)

	if o.correlator != nil {
		o.correlator.RemoveWaitersForRun(ctx, run.RunID)
	}

	o.cascadeCancelChildren(ctx, wfDef, run)
	o.deleteStickyBinding(ctx, run.RunID)

	if err := o.saveSnapshot(ctx, run); err != nil {
		return err
	}
	o.runsActive.Dec()
	if o.concurrency != nil {
		if err := o.concurrency.ReleaseRun(ctx, run.WorkflowID); err != nil {
			return fmt.Errorf("release run: %w", err)
		}
		if err := o.startNextPendingRun(
			ctx, run.WorkflowID,
		); err != nil {
			o.tel.Logger.Error(
				"failed to start next pending run", err,
				observe.String("workflow_id", run.WorkflowID),
			)
		}
	}
	return o.notifyParentIfChild(ctx, run, fmt.Errorf("cancelled"))
}

// cascadeCancelChildren publishes cancellation events for all
// non-detached child workflows that are still running. Detached
// children have no ParentRunID so they are not cancelled.
func (o *Orchestrator) cascadeCancelChildren(
	ctx context.Context,
	wfDef dag.WorkflowDef, run dag.WorkflowRun,
) {
	if run.RunID == "" {
		panic("cascadeCancelChildren: RunID must not be empty")
	}
	if run.Steps == nil {
		panic("cascadeCancelChildren: Steps must not be nil")
	}

	for _, stepDef := range wfDef.Steps {
		if stepDef.Type != dag.StepTypeSubWorkflow {
			continue
		}
		state := run.Steps[stepDef.ID]
		if state.ChildRunID == "" {
			continue
		}
		childRun, err := o.store.Load(ctx, state.ChildRunID)
		if err != nil {
			continue
		}
		// Detached children have no ParentRunID — skip them.
		if childRun.ParentRunID == "" {
			continue
		}
		if childRun.Status != dag.RunStatusRunning {
			continue
		}
		o.publishCancelEvent(ctx, state.ChildRunID)
	}
}

// publishCancelEvent publishes EventWorkflowCancelled for a run.
func (o *Orchestrator) publishCancelEvent(
	ctx context.Context, runID string,
) {
	if runID == "" {
		panic("publishCancelEvent: runID must not be empty")
	}
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowCancelled, runID, nil,
	)
	data, err := evt.Marshal()
	if err != nil {
		return
	}
	o.js.Publish(
		ctx, evt.NATSSubject(), data,
		jetstream.WithMsgID(evt.NATSMsgID()),
	)
}

const maxNestingDepth = 3

// nestingDepth walks the parent chain to compute current depth.
// Returns 0 for top-level runs, 1 for first child, etc.
func (o *Orchestrator) nestingDepth(
	ctx context.Context, runID string,
) int {
	depth := 0
	currentID := runID
	for i := 0; i < maxNestingDepth+1; i++ {
		run, err := o.store.Load(ctx, currentID)
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
		ChildRunID    string          `json:"child_run_id"`
		ChildWorkflow string          `json:"child_workflow"`
		ParentStepID  string          `json:"parent_step_id"`
		Input         json.RawMessage `json:"input"`
		Detach        bool            `json:"detach"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal spawn payload: %w", err)
	}
	if payload.ChildRunID == "" {
		panic("handleWorkflowSpawn: child_run_id must not be empty")
	}

	// Enforce max nesting depth by walking the parent chain.
	// The child would be at depth+1, so reject when depth+1 > max.
	depth := o.nestingDepth(ctx, evt.RunID)
	if depth+1 >= maxNestingDepth {
		o.tel.Logger.Error(
			"spawn rejected: max nesting depth exceeded",
			fmt.Errorf("depth %d >= max %d", depth, maxNestingDepth),
		)
		return fmt.Errorf(
			"max nesting depth %d exceeded", maxNestingDepth,
		)
	}

	return o.createChildRun(ctx, evt.RunID, payload.ChildRunID,
		payload.ChildWorkflow, payload.ParentStepID,
		payload.Input, payload.Detach)
}

// createChildRun loads the child workflow def, creates the child run,
// and enqueues its entry-point steps. For detached children the parent
// link is omitted so they run independently.
func (o *Orchestrator) createChildRun(
	ctx context.Context,
	parentRunID string,
	childRunID string,
	childWorkflow string,
	parentStepID string,
	input json.RawMessage,
	detach bool,
) error {
	if childRunID == "" {
		panic("createChildRun: childRunID must not be empty")
	}
	if childWorkflow == "" {
		panic("createChildRun: childWorkflow must not be empty")
	}

	entry, err := o.defKV.Get(ctx, childWorkflow)
	if err != nil {
		return fmt.Errorf(
			"load child workflow def %q: %w",
			childWorkflow, err,
		)
	}
	var childDef dag.WorkflowDef
	if err := json.Unmarshal(entry.Value(), &childDef); err != nil {
		return fmt.Errorf("unmarshal child def: %w", err)
	}

	childRun := dag.NewWorkflowRun(childDef, childRunID)
	childRun.Input = input
	childRun.Status = dag.RunStatusRunning
	if !detach {
		childRun.ParentRunID = parentRunID
		childRun.ParentStepID = parentStepID
	}

	if err := o.saveSnapshot(ctx, childRun); err != nil {
		return err
	}

	o.runsActive.Inc()
	return o.enqueueReady(ctx, childDef, childRun)
}

// notifyParentIfChild publishes a child completion or failure event on the
// parent's history subject when this run has a parent. No-op for top-level.
func (o *Orchestrator) notifyParentIfChild(
	ctx context.Context, run dag.WorkflowRun, childErr error,
) error {
	if run.ParentRunID == "" {
		return nil
	}

	eventType := protocol.EventWorkflowChildCompleted
	if childErr != nil {
		eventType = protocol.EventWorkflowChildFailed
	}

	payload, err := json.Marshal(map[string]any{
		"child_run_id":   run.RunID,
		"parent_step_id": run.ParentStepID,
		"error":          errString(childErr),
	})
	if err != nil {
		return fmt.Errorf("marshal child event payload: %w", err)
	}

	// Use NewStepEvent keyed by ParentStepID so that multiple child
	// completions from different sub-workflow steps produce distinct
	// dedup IDs instead of colliding on a single workflow-level MsgID.
	evt := protocol.NewStepEvent(
		eventType, run.ParentRunID, run.ParentStepID, payload,
	)
	data, err := evt.Marshal()
	if err != nil {
		return fmt.Errorf("marshal child event: %w", err)
	}
	msg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {evt.NATSMsgID()}},
	}
	_, err = o.js.PublishMsg(ctx, msg)
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

	// Per-run step concurrency: cap how many steps dispatch.
	if wfDef.Concurrency != nil &&
		wfDef.Concurrency.MaxSteps > 0 {
		activeCount := countActiveSteps(run)
		available := wfDef.Concurrency.MaxSteps - activeCount
		if available <= 0 {
			return nil
		}
		if len(ready) > available {
			ready = ready[:available]
		}
	}

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
	return o.dispatchReadySteps(ctx, wfDef, run, ready)
}

// dispatchReadySteps separates map steps from normal steps and
// dispatches each appropriately.
func (o *Orchestrator) dispatchReadySteps(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	ready []dag.StepDef,
) error {
	var normalSteps []dag.StepDef
	for _, step := range ready {
		switch step.Type {
		case dag.StepTypeSubWorkflow:
			if err := o.enqueueSubWorkflow(
				ctx, wfDef, &run, step,
			); err != nil {
				return err
			}
		case dag.StepTypeMap:
			if err := o.enqueueMapStep(
				ctx, wfDef, &run, step,
			); err != nil {
				return err
			}
		case dag.StepTypeSleep:
			if err := o.enqueueSleepStep(
				ctx, &run, step,
			); err != nil {
				return err
			}
		case dag.StepTypeWaitForEvent:
			if err := o.enqueueWaitForEventStep(
				ctx, wfDef, &run, step,
			); err != nil {
				return err
			}
		case dag.StepTypeApproval:
			if err := o.enqueueApprovalStep(
				ctx, wfDef, &run, step,
			); err != nil {
				return err
			}
		default:
			normalSteps = append(normalSteps, step)
		}
	}
	if len(normalSteps) > 0 {
		return o.publishReadyTasks(
			ctx, run.RunID, wfDef, run, normalSteps,
		)
	}
	return nil
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
// If the step has a rate limit configured and tokens are exhausted,
// schedules a timer for delayed re-attempt instead of publishing.
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

	// Check rate limit before concurrency acquisition so we
	// don't hold a concurrency slot while waiting for tokens.
	if delayed, err := o.checkRateLimit(
		ctx, step, runID, input,
	); err != nil {
		return err
	} else if delayed {
		return nil
	}

	// Check per-task-type concurrency before publishing.
	if step.MaxTaskConcurrency > 0 && o.concurrency != nil {
		acquired, err := o.concurrency.AcquireTask(
			ctx, step.Task, step.MaxTaskConcurrency,
		)
		if err != nil {
			return err
		}
		if !acquired {
			o.taskConcurrencyRejected.Inc()
			return o.scheduleTaskConcurrencyRetry(
				ctx, step, runID, input,
			)
		}
		o.taskConcurrencyAcquired.Inc()
	}

	// Check sticky binding — if a binding exists, route to the
	// bound worker instead of the normal subject.
	workerID := o.getStickyWorker(ctx, runID)
	if workerID != "" {
		wfDef, _, loadErr := o.loadRunAndDef(ctx, runID)
		if loadErr == nil && wfDef.Sticky != dag.StickyNone {
			return o.publishStickyTask(
				ctx, runID, step, input, attempt,
				workerID, wfDef.Sticky,
			)
		}
	}

	return o.doPublishTask(ctx, runID, step, input, attempt)
}

// checkRateLimit evaluates rate limits for the step. Returns
// delayed=true if the task was deferred via SleepTimer.
func (o *Orchestrator) checkRateLimit(
	ctx context.Context,
	step dag.StepDef, runID string, input []byte,
) (bool, error) {
	if o.rateLimiter == nil {
		return false, nil
	}
	if step.Task == "" {
		panic("checkRateLimit: step.Task must not be empty")
	}

	if step.RateLimit != nil {
		return o.applyGlobalRateLimit(ctx, step, runID, input)
	}
	if step.KeyedRateLimit != nil {
		return o.applyKeyedRateLimit(ctx, step, runID, input)
	}
	return false, nil
}

// applyGlobalRateLimit checks the global rate limit for this task type.
func (o *Orchestrator) applyGlobalRateLimit(
	ctx context.Context,
	step dag.StepDef, runID string, input []byte,
) (bool, error) {
	if step.RateLimit == nil {
		panic("applyGlobalRateLimit: RateLimit must not be nil")
	}
	if runID == "" {
		panic("applyGlobalRateLimit: runID must not be empty")
	}
	rl := step.RateLimit
	allowed, retryAfter, err := o.rateLimiter.Allow(
		ctx, step.Task, "_global", rl.Limit, rl.Period, 1,
	)
	if err != nil {
		return false, fmt.Errorf("rate limit check: %w", err)
	}
	if allowed {
		return false, nil
	}
	return true, o.scheduleRateRetry(
		ctx, step, runID, input, retryAfter,
	)
}

// applyKeyedRateLimit checks the per-key rate limit for this task.
func (o *Orchestrator) applyKeyedRateLimit(
	ctx context.Context,
	step dag.StepDef, runID string, input []byte,
) (bool, error) {
	if step.KeyedRateLimit == nil {
		panic("applyKeyedRateLimit: KeyedRateLimit must not be nil")
	}
	if runID == "" {
		panic("applyKeyedRateLimit: runID must not be empty")
	}
	krl := step.KeyedRateLimit
	keyVal, err := dag.ExtractDotPath(krl.Key, input)
	if err != nil {
		return false, fmt.Errorf(
			"extract rate limit key %q: %w", krl.Key, err,
		)
	}
	key := fmt.Sprintf("%v", keyVal)
	allowed, retryAfter, err := o.rateLimiter.Allow(
		ctx, step.Task, key, krl.Limit, krl.Period, krl.Units,
	)
	if err != nil {
		return false, fmt.Errorf("keyed rate limit: %w", err)
	}
	if allowed {
		return false, nil
	}
	return true, o.scheduleRateRetry(
		ctx, step, runID, input, retryAfter,
	)
}

// scheduleRateRetry schedules a timer to re-attempt task dispatch
// after the rate limit window allows more tokens.
func (o *Orchestrator) scheduleRateRetry(
	ctx context.Context, step dag.StepDef, runID string,
	input []byte, retryAfter time.Duration,
) error {
	if runID == "" {
		panic("scheduleRateRetry: runID must not be empty")
	}
	if step.ID == "" {
		panic("scheduleRateRetry: step.ID must not be empty")
	}
	durationMs := retryAfter.Milliseconds()
	if durationMs <= 0 {
		durationMs = 100
	}
	return o.sleepTimer.Schedule(ctx, TimerMessage{
		Action:     TimerActionRateRetry,
		RunID:      runID,
		StepID:     step.ID,
		DurationMs: durationMs,
		TaskType:   step.Task,
		Input:      input,
	})
}

// scheduleTaskConcurrencyRetry schedules a timer to re-attempt
// task dispatch after the task concurrency slot frees up.
func (o *Orchestrator) scheduleTaskConcurrencyRetry(
	ctx context.Context,
	step dag.StepDef, runID string, input []byte,
) error {
	if runID == "" {
		panic("scheduleTaskConcurrencyRetry: " +
			"runID must not be empty")
	}
	if step.ID == "" {
		panic("scheduleTaskConcurrencyRetry: " +
			"step.ID must not be empty")
	}
	return o.sleepTimer.Schedule(ctx, TimerMessage{
		Action:     TimerActionTaskConcurRetry,
		RunID:      runID,
		StepID:     step.ID,
		DurationMs: 1000,
		TaskType:   step.Task,
		Input:      input,
	})
}

// doPublishTask performs the actual NATS publish for a task message.
func (o *Orchestrator) doPublishTask(
	ctx context.Context,
	runID string,
	step dag.StepDef,
	input []byte,
	attempt int,
) error {
	if runID == "" {
		panic("doPublishTask: runID must not be empty")
	}
	if step.ID == "" {
		panic("doPublishTask: step.ID must not be empty")
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
		TaskID:  runID + "." + step.ID,
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
	_, err = o.js.PublishMsg(ctx, msg)
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
		TaskID:    runID + "." + step.ID,
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
	_, err = o.js.PublishMsg(ctx, msg)
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
	err := o.store.Save(ctx, run)
	elapsed := float64(time.Since(start).Milliseconds())
	o.snapshotDuration.Observe(elapsed)
	return err
}

// loadRunAndDef loads the workflow definition and current run snapshot.
func (o *Orchestrator) loadRunAndDef(
	ctx context.Context, runID string,
) (dag.WorkflowDef, dag.WorkflowRun, error) {
	if runID == "" {
		panic("loadRunAndDef: runID must not be empty")
	}
	run, err := o.store.Load(ctx, runID)
	if err != nil {
		return dag.WorkflowDef{}, dag.WorkflowRun{},
			fmt.Errorf("load run %q: %w", runID, err)
	}
	entry, err := o.defKV.Get(ctx, run.WorkflowID)
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
	wfDef = dag.EffectiveDef(wfDef, run)
	return wfDef, run, nil
}

// publishWorkflowCompleted publishes a workflow.completed event.
func (o *Orchestrator) publishWorkflowCompleted(
	ctx context.Context, runID string,
) error {
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
		ctx, evt.NATSSubject(), data,
		jetstream.WithMsgID(evt.NATSMsgID()),
	)
	return err
}

// publishWorkflowFailed publishes a workflow.failed event.
func (o *Orchestrator) publishWorkflowFailed(
	ctx context.Context, runID string,
) error {
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
		ctx, evt.NATSSubject(), data,
		jetstream.WithMsgID(evt.NATSMsgID()),
	)
	return err
}

// extractTraceCtxJS reads W3C traceparent from a jetstream.Msg header
// or event payload and returns a context with parent span info.
func extractTraceCtxJS(
	msg jetstream.Msg, evt *protocol.Event,
) context.Context {
	if msg == nil {
		panic("extractTraceCtxJS: msg must not be nil")
	}
	if evt == nil {
		panic("extractTraceCtxJS: evt must not be nil")
	}
	traceID, spanID, ok := parseTraceparentJS(msg, evt)
	if !ok {
		return context.Background()
	}
	return observe.ContextWithParentInfo(
		context.Background(), traceID, spanID,
	)
}

// parseTraceparentJS reads traceparent from jetstream.Msg header
// first, falling back to the event field.
func parseTraceparentJS(
	msg jetstream.Msg, evt *protocol.Event,
) (traceID, spanID string, ok bool) {
	if hdrs := msg.Headers(); hdrs != nil {
		if h := hdrs.Get("traceparent"); h != "" {
			return splitTraceparent(h)
		}
	}
	if evt.TraceParent != "" {
		return splitTraceparent(evt.TraceParent)
	}
	return "", "", false
}

// parseTraceparent reads traceparent from *nats.Msg header first,
// falling back to the event field. Used by tests.
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

// enqueueSleepStep marks the step as Running, publishes a
// SleepStarted event, and schedules a durable timer. No worker
// is involved — the timer fires the completion event directly.
func (o *Orchestrator) enqueueSleepStep(
	ctx context.Context,
	run *dag.WorkflowRun,
	step dag.StepDef,
) error {
	if step.Type != dag.StepTypeSleep {
		panic("enqueueSleepStep: step is not a Sleep step")
	}
	if run.RunID == "" {
		panic("enqueueSleepStep: RunID must not be empty")
	}

	sleepCfg, err := dag.ParseSleepConfig(step)
	if err != nil {
		return fmt.Errorf("enqueueSleepStep: %w", err)
	}

	// Mark step as Running and record wake time.
	state := run.Steps[step.ID]
	state.Status = dag.StepStatusRunning
	wakeAt := time.Now().Add(sleepCfg.Duration)
	state.WakeAt = &wakeAt
	run.Steps[step.ID] = state
	if err := o.saveSnapshot(ctx, *run); err != nil {
		return err
	}

	// Publish sleep started event for observability.
	o.publishSleepStarted(ctx, run.RunID, step.ID)

	// Schedule durable timer via NakWithDelay.
	durationMs := sleepCfg.Duration.Milliseconds()
	if durationMs <= 0 {
		durationMs = 1
	}
	return o.sleepTimer.Schedule(ctx, TimerMessage{
		Action:     TimerActionSleepComplete,
		RunID:      run.RunID,
		StepID:     step.ID,
		DurationMs: durationMs,
	})
}

// publishSleepStarted publishes an EventStepSleepStarted event.
func (o *Orchestrator) publishSleepStarted(
	ctx context.Context, runID string, stepID string,
) {
	if runID == "" {
		panic("publishSleepStarted: runID must not be empty")
	}
	if stepID == "" {
		panic("publishSleepStarted: stepID must not be empty")
	}
	evt := protocol.NewStepEvent(
		protocol.EventStepSleepStarted,
		runID, stepID, nil,
	)
	data, err := evt.Marshal()
	if err != nil {
		return
	}
	o.js.Publish(
		ctx, evt.NATSSubject(), data,
		jetstream.WithMsgID(evt.NATSMsgID()),
	)
}

// isMapInstanceID returns true if the step ID is a map instance
// (format: "{stepID}.map.{index}").
func isMapInstanceID(stepID string) bool {
	return strings.Contains(stepID, ".map.")
}

// parseMapInstanceID splits a compound map instance ID into the
// base step ID and instance index. Panics if the format is invalid.
func parseMapInstanceID(stepID string) (string, int) {
	parts := strings.Split(stepID, ".map.")
	if len(parts) != 2 {
		panic("parseMapInstanceID: invalid format: " + stepID)
	}
	idx, err := strconv.Atoi(parts[1])
	if err != nil {
		panic("parseMapInstanceID: invalid index: " + parts[1])
	}
	return parts[0], idx
}

// mapInstanceID constructs a compound step ID for a map instance.
func mapInstanceID(stepID string, index int) string {
	return stepID + ".map." + strconv.Itoa(index)
}

// enqueueMapStep reads the upstream output as a JSON array and
// publishes one task per element. MapInstances track each item's
// state on the Map step's StepState.
func (o *Orchestrator) enqueueMapStep(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	step dag.StepDef,
) error {
	if step.Type != dag.StepTypeMap {
		panic("enqueueMapStep: step is not a Map step")
	}
	if len(step.DependsOn) != 1 {
		panic("enqueueMapStep: Map step must have exactly one dep")
	}

	// Read upstream output as JSON array.
	upstream := run.Steps[step.DependsOn[0]]
	var items []json.RawMessage
	if err := json.Unmarshal(upstream.Output, &items); err != nil {
		return fmt.Errorf(
			"map step %q: upstream output is not a JSON array: %w",
			step.ID, err,
		)
	}

	if err := o.validateAndInitMapInstances(
		ctx, run, step, items,
	); err != nil {
		return err
	}

	return o.publishMapTasks(ctx, run.RunID, step, items)
}

// validateAndInitMapInstances checks MaxItems and initializes
// the MapInstances slice on the step state.
func (o *Orchestrator) validateAndInitMapInstances(
	ctx context.Context,
	run *dag.WorkflowRun,
	step dag.StepDef,
	items []json.RawMessage,
) error {
	mapCfg, err := dag.ParseMapConfig(step)
	if err != nil {
		panic("validateAndInitMapInstances: " + err.Error())
	}
	maxItems := mapCfg.MaxItems
	if len(items) > maxItems {
		return fmt.Errorf(
			"map step %q: %d items exceeds MaxItems %d",
			step.ID, len(items), maxItems,
		)
	}

	state := run.Steps[step.ID]
	state.Status = dag.StepStatusRunning
	state.MapInstances = make(
		[]dag.MapInstanceState, len(items),
	)
	for i := range items {
		state.MapInstances[i] = dag.MapInstanceState{
			Status: dag.StepStatusQueued,
		}
	}
	run.Steps[step.ID] = state
	return o.saveSnapshot(ctx, *run)
}

// publishMapTasks publishes one task per map item concurrently.
func (o *Orchestrator) publishMapTasks(
	ctx context.Context,
	runID string,
	step dag.StepDef,
	items []json.RawMessage,
) error {
	var g errgroup.Group
	for i, item := range items {
		i, item := i, item
		instanceStep := step
		instanceStep.ID = mapInstanceID(step.ID, i)
		g.Go(func() error {
			return o.publishTask(
				ctx, runID, instanceStep, item, 0,
			)
		})
	}
	return g.Wait()
}

// handleMapInstanceCompleted updates a single map instance's state.
// When all instances are done, collects outputs and completes the
// Map step.
func (o *Orchestrator) handleMapInstanceCompleted(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	evt protocol.Event,
) error {
	baseID, idx := parseMapInstanceID(evt.StepID)
	state := run.Steps[baseID]

	if idx < 0 || idx >= len(state.MapInstances) {
		return fmt.Errorf(
			"map instance index %d out of range for %q",
			idx, baseID,
		)
	}

	state.MapInstances[idx].Status = dag.StepStatusCompleted
	state.MapInstances[idx].Output = evt.Payload
	run.Steps[baseID] = state

	if !allMapInstancesDone(state.MapInstances) {
		return o.saveSnapshot(ctx, run)
	}

	return o.collectMapOutputs(ctx, wfDef, run, baseID, state)
}

// allMapInstancesDone returns true when every instance is completed.
func allMapInstancesDone(instances []dag.MapInstanceState) bool {
	for _, inst := range instances {
		if inst.Status != dag.StepStatusCompleted {
			return false
		}
	}
	return true
}

// collectMapOutputs gathers outputs from all instances into an
// ordered JSON array and completes the Map step.
func (o *Orchestrator) collectMapOutputs(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	baseID string,
	state dag.StepState,
) error {
	outputs := make(
		[]json.RawMessage, len(state.MapInstances),
	)
	for i, inst := range state.MapInstances {
		outputs[i] = inst.Output
	}
	collected, err := json.Marshal(outputs)
	if err != nil {
		return fmt.Errorf("marshal map outputs: %w", err)
	}

	state.Status = dag.StepStatusCompleted
	state.Output = collected
	run.Steps[baseID] = state

	completed := completedSet(run)
	if dag.IsComplete(wfDef, completed) {
		return o.completeWorkflow(ctx, run)
	}
	if err := o.saveSnapshot(ctx, run); err != nil {
		return err
	}
	return o.enqueueReady(ctx, wfDef, run)
}

// handleMapInstanceFailed marks the Map step as failed immediately
// (fail-fast). Remaining instances will expire via AckWait.
func (o *Orchestrator) handleMapInstanceFailed(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	evt protocol.Event,
) error {
	baseID, idx := parseMapInstanceID(evt.StepID)
	state := run.Steps[baseID]

	if idx < 0 || idx >= len(state.MapInstances) {
		return fmt.Errorf(
			"map instance index %d out of range for %q",
			idx, baseID,
		)
	}

	state.MapInstances[idx].Status = dag.StepStatusFailed
	if evt.Payload != nil {
		state.MapInstances[idx].Error = string(evt.Payload)
	}

	// Fail-fast: mark the Map step as failed.
	state.Status = dag.StepStatusFailed
	state.Error = fmt.Sprintf(
		"map instance %d failed: %s", idx,
		state.MapInstances[idx].Error,
	)
	run.Steps[baseID] = state

	return o.failMapStep(ctx, wfDef, run, baseID, state)
}

// failMapStep handles the on-failure handler or fails the workflow.
func (o *Orchestrator) failMapStep(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	baseID string,
	state dag.StepState,
) error {
	stepDef, _ := findStepDef(wfDef, baseID)

	// Check for on-failure handler.
	if stepDef.OnFailure != "" {
		return o.runMapOnFailure(
			ctx, wfDef, run, baseID, state, stepDef,
		)
	}

	// No on-failure — fail the workflow.
	run.Status = dag.RunStatusFailed
	if err := o.saveSnapshot(ctx, run); err != nil {
		return err
	}
	o.runsActive.Dec()
	o.runsFailed.Inc()
	if err := o.publishWorkflowFailed(ctx, run.RunID); err != nil {
		return err
	}
	o.publishDeadLetter(ctx, run.RunID, stepDef, state)
	return o.notifyParentIfChild(
		ctx, run, fmt.Errorf("%s", state.Error),
	)
}

// runMapOnFailure enqueues the on-failure step for a failed map.
func (o *Orchestrator) runMapOnFailure(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	baseID string,
	state dag.StepState,
	stepDef dag.StepDef,
) error {
	onFailStep, found := findStepDef(
		wfDef, stepDef.OnFailure,
	)
	if !found {
		return nil
	}
	ofState := run.Steps[onFailStep.ID]
	ofState.Status = dag.StepStatusQueued
	run.Steps[onFailStep.ID] = ofState
	if err := o.saveSnapshot(ctx, run); err != nil {
		return err
	}
	errorInput := []byte(fmt.Sprintf(
		`{"failed_step":"%s","error":%q}`,
		baseID, state.Error,
	))
	return o.publishTask(
		ctx, run.RunID, onFailStep, errorInput, 0,
	)
}

// enqueueWaitForEventStep marks the step as Running, resolves the
// match condition, publishes a WaitStarted event, registers the
// waiter with the correlator, and schedules a timeout timer.
func (o *Orchestrator) enqueueWaitForEventStep(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	step dag.StepDef,
) error {
	if step.Type != dag.StepTypeWaitForEvent {
		panic("enqueueWaitForEventStep: wrong step type")
	}
	if run.RunID == "" {
		panic("enqueueWaitForEventStep: RunID must not be empty")
	}

	opts, err := dag.ParseWaitForEventConfig(step)
	if err != nil {
		return fmt.Errorf(
			"step %q: WaitForEvent config is nil", step.ID,
		)
	}

	resolvedMatch, err := o.resolveWaitMatch(
		opts.Match, run,
	)
	if err != nil {
		return fmt.Errorf(
			"resolve match for step %q: %w", step.ID, err,
		)
	}

	return o.startWaitForEvent(
		ctx, run, step, &opts, resolvedMatch,
	)
}

// resolveWaitMatch resolves a builder-time Match to a runtime
// ResolvedMatch using step outputs and workflow input.
func (o *Orchestrator) resolveWaitMatch(
	match dag.Match,
	run *dag.WorkflowRun,
) (dag.ResolvedMatch, error) {
	if run == nil {
		panic("resolveWaitMatch: run must not be nil")
	}
	if run.Steps == nil {
		panic("resolveWaitMatch: run.Steps must not be nil")
	}
	stepOutputs := make(map[string][]byte, len(run.Steps))
	for id, state := range run.Steps {
		if state.Output != nil {
			stepOutputs[id] = state.Output
		}
	}
	return match.Resolve(stepOutputs, run.Input)
}

// startWaitForEvent marks the step Running, publishes
// WaitStarted, registers the correlator waiter, and schedules
// the timeout timer. Extracted to keep parent under 70 lines.
func (o *Orchestrator) startWaitForEvent(
	ctx context.Context,
	run *dag.WorkflowRun,
	step dag.StepDef,
	opts *dag.WaitForEventOpts,
	resolvedMatch dag.ResolvedMatch,
) error {
	if run.RunID == "" {
		panic("startWaitForEvent: RunID must not be empty")
	}
	if step.ID == "" {
		panic("startWaitForEvent: step.ID must not be empty")
	}

	state := run.Steps[step.ID]
	state.Status = dag.StepStatusRunning
	run.Steps[step.ID] = state
	if err := o.saveSnapshot(ctx, *run); err != nil {
		return err
	}

	o.publishWaitStarted(ctx, run.RunID, step.ID)

	waiter := EventWaiter{
		RunID:     run.RunID,
		StepID:    step.ID,
		EventType: opts.Event,
		Match:     resolvedMatch,
	}
	if err := o.correlator.AddWaiter(ctx, waiter); err != nil {
		return fmt.Errorf("add waiter: %w", err)
	}

	return o.scheduleWaitTimeout(ctx, run.RunID, step.ID, opts.Timeout)
}

// scheduleWaitTimeout schedules a timer for the wait-for-event
// timeout. Uses the same SleepTimer infrastructure as sleep steps.
func (o *Orchestrator) scheduleWaitTimeout(
	ctx context.Context,
	runID string, stepID string, timeout time.Duration,
) error {
	if runID == "" {
		panic("scheduleWaitTimeout: runID must not be empty")
	}
	if stepID == "" {
		panic("scheduleWaitTimeout: stepID must not be empty")
	}
	durationMs := timeout.Milliseconds()
	if durationMs <= 0 {
		durationMs = 1
	}
	return o.sleepTimer.Schedule(ctx, TimerMessage{
		Action:     TimerActionWaitTimeout,
		RunID:      runID,
		StepID:     stepID,
		DurationMs: durationMs,
	})
}

// publishWaitStarted publishes an EventStepWaitStarted event.
func (o *Orchestrator) publishWaitStarted(
	ctx context.Context, runID string, stepID string,
) {
	if runID == "" {
		panic("publishWaitStarted: runID must not be empty")
	}
	if stepID == "" {
		panic("publishWaitStarted: stepID must not be empty")
	}
	evt := protocol.NewStepEvent(
		protocol.EventStepWaitStarted,
		runID, stepID, nil,
	)
	data, err := evt.Marshal()
	if err != nil {
		return
	}
	o.js.Publish(
		ctx, evt.NATSSubject(), data,
		jetstream.WithMsgID(evt.NATSMsgID()),
	)
}

// handleWaitTimeout marks the wait step as completed with a timeout
// output so downstream steps can branch on it. Timeout is not a
// failure — it completes the step with {"timeout": true}.
func (o *Orchestrator) handleWaitTimeout(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleWaitTimeout: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleWaitTimeout: StepID must not be empty")
	}
	wfDef, run, err := o.loadRunAndDef(ctx, evt.RunID)
	if err != nil {
		return err
	}

	state := run.Steps[evt.StepID]
	// Only process if the step is still Running (not already matched).
	if state.Status != dag.StepStatusRunning {
		return nil
	}

	state.Status = dag.StepStatusCompleted
	state.Output = []byte(`{"timeout":true}`)
	run.Steps[evt.StepID] = state

	// Remove the waiter since the step timed out.
	if o.correlator != nil {
		o.correlator.RemoveWaitersForRun(ctx, evt.RunID)
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

// enqueueSubWorkflow resolves input, generates a child run ID, and
// publishes a spawn event. For detached sub-workflows the parent step
// completes immediately; otherwise it stays Running until the child
// finishes.
func (o *Orchestrator) enqueueSubWorkflow(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	step dag.StepDef,
) error {
	if step.Type != dag.StepTypeSubWorkflow {
		panic("enqueueSubWorkflow: wrong step type")
	}
	if run.RunID == "" {
		panic("enqueueSubWorkflow: RunID must not be empty")
	}

	cfg, err := dag.ParseSubWorkflowConfig(step)
	if err != nil {
		return fmt.Errorf("parse sub-workflow config: %w", err)
	}

	input, err := dag.ResolveInput(step, run.Steps)
	if err != nil {
		return fmt.Errorf(
			"resolve input for step %q: %w", step.ID, err,
		)
	}
	childRunID := nuid.Next()

	if err := o.spawnChild(
		ctx, wfDef, run, step, cfg, input, childRunID,
	); err != nil {
		return err
	}

	// Detached sub-workflows complete the parent step immediately,
	// which may unblock downstream steps or complete the workflow.
	if cfg.Detach {
		completed := completedSet(*run)
		if dag.IsComplete(wfDef, completed) {
			return o.completeWorkflow(ctx, *run)
		}
		return o.enqueueReady(ctx, wfDef, *run)
	}
	return nil
}

// spawnChild marks the parent step state, saves the snapshot, and
// publishes the spawn event. Extracted to keep enqueueSubWorkflow
// within the 70-line limit.
func (o *Orchestrator) spawnChild(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	step dag.StepDef,
	cfg dag.SubWorkflowConfig,
	input []byte,
	childRunID string,
) error {
	if childRunID == "" {
		panic("spawnChild: childRunID must not be empty")
	}
	if step.ID == "" {
		panic("spawnChild: step.ID must not be empty")
	}

	state := run.Steps[step.ID]
	if cfg.Detach {
		state.Status = dag.StepStatusCompleted
		state.ChildRunID = childRunID
		state.Output = []byte(fmt.Sprintf(
			`{"child_run_id":%q}`, childRunID,
		))
	} else {
		state.Status = dag.StepStatusRunning
		state.ChildRunID = childRunID
	}
	run.Steps[step.ID] = state
	if err := o.saveSnapshot(ctx, *run); err != nil {
		return err
	}

	return o.publishSpawnEvent(
		ctx, run.RunID, step.ID, cfg, input, childRunID,
	)
}

// publishSpawnEvent publishes EventWorkflowSpawn to the history
// stream with the child run metadata in the payload.
func (o *Orchestrator) publishSpawnEvent(
	ctx context.Context,
	parentRunID string,
	parentStepID string,
	cfg dag.SubWorkflowConfig,
	input []byte,
	childRunID string,
) error {
	if parentRunID == "" {
		panic("publishSpawnEvent: parentRunID must not be empty")
	}
	if parentStepID == "" {
		panic("publishSpawnEvent: parentStepID must not be empty")
	}

	payload, err := json.Marshal(map[string]interface{}{
		"child_run_id":   childRunID,
		"child_workflow": cfg.Workflow,
		"parent_step_id": parentStepID,
		"input":          json.RawMessage(input),
		"detach":         cfg.Detach,
	})
	if err != nil {
		return fmt.Errorf("marshal spawn payload: %w", err)
	}

	evt := protocol.NewStepEvent(
		protocol.EventWorkflowSpawn,
		parentRunID, parentStepID, payload,
	)
	data, err := evt.Marshal()
	if err != nil {
		return fmt.Errorf("marshal spawn event: %w", err)
	}

	msg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Data:    data,
		Header: nats.Header{
			"Nats-Msg-Id": {evt.NATSMsgID()},
		},
	}
	_, err = o.js.PublishMsg(ctx, msg)
	return err
}

// handleChildCompleted processes EventWorkflowChildCompleted: loads
// the child run's terminal output, marks the parent step Completed,
// and enqueues the next ready steps.
func (o *Orchestrator) handleChildCompleted(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleChildCompleted: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleChildCompleted: StepID must not be empty")
	}

	wfDef, run, err := o.loadRunAndDef(ctx, evt.RunID)
	if err != nil {
		return err
	}

	state := run.Steps[evt.StepID]
	if state.Status != dag.StepStatusRunning {
		return nil // Already handled or cancelled.
	}

	output, err := o.loadChildTerminalOutputs(ctx, state.ChildRunID)
	if err != nil {
		return fmt.Errorf("load child outputs: %w", err)
	}

	state.Status = dag.StepStatusCompleted
	state.Output = output
	run.Steps[evt.StepID] = state

	completed := completedSet(run)
	if dag.IsComplete(wfDef, completed) {
		return o.completeWorkflow(ctx, run)
	}
	if err := o.saveSnapshot(ctx, run); err != nil {
		return err
	}
	return o.enqueueReady(ctx, wfDef, run)
}

// loadChildTerminalOutputs loads the child run and its workflow def,
// finds terminal steps (steps no other step depends on), and returns
// their outputs. One terminal step returns raw output; multiple
// returns a JSON map keyed by step ID.
func (o *Orchestrator) loadChildTerminalOutputs(
	ctx context.Context, childRunID string,
) ([]byte, error) {
	if childRunID == "" {
		panic("loadChildTerminalOutputs: childRunID empty")
	}
	childDef, childRun, err := o.loadRunAndDef(ctx, childRunID)
	if err != nil {
		return nil, err
	}
	return collectTerminalOutputs(childDef, childRun)
}

// collectTerminalOutputs finds steps that no other step depends on
// and returns their outputs. Single terminal returns raw output;
// multiple terminals return a JSON map keyed by step ID.
func collectTerminalOutputs(
	def dag.WorkflowDef, run dag.WorkflowRun,
) ([]byte, error) {
	if len(def.Steps) == 0 {
		panic("collectTerminalOutputs: def has no steps")
	}
	if run.Steps == nil {
		panic("collectTerminalOutputs: run.Steps is nil")
	}
	depTargets := make(map[string]bool, len(def.Steps))
	for _, step := range def.Steps {
		for _, dep := range step.DependsOn {
			depTargets[dep] = true
		}
	}
	var terminals []dag.StepDef
	const maxTerminals = 1000
	for _, step := range def.Steps {
		if !depTargets[step.ID] {
			terminals = append(terminals, step)
		}
		if len(terminals) > maxTerminals {
			break
		}
	}
	if len(terminals) == 1 {
		return run.Steps[terminals[0].ID].Output, nil
	}
	collected := make(
		map[string]json.RawMessage, len(terminals),
	)
	for _, step := range terminals {
		collected[step.ID] = run.Steps[step.ID].Output
	}
	return json.Marshal(collected)
}

// handleChildFailed processes EventWorkflowChildFailed: marks the
// parent step Failed and delegates to failWorkflow.
func (o *Orchestrator) handleChildFailed(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleChildFailed: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleChildFailed: StepID must not be empty")
	}

	wfDef, run, err := o.loadRunAndDef(ctx, evt.RunID)
	if err != nil {
		return err
	}

	state := run.Steps[evt.StepID]
	if state.Status != dag.StepStatusRunning {
		return nil // Already handled or cancelled.
	}

	var payload struct {
		Error string `json:"error"`
	}
	if evt.Payload != nil {
		if err := json.Unmarshal(
			evt.Payload, &payload,
		); err != nil {
			return fmt.Errorf(
				"unmarshal child failed payload: %w", err,
			)
		}
	}

	state.Status = dag.StepStatusFailed
	state.Error = "child workflow failed: " + payload.Error
	run.Steps[evt.StepID] = state

	stepDef, _ := findStepDef(wfDef, evt.StepID)
	return o.failWorkflow(ctx, run, stepDef, state)
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

// releaseTaskSlot releases a task concurrency slot for the given
// step if MaxTaskConcurrency is configured.
func (o *Orchestrator) releaseTaskSlot(
	ctx context.Context, wfDef dag.WorkflowDef, stepID string,
) {
	if o.concurrency == nil {
		return
	}
	stepDef, found := findStepDef(wfDef, stepID)
	if !found || stepDef.MaxTaskConcurrency <= 0 {
		return
	}
	o.concurrency.ReleaseTask(ctx, stepDef.Task)
}

// releaseCancelledTaskSlots releases task concurrency slots for
// all steps that were cancelled while queued or running.
func (o *Orchestrator) releaseCancelledTaskSlots(
	ctx context.Context,
	wfDef dag.WorkflowDef, run dag.WorkflowRun,
) {
	if o.concurrency == nil {
		return
	}
	for id, state := range run.Steps {
		if state.Status != dag.StepStatusCancelled {
			continue
		}
		stepDef, found := findStepDef(wfDef, id)
		if !found || stepDef.MaxTaskConcurrency <= 0 {
			continue
		}
		o.concurrency.ReleaseTask(ctx, stepDef.Task)
	}
}

// countActiveSteps counts steps that are currently queued or running.
func countActiveSteps(run dag.WorkflowRun) int {
	if run.Steps == nil {
		panic("countActiveSteps: run.Steps must not be nil")
	}
	count := 0
	for _, state := range run.Steps {
		if state.Status == dag.StepStatusQueued ||
			state.Status == dag.StepStatusRunning {
			count++
		}
	}
	return count
}
