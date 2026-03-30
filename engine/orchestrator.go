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
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// Orchestrator subscribes to the history stream and drives workflow execution.
// It is intentionally stateless between events — all run state lives in the
// snapshot store (NATS KV), so the orchestrator can crash and resume safely.
type Orchestrator struct {
	nc       *nats.Conn
	js       nats.JetStreamContext
	defKV    nats.KeyValue
	store    *SnapshotStore
	tel      *observe.Telemetry
	sub      *nats.Subscription
	runLocks sync.Map // map[string]*sync.Mutex — per-run serialization

	// Pre-allocated metric instruments — created once in constructor.
	runsActive       observe.Gauge
	runsCompleted    observe.Counter
	runsFailed       observe.Counter
	stepEnqueueCount observe.Counter
	snapshotDuration observe.Histogram
}

// NewOrchestrator creates an Orchestrator bound to the given NATS connection.
// Panics if nc is nil or JetStream cannot be obtained — both are programmer
// errors. KV buckets must already exist (call natsutil.SetupAll first).
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
		panic(
			"NewOrchestrator: workflow_defs bucket not found: " +
				err.Error(),
		)
	}
	return &Orchestrator{
		nc:    nc,
		js:    js,
		defKV: defKV,
		store: NewSnapshotStore(js),
		tel:   tel,
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
		protocol.EventStepFailed:
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
	switch evt.Type {
	case protocol.EventWorkflowStarted:
		return o.handleWorkflowStarted(ctx, evt)
	case protocol.EventStepCompleted:
		return o.handleStepCompleted(ctx, evt)
	case protocol.EventStepContinue:
		return o.handleStepContinue(ctx, evt)
	case protocol.EventStepFailed:
		return o.handleStepFailed(ctx, evt)
	default:
		return nil
	}
}

// handleWorkflowStarted creates the initial WorkflowRun from the event
// payload, saves it, then enqueues all entry-point steps.
func (o *Orchestrator) handleWorkflowStarted(
	ctx context.Context, evt protocol.Event,
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
	completed := completedSet(run)
	if dag.IsComplete(wfDef, completed) {
		return o.completeWorkflow(ctx, run)
	}
	if err := o.saveSnapshot(ctx, run); err != nil {
		return err
	}
	return o.enqueueReady(ctx, wfDef, run)
}

// completeWorkflow marks the run complete, saves, publishes the event,
// and adjusts metrics.
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
	return o.publishWorkflowCompleted(run.RunID)
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
	return o.publishWorkflowFailed(run.RunID)
}

// handleStepFailed records a permanent failure reported by a worker.
// Transient failures are handled by JetStream NakWithDelay and never
// reach the orchestrator.
func (o *Orchestrator) handleStepFailed(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleStepFailed: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleStepFailed: StepID must not be empty")
	}
	_, run, err := o.loadRunAndDef(evt.RunID)
	if err != nil {
		return err
	}
	state := run.Steps[evt.StepID]
	state.Attempts++
	if evt.Payload != nil {
		state.Error = string(evt.Payload)
	}
	state.Status = dag.StepStatusFailed
	run.Steps[evt.StepID] = state
	run.Status = dag.RunStatusFailed
	if err := o.saveSnapshot(ctx, run); err != nil {
		return err
	}
	o.runsActive.Dec()
	o.runsFailed.Inc()
	return o.publishWorkflowFailed(run.RunID)
}

// enqueueReady resolves all currently-ready steps and publishes one task
// message per step. Steps already queued are skipped via dag.ResolveReady.
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
	ready := dag.ResolveReady(wfDef, completed, queued)
	span.SetAttributes(
		observe.Int64Attr("ready_steps_count", int64(len(ready))),
	)
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
	for _, step := range ready {
		input, err := dag.ResolveInput(step, run.Steps)
		if err != nil {
			return fmt.Errorf(
				"resolve input for step %q: %w", step.ID, err,
			)
		}
		if err := o.publishTask(ctx, runID, step, input); err != nil {
			return err
		}
	}
	return nil
}

// publishTask publishes a TaskPayload to task.{step.Task}.{runID} with
// dedup ID and trace context headers on the outgoing NATS message.
func (o *Orchestrator) publishTask(
	ctx context.Context,
	runID string,
	step dag.StepDef,
	input []byte,
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
		RunID:  runID,
		StepID: step.ID,
		Input:  input,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal TaskPayload: %w", err)
	}
	msgID := runID + "." + step.ID + ".queued"
	msg := buildTaskMsg(step.Task, runID, data, msgID)
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
	msg := buildTaskMsg(step.Task, runID, data, msgID)
	injectTraceCtx(ctx, span, msg)
	_, err = o.js.PublishMsg(msg)
	o.stepEnqueueCount.Inc()
	return err
}

// buildTaskMsg constructs a *nats.Msg with headers for task publishing.
func buildTaskMsg(
	task, runID string, data []byte, msgID string,
) *nats.Msg {
	if task == "" {
		panic("buildTaskMsg: task must not be empty")
	}
	if msgID == "" {
		panic("buildTaskMsg: msgID must not be empty")
	}
	return &nats.Msg{
		Subject: "task." + task + "." + runID,
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

// completedSet returns a set of step IDs with Completed status.
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
