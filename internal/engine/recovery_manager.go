// engine/recovery_manager.go
// RecoveryManager owns failure recovery: on-failure handlers, saga
// compensation chains, and dead-letter publishing. Extracted from
// Orchestrator to reduce its surface area. No behavioral change.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/runid"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// DLQ header keys carry structured metadata about the failed task so
// list/replay code can recover it without re-parsing the body. The
// canonical header set lands with the #200 body-preservation schema;
// listers detect "new shape" by looking for HeaderDLQRunID and fall
// back to the legacy JSON shape when it is absent.
const (
	HeaderDLQRunID         = "Dagnats-Dlq-Run-Id"
	HeaderDLQStepID        = "Dagnats-Dlq-Step-Id"
	HeaderDLQTask          = "Dagnats-Dlq-Task"
	HeaderDLQError         = "Dagnats-Dlq-Error"
	HeaderDLQAttempts      = "Dagnats-Dlq-Attempts"
	HeaderDLQDeliveryCount = "Dagnats-Dlq-Delivery-Count"
	HeaderDLQConsumer      = "Dagnats-Dlq-Consumer"
	// HeaderDLQTaskSubject preserves the original task subject so
	// replay can re-publish to the exact same path (incl. runID and
	// worker group routing) rather than reconstructing from
	// (task, runID) alone.
	HeaderDLQTaskSubject = "Dagnats-Dlq-Task-Subject"
	// HeaderDLQEventType carries the WORKFLOW_HISTORY event's Type
	// (e.g. "workflow.started") for history dead-letters (#508).
	// Absent on TASK_QUEUES-originated entries.
	HeaderDLQEventType = "Dagnats-Dlq-Event-Type"

	// DLQConsumerTaskQueues is the consumer-name marker for DLQ entries
	// originating from MaxDeliver exhaustion on the TASK_QUEUES stream.
	// Surfaces in the CLI so operators can distinguish DLQ paths once
	// other dispatch consumers gain DLQ semantics.
	DLQConsumerTaskQueues = "TASK_QUEUES"

	// DLQConsumerWorkflowHistory is the consumer-name marker for DLQ
	// entries originating from MaxDeliver exhaustion on the
	// WORKFLOW_HISTORY stream's "orchestrator" consumer (#508).
	DLQConsumerWorkflowHistory = "WORKFLOW_HISTORY"
)

// RecoveryManager handles step failure recovery: on-failure
// handlers, saga compensation chains, and dead-letter publishing.
// Delegates snapshot persistence and workflow-level transitions
// back to the Orchestrator via callbacks.
//
// Callback protocol: failFn triggers workflow-level failure,
// saveFn persists the run snapshot after state mutations,
// notifyFn propagates failure to a parent workflow, and
// releaseSlotFn frees task concurrency. RecoveryManager
// modifies run.Steps in-place, then calls saveFn so the caller
// persists. Compensation is sequential — one step at a time in
// reverse topological order. PublishDeadLetter (task-level) is
// fire-and-forget: errors are silently dropped because the
// workflow is already in a terminal failure state. By contrast
// PublishHistoryDeadLetter (#508) returns its publish error so the
// caller can NAK instead of Ack — a poison WORKFLOW_HISTORY event
// must never be silently dropped just because DEAD_LETTERS was
// transiently unavailable.
type RecoveryManager struct {
	js        jetstream.JetStream
	tp        *natsutil.TracingPublisher
	publisher *TaskPublisher
	tracer    trace.Tracer

	// Metrics for compensation failures.
	runsActive metric.Int64UpDownCounter
	runsFailed metric.Int64Counter
	// DLQ instruments. dlqEntries is incremented once per
	// PublishDeadLetter call (reason label drives the dashboard's
	// failure-mode breakdown); dlqDepth follows the same call so the
	// gauge reflects the current depth of the DLQ stream. Both nil
	// in legacy test seams that build a RecoveryManager via the
	// short constructor — guard before use.
	dlqEntries metric.Int64Counter
	dlqDepth   metric.Int64UpDownCounter
}

// NewRecoveryManager creates a RecoveryManager with the given
// dependencies. All parameters are required. dlqEntries and dlqDepth
// may be nil for tests that don't observe metrics — PublishDeadLetter
// nil-guards before recording.
func NewRecoveryManager(
	js jetstream.JetStream,
	tp *natsutil.TracingPublisher,
	publisher *TaskPublisher,
	tracer trace.Tracer,
	runsActive metric.Int64UpDownCounter,
	runsFailed metric.Int64Counter,
	dlqEntries metric.Int64Counter,
	dlqDepth metric.Int64UpDownCounter,
) *RecoveryManager {
	if js == nil {
		panic("NewRecoveryManager: js must not be nil")
	}
	if tp == nil {
		panic("NewRecoveryManager: tp must not be nil")
	}
	if publisher == nil {
		panic(
			"NewRecoveryManager: publisher must not be nil",
		)
	}
	if tracer == nil {
		panic(
			"NewRecoveryManager: tracer must not be nil",
		)
	}
	return &RecoveryManager{
		js:         js,
		tp:         tp,
		publisher:  publisher,
		tracer:     tracer,
		runsActive: runsActive,
		runsFailed: runsFailed,
		dlqEntries: dlqEntries,
		dlqDepth:   dlqDepth,
	}
}

// FailWorkflowFunc transitions a workflow to a failed terminal
// state. The Orchestrator provides this so RecoveryManager can
// trigger failure without owning the workflow lifecycle. The
// implementation is responsible for persisting and notifying.
type FailWorkflowFunc func(
	ctx context.Context,
	run dag.WorkflowRun,
	stepDef dag.StepDef,
	state dag.StepState,
) error

// NotifyParentFunc propagates a child workflow failure to its
// parent so the parent can handle or cascade the failure.
// Called after the child run has already been persisted in its
// terminal state.
type NotifyParentFunc func(
	ctx context.Context,
	run dag.WorkflowRun,
	childErr error,
) error

// ReleaseTaskSlotFunc releases a task concurrency slot so other
// runs can claim it. Called early in failure handling before any
// recovery logic, because the failed step no longer needs the slot.
type ReleaseTaskSlotFunc func(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	stepID string,
)

// HandlePermanentFailure handles a step whose retries are
// exhausted or that was marked non-retriable. Checks aux steps,
// on-failure handlers, compensation chains, then fails workflow.
// Callback order: releaseSlotFn first, then one of: saveFn →
// notifyFn (aux failure), saveFn → publish (on-failure/compensate),
// or failFn (no recovery path).
func (rm *RecoveryManager) HandlePermanentFailure(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	stepDef dag.StepDef,
	state dag.StepState,
	stepID string,
	saveFn SaveSnapshotFunc,
	failFn FailWorkflowFunc,
	notifyFn NotifyParentFunc,
	releaseSlotFn ReleaseTaskSlotFunc,
) error {
	if stepID == "" {
		panic(
			"HandlePermanentFailure: stepID must not be empty",
		)
	}
	if run.RunID == "" {
		panic(
			"HandlePermanentFailure: RunID must not be empty",
		)
	}

	// Release task concurrency slot if configured.
	releaseSlotFn(ctx, wfDef, stepID)

	// If this is an auxiliary step (compensate target) failing,
	// the compensation itself failed — critical state.
	if wfDef.AuxSteps[stepID] {
		return rm.failAuxStep(
			ctx, run, wfDef, stepDef, state, saveFn, notifyFn,
		)
	}

	// Check for on-failure handler before failing the workflow.
	if stepDef.OnFailure != "" {
		handled, err := rm.TryOnFailure(
			ctx, wfDef, run, stepDef, state, stepID, saveFn,
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
		return rm.StartCompensation(
			ctx, wfDef, &run, stepID, state.Error, saveFn,
		)
	}

	// No compensation either — fail the workflow.
	return failFn(ctx, run, stepDef, state)
}

// failAuxStep handles failure of an auxiliary (compensate) step.
// Marks the run as CompensateFailed and publishes a dead letter.
func (rm *RecoveryManager) failAuxStep(
	ctx context.Context,
	run dag.WorkflowRun,
	wfDef dag.WorkflowDef,
	stepDef dag.StepDef,
	state dag.StepState,
	saveFn SaveSnapshotFunc,
	notifyFn NotifyParentFunc,
) error {
	if run.RunID == "" {
		panic("failAuxStep: RunID must not be empty")
	}
	if stepDef.ID == "" {
		panic("failAuxStep: stepDef.ID must not be empty")
	}
	// Route through markTerminal so CompensateFailed (a terminal
	// status) records an honest CompletedAt rather than nil — both for
	// #443's duration/trace surfaces and so the run is reachable by the
	// #453 retention sweeper instead of leaking past it forever.
	run = markTerminal(run, dag.RunStatusCompensateFailed)
	if err := saveFn(ctx, run, stepDef.ID); err != nil {
		return err
	}
	rm.runsActive.Add(ctx, -1)
	rm.runsFailed.Add(ctx, 1)
	taskSubject := ""
	if stepDef.Task != "" {
		taskSubject = rm.publisher.StepSubject(stepDef, run.RunID)
	}
	rm.PublishDeadLetter(ctx, run, wfDef, stepDef, state, taskSubject)
	return notifyFn(
		ctx, run,
		fmt.Errorf("compensation failed: %s", state.Error),
	)
}

// TryOnFailure attempts to run the on-failure handler for a
// failed step. Returns (true, nil) if the handler was enqueued.
// Callback order: modify step Queued → saveFn → publish task.
func (rm *RecoveryManager) TryOnFailure(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	stepDef dag.StepDef,
	state dag.StepState,
	stepID string,
	saveFn SaveSnapshotFunc,
) (bool, error) {
	if stepDef.OnFailure == "" {
		panic(
			"TryOnFailure: OnFailure must not be empty",
		)
	}
	if stepID == "" {
		panic("TryOnFailure: stepID must not be empty")
	}
	onFailStep, found := findStepDef(
		wfDef, stepDef.OnFailure,
	)
	if !found {
		return false, nil
	}
	ofState := run.Steps[onFailStep.ID]
	ofState.Status = dag.StepStatusQueued
	ofState.DispatchNonce = runid.New()
	run.Steps[onFailStep.ID] = ofState
	if err := saveFn(ctx, run, onFailStep.ID); err != nil {
		return false, err
	}
	errorInput := []byte(fmt.Sprintf(
		`{"failed_step":"%s","error":%q}`,
		stepID, state.Error,
	))
	err := rm.publisher.Publish(
		ctx, run.RunID, onFailStep, errorInput, 0,
		run.WorkflowID, ofState.DispatchNonce,
	)
	return err == nil, err
}

// StartCompensation begins the saga compensation chain.
// Resolves compensate steps in reverse topo order and enqueues
// the first one. Subsequent steps are published one at a time
// by HandleCompensateCompleted as each completes.
// Callback order: modify all compensate steps Queued → saveFn →
// publish first step.
func (rm *RecoveryManager) StartCompensation(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	failedStepID string,
	failedError string,
	saveFn SaveSnapshotFunc,
) error {
	completed := completedSet(*run)
	chain := dag.ResolveCompensateChain(
		wfDef, completed, failedStepID,
	)
	if len(chain) == 0 {
		return nil
	}

	// Mark compensate steps as queued, stamping a fresh dispatch nonce on
	// each so every compensate dispatch is run-bound (#380).
	for _, step := range chain {
		state := run.Steps[step.ID]
		state.Status = dag.StepStatusQueued
		state.DispatchNonce = runid.New()
		run.Steps[step.ID] = state
	}
	// Compensation chain spans multiple steps — workflow-scoped save.
	if err := saveFn(ctx, *run, ""); err != nil {
		return err
	}

	// Build input for the first compensate step
	first := chain[0]
	originalID := findCompensateSource(wfDef, first.ID)
	input := buildCompensateInput(
		originalID, run.Steps[originalID].Output,
		failedStepID, failedError,
	)
	return rm.publisher.Publish(
		ctx, run.RunID, first, input, 0,
		run.WorkflowID, run.Steps[first.ID].DispatchNonce,
	)
}

// HandleCompensateCompleted checks if the completed step is
// part of a compensation chain. If the chain is done, marks
// the workflow as Compensated. If more steps remain, publishes
// the next one. Returns true if this was a compensate step.
// Callback order: saveFn → publish next (or saveFn to finalize).
func (rm *RecoveryManager) HandleCompensateCompleted(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	stepID string,
	saveFn SaveSnapshotFunc,
) bool {
	if !wfDef.AuxSteps[stepID] {
		return false
	}
	// Only handle steps that are Compensate targets,
	// not OnFailure.
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
		// Re-stamp a fresh dispatch nonce on this compensate step so the
		// next dispatch is run-bound (#380); it rides the save below.
		nextState := run.Steps[step.Compensate]
		nextState.DispatchNonce = runid.New()
		run.Steps[step.Compensate] = nextState
		saveFn(ctx, *run, step.Compensate)
		rm.publisher.Publish(
			ctx, run.RunID, compDef, input, 0,
			run.WorkflowID, nextState.DispatchNonce,
		)
		return true
	}

	// All compensate steps done — mark workflow Compensated. Route
	// through markTerminal so the terminal snapshot carries an honest
	// CompletedAt like every other terminal path.
	*run = markTerminal(*run, dag.RunStatusCompensated)
	saveFn(ctx, *run, "")
	rm.runsActive.Add(ctx, -1)
	return true
}

// PublishDeadLetter publishes failed step info to the
// dead-letter queue for manual inspection.
//
// As of #200, the DLQ entry body is the original task message
// payload (the marshalled protocol.TaskPayload bytes that would be
// dispatched on a re-publish), with run/step/task/error/attempts
// metadata carried in structured headers including the original
// task subject. Replay reads the stored body + subject verbatim and
// re-publishes — no synthesis from metadata. When body computation
// fails, the publish is skipped rather than writing a useless stub
// (TigerStyle: fail loudly).
//
// Idempotency: sets Nats-Msg-Id to a deterministic key over
// (runID, stepID, attempts) so duplicate calls — e.g. from engine
// consumer redelivery of a `step.failed` event before the terminal-run
// guard latches — produce exactly one DLQ entry. See issue #202.
// The DEAD_LETTERS stream's Duplicates window must cover the longest
// plausible engine redelivery interval (see natsutil.SetupStreams).
//
// taskSubject is the original task subject the engine dispatched on.
// When empty, the publish falls back to a derived default —
// "task.<task>.<runID>" — preserving the historical behavior for
// the test seam.
func (rm *RecoveryManager) PublishDeadLetter(
	ctx context.Context,
	run dag.WorkflowRun,
	wfDef dag.WorkflowDef,
	stepDef dag.StepDef,
	state dag.StepState,
	taskSubject string,
) {
	if ctx == nil {
		panic("PublishDeadLetter: ctx must not be nil")
	}
	if stepDef.ID == "" {
		panic(
			"PublishDeadLetter: stepDef.ID must not be empty",
		)
	}
	runID := run.RunID
	if runID == "" {
		panic("PublishDeadLetter: run.RunID must not be empty")
	}
	body, err := buildDLQBody(run, wfDef, stepDef, state)
	if err != nil || len(body) == 0 {
		slog.WarnContext(ctx,
			"skipping DLQ publish: body unavailable",
			"run_id", runID,
			"step_id", stepDef.ID,
			"error", err,
		)
		return
	}
	if taskSubject == "" {
		taskSubject = fmt.Sprintf("task.%s.%s",
			stepDef.Task, runID)
	}
	subject := fmt.Sprintf("dead.%s.%s.%s",
		stepDef.Task, runID, stepDef.ID)
	msgID := fmt.Sprintf("dlq:%s:%s:%d",
		runID, stepDef.ID, state.Attempts)
	header := nats.Header{
		"Nats-Msg-Id":          {msgID},
		HeaderDLQRunID:         {runID},
		HeaderDLQStepID:        {stepDef.ID},
		HeaderDLQTask:          {stepDef.Task},
		HeaderDLQError:         {state.Error},
		HeaderDLQAttempts:      {strconv.Itoa(state.Attempts)},
		HeaderDLQDeliveryCount: {strconv.Itoa(state.Attempts)},
		HeaderDLQConsumer:      {DLQConsumerTaskQueues},
		HeaderDLQTaskSubject:   {taskSubject},
	}
	msg := &nats.Msg{
		Subject: subject,
		Data:    body,
		Header:  header,
	}
	_, err = rm.tp.JSPublishMsg(ctx, msg)
	rm.recordDLQObservation(ctx, run.WorkflowID, state, err)
}

// recordDLQObservation increments the DLQ counter + depth gauge.
// Skipped when the publish itself failed. Reason labels come from a
// closed enum so the cardinality stays bounded — see resolveDLQReason
// for the enum.
func (rm *RecoveryManager) recordDLQObservation(
	ctx context.Context,
	workflowID string,
	state dag.StepState,
	publishErr error,
) {
	if ctx == nil {
		panic("recordDLQObservation: ctx must not be nil")
	}
	if publishErr != nil {
		return
	}
	rm.recordDLQEntry(ctx, workflowID, resolveDLQReason(state))
}

// recordDLQEntry increments dlqEntries + dlqDepth under one nil-guard.
// reason is a bounded enum label; workflowID must be low-cardinality
// (empty for history dead-letters — NEVER a runID).
func (rm *RecoveryManager) recordDLQEntry(
	ctx context.Context, workflowID, reason string,
) {
	if ctx == nil {
		panic("recordDLQEntry: ctx must not be nil")
	}
	if reason == "" {
		panic("recordDLQEntry: reason must not be empty")
	}
	if rm.dlqEntries == nil && rm.dlqDepth == nil {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("reason", reason),
		attribute.String("workflow", workflowID),
	}
	if rm.dlqEntries != nil {
		rm.dlqEntries.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	if rm.dlqDepth != nil {
		rm.dlqDepth.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

// dlqSubjectSafe replaces NATS-subject-hostile characters — dots,
// wildcards, and spaces — with "-" so an event type or runID can
// become a literal subject token. Event types are dotted
// ("workflow.started"); a runID is event-payload-derived and gets the
// same defensive treatment before landing in a subject.
func dlqSubjectSafe(s string) string {
	replacer := strings.NewReplacer(
		".", "-", "*", "-", ">", "-", " ", "-",
	)
	return replacer.Replace(s)
}

// dlqSubjectSafeOrUnknown is dlqSubjectSafe with an "unknown"
// fallback for the empty string — used for runID, which may be
// unresolved at the unmarshal-failure call site.
func dlqSubjectSafeOrUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return dlqSubjectSafe(s)
}

// PublishHistoryDeadLetter dead-letters a poison WORKFLOW_HISTORY event
// whose handler kept failing past the MaxDeliver cap (#508). Body is the
// raw undecoded event bytes (always available; an operator diagnoses and
// manually replays to history.<runID> after fixing root cause). Distinct
// from PublishDeadLetter, which reconstructs a re-publishable TaskPayload
// and would silently skip the publish when a poison event has no
// resolvable run/wfDef/stepDef.
//
// Returns the JSPublishMsg error (nil on success) rather than swallowing
// it: the caller, nakOrDeadLetterHistory, NAKs instead of Acking on a
// non-nil error so a transient DEAD_LETTERS outage (unavailable, at
// limit, connection blip) preserves the poison event in WORKFLOW_HISTORY
// instead of silently consuming it with no durable record.
func (rm *RecoveryManager) PublishHistoryDeadLetter(
	ctx context.Context, rawData []byte,
	runID, stepID, eventType string,
	numDelivered, streamSeq uint64, handlerErr error,
) error {
	if ctx == nil {
		panic("PublishHistoryDeadLetter: ctx must not be nil")
	}
	if len(rawData) == 0 {
		panic("PublishHistoryDeadLetter: rawData must not be empty")
	}
	subject := fmt.Sprintf("dead.orchestrator.%s.%s",
		dlqSubjectSafe(eventType), dlqSubjectSafeOrUnknown(runID))
	errText := ""
	if handlerErr != nil {
		errText = handlerErr.Error()
	}
	// Nats-Msg-Id keys off the stream sequence of the STORED
	// WORKFLOW_HISTORY message, not attempt count: the stream
	// sequence is stable across every redelivery of the same message,
	// so republishing on a later delivery (which nakOrDeadLetterHistory
	// prevents, but defense in depth) dedups within the 24h Duplicates
	// window instead of creating a second DLQ entry.
	header := nats.Header{
		"Nats-Msg-Id":          {fmt.Sprintf("dlq:history:%d", streamSeq)},
		HeaderDLQRunID:         {runID},
		HeaderDLQStepID:        {stepID},
		HeaderDLQEventType:     {eventType},
		HeaderDLQError:         {errText},
		HeaderDLQDeliveryCount: {strconv.FormatUint(numDelivered, 10)},
		HeaderDLQConsumer:      {DLQConsumerWorkflowHistory},
	}
	msg := &nats.Msg{
		Subject: subject,
		Data:    rawData,
		Header:  header,
	}
	_, err := rm.tp.JSPublishMsg(ctx, msg)
	if err != nil {
		// Propagated to the caller (see doc comment above) instead of
		// swallowed: the message must NOT be Ack'd when the DLQ write
		// itself failed.
		slog.ErrorContext(ctx,
			"PublishHistoryDeadLetter: publish failed",
			"error", err,
			"run_id", runID,
			"step_id", stepID,
			"event_type", eventType,
		)
		return err
	}
	// workflow label is deliberately "" — the poison event's run/def
	// may not be resolvable, and even when it is, "workflow" is a
	// bounded-cardinality metric label; a runID must never appear here.
	rm.recordDLQEntry(ctx, "", "history_redeliver_exhausted")
	return nil
}

// resolveDLQReason maps a failed step's state to a bounded reason
// label. Three cases today: "max_deliveries" when the step exhausted
// MaxDeliver retries (most common path); "non_retriable" when the
// task returned a permanent failure; "unknown" as the catch-all so
// the label is always present. Bounded enum prevents cardinality
// blow-up in the metrics store.
func resolveDLQReason(state dag.StepState) string {
	if state.Status == dag.StepStatusFailed && state.Attempts > 0 {
		return "max_deliveries"
	}
	if state.Error != "" {
		return "non_retriable"
	}
	return "unknown"
}

// buildDLQBody reconstructs the bytes that the engine would have
// dispatched on the task subject — the marshalled
// protocol.TaskPayload with resolved input. Returns an error if
// input resolution fails so PublishDeadLetter can skip the publish
// (TigerStyle: fail loudly rather than write a useless stub).
func buildDLQBody(
	run dag.WorkflowRun,
	wfDef dag.WorkflowDef,
	stepDef dag.StepDef,
	state dag.StepState,
) ([]byte, error) {
	if stepDef.ID == "" {
		panic("buildDLQBody: stepDef.ID must not be empty")
	}
	if run.RunID == "" {
		panic("buildDLQBody: run.RunID must not be empty")
	}
	input, err := resolveDLQInput(run, wfDef, stepDef)
	if err != nil {
		return nil, fmt.Errorf("resolve input: %w", err)
	}
	payload := protocol.TaskPayload{
		TaskID:       run.RunID + "." + stepDef.ID,
		RunID:        run.RunID,
		StepID:       stepDef.ID,
		Attempt:      state.Attempts,
		Input:        input,
		WorkflowName: wfDef.Name,
	}
	return json.Marshal(payload)
}

// resolveDLQInput resolves the step input bytes using the same path
// the task publisher takes (dag.ResolveInput). When wfDef is empty
// (legacy test seam), falls back to the run's input. When the step
// has no upstream and run.Input is empty, returns a JSON null so
// callers always get a non-empty body — DLQ entries are never empty.
func resolveDLQInput(
	run dag.WorkflowRun,
	wfDef dag.WorkflowDef,
	stepDef dag.StepDef,
) ([]byte, error) {
	if wfDef.Name == "" {
		// Legacy / test-seam path: no DAG to resolve against.
		if len(run.Input) > 0 {
			return run.Input, nil
		}
		return []byte("null"), nil
	}
	input, err := dag.ResolveInput(stepDef, run.Steps, run.Input)
	if err != nil {
		return nil, err
	}
	if len(input) == 0 {
		return []byte("null"), nil
	}
	return input, nil
}

// RecoverIfOnFailure checks if stepID is an OnFailure target
// for a failed step. If so, transitions the failed step to
// Recovered and marks dependents as Skipped. No callbacks —
// modifies run.Steps in-place; caller must persist afterward.
func (rm *RecoveryManager) RecoverIfOnFailure(
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

		// Skip dependents of the failed step — they can't
		// run without its output
		for _, s := range wfDef.Steps {
			for _, dep := range s.DependsOn {
				if dep == stepDef.ID {
					depState := run.Steps[s.ID]
					if depState.Status ==
						dag.StepStatusPending {
						depState.Status =
							dag.StepStatusSkipped
						run.Steps[s.ID] = depState
					}
				}
			}
		}
		return
	}
}

func jsonOrNull(b []byte) string {
	if len(b) == 0 {
		return "null"
	}
	return string(b)
}

// findCompensateSource returns the step ID whose Compensate
// field points to the given compensate step ID.
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

// buildCompensateInput creates the JSON input for a compensate
// step containing original step context and failure details.
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
