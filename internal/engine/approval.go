// engine/approval.go
// Approval gate engine logic: token generation, step enqueue, and
// event handlers for granted/rejected/expired outcomes. Tokens are
// stored in the approval_tokens KV bucket with atomic CAS operations
// to prevent double-approval races.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

// TimerActionApprovalTimeout fires when an approval gate expires.
const TimerActionApprovalTimeout TimerAction = "approval_timeout"

// approvalTokenRecord is stored in the approval_tokens KV bucket.
// The token field is compared during API consumption to prevent
// forged or replayed requests.
type approvalTokenRecord struct {
	Token     string    `json:"token"`
	RunID     string    `json:"run_id"`
	StepID    string    `json:"step_id"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

// approvalRequestedPayload is published to history and the
// notification subject when an approval gate activates.
type approvalRequestedPayload struct {
	Token       string            `json:"token"`
	RunID       string            `json:"run_id"`
	StepID      string            `json:"step_id"`
	Subject     string            `json:"subject"`
	Description string            `json:"description,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	ExpiresAt   time.Time         `json:"expires_at"`
}

// SaveSnapshotFunc persists a WorkflowRun snapshot after a
// subsystem has modified run.Steps state. The Orchestrator
// provides its saveSnapshot method as the concrete callback.
// Callers must not modify the run further after saveFn returns
// successfully — the persisted state is the source of truth.
type SaveSnapshotFunc func(
	ctx context.Context, run dag.WorkflowRun,
) error

// Enqueue activates an approval gate: generates a token, stores
// it in KV, publishes events, and schedules timeout.
// Callback order: marks step Running → saveFn → publish → schedule.
func (ag *ApprovalGate) Enqueue(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	step dag.StepDef,
	saveFn SaveSnapshotFunc,
) error {
	if step.Type != dag.StepTypeApproval {
		panic("Enqueue: wrong step type")
	}
	if run.RunID == "" {
		panic("Enqueue: RunID must not be empty")
	}

	cfg, err := dag.ParseApprovalConfig(step)
	if err != nil {
		return fmt.Errorf("enqueueApprovalStep: %w", err)
	}

	token, err := generateApprovalToken()
	if err != nil {
		return err
	}

	return ag.activate(
		ctx, run, step, cfg, token, saveFn,
	)
}

// activate stores the token, marks the step running, publishes
// events, and schedules the timeout timer.
func (ag *ApprovalGate) activate(
	ctx context.Context,
	run *dag.WorkflowRun,
	step dag.StepDef,
	cfg dag.ApprovalConfig,
	token string,
	saveFn SaveSnapshotFunc,
) error {
	if token == "" {
		panic("activate: token must not be empty")
	}
	if cfg.Subject == "" {
		panic("activate: Subject must not be empty")
	}
	now := time.Now().UTC()
	expiresAt := now.Add(cfg.Timeout)

	if err := ag.storeToken(
		ctx, run.RunID, step.ID, token, now, expiresAt,
	); err != nil {
		return err
	}

	state := run.Steps[step.ID]
	state.Status = dag.StepStatusRunning
	run.Steps[step.ID] = state
	if err := saveFn(ctx, *run); err != nil {
		return err
	}

	ag.publishRequested(
		ctx, run.RunID, step.ID, cfg, token, expiresAt,
	)

	return ag.scheduleTimeout(
		ctx, run.RunID, step.ID, cfg.Timeout,
	)
}

// storeToken writes the token record to KV with key
// {runID}.{stepID}. Uses Put to store the record.
func (ag *ApprovalGate) storeToken(
	ctx context.Context,
	runID, stepID, token string,
	createdAt, expiresAt time.Time,
) error {
	if runID == "" {
		panic("storeToken: runID must not be empty")
	}
	if stepID == "" {
		panic("storeToken: stepID must not be empty")
	}
	kv, err := ag.js.KeyValue(ctx, "approval_tokens")
	if err != nil {
		return fmt.Errorf(
			"get approval_tokens bucket: %w", err,
		)
	}
	record := approvalTokenRecord{
		Token:     token,
		RunID:     runID,
		StepID:    stepID,
		CreatedAt: createdAt,
		ExpiresAt: expiresAt,
	}
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal approval token: %w", err)
	}
	key := runID + "." + stepID
	_, err = kv.Put(ctx, key, data)
	return err
}

// publishRequested publishes the approval.requested event to
// history and a notification to the configured NATS subject.
func (ag *ApprovalGate) publishRequested(
	ctx context.Context,
	runID, stepID string,
	cfg dag.ApprovalConfig,
	token string,
	expiresAt time.Time,
) {
	if runID == "" {
		panic("publishRequested: runID must not be empty")
	}
	if stepID == "" {
		panic(
			"publishRequested: stepID must not be empty",
		)
	}
	payload := approvalRequestedPayload{
		Token:       token,
		RunID:       runID,
		StepID:      stepID,
		Subject:     cfg.Subject,
		Description: cfg.Description,
		Metadata:    cfg.Metadata,
		ExpiresAt:   expiresAt,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}

	evt := protocol.NewStepEvent(
		protocol.EventApprovalRequested,
		runID, stepID, data,
	)
	evtData, err := evt.Marshal()
	if err != nil {
		return
	}
	ag.js.Publish(
		ctx, evt.NATSSubject(), evtData,
		jetstream.WithMsgID(evt.NATSMsgID()),
	)

	// Publish notification to the configured subject.
	ag.nc.Publish(cfg.Subject, data)
}

// scheduleTimeout schedules a durable timer that fires an
// approval.expired event after the configured timeout.
func (ag *ApprovalGate) scheduleTimeout(
	ctx context.Context,
	runID, stepID string, timeout time.Duration,
) error {
	if runID == "" {
		panic(
			"scheduleTimeout: runID must not be empty",
		)
	}
	if stepID == "" {
		panic(
			"scheduleTimeout: stepID must not be empty",
		)
	}
	durationMs := timeout.Milliseconds()
	if durationMs <= 0 {
		return fmt.Errorf(
			"approval timeout must be positive, got %v",
			timeout,
		)
	}
	return ag.sleepTimer.Schedule(ctx, TimerMessage{
		Action:     TimerActionApprovalTimeout,
		RunID:      runID,
		StepID:     stepID,
		DurationMs: durationMs,
	})
}

// HandleGranted completes the approval step with the granted
// payload as output. Guards against duplicate processing.
// Callback order: loadFn → modify step Completed → saveFn →
// enqueueFn (or completeFn if DAG is done).
func (ag *ApprovalGate) HandleGranted(
	ctx context.Context,
	evt protocol.Event,
	loadFn func(ctx context.Context, runID string) (
		dag.WorkflowDef, dag.WorkflowRun, error,
	),
	completeFn func(
		ctx context.Context, run dag.WorkflowRun,
	) error,
	saveFn SaveSnapshotFunc,
	enqueueFn func(
		ctx context.Context,
		wfDef dag.WorkflowDef,
		run dag.WorkflowRun,
	) error,
) error {
	if evt.RunID == "" {
		panic("HandleGranted: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("HandleGranted: StepID must not be empty")
	}
	wfDef, run, err := loadFn(ctx, evt.RunID)
	if err != nil {
		return err
	}
	state := run.Steps[evt.StepID]
	if state.Status != dag.StepStatusRunning {
		return nil // Already processed — idempotent guard.
	}

	state.Status = dag.StepStatusCompleted
	state.Output = evt.Payload
	run.Steps[evt.StepID] = state

	completed := completedSet(run)
	if dag.IsComplete(wfDef, completed) {
		return completeFn(ctx, run)
	}
	if err := saveFn(ctx, run); err != nil {
		return err
	}
	return enqueueFn(ctx, wfDef, run)
}

// HandleRejected fails the approval step and delegates to the
// standard failure path (on-failure, compensate, or fail).
// Callback order: loadFn → modify step Failed → failFn.
func (ag *ApprovalGate) HandleRejected(
	ctx context.Context,
	evt protocol.Event,
	loadFn func(ctx context.Context, runID string) (
		dag.WorkflowDef, dag.WorkflowRun, error,
	),
	failFn func(
		ctx context.Context,
		run dag.WorkflowRun,
		stepDef dag.StepDef,
		state dag.StepState,
	) error,
) error {
	if evt.RunID == "" {
		panic(
			"HandleRejected: RunID must not be empty",
		)
	}
	if evt.StepID == "" {
		panic(
			"HandleRejected: StepID must not be empty",
		)
	}
	wfDef, run, err := loadFn(ctx, evt.RunID)
	if err != nil {
		return err
	}
	state := run.Steps[evt.StepID]
	if state.Status != dag.StepStatusRunning {
		return nil
	}

	state.Status = dag.StepStatusFailed
	state.Error = "approval rejected"
	if evt.Payload != nil {
		state.Error = string(evt.Payload)
	}
	run.Steps[evt.StepID] = state

	stepDef, _ := findStepDef(wfDef, evt.StepID)
	return failFn(ctx, run, stepDef, state)
}

// HandleExpired fails the approval step when the timeout fires.
// Guards against steps already approved or rejected.
// Callback order: loadFn → modify step Failed → failFn.
func (ag *ApprovalGate) HandleExpired(
	ctx context.Context,
	evt protocol.Event,
	loadFn func(ctx context.Context, runID string) (
		dag.WorkflowDef, dag.WorkflowRun, error,
	),
	failFn func(
		ctx context.Context,
		run dag.WorkflowRun,
		stepDef dag.StepDef,
		state dag.StepState,
	) error,
) error {
	if evt.RunID == "" {
		panic(
			"HandleExpired: RunID must not be empty",
		)
	}
	if evt.StepID == "" {
		panic(
			"HandleExpired: StepID must not be empty",
		)
	}
	wfDef, run, err := loadFn(ctx, evt.RunID)
	if err != nil {
		return err
	}
	state := run.Steps[evt.StepID]
	if state.Status != dag.StepStatusRunning {
		return nil // Already resolved — no-op.
	}

	state.Status = dag.StepStatusFailed
	state.Error = "approval timed out"
	run.Steps[evt.StepID] = state

	stepDef, _ := findStepDef(wfDef, evt.StepID)
	return failFn(ctx, run, stepDef, state)
}

// CleanupTokens deletes tokens for any approval steps that were
// cancelled. Called during workflow cancellation.
func (ag *ApprovalGate) CleanupTokens(
	ctx context.Context,
	wfDef dag.WorkflowDef, run dag.WorkflowRun,
) {
	if run.RunID == "" {
		panic("CleanupTokens: RunID must not be empty")
	}
	if run.Steps == nil {
		panic("CleanupTokens: Steps must not be nil")
	}
	for _, step := range wfDef.Steps {
		if step.Type != dag.StepTypeApproval {
			continue
		}
		state := run.Steps[step.ID]
		if state.Status == dag.StepStatusCancelled {
			ag.deleteToken(ctx, run.RunID, step.ID)
		}
	}
}

// deleteToken removes a token from KV during cancellation
// cleanup. Errors are logged but not fatal — the timeout timer
// will fire and see the step is already cancelled.
func (ag *ApprovalGate) deleteToken(
	ctx context.Context, runID, stepID string,
) {
	if runID == "" {
		panic("deleteToken: runID must not be empty")
	}
	if stepID == "" {
		panic("deleteToken: stepID must not be empty")
	}
	kv, err := ag.js.KeyValue(ctx, "approval_tokens")
	if err != nil {
		return
	}
	key := runID + "." + stepID
	kv.Delete(ctx, key)
}
