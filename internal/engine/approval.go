// engine/approval.go
// Approval gate engine logic: token generation, step enqueue, and
// event handlers for granted/rejected/expired outcomes. Tokens are
// stored in the approval_tokens KV bucket with atomic CAS operations
// to prevent double-approval races.
package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
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

// generateApprovalToken returns a 64-character hex string from
// 32 crypto-random bytes. Panics if OS entropy is unavailable.
func generateApprovalToken() (string, error) {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	if err != nil {
		return "", fmt.Errorf("generate token: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// enqueueApprovalStep activates an approval gate: generates a
// token, stores it in KV, publishes events, and schedules timeout.
func (o *Orchestrator) enqueueApprovalStep(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	step dag.StepDef,
) error {
	if step.Type != dag.StepTypeApproval {
		panic("enqueueApprovalStep: wrong step type")
	}
	if run.RunID == "" {
		panic("enqueueApprovalStep: RunID must not be empty")
	}

	cfg, err := dag.ParseApprovalConfig(step)
	if err != nil {
		return fmt.Errorf("enqueueApprovalStep: %w", err)
	}

	token, err := generateApprovalToken()
	if err != nil {
		return err
	}

	return o.activateApprovalGate(
		ctx, run, step, cfg, token,
	)
}

// activateApprovalGate stores the token, marks the step running,
// publishes events, and schedules the timeout timer. Extracted
// to keep enqueueApprovalStep under 70 lines.
func (o *Orchestrator) activateApprovalGate(
	ctx context.Context,
	run *dag.WorkflowRun,
	step dag.StepDef,
	cfg dag.ApprovalConfig,
	token string,
) error {
	if token == "" {
		panic("activateApprovalGate: token must not be empty")
	}
	if cfg.Subject == "" {
		panic(
			"activateApprovalGate: Subject must not be empty",
		)
	}
	now := time.Now().UTC()
	expiresAt := now.Add(cfg.Timeout)

	if err := o.storeApprovalToken(
		run.RunID, step.ID, token, now, expiresAt,
	); err != nil {
		return err
	}

	state := run.Steps[step.ID]
	state.Status = dag.StepStatusRunning
	run.Steps[step.ID] = state
	if err := o.saveSnapshot(ctx, *run); err != nil {
		return err
	}

	o.publishApprovalRequested(
		run.RunID, step.ID, cfg, token, expiresAt,
	)

	return o.scheduleApprovalTimeout(
		run.RunID, step.ID, cfg.Timeout,
	)
}

// storeApprovalToken writes the token record to KV with key
// {runID}.{stepID}. Uses Create to prevent overwrites.
func (o *Orchestrator) storeApprovalToken(
	runID, stepID, token string,
	createdAt, expiresAt time.Time,
) error {
	if runID == "" {
		panic("storeApprovalToken: runID must not be empty")
	}
	if stepID == "" {
		panic("storeApprovalToken: stepID must not be empty")
	}
	kv, err := o.jsLegacy.KeyValue("approval_tokens")
	if err != nil {
		return fmt.Errorf("get approval_tokens bucket: %w", err)
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
	_, err = kv.Put(key, data)
	return err
}

// publishApprovalRequested publishes the approval.requested event
// to history and a notification to the configured NATS subject.
func (o *Orchestrator) publishApprovalRequested(
	runID, stepID string,
	cfg dag.ApprovalConfig,
	token string,
	expiresAt time.Time,
) {
	if runID == "" {
		panic(
			"publishApprovalRequested: runID must not be empty",
		)
	}
	if stepID == "" {
		panic(
			"publishApprovalRequested: stepID must not be empty",
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
	o.jsLegacy.Publish(
		evt.NATSSubject(), evtData,
		nats.MsgId(evt.NATSMsgID()),
	)

	// Publish notification to the configured subject.
	o.nc.Publish(cfg.Subject, data)
}

// scheduleApprovalTimeout schedules a durable timer that fires
// an approval.expired event after the configured timeout.
func (o *Orchestrator) scheduleApprovalTimeout(
	runID, stepID string, timeout time.Duration,
) error {
	if runID == "" {
		panic(
			"scheduleApprovalTimeout: runID must not be empty",
		)
	}
	if stepID == "" {
		panic(
			"scheduleApprovalTimeout: stepID must not be empty",
		)
	}
	durationMs := timeout.Milliseconds()
	if durationMs <= 0 {
		durationMs = 1
	}
	return o.sleepTimer.Schedule(TimerMessage{
		Action:     TimerActionApprovalTimeout,
		RunID:      runID,
		StepID:     stepID,
		DurationMs: durationMs,
	})
}

// handleApprovalGranted completes the approval step with the
// granted payload as output. Guards against duplicate processing.
func (o *Orchestrator) handleApprovalGranted(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic(
			"handleApprovalGranted: RunID must not be empty",
		)
	}
	if evt.StepID == "" {
		panic(
			"handleApprovalGranted: StepID must not be empty",
		)
	}
	wfDef, run, err := o.loadRunAndDef(evt.RunID)
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
		return o.completeWorkflow(ctx, run)
	}
	if err := o.saveSnapshot(ctx, run); err != nil {
		return err
	}
	return o.enqueueReady(ctx, wfDef, run)
}

// handleApprovalRejected fails the approval step and delegates
// to the standard failure path (on-failure, compensate, or fail).
func (o *Orchestrator) handleApprovalRejected(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic(
			"handleApprovalRejected: RunID must not be empty",
		)
	}
	if evt.StepID == "" {
		panic(
			"handleApprovalRejected: StepID must not be empty",
		)
	}
	wfDef, run, err := o.loadRunAndDef(evt.RunID)
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
	return o.failWorkflow(ctx, run, stepDef, state)
}

// handleApprovalExpired fails the approval step when the timeout
// fires. Guards against steps already approved or rejected.
func (o *Orchestrator) handleApprovalExpired(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic(
			"handleApprovalExpired: RunID must not be empty",
		)
	}
	if evt.StepID == "" {
		panic(
			"handleApprovalExpired: StepID must not be empty",
		)
	}
	wfDef, run, err := o.loadRunAndDef(evt.RunID)
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
	return o.failWorkflow(ctx, run, stepDef, state)
}

// cleanupApprovalTokens deletes tokens for any approval steps
// that were cancelled. Called during workflow cancellation.
func (o *Orchestrator) cleanupApprovalTokens(
	wfDef dag.WorkflowDef, run dag.WorkflowRun,
) {
	if run.RunID == "" {
		panic(
			"cleanupApprovalTokens: RunID must not be empty",
		)
	}
	if run.Steps == nil {
		panic(
			"cleanupApprovalTokens: Steps must not be nil",
		)
	}
	for _, step := range wfDef.Steps {
		if step.Type != dag.StepTypeApproval {
			continue
		}
		state := run.Steps[step.ID]
		if state.Status == dag.StepStatusCancelled {
			o.deleteApprovalToken(run.RunID, step.ID)
		}
	}
}

// deleteApprovalToken removes a token from KV during cancellation
// cleanup. Errors are logged but not fatal — the timeout timer
// will fire and see the step is already cancelled.
func (o *Orchestrator) deleteApprovalToken(
	runID, stepID string,
) {
	if runID == "" {
		panic(
			"deleteApprovalToken: runID must not be empty",
		)
	}
	if stepID == "" {
		panic(
			"deleteApprovalToken: stepID must not be empty",
		)
	}
	kv, err := o.jsLegacy.KeyValue("approval_tokens")
	if err != nil {
		return
	}
	key := runID + "." + stepID
	kv.Delete(key)
}
