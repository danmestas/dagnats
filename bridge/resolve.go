package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/danmestas/dagnats/internal/runstate"
	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// errResponseAlreadyWritten signals that the handler wrote the HTTP
// response directly and handleResolve should not write status 200.
var errResponseAlreadyWritten = errors.New(
	"response already written",
)

// resolveRequest is the JSON body for POST /v1/tasks/{id}/resolve.
type resolveRequest struct {
	Action       string          `json:"action"`
	Output       json.RawMessage `json:"output,omitempty"`
	Error        string          `json:"error,omitempty"`
	FailureType  string          `json:"failure_type,omitempty"`
	RetryAfterMs int64           `json:"retry_after_ms,omitempty"`
	Name         string          `json:"name,omitempty"`
	DurationMs   int64           `json:"duration_ms,omitempty"`
	Checkpoint   json.RawMessage `json:"checkpoint,omitempty"`
	Data         json.RawMessage `json:"data,omitempty"`
	RunID        string          `json:"run_id,omitempty"`
	TimeoutMs    int64           `json:"timeout_ms,omitempty"`
	// Type is the journal.append record type; "entry" when empty.
	Type string `json:"type,omitempty"`
}

// pauseDurationMaxMs caps the maximum pause at 1 hour.
const pauseDurationMaxMs = 3_600_000

// handleResolve resolves a polled task by completing, failing,
// pausing, or checkpointing it.
func (b *Bridge) handleResolve(
	w http.ResponseWriter, r *http.Request,
) {
	if b.ackMap == nil {
		panic("handleResolve: ackMap must not be nil")
	}
	if b.js == nil {
		panic("handleResolve: js must not be nil")
	}
	// Extract W3C trace context so the span we open joins the
	// upstream caller's trace instead of rooting a fresh one.
	incoming := otel.GetTextMapPropagator().Extract(
		r.Context(), propagation.HeaderCarrier(r.Header),
	)
	ctx, span := b.tracer.Start(incoming, "bridge.resolve")
	defer span.End()

	taskID := r.PathValue("id")
	if taskID == "" {
		http.Error(
			w, "task id is required", http.StatusBadRequest,
		)
		return
	}

	// r.Context() carries the same values as ctx below (read here,
	// ahead of tracing/parsing, to fail auth fast).
	claims := claimsFromContext(r.Context())
	msg, claimingTokenID, ok := b.ackMap.LoadWithTokenID(taskID)
	if !ok {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}
	// #627: see authorizeTaskOwner's doc comment. 403 not 404: the
	// caller already knows the task ID exists (its own poll, or leaked).
	if !b.authorizeTaskOwner(claims, claimingTokenID) {
		http.Error(w, "task not claimed by this token", http.StatusForbidden)
		return
	}

	if stop := b.ackMap.JournalStop(taskID); stop != nil {
		writeJournalOutcome(w, stop)
		return
	}
	req, err := parseResolveRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if b.withdrawIfRunCancelled(ctx, taskID, claimingTokenID, msg) {
		http.Error(w, "run cancelled", http.StatusConflict)
		return
	}

	b.requestCount.Add(ctx, 1, routeResolve)
	slog.InfoContext(ctx, "task resolved",
		"task_id", taskID,
		"action", req.Action,
	)

	err = b.dispatchAction(ctx, taskID, claimingTokenID, msg, req, w, r)
	if err != nil {
		if errors.Is(err, errResponseAlreadyWritten) {
			return
		}
		var badRequest *badRequestError
		if errors.As(err, &badRequest) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if writeJournalOutcome(w, err) {
			return
		}
		http.Error(
			w, err.Error(), http.StatusInternalServerError,
		)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// withdrawIfRunCancelled withdraws a claimed task whose run has been
// cancelled (#737) and reports whether it did. Every resolve action asks,
// heartbeat included: a 409 is the signal a worker acts on to stop the
// work, and withdrawing (ack, then drop the claim) means no later resolve
// can record the step. Nothing is published: the engine already finished
// the run, and a step event would only describe work nobody wants.
func (b *Bridge) withdrawIfRunCancelled(
	ctx context.Context, taskID, claimingTokenID string, msg jetstream.Msg,
) bool {
	if msg == nil {
		panic("withdrawIfRunCancelled: msg must not be nil")
	}
	if taskID == "" {
		panic("withdrawIfRunCancelled: taskID must not be empty")
	}
	runID, stepID := splitTaskID(taskID)
	if !runstate.Cancelled(ctx, b.workflowRunsKV, runID) {
		return false
	}
	slog.InfoContext(ctx, "withdrawing claimed task of a cancelled run",
		"run_id", runID, "step_id", stepID)
	// End the attempt's log like every other resolve does, so a
	// logs?follow stream on it gets its eof instead of waiting out
	// LogFollowDurationMax. "failed" is the existing attempt-ending
	// marker for an attempt that produced no result.
	b.emitLogMarker(ctx, taskID, msg, protocol.LogMarkerFailed)
	if err := msg.Ack(); err != nil {
		slog.WarnContext(ctx, "ack withdrawn task failed; it may redeliver",
			"run_id", runID, "step_id", stepID, "error", err)
	}
	b.ackMap.Delete(taskID)
	b.ackMap.MarkResolved(taskID, claimingTokenID)
	return true
}

// parseResolveRequest validates the resolve JSON body.
func parseResolveRequest(
	r *http.Request,
) (resolveRequest, error) {
	if r == nil {
		panic("parseResolveRequest: r must not be nil")
	}
	if r.Body == nil {
		panic("parseResolveRequest: r.Body must not be nil")
	}
	var req resolveRequest
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		return req, fmt.Errorf("invalid JSON: %w", err)
	}
	if req.Action == "" {
		return req, fmt.Errorf("action is required")
	}
	validActions := map[string]bool{
		"complete":    true,
		"fail":        true,
		"pause":       true,
		"checkpoint":  true,
		"send_signal": true,
		"wait_signal": true,
		"continue":    true,
		"heartbeat":   true,
		"stream":      true,
		// Agent journal (ADR-026), see journal.go.
		"journal.append":  true,
		"journal.read":    true,
		"load_checkpoint": true,
	}
	if !validActions[req.Action] {
		return req, fmt.Errorf("invalid action: %s", req.Action)
	}
	return req, nil
}

// dispatchAction routes the resolve request to the correct handler.
func (b *Bridge) dispatchAction(
	ctx context.Context,
	taskID string,
	claimingTokenID string,
	msg jetstream.Msg,
	req resolveRequest,
	w http.ResponseWriter,
	r *http.Request,
) error {
	if ctx == nil {
		panic("dispatchAction: ctx must not be nil")
	}
	if taskID == "" {
		panic("dispatchAction: taskID must not be empty")
	}
	if msg == nil {
		panic("dispatchAction: msg must not be nil")
	}
	switch req.Action {
	case "complete":
		return b.resolveComplete(ctx, taskID, claimingTokenID, msg, req)
	case "fail":
		return b.resolveFail(ctx, taskID, claimingTokenID, msg, req)
	case "pause":
		return b.resolvePause(ctx, taskID, claimingTokenID, msg, req)
	case "checkpoint":
		return b.resolveCheckpoint(ctx, taskID, msg, req)
	case "send_signal":
		return b.resolveSendSignal(ctx, taskID, msg, req, w)
	case "wait_signal":
		return b.resolveWaitSignal(ctx, taskID, msg, req, w, r)
	case "continue":
		return b.resolveContinue(ctx, taskID, claimingTokenID, msg, req)
	case "heartbeat":
		return b.resolveHeartbeat(ctx, taskID, msg)
	case "stream":
		return b.resolveStream(ctx, taskID, msg, req)
	case "journal.append":
		return b.resolveJournalAppend(ctx, taskID, claimingTokenID, msg, req, w)
	case "journal.read":
		return b.resolveJournalRead(ctx, taskID, claimingTokenID, msg, w)
	case "load_checkpoint":
		return b.resolveLoadCheckpoint(ctx, taskID, msg, w)
	default:
		return fmt.Errorf("unhandled action: %s", req.Action)
	}
}

// resolveComplete publishes step.completed, acks the NATS message,
// and removes the task from the ackMap.
func (b *Bridge) resolveComplete(
	ctx context.Context,
	taskID string, claimingTokenID string, msg jetstream.Msg, req resolveRequest,
) error {
	if ctx == nil {
		panic("resolveComplete: ctx must not be nil")
	}
	if taskID == "" {
		panic("resolveComplete: taskID must not be empty")
	}
	if msg == nil {
		panic("resolveComplete: msg must not be nil")
	}
	dispatch, err := resolveDispatch(msg)
	if err != nil {
		return err
	}
	runID, stepID := splitTaskID(taskID)
	journalSeq, err := b.commitJournal(ctx, taskID, claimingTokenID, msg, journal.Commit{
		Action: journal.ActionComplete, Output: req.Output,
	})
	if err != nil {
		return err
	}
	// #624 review round 2: emit the "completed" marker (using the
	// AckMap's own seq/attempt state) BEFORE the resolution publish and
	// BEFORE the task leaves the AckMap — the drain-before-resolve
	// invariant the worker SDK already guarantees natively.
	b.emitLogMarker(ctx, taskID, msg, protocol.LogMarkerCompleted)
	evt := protocol.NewStepEvent(
		protocol.EventStepCompleted, runID, stepID, req.Output,
	)
	evt.SetIdentity(dispatch.iteration, dispatch.attempt, dispatch.nonce)
	evt.JournalSeq = journalSeq
	if err := b.publishEvent(ctx, evt); err != nil {
		return fmt.Errorf("publish complete event: %w", err)
	}
	if err := msg.Ack(); err != nil {
		return fmt.Errorf("ack message: %w", err)
	}
	b.ackMap.Delete(taskID)
	b.ackMap.MarkResolved(taskID, claimingTokenID)
	return nil
}

// resolveFail publishes step.failed, acks the NATS message, and
// removes the task from the ackMap.
func (b *Bridge) resolveFail(
	ctx context.Context,
	taskID string, claimingTokenID string, msg jetstream.Msg, req resolveRequest,
) error {
	if ctx == nil {
		panic("resolveFail: ctx must not be nil")
	}
	if taskID == "" {
		panic("resolveFail: taskID must not be empty")
	}
	if msg == nil {
		panic("resolveFail: msg must not be nil")
	}
	dispatch, err := resolveDispatch(msg)
	if err != nil {
		return err
	}
	runID, stepID := splitTaskID(taskID)
	failureType := protocol.FailureType(req.FailureType)
	if failureType == "" {
		failureType = protocol.FailureTypeRetriable
	}
	payload := protocol.StepFailedPayload{
		Error:        req.Error,
		FailureType:  failureType,
		RetryAfterMs: req.RetryAfterMs,
	}
	payloadData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal fail payload: %w", err)
	}
	journalSeq, err := b.commitJournal(ctx, taskID, claimingTokenID, msg, journal.Commit{
		Action: journal.ActionFail, Error: req.Error,
		FailureType: string(failureType), RetryAfterMs: req.RetryAfterMs,
	})
	if err != nil {
		return err
	}
	// #624 review round 2: "failed" covers fail/fail_permanent/
	// fail_retry_after alike — bridge represents all three as one
	// "fail" action distinguished only by FailureType, and every one
	// of them ends this attempt the same way from the log lane's
	// perspective. Emit BEFORE the resolution publish and BEFORE the
	// task leaves the AckMap.
	b.emitLogMarker(ctx, taskID, msg, protocol.LogMarkerFailed)
	evt := protocol.NewStepEvent(
		protocol.EventStepFailed, runID, stepID, payloadData,
	)
	evt.SetIdentity(dispatch.iteration, dispatch.attempt, dispatch.nonce)
	evt.JournalSeq = journalSeq
	if err := b.publishEvent(ctx, evt); err != nil {
		return fmt.Errorf("publish fail event: %w", err)
	}
	if err := msg.Ack(); err != nil {
		return fmt.Errorf("ack message: %w", err)
	}
	b.ackMap.Delete(taskID)
	b.ackMap.MarkResolved(taskID, claimingTokenID)
	return nil
}

// resolvePause writes checkpoint to KV and NAKs with delay.
func (b *Bridge) resolvePause(
	ctx context.Context,
	taskID string, claimingTokenID string, msg jetstream.Msg, req resolveRequest,
) error {
	if ctx == nil {
		panic("resolvePause: ctx must not be nil")
	}
	if taskID == "" {
		panic("resolvePause: taskID must not be empty")
	}
	if msg == nil {
		panic("resolvePause: msg must not be nil")
	}
	if req.DurationMs <= 0 || req.DurationMs > pauseDurationMaxMs {
		return fmt.Errorf(
			"duration_ms must be in (0, %d]", pauseDurationMaxMs,
		)
	}
	if err := b.pauseMarker(ctx, taskID, claimingTokenID, msg, req); err != nil {
		return err
	}
	// #624 review round 2: emit BEFORE the NAK — the NAK is this
	// attempt's own "resolution" (the message is redelivered fresh on
	// resume), so the marker must land before it, same ordering as
	// every other resolve path.
	b.emitLogMarker(ctx, taskID, msg, protocol.LogMarkerPaused)
	duration := time.Duration(req.DurationMs) * time.Millisecond
	if err := msg.NakWithDelay(duration); err != nil {
		return fmt.Errorf("nak with delay: %w", err)
	}
	b.ackMap.Delete(taskID)
	b.ackMap.MarkResolved(taskID, claimingTokenID)
	return nil
}

// pauseMarker records that the task is pausing: a pause record for a
// journal step, whose next claim reads it as the resume marker, else
// the checkpoint in the checkpoints bucket.
func (b *Bridge) pauseMarker(
	ctx context.Context, taskID, tokenID string, msg jetstream.Msg, req resolveRequest,
) error {
	if taskID == "" {
		panic("pauseMarker: taskID must not be empty")
	}
	mu := b.ackMap.JournalLock(taskID)
	if mu != nil {
		mu.Lock()
		defer mu.Unlock()
	}
	j, ok := b.ackMap.Journal(taskID)
	if !ok {
		return b.writeCheckpoint(ctx, taskID, req.Checkpoint)
	}
	name := req.Name
	if name == "" {
		name = "pause"
	}
	duration := time.Duration(req.DurationMs) * time.Millisecond
	if _, err := j.Pause(ctx, name, duration); err != nil {
		if errors.Is(err, journal.ErrFenced) {
			return b.settleJournalStop(ctx, taskID, tokenID, msg, err)
		}
		return fmt.Errorf("journal pause: %w", err)
	}
	return nil
}

// resolveCheckpoint buffers checkpoint data for the task's next
// `continue` (#801) and extends the ack deadline so the task stays
// in-flight. Nothing is written until the continue commits it, so a
// worker that dies between the two leaves no state behind.
func (b *Bridge) resolveCheckpoint(
	ctx context.Context,
	taskID string, msg jetstream.Msg, req resolveRequest,
) error {
	if ctx == nil {
		panic("resolveCheckpoint: ctx must not be nil")
	}
	if taskID == "" {
		panic("resolveCheckpoint: taskID must not be empty")
	}
	if msg == nil {
		panic("resolveCheckpoint: msg must not be nil")
	}
	if !b.ackMap.SetPendingState(taskID, req.Data) {
		return fmt.Errorf("task %q is no longer claimed", taskID)
	}
	if err := b.extendClaim(taskID, msg); err != nil {
		return fmt.Errorf("in-progress: %w", err)
	}
	return nil
}

// resolveContinue publishes a step.continue event, acks the NATS
// message, and removes the task from the ackMap. This enables
// non-Go agents to implement agent loops via the HTTP bridge. The
// event carries the state a `checkpoint` buffered (else the state the
// task was polled with), the dispatch identity, and the stable msg-id
// the worker SDK's Continue uses (#801).
func (b *Bridge) resolveContinue(
	ctx context.Context,
	taskID string, claimingTokenID string, msg jetstream.Msg, req resolveRequest,
) error {
	if ctx == nil {
		panic("resolveContinue: ctx must not be nil")
	}
	if taskID == "" {
		panic("resolveContinue: taskID must not be empty")
	}
	if msg == nil {
		panic("resolveContinue: msg must not be nil")
	}
	dispatch, err := resolveDispatch(msg)
	if err != nil {
		return err
	}
	payload, journalSeq, err := b.continuePayload(
		ctx, taskID, claimingTokenID, msg, dispatch, req,
	)
	if err != nil {
		return err
	}
	runID, stepID := splitTaskID(taskID)
	// #624 review round 3: emit BEFORE the step.continue publish —
	// this ITERATION's subject is done; the next iteration gets a
	// fresh iteration-scoped subject (logs.{runID}.{stepID}.{attempt}.
	// {iteration}), same as the worker SDK's Continue. The attempt
	// stays the SAME across iterations — a round-2 version of this
	// comment incorrectly said the opposite ("a fresh attempt-scoped
	// subject"); bumping attempt per iteration would consume retry
	// budget the step never spent, which is exactly why iteration
	// exists as its own dimension of subject identity.
	b.emitLogMarker(ctx, taskID, msg, protocol.LogMarkerContinued)
	evt := protocol.NewStepEvent(
		protocol.EventStepContinue, runID, stepID, payload,
	)
	evt.SetIdentity(dispatch.iteration, dispatch.attempt, dispatch.nonce)
	evt.JournalSeq = journalSeq
	if err := b.publishEvent(ctx, evt); err != nil {
		return fmt.Errorf("publish continue event: %w", err)
	}
	if err := msg.Ack(); err != nil {
		return fmt.Errorf("ack message: %w", err)
	}
	b.ackMap.Delete(taskID)
	b.ackMap.MarkResolved(taskID, claimingTokenID)
	return nil
}

// continuePayload builds the step.continue payload and, for a journal
// step, commits the iteration first. A journal step carries no state
// in the event: the commit record is the state of record (ADR-026).
// Otherwise the state is the one a `checkpoint` buffered, else the
// state the task was polled with, under the engine's cap.
func (b *Bridge) continuePayload(
	ctx context.Context, taskID, tokenID string, msg jetstream.Msg,
	dispatch taskDispatch, req resolveRequest,
) ([]byte, uint64, error) {
	if taskID == "" {
		panic("continuePayload: taskID must not be empty")
	}
	state := dispatch.state
	pending, pendingSet := b.ackMap.PendingState(taskID)
	if pendingSet {
		state = pending
	}
	if j, ok := b.ackMap.Journal(taskID); ok {
		if !pendingSet {
			state = j.State()
		}
		seq, err := b.commitJournal(ctx, taskID, tokenID, msg, journal.Commit{
			Action: journal.ActionContinue, Output: req.Output, State: state,
		})
		if err != nil {
			return nil, 0, err
		}
		payload, err := json.Marshal(protocol.StepContinuePayload{Output: req.Output})
		return payload, seq, err
	}
	if len(state) > dispatch.stateMaxBytes {
		return nil, 0, fmt.Errorf(
			"continue: state is %d bytes, over the %d byte cap (loop_state_max_bytes)",
			len(state), dispatch.stateMaxBytes,
		)
	}
	payload, err := json.Marshal(protocol.StepContinuePayload{
		Output: req.Output, State: state, StateSet: true,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("marshal continue payload: %w", err)
	}
	return payload, 0, nil
}

// resolveHeartbeat extends the ack deadline without completing
// or removing the task from the ackMap. Non-Go agents call this
// periodically to prevent the message from being redelivered.
func (b *Bridge) resolveHeartbeat(
	ctx context.Context,
	taskID string, msg jetstream.Msg,
) error {
	if ctx == nil {
		panic("resolveHeartbeat: ctx must not be nil")
	}
	if taskID == "" {
		panic("resolveHeartbeat: taskID must not be empty")
	}
	if msg == nil {
		panic("resolveHeartbeat: msg must not be nil")
	}
	return b.extendClaim(taskID, msg)
}

// extendClaim resets the delivery's AckWait and restarts the ack map's
// reap window to match (#741). Every action that keeps a claimed task
// in flight goes through here, never bare msg.InProgress: a heartbeat
// that extends NATS but not the ack map loses the task to the next
// claim's sweep 5m30s after it was claimed.
func (b *Bridge) extendClaim(taskID string, msg jetstream.Msg) error {
	if taskID == "" {
		panic("extendClaim: taskID must not be empty")
	}
	if msg == nil {
		panic("extendClaim: msg must not be nil")
	}
	if err := msg.InProgress(); err != nil {
		return err
	}
	// A missing entry means the task was already resolved or reaped;
	// there is no claim left to extend, so the result is ignored.
	b.ackMap.Touch(taskID)
	return nil
}

// resolveStream publishes data to core NATS pub/sub on
// stream.{runID}.{stepID} for real-time consumption, then extends
// the ack deadline. The task remains in-flight in the ackMap.
func (b *Bridge) resolveStream(
	ctx context.Context,
	taskID string, msg jetstream.Msg, req resolveRequest,
) error {
	if ctx == nil {
		panic("resolveStream: ctx must not be nil")
	}
	if taskID == "" {
		panic("resolveStream: taskID must not be empty")
	}
	if msg == nil {
		panic("resolveStream: msg must not be nil")
	}
	runID, stepID := splitTaskID(taskID)
	subject := fmt.Sprintf("stream.%s.%s", runID, stepID)
	if err := b.pub.Publish(ctx, subject, req.Data); err != nil {
		return fmt.Errorf("publish stream: %w", err)
	}
	return b.extendClaim(taskID, msg)
}

// writeCheckpoint stores data in the checkpoints KV bucket.
func (b *Bridge) writeCheckpoint(
	ctx context.Context, taskID string, data []byte,
) error {
	if ctx == nil {
		panic("writeCheckpoint: ctx must not be nil")
	}
	if taskID == "" {
		panic("writeCheckpoint: taskID must not be empty")
	}
	if b.checkpointKV == nil {
		return fmt.Errorf("checkpoint KV not configured")
	}
	_, err := b.checkpointKV.Put(ctx, taskID, data)
	if err != nil {
		return fmt.Errorf("write checkpoint: %w", err)
	}
	return nil
}

// publishEvent marshals and publishes an event to the history
// stream with deduplication.
func (b *Bridge) publishEvent(
	ctx context.Context, evt protocol.Event,
) error {
	if ctx == nil {
		panic("publishEvent: ctx must not be nil")
	}
	if evt.RunID == "" {
		panic("publishEvent: RunID must not be empty")
	}
	if evt.Type == "" {
		panic("publishEvent: Type must not be empty")
	}
	data, err := evt.Marshal()
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	msg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Data:    data,
		Header: nats.Header{
			"Nats-Msg-Id": {evt.NATSMsgID()},
		},
	}
	_, err = b.pub.JSPublishMsg(ctx, msg)
	return err
}

// resolveAttemptNumber derives the attempt number for a task being
// resolved, from the original polled message held in the ackMap.
// step.completed / step.failed msg-ids embed AttemptNumber: without
// it, a retrying step's 2nd..Nth step.failed dedups against the 1st
// in the history stream and the engine never sees the later
// failures (issue #381).
func resolveAttemptNumber(msg jetstream.Msg) (int, error) {
	if msg == nil {
		panic("resolveAttemptNumber: msg must not be nil")
	}
	var payload protocol.TaskPayload
	if err := json.Unmarshal(msg.Data(), &payload); err != nil {
		return 0, fmt.Errorf("unmarshal task payload: %w", err)
	}
	attemptNumber, err := taskAttemptNumber(msg, payload.Attempt)
	if err != nil {
		return 0, err
	}
	if attemptNumber < 1 {
		panic("resolveAttemptNumber: attempt must be >= 1")
	}
	return attemptNumber, nil
}

// taskDispatch is what a resolve needs from the polled task message:
// the dispatch identity every lifecycle event echoes (#801) and the
// loop state and cap the task was polled with.
type taskDispatch struct {
	iteration     int
	attempt       int
	nonce         string
	state         []byte
	stateMaxBytes int
}

// resolveDispatch reads the task's dispatch identity and loop state
// from the polled message held in the ackMap.
func resolveDispatch(msg jetstream.Msg) (taskDispatch, error) {
	if msg == nil {
		panic("resolveDispatch: msg must not be nil")
	}
	if len(msg.Data()) == 0 {
		panic("resolveDispatch: msg.Data must not be empty")
	}
	var payload protocol.TaskPayload
	if err := json.Unmarshal(msg.Data(), &payload); err != nil {
		return taskDispatch{}, fmt.Errorf("unmarshal task payload: %w", err)
	}
	attempt, err := resolveAttemptNumber(msg)
	if err != nil {
		return taskDispatch{}, fmt.Errorf("derive attempt number: %w", err)
	}
	stateMaxBytes := payload.StateMaxBytes
	if stateMaxBytes <= 0 {
		stateMaxBytes = protocol.LoopStateMaxBytesDefault
	}
	return taskDispatch{
		iteration:     payload.Iteration,
		attempt:       attempt,
		nonce:         payload.DispatchNonce,
		state:         payload.State,
		stateMaxBytes: stateMaxBytes,
	}, nil
}

// splitTaskID splits a task ID into runID and stepID.
// Task IDs are formatted as {runID}.{stepID}.
func splitTaskID(taskID string) (runID, stepID string) {
	if taskID == "" {
		panic("splitTaskID: taskID must not be empty")
	}
	// Find the first dot — runID is everything before it,
	// stepID is everything after.
	for i := 0; i < len(taskID); i++ {
		if taskID[i] == '.' {
			runID = taskID[:i]
			stepID = taskID[i+1:]
			if runID == "" || stepID == "" {
				panic("splitTaskID: runID and stepID must not be empty")
			}
			return runID, stepID
		}
	}
	// No dot found — programmer error with malformed task ID.
	panic("splitTaskID: taskID must contain a dot separator")
}

// resolveSendSignal writes a signal to the signals KV bucket,
// then extends the ack deadline so the task remains in-flight.
func (b *Bridge) resolveSendSignal(
	ctx context.Context,
	taskID string,
	msg jetstream.Msg,
	req resolveRequest,
	w http.ResponseWriter,
) error {
	if ctx == nil {
		panic("resolveSendSignal: ctx must not be nil")
	}
	if taskID == "" {
		panic("resolveSendSignal: taskID must not be empty")
	}
	if msg == nil {
		panic("resolveSendSignal: msg must not be nil")
	}
	if b.signalKV == nil {
		return fmt.Errorf("signal KV not configured")
	}
	if req.RunID == "" {
		return fmt.Errorf("run_id is required")
	}
	if req.Name == "" {
		return fmt.Errorf("name is required")
	}
	key := req.RunID + "." + req.Name
	_, err := b.signalKV.Put(ctx, key, req.Data)
	if err != nil {
		return fmt.Errorf("write signal: %w", err)
	}
	if err := b.extendClaim(taskID, msg); err != nil {
		return fmt.Errorf("in-progress: %w", err)
	}
	return nil
}

// signalTimeoutMaxMs caps wait_signal at 1 hour for safety.
const signalTimeoutMaxMs = 3_600_000

// resolveWaitSignal watches the signals KV bucket for a signal,
// blocking until it arrives or timeout expires.
func (b *Bridge) resolveWaitSignal(
	ctx context.Context,
	taskID string,
	msg jetstream.Msg,
	req resolveRequest,
	w http.ResponseWriter,
	r *http.Request,
) error {
	if ctx == nil {
		panic("resolveWaitSignal: ctx must not be nil")
	}
	if taskID == "" {
		panic("resolveWaitSignal: taskID must not be empty")
	}
	if msg == nil {
		panic("resolveWaitSignal: msg must not be nil")
	}
	if b.signalKV == nil {
		return fmt.Errorf("signal KV not configured")
	}
	if req.Name == "" {
		return fmt.Errorf("name is required")
	}
	if req.TimeoutMs <= 0 || req.TimeoutMs > signalTimeoutMaxMs {
		return fmt.Errorf(
			"timeout_ms must be in (0, %d]", signalTimeoutMaxMs,
		)
	}
	signalData, err := b.waitForSignalOrTimeout(
		ctx, taskID, req.Name, req.TimeoutMs, msg, r,
	)
	if err != nil {
		if err.Error() == "timeout" {
			w.WriteHeader(http.StatusRequestTimeout)
			return errResponseAlreadyWritten
		}
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	_, writeErr := w.Write(signalData)
	if writeErr != nil {
		return writeErr
	}
	return errResponseAlreadyWritten
}

// inProgressIntervalMs is how often to extend the ack deadline
// while waiting for a signal.
const inProgressIntervalMs = 15_000

// waitForSignalOrTimeout checks if a signal exists, or watches for
// it, periodically extending the ack deadline until signal arrives,
// timeout expires, or client disconnects.
func (b *Bridge) waitForSignalOrTimeout(
	ctx context.Context,
	taskID, name string,
	timeoutMs int64,
	msg jetstream.Msg,
	r *http.Request,
) ([]byte, error) {
	if ctx == nil {
		panic("waitForSignalOrTimeout: ctx must not be nil")
	}
	if taskID == "" {
		panic("waitForSignalOrTimeout: taskID must not be empty")
	}
	if name == "" {
		panic("waitForSignalOrTimeout: name must not be empty")
	}
	runID, _ := splitTaskID(taskID)
	key := runID + "." + name
	entry, err := b.signalKV.Get(ctx, key)
	if err == nil {
		return entry.Value(), nil
	}
	if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, fmt.Errorf("get signal: %w", err)
	}
	return b.watchForSignal(ctx, taskID, key, timeoutMs, msg, r)
}

// watchForSignal creates a KV watcher and blocks until signal
// arrives, timeout expires, or client disconnects.
func (b *Bridge) watchForSignal(
	ctx context.Context,
	taskID, key string,
	timeoutMs int64,
	msg jetstream.Msg,
	r *http.Request,
) ([]byte, error) {
	if ctx == nil {
		panic("watchForSignal: ctx must not be nil")
	}
	if key == "" {
		panic("watchForSignal: key must not be empty")
	}
	if msg == nil {
		panic("watchForSignal: msg must not be nil")
	}
	watcher, err := b.signalKV.Watch(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("create watcher: %w", err)
	}
	defer watcher.Stop()
	timeout := time.Duration(timeoutMs) * time.Millisecond
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(
		time.Duration(inProgressIntervalMs) * time.Millisecond,
	)
	defer ticker.Stop()
	for {
		select {
		case entry := <-watcher.Updates():
			if entry == nil {
				continue
			}
			return entry.Value(), nil
		case <-timer.C:
			return nil, fmt.Errorf("timeout")
		case <-ticker.C:
			if err := b.extendClaim(taskID, msg); err != nil {
				return nil, fmt.Errorf("in-progress: %w", err)
			}
		case <-r.Context().Done():
			return nil, fmt.Errorf("client disconnect")
		}
	}
}
