// bridge/journal.go
// The agent journal over the HTTP bridge (ADR-026). A non-Go worker
// claims a task's journal with its first journal.append or
// journal.read, and from then on its complete, fail, continue and
// pause commit through the same journal package a Go worker uses, so
// the fencing is the package's and not a copy of it. The claim carries
// the delivery count of the polled message: a redelivery to a second
// poller claims with a higher count and fences this one.
//
// Outcomes the worker must handle, mirrored from the Go SDK's errors:
//
//	409 {"code":"fenced"}     another claim follows this worker's. The
//	                          task is the successor's: stop, write and
//	                          resolve nothing more.
//	410 {"code":"stale"}      a later iteration holds the step. The task
//	                          was acknowledged; stop.
//	410 {"code":"committed"}  this attempt already committed. The bridge
//	                          republished its lifecycle event and
//	                          acknowledged the task; stop.
//	503 {"code":"not_provisioned"}  the AGENT_JOURNAL stream is missing.
package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

// journalErrorBody is the JSON body of a journal outcome.
type journalErrorBody struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

// journalHTTPStatus maps a journal outcome to its HTTP status and
// code; ok is false for an error that is not one.
func journalHTTPStatus(err error) (status int, code string, ok bool) {
	if err == nil {
		panic("journalHTTPStatus: err must not be nil")
	}
	switch {
	case errors.Is(err, journal.ErrFenced):
		return http.StatusConflict, "fenced", true
	case errors.Is(err, journal.ErrStale):
		return http.StatusGone, "stale", true
	case errors.Is(err, journal.ErrCommitted):
		return http.StatusGone, "committed", true
	case errors.Is(err, journal.ErrNotProvisioned):
		return http.StatusServiceUnavailable, "not_provisioned", true
	}
	return 0, "", false
}

// writeJournalOutcome writes err as a journal outcome and reports
// whether it was one.
func writeJournalOutcome(w http.ResponseWriter, err error) bool {
	if w == nil {
		panic("writeJournalOutcome: w must not be nil")
	}
	status, code, ok := journalHTTPStatus(err)
	if !ok {
		return false
	}
	writeJSON(w, status, journalErrorBody{Error: err.Error(), Code: code})
	return true
}

// writeJSON writes v as the JSON response body with status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	if w == nil {
		panic("writeJSON: w must not be nil")
	}
	data, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "marshal response: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(data); err != nil {
		panic(fmt.Sprintf("writeJSON: %v", err))
	}
}

// journalTask is the claim identity of the polled delivery: the
// dispatch identity plus the delivery count, as the Go SDK builds it.
func journalTask(taskID string, msg jetstream.Msg) (journal.Task, error) {
	if taskID == "" {
		panic("journalTask: taskID must not be empty")
	}
	dispatch, err := resolveDispatch(msg)
	if err != nil {
		return journal.Task{}, err
	}
	delivered := dispatch.attempt
	var streamSeq uint64
	if meta, merr := msg.Metadata(); merr == nil && meta != nil {
		streamSeq = meta.Sequence.Stream
		if meta.NumDelivered >= 1 {
			delivered = int(meta.NumDelivered)
		}
	}
	if delivered < 1 {
		panic("journalTask: delivery count must be >= 1")
	}
	runID, stepID := splitTaskID(taskID)
	return journal.Task{
		RunID: runID, StepID: stepID, Iteration: dispatch.iteration,
		Attempt: dispatch.attempt, DispatchNonce: dispatch.nonce, Delivered: delivered,
		StreamSeq: streamSeq,
	}, nil
}

// openJournal returns the task's journal claim, claiming on first use.
// The caller holds the task's journal lock. Stale, fenced and
// committed outcomes settle the task here (see settleJournalStop) and
// come back as the journal package's errors.
func (b *Bridge) openJournal(
	ctx context.Context, taskID, tokenID string, msg jetstream.Msg,
) (*journal.Journal, error) {
	if ctx == nil {
		panic("openJournal: ctx must not be nil")
	}
	if existing, ok := b.ackMap.Journal(taskID); ok {
		return existing, nil
	}
	task, err := journalTask(taskID, msg)
	if err != nil {
		return nil, err
	}
	j, err := journal.Open(ctx, b.js, b.pub, task)
	if err != nil {
		return nil, b.settleJournalStop(ctx, taskID, tokenID, msg, err)
	}
	if _, ok := j.Committed(); ok {
		return nil, b.finishCommitted(ctx, taskID, tokenID, msg, j)
	}
	if !b.ackMap.SetJournal(taskID, j) {
		return nil, fmt.Errorf("task %q is no longer claimed", taskID)
	}
	return j, nil
}

// settleJournalStop ends the bridge's side of a task the journal says
// may not proceed, and returns err. A stale or already-committed task
// is acknowledged and forgotten (nothing is left to do for it); a
// fenced one is only marked, since its message belongs to the
// successor's delivery.
func (b *Bridge) settleJournalStop(
	ctx context.Context, taskID, tokenID string, msg jetstream.Msg, err error,
) error {
	if err == nil {
		panic("settleJournalStop: err must not be nil")
	}
	switch {
	case errors.Is(err, journal.ErrFenced):
		// The message is the successor's: no ack, and the entry stays
		// so the fence answers every later action (handleResolve).
		b.emitLogMarker(ctx, taskID, msg, protocol.LogMarkerFenced)
		b.ackMap.SetJournalStop(taskID, journal.ErrFenced)
		return err
	case errors.Is(err, journal.ErrStale), errors.Is(err, journal.ErrCommitted):
		b.emitLogMarker(ctx, taskID, msg, protocol.LogMarkerCompleted)
		if aerr := msg.Ack(); aerr != nil {
			return fmt.Errorf("%w (ack failed: %v)", err, aerr)
		}
	default:
		return err
	}
	b.ackMap.Delete(taskID)
	b.ackMap.MarkResolved(taskID, tokenID)
	return err
}

// republishCommitted publishes the lifecycle event of a commit a
// previous holder of this attempt stored but never announced. The
// event's msg-id is the stable one, so a publish that did land is
// deduplicated.
func (b *Bridge) republishCommitted(
	ctx context.Context, taskID string, msg jetstream.Msg,
	task journal.Task, commit journal.Commit, seq uint64,
) error {
	if seq == 0 {
		panic("republishCommitted: seq must be positive")
	}
	var evtType protocol.EventType
	var payload []byte
	var err error
	switch commit.Action {
	case journal.ActionContinue:
		evtType = protocol.EventStepContinue
		payload, err = json.Marshal(protocol.StepContinuePayload{Output: commit.Output})
	case journal.ActionComplete:
		evtType, payload = protocol.EventStepCompleted, commit.Output
	case journal.ActionFail:
		evtType = protocol.EventStepFailed
		payload, err = json.Marshal(protocol.StepFailedPayload{
			Error: commit.Error, FailureType: protocol.FailureType(commit.FailureType),
			RetryAfterMs: commit.RetryAfterMs,
		})
	default:
		return fmt.Errorf("journal: commit %d has unknown action %q", seq, commit.Action)
	}
	if err != nil {
		return fmt.Errorf("marshal republished %s: %w", commit.Action, err)
	}
	evt := protocol.NewStepEvent(evtType, task.RunID, task.StepID, payload)
	evt.SetIdentity(task.Iteration, task.Attempt, task.DispatchNonce)
	evt.JournalSeq = seq
	if err := b.publishEvent(ctx, evt); err != nil {
		return fmt.Errorf("republish committed %s: %w", commit.Action, err)
	}
	return nil
}

// commitJournal writes the iteration's commit before a lifecycle event
// when the task has a journal claim, and returns the commit's sequence
// (zero without a journal). A fenced commit settles the task and the
// caller must publish nothing.
func (b *Bridge) commitJournal(
	ctx context.Context, taskID, tokenID string, msg jetstream.Msg, commit journal.Commit,
) (uint64, error) {
	if commit.Action == "" {
		panic("commitJournal: action must not be empty")
	}
	mu := b.ackMap.JournalLock(taskID)
	if mu == nil {
		return 0, nil
	}
	mu.Lock()
	defer mu.Unlock()
	j, ok := b.ackMap.Journal(taskID)
	if !ok {
		return 0, nil
	}
	seq, err := j.Commit(ctx, commit)
	switch {
	case err == nil:
		return seq, nil
	case errors.Is(err, journal.ErrFenced):
		return 0, b.settleJournalStop(ctx, taskID, tokenID, msg, err)
	case errors.Is(err, journal.ErrCommitted):
		return 0, b.finishCommitted(ctx, taskID, tokenID, msg, j)
	}
	return 0, fmt.Errorf("journal commit: %w", err)
}

// finishCommitted answers a resolve retried after its commit landed
// but its publish did not: the commit's event is republished (the
// stable msg-id dedups one that did land), then the task is settled, so
// the "committed" outcome the worker sees is true.
func (b *Bridge) finishCommitted(
	ctx context.Context, taskID, tokenID string, msg jetstream.Msg, j *journal.Journal,
) error {
	commit, ok := j.Committed()
	if !ok {
		panic("finishCommitted: journal reports committed without a commit")
	}
	seq := j.CommittedSeq()
	if err := b.republishCommitted(ctx, taskID, msg, j.Task(), commit, seq); err != nil {
		return err
	}
	return b.settleJournalStop(ctx, taskID, tokenID, msg, journal.ErrCommitted)
}

// withJournal runs fn on the task's journal claim under its lock,
// claiming first when the task has none. Journal outcomes are written
// to w; the caller returns errResponseAlreadyWritten.
func (b *Bridge) withJournal(
	ctx context.Context, taskID, tokenID string, msg jetstream.Msg, w http.ResponseWriter,
	fn func(j *journal.Journal) (any, error),
) error {
	if fn == nil {
		panic("withJournal: fn must not be nil")
	}
	mu := b.ackMap.JournalLock(taskID)
	if mu == nil {
		return fmt.Errorf("task %q is no longer claimed", taskID)
	}
	mu.Lock()
	defer mu.Unlock()
	j, err := b.openJournal(ctx, taskID, tokenID, msg)
	var body any
	if err == nil {
		body, err = fn(j)
		if errors.Is(err, journal.ErrFenced) {
			err = b.settleJournalStop(ctx, taskID, tokenID, msg, err)
		}
	}
	if err == nil {
		err = b.extendClaim(taskID, msg)
	}
	if err != nil {
		if writeJournalOutcome(w, err) {
			return errResponseAlreadyWritten
		}
		return err
	}
	writeJSON(w, http.StatusOK, body)
	return errResponseAlreadyWritten
}

// resolveJournalAppend appends a handler record to the journal.
func (b *Bridge) resolveJournalAppend(
	ctx context.Context, taskID, tokenID string, msg jetstream.Msg,
	req resolveRequest, w http.ResponseWriter,
) error {
	if taskID == "" {
		panic("resolveJournalAppend: taskID must not be empty")
	}
	recordType := req.Type
	if recordType == "" {
		recordType = journal.TypeEntry
	}
	if recordType == journal.TypeClaim || recordType == journal.TypeCommit {
		return errBadRequest("type %q is written by the bridge, not the worker", recordType)
	}
	if len(req.Data) == 0 {
		return errBadRequest("data is required")
	}
	return b.withJournal(ctx, taskID, tokenID, msg, w,
		func(j *journal.Journal) (any, error) {
			seq, err := j.Append(ctx, recordType, req.Data)
			if err != nil {
				return nil, err
			}
			return map[string]uint64{"seq": seq}, nil
		})
}

// resolveJournalRead claims the journal if needed and returns the
// step's records.
func (b *Bridge) resolveJournalRead(
	ctx context.Context, taskID, tokenID string, msg jetstream.Msg, w http.ResponseWriter,
) error {
	if taskID == "" {
		panic("resolveJournalRead: taskID must not be empty")
	}
	runID, stepID := splitTaskID(taskID)
	return b.withJournal(ctx, taskID, tokenID, msg, w,
		func(j *journal.Journal) (any, error) {
			records, err := journal.Read(ctx, b.js, runID, stepID)
			if err != nil {
				return nil, err
			}
			return map[string]any{"records": records}, nil
		})
}

// loadCheckpointResponse is the body of a load_checkpoint reply.
// Source names where State came from: "journal", "checkpoint" (the
// checkpoints bucket) or "payload" (the state the task was polled
// with). State is the raw JSON; StateBase64 carries state that is
// not JSON.
type loadCheckpointResponse struct {
	Source      string          `json:"source"`
	State       json.RawMessage `json:"state"`
	StateBase64 []byte          `json:"state_base64,omitempty"`
}

// resolveLoadCheckpoint returns the task's state the way the Go SDK's
// LoadCheckpoint does: state buffered by this claim's checkpoint
// action, else the journal's when the task has claimed one, else the
// checkpoints bucket, else the state the task was polled with.
func (b *Bridge) resolveLoadCheckpoint(
	ctx context.Context, taskID string, msg jetstream.Msg, w http.ResponseWriter,
) error {
	if taskID == "" {
		panic("resolveLoadCheckpoint: taskID must not be empty")
	}
	state, source, err := b.loadState(ctx, taskID, msg)
	if err != nil {
		return err
	}
	resp := loadCheckpointResponse{Source: source, State: json.RawMessage("null")}
	switch {
	case len(state) == 0:
	case json.Valid(state):
		resp.State = state
	default:
		resp.StateBase64 = state
	}
	writeJSON(w, http.StatusOK, resp)
	if err := b.extendClaim(taskID, msg); err != nil {
		return fmt.Errorf("in-progress: %w", err)
	}
	return errResponseAlreadyWritten
}

// loadState resolves the state and its source for resolveLoadCheckpoint.
func (b *Bridge) loadState(
	ctx context.Context, taskID string, msg jetstream.Msg,
) ([]byte, string, error) {
	if pending, ok := b.ackMap.PendingState(taskID); ok {
		return pending, "checkpoint", nil
	}
	if mu := b.ackMap.JournalLock(taskID); mu != nil {
		mu.Lock()
		j, ok := b.ackMap.Journal(taskID)
		mu.Unlock()
		if ok {
			state, err := journalState(j)
			return state, "journal", err
		}
	}
	if b.checkpointKV != nil {
		entry, err := b.checkpointKV.Get(ctx, taskID)
		if err == nil {
			return entry.Value(), "checkpoint", nil
		}
		if !errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, "", fmt.Errorf("read checkpoint: %w", err)
		}
	}
	dispatch, err := resolveDispatch(msg)
	if err != nil {
		return nil, "", err
	}
	return dispatch.state, "payload", nil
}

// journalState is the state a journal step reports: the pause marker
// when this delivery resumes a pause, else the last committed state.
func journalState(j *journal.Journal) ([]byte, error) {
	if j == nil {
		panic("journalState: journal must not be nil")
	}
	if name, ok := j.Resume(); ok {
		return json.Marshal(map[string]any{"pause_resume": name})
	}
	return j.State(), nil
}

// badRequestError marks an error as the caller's, for a 400.
type badRequestError struct{ msg string }

func (e *badRequestError) Error() string { return e.msg }

func errBadRequest(format string, args ...any) error {
	return &badRequestError{msg: fmt.Sprintf(format, args...)}
}
