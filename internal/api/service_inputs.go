// internal/api/service_inputs.go
// Agent inputs on the control plane (ADR-027): submit an input to a
// running step (once per request_id), read its status (optionally
// waiting for it to change), withdraw it while queued. Submitting is a
// write into a running agent's context and carries the same
// authorization as signalling the run: both are served by the same
// REST and micro surfaces, so whatever fronts one fronts the other.
//
// The journal is the record of placement; the bucket's status is a
// projection, and a read repairs it: a queued or placed submission
// whose journal holds an input.placed record is placed, done once a
// commit follows the record, and a queued one on a finished step is
// unanswered.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/inputs"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
)

// Sentinel errors of the input endpoints; the transports map them to
// status codes.
var (
	// ErrStepNotFound: the run has no such step.
	ErrStepNotFound = errors.New("step not found")
	// ErrStepFinished: the step (or its run) has finished; no input can
	// reach it.
	ErrStepFinished = errors.New("step has finished")
)

// inputWaitMax bounds a status long-poll.
const inputWaitMax = 60 * time.Second

// InputRequest is the body of a submission.
type InputRequest struct {
	RequestID string          `json:"request_id"`
	Mode      inputs.Mode     `json:"mode"`
	Content   json.RawMessage `json:"content"`
}

// inputsStore binds the submissions bucket, caching only success so a
// bucket provisioned (or reachable) after the service started is
// picked up on the next request instead of never.
func (s *Service) inputsStore(ctx context.Context) (*inputs.Store, error) {
	if s.js == nil {
		panic("inputsStore: js must not be nil")
	}
	if ctx == nil {
		panic("inputsStore: ctx must not be nil")
	}
	s.inputsMu.Lock()
	defer s.inputsMu.Unlock()
	if s.inputs != nil {
		return s.inputs, nil
	}
	store, err := inputs.Open(ctx, s.js)
	if err != nil {
		return nil, err
	}
	s.inputs = store
	return store, nil
}

// SubmitInput creates the submission (once per request_id), announces
// it on the run's history, and settles it unanswered if the step
// finished meanwhile. created is false for a repeat.
func (s *Service) SubmitInput(
	ctx context.Context, runID, stepID string, req InputRequest,
) (inputs.Submission, bool, error) {
	if ctx == nil {
		panic("SubmitInput: ctx must not be nil")
	}
	if runID == "" || stepID == "" {
		panic("SubmitInput: run and step must not be empty")
	}
	var sub inputs.Submission
	created := false
	err := s.observed(ctx, "submitInput", []attribute.KeyValue{
		attribute.String("run_id", runID), attribute.String("step_id", stepID),
	}, func(ctx context.Context) error {
		var err error
		sub, created, err = s.submitInput(ctx, runID, stepID, req)
		return err
	})
	return sub, created, err
}

// submitInput is SubmitInput's body: pre-check, create, announce,
// re-check against the leader's copy of the run.
func (s *Service) submitInput(
	ctx context.Context, runID, stepID string, req InputRequest,
) (inputs.Submission, bool, error) {
	if runID == "" || stepID == "" {
		panic("submitInput: run and step must not be empty")
	}
	sub := inputs.Submission{
		RunID: runID, StepID: stepID, RequestID: req.RequestID, Mode: req.Mode,
		Content: req.Content,
	}
	if err := sub.Validate(); err != nil {
		return inputs.Submission{}, false, err
	}
	store, err := s.inputsStore(ctx)
	if err != nil {
		return inputs.Submission{}, false, err
	}
	if _, err := s.stepOpenForInput(ctx, runID, stepID, false); err != nil {
		return inputs.Submission{}, false, err
	}
	stored, created, err := store.Submit(ctx, sub)
	if err != nil || !created {
		return stored, created, err
	}
	evt := protocol.NewInputQueuedEvent(runID, stepID, req.RequestID)
	if err := s.publishHistoryEvent(ctx, evt); err != nil {
		return inputs.Submission{}, false, fmt.Errorf("announce input: %w", err)
	}
	// The step may have finished between the pre-check and the create,
	// after the engine's own sweep ran; the leader's copy says so.
	if _, err := s.stepOpenForInput(ctx, runID, stepID, true); errors.Is(err, ErrStepFinished) {
		if _, settleErr := store.SettleUnanswered(ctx, runID, stepID, nil); settleErr != nil {
			return inputs.Submission{}, false, settleErr
		}
		stored.Status = inputs.StatusUnanswered
	} else if err != nil {
		return inputs.Submission{}, false, err
	}
	return stored, true, nil
}

// stepOpenForInput loads the run (from the stream leader when leader
// is set) and reports whether its step can still take input.
func (s *Service) stepOpenForInput(
	ctx context.Context, runID, stepID string, leader bool,
) (dag.StepState, error) {
	if runID == "" || stepID == "" {
		panic("stepOpenForInput: run and step must not be empty")
	}
	if s.store == nil {
		panic("stepOpenForInput: store must not be nil")
	}
	var run dag.WorkflowRun
	var err error
	if leader {
		run, err = s.loadRunFromLeader(ctx, runID)
	} else {
		run, err = s.store.Load(ctx, runID)
	}
	if err != nil {
		return dag.StepState{}, err
	}
	state, ok := run.Steps[stepID]
	if !ok {
		return dag.StepState{}, ErrStepNotFound
	}
	if run.Status.IsTerminal() || stepFinished(state) {
		return state, ErrStepFinished
	}
	return state, nil
}

// loadRunFromLeader reads the run snapshot through the stream leader,
// so a replica that lags the engine's terminal save cannot answer.
func (s *Service) loadRunFromLeader(ctx context.Context, runID string) (dag.WorkflowRun, error) {
	if s.nc == nil {
		panic("loadRunFromLeader: nc must not be nil")
	}
	if runID == "" {
		panic("loadRunFromLeader: runID must not be empty")
	}
	entry, err := natsutil.KVGetLeader(ctx, s.nc, "workflow_runs", "run."+runID)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return dag.WorkflowRun{}, engine.ErrRunNotFound
		}
		return dag.WorkflowRun{}, fmt.Errorf("load run %s: %w", runID, err)
	}
	var run dag.WorkflowRun
	if err := json.Unmarshal(entry.Value, &run); err != nil {
		return dag.WorkflowRun{}, fmt.Errorf("decode run %s: %w", runID, err)
	}
	return run, nil
}

// stepFinished reports a step no input can reach.
func stepFinished(state dag.StepState) bool {
	switch state.Status {
	case dag.StepStatusCompleted, dag.StepStatusFailed, dag.StepStatusCancelled:
		return true
	}
	return false
}

// GetInput reads a submission, waiting up to wait (at most a minute)
// for its status to leave after when wait is positive, then repairs
// the projection from the journal and the step's state.
func (s *Service) GetInput(
	ctx context.Context, runID, stepID, requestID string, after inputs.Status, wait time.Duration,
) (inputs.Submission, error) {
	if ctx == nil {
		panic("GetInput: ctx must not be nil")
	}
	if runID == "" || stepID == "" {
		panic("GetInput: run and step must not be empty")
	}
	var sub inputs.Submission
	err := s.observed(ctx, "getInput", []attribute.KeyValue{
		attribute.String("run_id", runID), attribute.String("step_id", stepID),
	}, func(ctx context.Context) error {
		store, err := s.inputsStore(ctx)
		if err != nil {
			return err
		}
		var entry inputs.Entry
		if wait > 0 {
			entry, err = store.Await(ctx, runID, stepID, requestID, after, min(wait, inputWaitMax))
		} else {
			entry, err = store.Get(ctx, runID, stepID, requestID)
		}
		if err != nil {
			return err
		}
		sub, err = s.repairInput(ctx, store, entry)
		return err
	})
	return sub, err
}

// repairInput reconciles a queued or placed projection with the
// journal and the step: the journal's record wins over the bucket, and
// a finished step leaves nothing queued.
func (s *Service) repairInput(
	ctx context.Context, store *inputs.Store, entry inputs.Entry,
) (inputs.Submission, error) {
	if store == nil {
		panic("repairInput: store must not be nil")
	}
	if entry.Revision == 0 {
		panic("repairInput: entry must have been read from the store")
	}
	if entry.Status != inputs.StatusQueued && entry.Status != inputs.StatusPlaced {
		return entry.Submission, nil
	}
	want, iteration := entry.Status, entry.Iteration
	if placed, ok := s.journalPlacement(ctx, entry); ok {
		want, iteration = placed.status, placed.iteration
	} else if entry.Status == inputs.StatusQueued {
		_, err := s.stepOpenForInput(ctx, entry.RunID, entry.StepID, false)
		if errors.Is(err, ErrStepFinished) {
			want = inputs.StatusUnanswered
		}
	}
	if want == entry.Status {
		return entry.Submission, nil
	}
	repaired, err := store.Transition(ctx, entry, want, iteration)
	if errors.Is(err, inputs.ErrConflict) {
		return entry.Submission, nil // another repair or the worker got there first
	}
	if err != nil {
		return inputs.Submission{}, err
	}
	slog.InfoContext(ctx, "repaired input status from the journal",
		"run_id", entry.RunID, "step_id", entry.StepID,
		"request_id", entry.RequestID, "from", string(entry.Status), "to", string(want))
	return repaired.Submission, nil
}

// placement is what the journal says about one request.
type placement struct {
	status    inputs.Status
	iteration int
}

// journalPlacement reads the step's journal for the request's
// input.placed record: placed, or done once a commit follows it. ok is
// false when the journal has no record (or cannot be read: the bucket
// is then left as it is).
func (s *Service) journalPlacement(ctx context.Context, entry inputs.Entry) (placement, bool) {
	if s.js == nil {
		panic("journalPlacement: js must not be nil")
	}
	if entry.RequestID == "" {
		panic("journalPlacement: entry must name its request")
	}
	records, err := journal.Read(ctx, s.js, entry.RunID, entry.StepID)
	if err != nil {
		if !errors.Is(err, journal.ErrNotProvisioned) {
			slog.WarnContext(ctx, "read journal to repair input status", "error", err)
		}
		return placement{}, false
	}
	var found placement
	seen := false
	for _, r := range records {
		switch {
		case !seen && r.Type == journal.TypeInputPlaced:
			var p journal.InputPlaced
			if json.Unmarshal(r.Data, &p) == nil && p.RequestID == entry.RequestID {
				found, seen = placement{status: inputs.StatusPlaced, iteration: r.Iteration}, true
			}
		case seen && r.Type == journal.TypeCommit:
			found.status = inputs.StatusDone
			return found, true
		}
	}
	return found, seen
}

// WithdrawInput deletes a submission that is still queued.
func (s *Service) WithdrawInput(ctx context.Context, runID, stepID, requestID string) error {
	if ctx == nil {
		panic("WithdrawInput: ctx must not be nil")
	}
	if runID == "" || stepID == "" {
		panic("WithdrawInput: run and step must not be empty")
	}
	return s.observed(ctx, "withdrawInput", []attribute.KeyValue{
		attribute.String("run_id", runID), attribute.String("step_id", stepID),
	}, func(ctx context.Context) error {
		store, err := s.inputsStore(ctx)
		if err != nil {
			return err
		}
		return store.Withdraw(ctx, runID, stepID, requestID)
	})
}

// publishHistoryEvent publishes evt on the run's history under its
// stable msg-id.
func (s *Service) publishHistoryEvent(ctx context.Context, evt protocol.Event) error {
	if s.tp == nil {
		panic("publishHistoryEvent: tp must not be nil")
	}
	if evt.RunID == "" {
		panic("publishHistoryEvent: RunID must not be empty")
	}
	_, err := s.tp.JSPublishMsgEvent(ctx, &nats.Msg{
		Subject: evt.NATSSubject(),
		Header:  nats.Header{"Nats-Msg-Id": {evt.NATSMsgID()}},
	}, &evt)
	return err
}
