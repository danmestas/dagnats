package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/inputs"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
	"github.com/nats-io/nats.go"
)

// A relayed worker (a program's, WithEngineRelay) reaches its step's
// checkpoint, signals and agent inputs only through the engine (#885);
// these are the task context's paths for that.

// signalPollInterval paces a relayed WaitForSignal: a program cannot
// watch the signals bucket, so it asks the engine. Bounded by the wait's
// own timeout (at most an hour).
const signalPollInterval = 250 * time.Millisecond

func (c *taskContext) relayed() bool { return c.tp != nil && c.tp.Relayed() }

// stepHeader names this task's step and dispatch for a relay call.
func (c *taskContext) stepHeader(op string) nats.Header {
	if c.runID == "" || c.stepID == "" {
		panic("stepHeader: run and step must be set")
	}
	header := nats.Header{}
	header.Set(natsutil.RelayOpHeader, op)
	header.Set(natsutil.RelayRunHeader, c.runID)
	header.Set(natsutil.RelayStepHeader, c.stepID)
	header.Set(journal.HeaderNonce, c.dispatchNonce)
	return header
}

// relayCheckpoint runs one checkpoint op; get reports found.
func (c *taskContext) relayCheckpoint(op string, data []byte) ([]byte, bool, error) {
	if !c.relayed() {
		panic("relayCheckpoint: worker is not relayed")
	}
	reply, err := c.tp.RelayCall(c.ctx, natsutil.RelayCheckpointOp, c.stepHeader(op), data)
	if err != nil {
		return nil, false, err
	}
	if reply.Header.Get(natsutil.RelayEmptyHeader) == "true" {
		return nil, false, nil
	}
	return reply.Data, true, nil
}

// relaySignal sends a signal, or reads one (found false when unset).
func (c *taskContext) relaySignal(
	ctx context.Context, op, runID, name string, data []byte,
) ([]byte, bool, error) {
	if runID == "" || name == "" {
		panic("relaySignal: run and name must be set")
	}
	// A read carries this task's own dispatch (the engine serves signal
	// reads only for it); a send names just the target run.
	header := c.stepHeader(op)
	header.Set(natsutil.RelayRunHeader, runID)
	header.Set(natsutil.RelayKeyHeader, name)
	reply, err := c.tp.RelayCall(ctx, natsutil.RelaySignalOp, header, data)
	if err != nil {
		return nil, false, err
	}
	if reply.Header.Get(natsutil.RelayEmptyHeader) == "true" {
		return nil, false, nil
	}
	return reply.Data, true, nil
}

// relayWaitForSignal polls the engine for this run's signal until it is
// set or timeout passes.
func (c *taskContext) relayWaitForSignal(name string, timeout time.Duration) ([]byte, error) {
	if name == "" || timeout <= 0 {
		panic("relayWaitForSignal: name and timeout must be set")
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(signalPollInterval)
	defer tick.Stop()
	for {
		value, found, err := c.relaySignal(c.ctx, "get", c.runID, name, nil)
		if err != nil {
			return nil, err
		}
		if found {
			return value, nil
		}
		select {
		case <-c.ctx.Done():
			return nil, c.ctx.Err()
		case <-deadline.C:
			return nil, fmt.Errorf("signal %q timed out after %s", name, timeout)
		case <-tick.C:
		}
	}
}

// relayInputs is the inputs store a relayed worker places through: the
// same four operations inputplace.Placer uses, served by the engine for
// this task's own step and dispatch.
type relayInputs struct {
	c *taskContext
}

type relayInputsBody struct {
	Entry     inputs.Submission `json:"entry"`
	Revision  uint64            `json:"revision"`
	RequestID string            `json:"request_id,omitempty"`
	Status    inputs.Status     `json:"status,omitempty"`
	Iteration int               `json:"iteration"`
}

func (r relayInputs) call(
	ctx context.Context, op string, body relayInputsBody,
) ([]inputs.Entry, error) {
	if r.c == nil || !r.c.relayed() {
		panic("relayInputs: worker is not relayed")
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	reply, err := r.c.tp.RelayCall(ctx, natsutil.RelayInputsOp, r.c.stepHeader(op), data)
	if err != nil {
		return nil, inputsOutcome(err)
	}
	var decoded struct {
		Entries []relayInputsBody `json:"entries"`
	}
	if err := json.Unmarshal(reply.Data, &decoded); err != nil {
		return nil, fmt.Errorf("inputs relay reply: %w", err)
	}
	out := make([]inputs.Entry, len(decoded.Entries))
	for i, e := range decoded.Entries {
		out[i] = inputs.Entry{Submission: e.Entry, Revision: e.Revision}
	}
	return out, nil
}

func (r relayInputs) one(entries []inputs.Entry, err error) (inputs.Entry, error) {
	if err != nil {
		return inputs.Entry{}, err
	}
	if len(entries) != 1 {
		return inputs.Entry{}, fmt.Errorf("inputs relay: %d entries, want 1", len(entries))
	}
	return entries[0], nil
}

func (r relayInputs) Get(
	ctx context.Context, runID, stepID, requestID string,
) (inputs.Entry, error) {
	r.mustBeOwnStep(runID, stepID)
	return r.one(r.call(ctx, "get", relayInputsBody{RequestID: requestID}))
}

func (r relayInputs) List(ctx context.Context, runID, stepID string) ([]inputs.Entry, error) {
	r.mustBeOwnStep(runID, stepID)
	return r.call(ctx, "list", relayInputsBody{})
}

func (r relayInputs) Transition(
	ctx context.Context, entry inputs.Entry, status inputs.Status, iteration int,
) (inputs.Entry, error) {
	return r.one(r.call(ctx, "transition", relayInputsBody{
		Entry: entry.Submission, Revision: entry.Revision, Status: status, Iteration: iteration,
	}))
}

func (r relayInputs) Claim(
	ctx context.Context, entry inputs.Entry, iteration int,
) (inputs.Entry, error) {
	return r.one(r.call(ctx, "claim", relayInputsBody{
		Entry: entry.Submission, Revision: entry.Revision, Iteration: iteration,
	}))
}

// inputsOutcome turns the relay's inputs refusal codes back into the
// inputs errors the placer branches on, so a lost claim over the relay is
// inputs.ErrConflict exactly as it is against the store.
func inputsOutcome(err error) error {
	var refusal *natsutil.RelayRefusal
	if !errors.As(err, &refusal) {
		return err
	}
	switch refusal.Code {
	case natsutil.RefusalInputsConflict:
		return fmt.Errorf("%w: %s", inputs.ErrConflict, refusal.Detail)
	case natsutil.RefusalInputsNotFound:
		return fmt.Errorf("%w: %s", inputs.ErrNotFound, refusal.Detail)
	case natsutil.RefusalInputsNotQueued:
		return fmt.Errorf("%w: %s", inputs.ErrNotQueued, refusal.Detail)
	}
	return err
}

// mustBeOwnStep asserts the placer asks about this task's step, the
// only one the relay serves.
func (r relayInputs) mustBeOwnStep(runID, stepID string) {
	if runID != r.c.runID || stepID != r.c.stepID {
		panic("relayInputs: the placer asked about another step: " +
			runID + "." + stepID + " (this task is " + r.c.runID + "." + r.c.stepID + ")")
	}
}

// relayStreamSubject is PutStream's subject for a relayed worker: its
// program's namespace, the only stream subjects its NATS user may
// publish.
func (c *taskContext) relayStreamSubject() string {
	return "stream.p." + c.tp.RelayProgram() + "." + c.runID + "." + c.stepID
}
