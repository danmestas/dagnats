// worker/inputs.go
// Agent inputs and durable waits on a taskContext (ADR-027). A handler
// places the inputs queued for its step at a turn boundary (steers
// after a tool round, follow-ups after a final answer); each placement
// is one input.placed journal record, the truth about placement, and
// the submission's bucket status is a projection repaired from it. A
// Continue may ask the engine to wait, durably and with no worker
// claim held, for a delay, a deadline, or the next input.
package worker

import (
	"errors"
	"time"

	"github.com/danmestas/dagnats/inputs"
	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/protocol"
)

// Sentinel errors.
var (
	// ErrNoJournal: inputs are placed through the journal; the handler
	// must call Journal() first.
	ErrNoJournal = errors.New("worker: inputs need a journal; call Journal() first")
	// ErrInputsQueued: Continue(UntilInput()) was asked while inputs
	// are queued. The wait would never be woken for them (they arrived
	// before it), so the handler must place them and run another turn.
	// It is the same error value as inputs.ErrQueued, which the HTTP
	// bridge matches, so errors.Is works with either.
	ErrInputsQueued = inputs.ErrQueued
	// ErrInputsNotProvisioned: the submissions bucket is missing.
	ErrInputsNotProvisioned = inputs.ErrNotProvisioned
)

// ContinueOption asks the engine to wait before the next iteration
// (ADR-027). Options combine: a time and UntilInput together wake on
// whichever comes first; Until wins over Delay.
type ContinueOption func(*protocol.ContinueWait)

// Delay wakes the step after d.
func Delay(d time.Duration) ContinueOption {
	if d <= 0 {
		panic("worker.Delay: duration must be positive")
	}
	return func(w *protocol.ContinueWait) { w.DelayMs = d.Milliseconds() }
}

// Until wakes the step at t.
func Until(t time.Time) ContinueOption {
	if t.IsZero() {
		panic("worker.Until: time must not be zero")
	}
	utc := t.UTC()
	return func(w *protocol.ContinueWait) { w.Until = &utc }
}

// UntilInput wakes the step when an input is queued for it. Refused
// with ErrInputsQueued while any input is already queued.
func UntilInput() ContinueOption {
	return func(w *protocol.ContinueWait) { w.Input = true }
}

// continueWait resolves opts into the wait a Continue commits, nil
// when there is none, and refuses an input wait while inputs are
// queued (no lost wakeup, rule 1).
func (c *taskContext) continueWait(opts []ContinueOption) (*protocol.ContinueWait, error) {
	if c.runID == "" {
		panic("continueWait: runID must not be empty")
	}
	if len(opts) > 8 {
		panic("continueWait: too many options")
	}
	if len(opts) == 0 {
		return nil, nil
	}
	wait := &protocol.ContinueWait{}
	for _, opt := range opts {
		if opt == nil {
			panic("continueWait: option must not be nil")
		}
		opt(wait)
	}
	if wait.IsZero() {
		return nil, nil
	}
	if !wait.Input {
		return wait, nil
	}
	store, err := c.inputsStore()
	if err != nil {
		return nil, err
	}
	if err := store.RefuseIfQueued(c.ctx, c.runID, c.stepID); err != nil {
		return nil, err
	}
	return wait, nil
}

// inputsStore binds the submissions bucket once per delivery.
func (c *taskContext) inputsStore() (*inputs.Store, error) {
	if c.runID == "" {
		panic("inputsStore: runID must not be empty")
	}
	if c.inputs != nil {
		return c.inputs, nil
	}
	if c.js == nil {
		return nil, ErrInputsNotProvisioned
	}
	store, err := inputs.Open(c.ctx, c.js)
	if err != nil {
		return nil, err
	}
	c.inputs = store
	return store, nil
}

// PlaceInputs moves every queued input of mode into the conversation
// and returns them in submission order, after any input of mode an
// earlier delivery of this iteration already placed (see
// journal.Placer.Place, the shared implementation). Call it at a turn
// boundary: ModeSteer after a tool round, ModeFollowUp after a final
// answer. Needs a journal (ErrNoJournal). A fenced journal stops the
// task like any other journal write.
func (c *taskContext) PlaceInputs(mode inputs.Mode) ([]journal.InputPlaced, error) {
	if c.runID == "" {
		panic("PlaceInputs: runID must not be empty")
	}
	if mode != inputs.ModeSteer && mode != inputs.ModeFollowUp {
		panic("PlaceInputs: mode must be steer or follow_up")
	}
	if err := c.stopError(); err != nil {
		return nil, err
	}
	if c.journal == nil {
		return nil, ErrNoJournal
	}
	store, err := c.inputsStore()
	if err != nil {
		return nil, err
	}
	if c.placer == nil {
		c.placer = &journal.Placer{
			Journal: c.journal, Store: store, RunID: c.runID, StepID: c.stepID,
			Iteration: c.iteration,
			BeforeClaim: func() {
				if c.beforeClaim != nil {
					c.beforeClaim()
				}
			},
			Fault: func(requestID string) error {
				if c.placeFault != nil {
					return c.placeFault(requestID)
				}
				return nil
			},
		}
	}
	placed, err := c.placer.Place(c.ctx, mode)
	if errors.Is(err, journal.ErrFenced) {
		c.journalStop = journalFenced
	}
	return placed, err
}

// settlePlacedInputs moves the inputs this claim placed to done, once
// the iteration's commit is stored.
func (c *taskContext) settlePlacedInputs() {
	if c.runID == "" {
		panic("settlePlacedInputs: runID must not be empty")
	}
	if c.placer != nil {
		c.placer.Settle(c.ctx)
	}
}
