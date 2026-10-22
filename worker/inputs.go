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
	"fmt"
	"log/slog"
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
	ErrInputsQueued = errors.New(
		"worker: inputs are queued; place them before waiting for input")
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
	queued, err := store.Queued(c.ctx, c.runID, c.stepID)
	if err != nil {
		return nil, err
	}
	if len(queued) > 0 {
		return nil, fmt.Errorf("%w (%d queued, first %s)",
			ErrInputsQueued, len(queued), queued[0].RequestID)
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
// and returns them in submission order. Call it at a turn boundary:
// ModeSteer after a tool round, ModeFollowUp after a final answer.
// Each input is placed at most once across any crash or redelivery:
// one the journal already holds (a previous delivery placed it and
// died before projecting the status) is skipped and its status
// repaired. Needs a journal (ErrNoJournal). A fenced journal stops the
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
	queued, err := store.Queued(c.ctx, c.runID, c.stepID)
	if err != nil {
		return nil, err
	}
	var placed []journal.InputPlaced
	for _, entry := range queued {
		if entry.Mode != mode {
			continue
		}
		if c.journal.InputPlaced(entry.RequestID) {
			c.projectInput(store, entry, inputs.StatusPlaced)
			continue
		}
		record := journal.InputPlaced{
			RequestID: entry.RequestID, Mode: string(entry.Mode), Content: entry.Content,
		}
		if _, err := c.journal.PlaceInput(c.ctx, record); err != nil {
			if errors.Is(err, journal.ErrFenced) {
				c.journalStop = journalFenced
			}
			return placed, err
		}
		record.Iteration = c.iteration
		placed = append(placed, record)
		c.projectInput(store, entry, inputs.StatusPlaced)
		c.placedEntries = append(c.placedEntries, entry)
	}
	if len(placed) > len(queued) {
		panic("PlaceInputs: placed more than were queued")
	}
	return placed, nil
}

// projectInput writes a submission's status after the journal has
// recorded the fact. Best effort and journal-wins: a compare-and-swap
// that loses (a withdrawal or another projection raced it) is
// overwritten, and a failure is logged, because the next PlaceInputs or
// status read repairs the projection from the journal.
func (c *taskContext) projectInput(store *inputs.Store, entry inputs.Entry, to inputs.Status) {
	if store == nil {
		panic("projectInput: store must not be nil")
	}
	if entry.RequestID == "" {
		panic("projectInput: entry must name its request")
	}
	_, err := store.Transition(c.ctx, entry, to, c.iteration)
	if errors.Is(err, inputs.ErrConflict) || errors.Is(err, inputs.ErrNotFound) {
		err = store.Project(c.ctx, entry.Submission, to, c.iteration)
	}
	if err != nil {
		slog.WarnContext(c.ctx, "project input status; the journal is the record",
			"run_id", c.runID, "step_id", c.stepID,
			"request_id", entry.RequestID, "status", string(to), "error", err)
	}
}

// settlePlacedInputs moves the inputs this claim placed to done, once
// the iteration's commit is stored. Best effort like projectInput.
func (c *taskContext) settlePlacedInputs() {
	if c.runID == "" {
		panic("settlePlacedInputs: runID must not be empty")
	}
	if len(c.placedEntries) > 0 && c.inputs == nil {
		panic("settlePlacedInputs: placements need the store they came from")
	}
	for _, entry := range c.placedEntries {
		current, err := c.inputs.Get(c.ctx, c.runID, c.stepID, entry.RequestID)
		if err != nil {
			current = entry
		}
		c.projectInput(c.inputs, current, inputs.StatusDone)
	}
	c.placedEntries = nil
}
