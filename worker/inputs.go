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
// Needs a journal (ErrNoJournal). A fenced journal stops the task like
// any other journal write.
//
// Each input is claimed before it is recorded: a compare-and-swap of
// its status to placed at the revision it was read at, then the
// input.placed append. A withdrawal that lands in between wins the
// swap, so a 204 never precedes a placement (ADR-027 R7). A claim
// whose writer died before appending leaves placed with no record;
// only a claim of this step writes that state, so a later delivery
// reclaims it, and the journal (which knows every record) is what
// keeps any input from being placed twice. An input whose record
// cannot be appended for a reason other than fencing is settled
// unanswered rather than blocking every later turn.
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
	candidates, err := c.placeable(store, mode)
	if err != nil {
		return nil, err
	}
	if c.beforeClaim != nil {
		c.beforeClaim()
	}
	var placed []journal.InputPlaced
	for _, entry := range candidates {
		claimed, err := c.claimInput(store, entry)
		if errors.Is(err, inputs.ErrConflict) || errors.Is(err, inputs.ErrNotFound) {
			continue // withdrawn, or placed by a rival this journal already knows
		}
		if err != nil {
			return placed, err
		}
		record, err := c.recordPlacement(store, claimed)
		if err != nil {
			return placed, err
		}
		if record != nil {
			placed = append(placed, *record)
		}
	}
	if len(placed) > len(candidates) {
		panic("PlaceInputs: placed more than were claimable")
	}
	return placed, nil
}

// claimInput claims entry, re-reading it once when the claim is lost:
// a stale holder that claimed it between this read and this claim will
// be fenced on its append, which would leave the input placed with no
// record and nothing to reclaim it, so a placed input this journal has
// no record of is claimed again, as a dead claim is on redelivery.
func (c *taskContext) claimInput(store *inputs.Store, entry inputs.Entry) (inputs.Entry, error) {
	if store == nil {
		panic("claimInput: store must not be nil")
	}
	if c.journal == nil {
		panic("claimInput: journal must be open")
	}
	claimed, err := store.Claim(c.ctx, entry, c.iteration)
	if !errors.Is(err, inputs.ErrConflict) {
		return claimed, err
	}
	current, err := store.Get(c.ctx, c.runID, c.stepID, entry.RequestID)
	if err != nil {
		return inputs.Entry{}, err
	}
	if current.Status != inputs.StatusPlaced || c.journal.InputPlaced(entry.RequestID) {
		return inputs.Entry{}, inputs.ErrConflict
	}
	return store.Claim(c.ctx, current, c.iteration)
}

// placeable lists the step's inputs of mode this claim may place:
// queued ones, and placed ones the journal has no record of (a dead
// claim). One the journal already holds is never a candidate.
func (c *taskContext) placeable(store *inputs.Store, mode inputs.Mode) ([]inputs.Entry, error) {
	if store == nil {
		panic("placeable: store must not be nil")
	}
	if c.journal == nil {
		panic("placeable: journal must be open")
	}
	all, err := store.List(c.ctx, c.runID, c.stepID)
	if err != nil {
		return nil, err
	}
	candidates := all[:0]
	for _, entry := range all {
		if entry.Mode != mode || c.journal.InputPlaced(entry.RequestID) {
			continue
		}
		if entry.Status == inputs.StatusQueued || entry.Status == inputs.StatusPlaced {
			candidates = append(candidates, entry)
		}
	}
	return candidates, nil
}

// recordPlacement appends the input.placed record for a claimed input.
// nil without error means the input was settled unanswered instead.
func (c *taskContext) recordPlacement(
	store *inputs.Store, claimed inputs.Entry,
) (*journal.InputPlaced, error) {
	if claimed.Status != inputs.StatusPlaced {
		panic("recordPlacement: input must be claimed")
	}
	if c.journal == nil {
		panic("recordPlacement: journal must be open")
	}
	record := journal.InputPlaced{
		RequestID: claimed.RequestID, Mode: string(claimed.Mode), Content: claimed.Content,
	}
	err := c.placeFaultOrAppend(record)
	switch {
	case err == nil:
		record.Iteration = c.iteration
		c.placedEntries = append(c.placedEntries, claimed)
		return &record, nil
	case errors.Is(err, journal.ErrFenced):
		c.journalStop = journalFenced
		return nil, err
	case errors.Is(err, journal.ErrCommitted):
		return nil, err
	}
	slog.WarnContext(c.ctx, "input cannot be journaled; settling it unanswered",
		"run_id", c.runID, "step_id", c.stepID, "request_id", claimed.RequestID, "error", err)
	if _, err := store.Transition(c.ctx, claimed, inputs.StatusUnanswered, 0); err != nil {
		return nil, err
	}
	return nil, nil
}

// placeFaultOrAppend appends record, or fails it through the test
// seam that stands in for a server rejection.
func (c *taskContext) placeFaultOrAppend(record journal.InputPlaced) error {
	if record.RequestID == "" {
		panic("placeFaultOrAppend: record must name its request")
	}
	if c.journal == nil {
		panic("placeFaultOrAppend: journal must be open")
	}
	if c.placeFault != nil {
		if err := c.placeFault(record.RequestID); err != nil {
			return err
		}
	}
	_, err := c.journal.PlaceInput(c.ctx, record)
	return err
}

// projectInput moves a submission this claim holds to a later status.
// Best effort: the journal is the record, and a status read repairs
// the projection from it, so a lost swap is logged, not retried.
func (c *taskContext) projectInput(store *inputs.Store, entry inputs.Entry, to inputs.Status) {
	if store == nil {
		panic("projectInput: store must not be nil")
	}
	if entry.RequestID == "" {
		panic("projectInput: entry must name its request")
	}
	if _, err := store.Transition(c.ctx, entry, to, c.iteration); err != nil {
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
