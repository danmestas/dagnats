// internal/inputplace/place.go
// Placer is the one implementation of placing queued inputs into a
// step's conversation (ADR-027), used by the Go worker SDK and the HTTP
// bridge so the two cannot differ. Each placement is one input.placed
// record, the truth about placement; the submission's bucket status is
// a projection repaired from it.
package inputplace

import (
	"context"
	"errors"
	"log/slog"

	"github.com/danmestas/dagnats/inputs"
	"github.com/danmestas/dagnats/journal"
)

// Placer places inputs for one claim of one iteration. It belongs to
// the goroutine (or bridge claim) that owns the Journal.
type Placer struct {
	Journal   *journal.Journal
	Store     *inputs.Store
	RunID     string
	StepID    string
	Iteration int
	// Test seams: BeforeClaim runs between reading the inputs and
	// claiming them; Fault fails an append by request id.
	BeforeClaim func()
	Fault       func(requestID string) error
	// ClaimFault fails a claim by request id with the error it returns,
	// standing in for a transient store failure.
	ClaimFault func(requestID string) error

	placed   []inputs.Entry
	returned map[string]bool
}

// Place moves every queued input of mode into the conversation and
// returns them in submission order, after any input of mode an earlier
// delivery of this iteration already placed: a redelivery rebuilds the
// conversation from the last checkpoint, so it gets those back once,
// as Tool and Model give back recorded results.
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
func (p *Placer) Place(ctx context.Context, mode inputs.Mode) ([]journal.InputPlaced, error) {
	if p.Journal == nil || p.Store == nil || p.RunID == "" || p.StepID == "" {
		panic("Placer.Place: journal, store, run and step are required")
	}
	if mode != inputs.ModeSteer && mode != inputs.ModeFollowUp {
		panic("Placer.Place: mode must be steer or follow_up")
	}
	candidates, err := p.placeable(ctx, mode)
	if err != nil {
		return nil, err
	}
	if p.BeforeClaim != nil {
		p.BeforeClaim()
	}
	placed := p.recordedPlacements(ctx, mode)
	recorded := len(placed)
	for _, entry := range candidates {
		claimed, err := p.claim(ctx, entry)
		if errors.Is(err, inputs.ErrConflict) || errors.Is(err, inputs.ErrNotFound) {
			continue // withdrawn, or placed by a rival this journal already knows
		}
		if err != nil {
			return placed, err
		}
		record, err := p.record(ctx, claimed)
		if err != nil {
			return placed, err
		}
		if record != nil {
			p.returned[record.RequestID] = true
			placed = append(placed, *record)
		}
	}
	if len(placed) > recorded+len(candidates) {
		panic("Placer.Place: placed more than were claimable")
	}
	return placed, nil
}

// Settle moves the inputs this claim placed to done, once the
// iteration's commit is stored. Best effort: the journal is the record,
// and a status read repairs the projection from it.
func (p *Placer) Settle(ctx context.Context) {
	if p.RunID == "" {
		panic("Placer.Settle: run must not be empty")
	}
	if len(p.placed) > 0 && p.Store == nil {
		panic("Placer.Settle: placements need the store they came from")
	}
	for _, entry := range p.placed {
		current, err := p.Store.Get(ctx, p.RunID, p.StepID, entry.RequestID)
		if err != nil {
			current = entry
		}
		if _, err := p.Store.Transition(ctx, current, inputs.StatusDone, p.Iteration); err != nil {
			slog.WarnContext(ctx, "project input status; the journal is the record",
				"run_id", p.RunID, "step_id", p.StepID,
				"request_id", entry.RequestID, "status", string(inputs.StatusDone), "error", err)
		}
	}
	p.placed = nil
}

// recordedPlacements returns the placements of mode this iteration's
// journal holds that this delivery has not returned yet, and adds them
// to the inputs settled done when the iteration commits.
func (p *Placer) recordedPlacements(ctx context.Context, mode inputs.Mode) []journal.InputPlaced {
	if p.returned == nil {
		p.returned = map[string]bool{}
	}
	var recorded []journal.InputPlaced
	for _, rec := range p.Journal.PlacedInputs() {
		if rec.Iteration != p.Iteration || rec.Mode != string(mode) || p.returned[rec.RequestID] {
			continue
		}
		p.returned[rec.RequestID] = true
		recorded = append(recorded, rec)
		entry, err := p.Store.Get(ctx, p.RunID, p.StepID, rec.RequestID)
		if err != nil {
			entry = inputs.Entry{RequestID: rec.RequestID, Mode: mode, Status: inputs.StatusPlaced}
		}
		p.placed = append(p.placed, entry)
	}
	return recorded
}

// claim claims entry, re-reading it once when the claim is lost: a
// stale holder that claimed it between this read and this claim will
// be fenced on its append, which would leave the input placed with no
// record and nothing to reclaim it, so a placed input this journal has
// no record of is claimed again, as a dead claim is on redelivery.
func (p *Placer) claim(ctx context.Context, entry inputs.Entry) (inputs.Entry, error) {
	if p.ClaimFault != nil {
		if err := p.ClaimFault(entry.RequestID); err != nil {
			return inputs.Entry{}, err
		}
	}
	claimed, err := p.Store.Claim(ctx, entry, p.Iteration)
	if !errors.Is(err, inputs.ErrConflict) {
		return claimed, err
	}
	current, err := p.Store.Get(ctx, p.RunID, p.StepID, entry.RequestID)
	if err != nil {
		return inputs.Entry{}, err
	}
	if current.Status != inputs.StatusPlaced || p.Journal.InputPlaced(entry.RequestID) {
		return inputs.Entry{}, inputs.ErrConflict
	}
	return p.Store.Claim(ctx, current, p.Iteration)
}

// placeable lists the step's inputs of mode this claim may place:
// queued ones, and placed ones the journal has no record of (a dead
// claim). One the journal already holds is never a candidate.
func (p *Placer) placeable(ctx context.Context, mode inputs.Mode) ([]inputs.Entry, error) {
	all, err := p.Store.List(ctx, p.RunID, p.StepID)
	if err != nil {
		return nil, err
	}
	candidates := all[:0]
	for _, entry := range all {
		if entry.Mode != mode || p.Journal.InputPlaced(entry.RequestID) {
			continue
		}
		if entry.Status == inputs.StatusQueued || entry.Status == inputs.StatusPlaced {
			candidates = append(candidates, entry)
		}
	}
	return candidates, nil
}

// record appends the input.placed record for a claimed input. nil
// without error means the input was settled unanswered instead.
func (p *Placer) record(ctx context.Context, claimed inputs.Entry) (*journal.InputPlaced, error) {
	if claimed.Status != inputs.StatusPlaced {
		panic("Placer.record: input must be claimed")
	}
	rec := journal.InputPlaced{
		RequestID: claimed.RequestID, Mode: string(claimed.Mode), Content: claimed.Content,
	}
	err := p.appendOrFault(ctx, rec)
	switch {
	case err == nil:
		rec.Iteration = p.Iteration
		p.placed = append(p.placed, claimed)
		return &rec, nil
	case errors.Is(err, journal.ErrFenced), errors.Is(err, journal.ErrCommitted):
		return nil, err
	}
	slog.WarnContext(ctx, "input cannot be journaled; settling it unanswered",
		"run_id", p.RunID, "step_id", p.StepID, "request_id", claimed.RequestID, "error", err)
	if _, err := p.Store.Transition(ctx, claimed, inputs.StatusUnanswered, 0); err != nil {
		return nil, err
	}
	return nil, nil
}

func (p *Placer) appendOrFault(ctx context.Context, rec journal.InputPlaced) error {
	if p.Fault != nil {
		if err := p.Fault(rec.RequestID); err != nil {
			return err
		}
	}
	_, err := p.Journal.PlaceInput(ctx, rec)
	return err
}
