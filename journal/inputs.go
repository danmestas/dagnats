// journal/inputs.go
// Placement records (ADR-027). An input moves from the submissions
// bucket into the conversation by one input.placed record; the record
// is the truth about placement, so a journal knows every request_id
// placed in any iteration and a successor never places one twice.
package journal

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/inputs"
)

// InputPlaced is the body of an input.placed record: the submission as
// it joined the conversation. Content is copied in so the conversation
// can be rebuilt from the journal alone, after the bucket's TTL.
type InputPlaced struct {
	RequestID string          `json:"request_id"`
	Mode      string          `json:"mode"`
	Content   json.RawMessage `json:"content,omitempty"`
	// Iteration is set from the record on read; ignored on write.
	Iteration int `json:"-"`
}

// PlaceInput appends an input.placed record. The caller checks
// InputPlaced first; placing a request twice is a programmer error.
func (j *Journal) PlaceInput(ctx context.Context, p InputPlaced) (uint64, error) {
	if p.RequestID == "" {
		panic("journal.PlaceInput: request id must not be empty")
	}
	defer j.enter("PlaceInput")()
	if j.InputPlaced(p.RequestID) {
		panic("journal.PlaceInput: " + p.RequestID + " is already placed")
	}
	data, err := json.Marshal(p)
	if err != nil {
		return 0, fmt.Errorf("marshal input.placed: %w", err)
	}
	seq, err := j.append(ctx, TypeInputPlaced, data)
	if err != nil {
		return 0, err
	}
	p.Iteration = j.task.Iteration
	j.placed = append(j.placed, p)
	return seq, nil
}

// InputPlaced reports whether requestID has an input.placed record in
// this journal, in any iteration, by any claim.
func (j *Journal) InputPlaced(requestID string) bool {
	if requestID == "" {
		panic("journal.InputPlaced: request id must not be empty")
	}
	if len(j.placed) > recordsMax {
		panic("journal.InputPlaced: placements exceed the read bound")
	}
	for _, p := range j.placed {
		if p.RequestID == requestID {
			return true
		}
	}
	return false
}

// PlacedInputs returns every placement in journal order, with the
// iteration each joined.
func (j *Journal) PlacedInputs() []InputPlaced {
	return append([]InputPlaced(nil), j.placed...)
}

// placedInput decodes an input.placed record; ok is false on a body
// without a request id.
func placedInput(r Record) (InputPlaced, bool) {
	if r.Type != TypeInputPlaced {
		panic("placedInput: record must be an input.placed")
	}
	if r.Seq == 0 {
		panic("placedInput: record must have a sequence")
	}
	var p InputPlaced
	if err := json.Unmarshal(r.Data, &p); err != nil || p.RequestID == "" {
		return InputPlaced{}, false
	}
	p.Iteration = r.Iteration
	return p, true
}

// checkInputs is the bucket half of invariant 5: a placed or done
// submission has exactly one input.placed record and an unanswered one
// has none; once the step has finished, nothing is still queued.
func checkInputs(state dag.StepState, records []Record, subs []inputs.Entry) []string {
	if records == nil {
		panic("checkInputs: records must not be nil")
	}
	if len(records) > recordsMax {
		panic("checkInputs: records exceed the read bound")
	}
	placed := map[string]int{}
	for _, r := range records {
		if r.Type != TypeInputPlaced {
			continue
		}
		if p, ok := placedInput(r); ok {
			placed[p.RequestID]++
		}
	}
	finished := state.Status == dag.StepStatusCompleted ||
		state.Status == dag.StepStatusFailed || state.Status == dag.StepStatusCancelled
	var violations []string
	for _, s := range subs {
		n := placed[s.RequestID]
		switch s.Status {
		case inputs.StatusPlaced, inputs.StatusDone:
			// placed is claimed before the record is appended
			// (worker.PlaceInputs), so a live step may hold a claim
			// with no record yet; a finished one may not.
			if n == 0 && s.Status == inputs.StatusPlaced && !finished {
				continue
			}
			if n != 1 {
				violations = append(violations, fmt.Sprintf(
					"input %s is %s with %d input.placed records, want 1", s.RequestID, s.Status, n))
			}
		case inputs.StatusUnanswered:
			if n != 0 {
				violations = append(violations, fmt.Sprintf(
					"input %s is unanswered but has %d input.placed records", s.RequestID, n))
			}
		case inputs.StatusQueued:
			if finished {
				violations = append(violations, fmt.Sprintf(
					"input %s is still queued after the step finished", s.RequestID))
			}
		}
	}
	return violations
}
