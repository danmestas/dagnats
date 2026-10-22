// worker/journal.go
// The agent journal on a taskContext (ADR-026). A handler opts in by
// calling Journal(): that claims the dispatch, and from then on every
// attempt-ending call commits one journal record before publishing
// its lifecycle event, Checkpoint and LoadCheckpoint read and write the
// journal's state, and Pause writes a pause record. A task the journal
// says is stale is acknowledged untouched; a fenced one is left to its
// successor; an attempt whose commit already exists republishes it.
package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/protocol"
)

// journalStop is why a journal step must not proceed.
type journalStop int

const (
	journalLive journalStop = iota
	// journalStale: a later iteration holds the step; ack and forget.
	journalStale
	// journalFenced: another claim follows ours; touch nothing, the
	// message belongs to the successor.
	journalFenced
	// journalRecovered: our own commit was found and republished; ack.
	journalRecovered
)

// Journal claims this dispatch's journal, once per delivery. It
// returns journal.ErrStale for a late redelivery (nothing is written
// and the task is acknowledged), journal.ErrFenced when a rival claim
// won, and journal.ErrCommitted when this attempt already committed:
// the commit's lifecycle event has been republished and the handler
// must return without repeating the turn. The handler returns any of
// these as its error.
func (c *taskContext) Journal() (*journal.Journal, error) {
	if c.msg == nil {
		panic("Journal: msg already consumed or nil")
	}
	if c.runID == "" {
		panic("Journal: runID must not be empty")
	}
	if c.journal != nil {
		return c.journal, nil
	}
	if err := c.stopError(); err != nil {
		return nil, err
	}
	if c.js == nil {
		return nil, journal.ErrNotProvisioned
	}
	j, err := journal.Open(c.ctx, c.js, c.tp, journal.Task{
		RunID: c.runID, StepID: c.stepID, Iteration: c.iteration,
		Attempt: c.effectiveAttempt(), DispatchNonce: c.dispatchNonce,
		Delivered: c.deliveredCount(), StreamSeq: c.streamSequence(),
	})
	switch {
	case errors.Is(err, journal.ErrStale):
		c.journalStop = journalStale
		return nil, err
	case errors.Is(err, journal.ErrFenced):
		c.journalStop = journalFenced
		return nil, err
	case err != nil:
		return nil, err
	}
	c.journal = j
	if commit, ok := j.Committed(); ok {
		return nil, c.republishCommitted(commit, j.CommittedSeq())
	}
	return j, nil
}

// deliveredCount is the task message's delivery count, for the
// claim record; the attempt when the message carries no metadata.
func (c *taskContext) deliveredCount() int {
	if c.msg == nil {
		panic("deliveredCount: msg must not be nil")
	}
	meta, err := c.msg.Metadata()
	if err != nil || meta == nil || meta.NumDelivered < 1 {
		return c.effectiveAttempt()
	}
	if meta.NumDelivered > 1<<30 {
		panic("deliveredCount: delivery count out of range")
	}
	return int(meta.NumDelivered)
}

// streamSequence is the task message's sequence in the task stream,
// zero when the message carries no metadata. It is part of the claim
// identity: a dispatch resend is a fresh message with a new one.
func (c *taskContext) streamSequence() uint64 {
	if c.msg == nil {
		panic("streamSequence: msg must not be nil")
	}
	meta, err := c.msg.Metadata()
	if err != nil || meta == nil {
		return 0
	}
	return meta.Sequence.Stream
}

// stopError maps the journal stop to the error the handler sees.
func (c *taskContext) stopError() error {
	switch c.journalStop {
	case journalLive:
		return nil
	case journalStale:
		return journal.ErrStale
	case journalFenced:
		return journal.ErrFenced
	case journalRecovered:
		return journal.ErrCommitted
	}
	panic(fmt.Sprintf("stopError: unknown journal stop %d", c.journalStop))
}

// commitJournal writes the iteration's commit before a lifecycle
// event, when the step has a journal. A fenced commit stops the task:
// the event is not published.
func (c *taskContext) commitJournal(commit journal.Commit) error {
	if commit.Action == "" {
		panic("commitJournal: action must not be empty")
	}
	if err := c.stopError(); err != nil {
		return err
	}
	if c.journal == nil {
		return nil
	}
	seq, err := c.journal.Commit(c.ctx, commit)
	if errors.Is(err, journal.ErrFenced) {
		c.journalStop = journalFenced
		slog.WarnContext(c.ctx, "journal commit fenced by another claim",
			"run_id", c.runID, "step_id", c.stepID,
			"iteration", c.iteration, "attempt", c.effectiveAttempt())
		return err
	}
	if errors.Is(err, journal.ErrCommitted) {
		// A retry after the commit landed but its event did not: the
		// stored commit's event is republished and the task settles.
		stored, ok := c.journal.Committed()
		if !ok {
			panic("commitJournal: journal reports committed without a commit")
		}
		return c.republishCommitted(stored, c.journal.CommittedSeq())
	}
	if err != nil {
		return fmt.Errorf("journal commit: %w", err)
	}
	c.journalSeq = seq
	return nil
}

// republishCommitted publishes the lifecycle event of a commit a
// previous holder of this attempt stored but never announced. The
// event's msg-id is the stable one, so a publish that did land is
// deduplicated.
func (c *taskContext) republishCommitted(commit journal.Commit, seq uint64) error {
	if seq == 0 {
		panic("republishCommitted: seq must be positive")
	}
	if c.journalStop != journalLive {
		panic("republishCommitted: task already stopped")
	}
	c.journalSeq = seq
	var err error
	switch commit.Action {
	case journal.ActionContinue:
		var payload []byte
		payload, err = json.Marshal(protocol.StepContinuePayload{Output: commit.Output})
		if err == nil {
			err = c.publishEvent(protocol.EventStepContinue, payload)
		}
	case journal.ActionComplete:
		err = c.publishEvent(protocol.EventStepCompleted, commit.Output)
	case journal.ActionFail:
		var payload []byte
		payload, err = json.Marshal(protocol.StepFailedPayload{
			Error:        commit.Error,
			FailureType:  protocol.FailureType(commit.FailureType),
			RetryAfterMs: commit.RetryAfterMs,
		})
		if err == nil {
			err = c.publishEvent(protocol.EventStepFailed, payload)
		}
	default:
		return fmt.Errorf("journal: commit %d has unknown action %q", seq, commit.Action)
	}
	if err != nil {
		return fmt.Errorf("republish committed %s: %w", commit.Action, err)
	}
	c.journalStop = journalRecovered
	return journal.ErrCommitted
}

// journalOutcome tells the dispatch loop whether the journal already
// decided the task's fate after the handler returned err, and whether
// to acknowledge the message. A fenced task is left alone: the
// message is the successor's to acknowledge.
func (c *taskContext) journalOutcome(err error) (done, ack bool) {
	if c.runID == "" {
		panic("journalOutcome: runID must not be empty")
	}
	if c.msg == nil {
		panic("journalOutcome: msg already consumed or nil")
	}
	// A fence the handler met on its own j.Append never went through
	// this context; the journal remembers it.
	if c.journalStop == journalLive && c.journal != nil && c.journal.Fenced() {
		c.journalStop = journalFenced
	}
	switch c.journalStop {
	case journalLive:
		return false, false
	case journalStale, journalRecovered:
		c.drainLogs(protocol.LogMarkerCompleted)
		return true, true
	case journalFenced:
		c.drainLogs(protocol.LogMarkerFenced)
		slog.WarnContext(c.ctx, "task fenced by a later claim; leaving it to the successor",
			"run_id", c.runID, "step_id", c.stepID, "error", err)
		return true, false
	}
	panic(fmt.Sprintf("journalOutcome: unknown journal stop %d", c.journalStop))
}

// journalCheckpoint returns the state LoadCheckpoint reports for a
// journal step: the pause marker when this delivery resumes a Pause,
// else the last committed state.
func (c *taskContext) journalCheckpoint() ([]byte, error) {
	if c.journal == nil {
		panic("journalCheckpoint: journal must be open")
	}
	if c.runID == "" {
		panic("journalCheckpoint: runID must not be empty")
	}
	if name, ok := c.journal.Resume(); ok {
		return json.Marshal(map[string]any{"pause_resume": name})
	}
	return c.journal.State(), nil
}

// journalPause writes the pause record for Pause on a journal step.
func (c *taskContext) journalPause(name string, duration time.Duration) error {
	if c.journal == nil {
		panic("journalPause: journal must be open")
	}
	if duration <= 0 {
		panic("journalPause: duration must be positive")
	}
	if err := c.stopError(); err != nil {
		return err
	}
	_, err := c.journal.Pause(c.ctx, name, duration)
	if errors.Is(err, journal.ErrFenced) {
		c.journalStop = journalFenced
	}
	return err
}
