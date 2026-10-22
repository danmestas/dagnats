// journal/check_test.go
// The invariant checker's rules (ADR-026): one commit per iteration
// from the attempt the engine accepted, no record after the claim that
// fenced its writer, tool intents paired with results, placed inputs
// unique, and the engine's iteration count equal to the accepted
// continue commits. Invariant 3 (side-effect counter) needs an
// external counter and is reported as skipped.
//
// Methodology: pure unit tests on checkStep with hand-built records
// and step.started events, no NATS. Each case asserts both that the
// expected violation is reported and that a clean journal is not
// flagged by the same rule.
package journal

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func rec(seq uint64, typ string, iteration, attempt int, data string) Record {
	return Record{
		Seq: seq, Type: typ, Iteration: iteration, Attempt: attempt, Delivered: attempt,
		DispatchNonce: "n" + string(rune('0'+iteration)) + "-" + string(rune('0'+attempt)),
		Data:          json.RawMessage(data),
	}
}

// applied is a lifecycle event the engine saw, naming the commit it
// carried.
func applied(typ protocol.EventType, iteration, attempt int, journalSeq uint64) protocol.Event {
	evt := protocol.NewStepEvent(typ, "r", "s", nil)
	evt.SetIdentity(iteration, attempt, "n")
	evt.JournalSeq = journalSeq
	return evt
}

func commitRec(seq uint64, iteration, attempt int, action string) Record {
	data, _ := json.Marshal(Commit{Action: action})
	return rec(seq, TypeCommit, iteration, attempt, string(data))
}

func started(iteration, attempt int) protocol.Event {
	evt := protocol.NewStepEvent(protocol.EventStepStarted, "r", "s", nil)
	evt.AttemptNumber = attempt
	evt.DispatchNonce = "n" + string(rune('0'+iteration)) + "-" + string(rune('0'+attempt))
	return evt
}

func hasViolation(violations []string, fragment string) bool {
	for _, v := range violations {
		if strings.Contains(v, fragment) {
			return true
		}
	}
	return false
}

func TestCheckStep_CleanLoopPasses(t *testing.T) {
	records := []Record{
		rec(1, TypeClaim, 0, 1, `{}`), rec(2, TypeEntry, 0, 1, `1`),
		commitRec(3, 0, 1, ActionContinue),
		rec(4, TypeClaim, 1, 1, `{}`), commitRec(5, 1, 1, ActionComplete),
	}
	state := dag.StepState{Status: dag.StepStatusCompleted, Iterations: 1, Attempts: 1}
	report := checkStep("s", state, records, []protocol.Event{started(0, 1), started(1, 1)})
	if len(report.Violations) != 0 {
		t.Fatalf("violations = %v, want none", report.Violations)
	}
	if report.Commits != 2 || report.Records != 5 {
		t.Errorf("counts = %d commits, %d records; want 2, 5", report.Commits, report.Records)
	}
}

func TestCheckStep_OneCommitPerIterationFromAcceptedAttempt(t *testing.T) {
	// Attempt 1 committed iteration 0 and crashed before publishing;
	// attempt 2 redid it. The engine adopted attempt 2.
	records := []Record{
		rec(1, TypeClaim, 0, 1, `{}`), commitRec(2, 0, 1, ActionContinue),
		rec(3, TypeClaim, 0, 2, `{}`), commitRec(4, 0, 2, ActionContinue),
		rec(5, TypeClaim, 1, 1, `{}`), commitRec(6, 1, 1, ActionComplete),
	}
	state := dag.StepState{Status: dag.StepStatusCompleted, Iterations: 1, Attempts: 1}
	events := []protocol.Event{started(0, 1), started(0, 2), started(1, 1)}
	if v := checkStep("s", state, records, events).Violations; len(v) != 0 {
		t.Fatalf("recovered iteration flagged: %v", v)
	}
	// Two commits from the accepted attempt is a violation.
	doubled := append(records[:4:4], commitRec(5, 0, 2, ActionContinue),
		rec(6, TypeClaim, 1, 1, `{}`), commitRec(7, 1, 1, ActionComplete))
	v := checkStep("s", state, doubled, events).Violations
	if !hasViolation(v, "iteration 0") || !hasViolation(v, "2 commits") {
		t.Errorf("violations = %v, want iteration 0 with 2 commits", v)
	}
	// A missing commit for a completed iteration is one too.
	missing := []Record{rec(1, TypeClaim, 0, 1, `{}`), rec(2, TypeClaim, 1, 1, `{}`),
		commitRec(3, 1, 1, ActionComplete)}
	v = checkStep("s", state, missing, events).Violations
	if !hasViolation(v, "iteration 0") || !hasViolation(v, "0 commits") {
		t.Errorf("violations = %v, want iteration 0 with 0 commits", v)
	}
}

func TestCheckStep_AcceptedCommitIsTheOneHistoryNames(t *testing.T) {
	// Attempt 1 committed and published iteration 0's continue, which the
	// engine applied, then crashed before acking; the redelivery's
	// step.started(2) landed after. The started heuristic would pick
	// attempt 2 and flag a missing commit; the continue's journal_seq
	// names attempt 1's commit.
	records := []Record{
		rec(1, TypeClaim, 0, 1, `{}`), commitRec(2, 0, 1, ActionContinue),
		rec(3, TypeClaim, 0, 2, `{}`),
		rec(4, TypeClaim, 1, 1, `{}`), commitRec(5, 1, 1, ActionComplete),
	}
	state := dag.StepState{Status: dag.StepStatusCompleted, Iterations: 1, Attempts: 1}
	history := []protocol.Event{
		started(0, 1), applied(protocol.EventStepContinue, 0, 1, 2), started(0, 2),
		started(1, 1), applied(protocol.EventStepCompleted, 1, 1, 5),
	}
	if v := checkStep("s", state, records, history).Violations; len(v) != 0 {
		t.Fatalf("violations = %v, want none: history names attempt 1's commit", v)
	}
	// A commit history never named is not the accepted one.
	history[1] = applied(protocol.EventStepContinue, 0, 2, 99)
	v := checkStep("s", state, records, history).Violations
	if !hasViolation(v, "iteration 0") || !hasViolation(v, "0 commits") {
		t.Errorf("violations = %v, want iteration 0 without its named commit", v)
	}
}

func TestCheckStep_RetryAfterFailureSupersedesTheFailedCommit(t *testing.T) {
	// Attempt 1 failed iteration 0 (commit + step.failed), attempt 2
	// retried it and continued. History names both commits; the latest
	// lifecycle event for the iteration is the one that counts.
	records := []Record{
		rec(1, TypeClaim, 0, 1, `{}`), commitRec(2, 0, 1, ActionFail),
		rec(3, TypeClaim, 0, 2, `{}`), commitRec(4, 0, 2, ActionContinue),
		rec(5, TypeClaim, 1, 2, `{}`), commitRec(6, 1, 2, ActionComplete),
	}
	state := dag.StepState{Status: dag.StepStatusCompleted, Iterations: 1, Attempts: 2}
	history := []protocol.Event{
		started(0, 1), applied(protocol.EventStepFailed, 0, 1, 2),
		started(0, 2), applied(protocol.EventStepContinue, 0, 2, 4),
		started(1, 2), applied(protocol.EventStepCompleted, 1, 2, 6),
	}
	if v := checkStep("s", state, records, history).Violations; len(v) != 0 {
		t.Fatalf("violations = %v, want none: the retry supersedes the failure", v)
	}
	// The same journal with the events reversed in history names the
	// failure last: the continue is then not the accepted commit.
	reversed := []protocol.Event{
		started(0, 1), applied(protocol.EventStepContinue, 0, 2, 4),
		started(0, 2), applied(protocol.EventStepFailed, 0, 1, 2),
		started(1, 2), applied(protocol.EventStepCompleted, 1, 2, 6),
	}
	v := checkStep("s", state, records, reversed).Violations
	if !hasViolation(v, "iteration 0") || !hasViolation(v, `"fail"`) {
		t.Errorf("violations = %v, want iteration 0's accepted commit to be the fail", v)
	}
}

func TestCheckStep_RecordAfterFencingClaimIsAViolation(t *testing.T) {
	records := []Record{
		rec(1, TypeClaim, 0, 1, `{}`),
		rec(2, TypeClaim, 0, 2, `{}`),
		rec(3, TypeEntry, 0, 1, `"from the stalled worker"`),
		commitRec(4, 0, 2, ActionComplete),
	}
	state := dag.StepState{Status: dag.StepStatusCompleted, Attempts: 2}
	v := checkStep("s", state, records, []protocol.Event{started(0, 1), started(0, 2)}).Violations
	if !hasViolation(v, "seq 3") || !hasViolation(v, "fenced") {
		t.Errorf("violations = %v, want seq 3 after the fencing claim", v)
	}
	// An earlier-iteration claim does not fence a later iteration's records.
	scoped := []Record{
		rec(1, TypeClaim, 0, 1, `{}`), commitRec(2, 0, 1, ActionContinue),
		rec(3, TypeClaim, 1, 1, `{}`), rec(4, TypeClaim, 0, 2, `{}`),
		rec(5, TypeEntry, 1, 1, `1`), commitRec(6, 1, 1, ActionComplete),
	}
	state = dag.StepState{Status: dag.StepStatusCompleted, Iterations: 1, Attempts: 1}
	v = checkStep("s", state, scoped, []protocol.Event{started(0, 1), started(1, 1)}).Violations
	if hasViolation(v, "fenced") {
		t.Errorf("violations = %v; an earlier-iteration claim must not fence", v)
	}
}

func TestCheckStep_ToolIntentsPairWithResults(t *testing.T) {
	records := []Record{
		rec(1, TypeClaim, 0, 1, `{}`), rec(2, TypeToolIntent, 0, 1, `{}`),
		commitRec(3, 0, 1, ActionComplete),
	}
	state := dag.StepState{Status: dag.StepStatusCompleted, Attempts: 1}
	v := checkStep("s", state, records, []protocol.Event{started(0, 1)}).Violations
	if !hasViolation(v, "tool.intent") {
		t.Errorf("violations = %v, want an unpaired tool.intent", v)
	}
	paired := []Record{
		rec(1, TypeClaim, 0, 1, `{}`), rec(2, TypeToolIntent, 0, 1, `{}`),
		rec(3, TypeToolResult, 0, 1, `{}`), commitRec(4, 0, 1, ActionComplete),
	}
	if v := checkStep("s", state, paired, []protocol.Event{started(0, 1)}).Violations; len(v) != 0 {
		t.Errorf("paired intent flagged: %v", v)
	}
}

func TestCheckStep_IterationCountMatchesContinueCommits(t *testing.T) {
	records := []Record{
		rec(1, TypeClaim, 0, 1, `{}`), commitRec(2, 0, 1, ActionContinue),
		rec(3, TypeClaim, 1, 1, `{}`), commitRec(4, 1, 1, ActionContinue),
	}
	// The engine says one iteration; the journal has two continues.
	state := dag.StepState{Status: dag.StepStatusRunning, Iterations: 1, Attempts: 1}
	v := checkStep("s", state, records, []protocol.Event{started(0, 1), started(1, 1)}).Violations
	if !hasViolation(v, "iterations") {
		t.Errorf("violations = %v, want an iteration count mismatch", v)
	}
	state.Iterations = 2
	events := []protocol.Event{started(0, 1), started(1, 1)}
	if v := checkStep("s", state, records, events).Violations; len(v) != 0 {
		t.Errorf("matching count flagged: %v", v)
	}
}

func TestCheckStep_PlacedInputsUnique(t *testing.T) {
	records := []Record{
		rec(1, TypeClaim, 0, 1, `{}`),
		rec(2, TypeInputPlaced, 0, 1, `{"request_id":"q1"}`),
		rec(3, TypeInputPlaced, 0, 1, `{"request_id":"q1"}`),
		commitRec(4, 0, 1, ActionComplete),
	}
	state := dag.StepState{Status: dag.StepStatusCompleted, Attempts: 1}
	v := checkStep("s", state, records, []protocol.Event{started(0, 1)}).Violations
	if !hasViolation(v, "q1") {
		t.Errorf("violations = %v, want the duplicate placed input q1", v)
	}
	records[3-1] = rec(3, TypeInputPlaced, 0, 1, `{"request_id":"q2"}`)
	if v := checkStep("s", state, records, []protocol.Event{started(0, 1)}).Violations; len(v) != 0 {
		t.Errorf("distinct inputs flagged: %v", v)
	}
}

// A record written before the stream-sequence header existed, or by a
// writer that omits it, decodes as stream sequence 0, not as garbage
// that matches no claim.
func TestDecodeRecord_MissingStreamSeqIsZero(t *testing.T) {
	rec := decodeRecord(1, nats.Header{HeaderType: {TypeEntry}}, nil, time.Time{})
	if rec.StreamSeq != 0 {
		t.Fatalf("StreamSeq = %d, want 0", rec.StreamSeq)
	}
	rec = decodeRecord(2, nats.Header{HeaderStreamSeq: {"42"}}, nil, time.Time{})
	if rec.StreamSeq != 42 {
		t.Fatalf("StreamSeq = %d, want 42", rec.StreamSeq)
	}
}
