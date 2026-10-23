package journal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/inputs"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Report is the result of Check for one run.
type Report struct {
	RunID string       `json:"run_id"`
	Steps []StepReport `json:"steps"`
	// Skipped names the invariants that could not be checked and why.
	Skipped []string `json:"skipped,omitempty"`
}

// OK reports whether no step has a violation.
func (r Report) OK() bool {
	for _, step := range r.Steps {
		if len(step.Violations) > 0 {
			return false
		}
	}
	return true
}

// StepReport is one step's audit.
type StepReport struct {
	StepID     string   `json:"step_id"`
	Records    int      `json:"records"`
	Commits    int      `json:"commits"`
	Violations []string `json:"violations,omitempty"`
}

// Bounds on what Check reads.
const (
	historyEventsMax = 100_000
	historyStream    = "WORKFLOW_HISTORY"
	// runsBucket and runKeyPrefix locate the run snapshot the engine
	// writes (internal/engine/snapshot.go); read here through the
	// leader so the audit sees the latest save.
	runsBucket   = "workflow_runs"
	runKeyPrefix = "run."
)

// Check audits a run's journals against its history and snapshot:
// each iteration has exactly one commit from the attempt the engine
// accepted (1); no record follows the claim that fenced its writer
// (2); every tool.intent has one tool.result (4); every placed input
// appears once (5); the engine's iteration count equals the accepted
// continue commits (6). Invariant 3 needs a side-effect counter
// outside the journal and is reported as skipped, as is 5 while the
// submissions bucket does not exist.
func Check(ctx context.Context, nc *nats.Conn, runID string) (Report, error) {
	if ctx == nil {
		panic("journal.Check: ctx must not be nil")
	}
	if runID == "" {
		panic("journal.Check: runID must not be empty")
	}
	run, err := loadRun(ctx, nc, runID)
	if err != nil {
		return Report{}, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return Report{}, fmt.Errorf("journal: jetstream: %w", err)
	}
	historyByStep, err := readHistory(ctx, js, runID)
	if err != nil {
		return Report{}, err
	}
	report := Report{RunID: runID, Skipped: []string{
		"invariant 3 (unsafe tool executed at most once): not visible in the journal; " +
			"checked by the durability tests",
	}}
	submissions, err := inputs.Open(ctx, js)
	if err != nil {
		if !errors.Is(err, inputs.ErrNotProvisioned) {
			return Report{}, fmt.Errorf("journal: submissions bucket: %w", err)
		}
		report.Skipped = append(report.Skipped,
			"invariant 5 (placed inputs vs submissions) skipped: submissions bucket absent")
	}
	stepIDs := make([]string, 0, len(run.Steps))
	for stepID := range run.Steps {
		stepIDs = append(stepIDs, stepID)
	}
	sort.Strings(stepIDs)
	for _, stepID := range stepIDs {
		records, err := Read(ctx, js, runID, stepID)
		if err != nil {
			return Report{}, err
		}
		if len(records) == 0 {
			continue
		}
		step := checkStep(stepID, run.Steps[stepID], records, historyByStep[stepID])
		if submissions != nil {
			subs, err := submissions.List(ctx, runID, stepID)
			if err != nil {
				return Report{}, fmt.Errorf("journal: submissions of %s: %w", stepID, err)
			}
			step.Violations = append(step.Violations,
				checkInputs(run.Steps[stepID], records, subs)...)
		}
		report.Steps = append(report.Steps, step)
	}
	return report, nil
}

// loadRun reads the run snapshot the engine keeps in workflow_runs.
func loadRun(ctx context.Context, nc *nats.Conn, runID string) (dag.WorkflowRun, error) {
	if nc == nil {
		panic("loadRun: nc must not be nil")
	}
	if runID == "" {
		panic("loadRun: runID must not be empty")
	}
	entry, err := natsutil.KVGetLeader(ctx, nc, runsBucket, runKeyPrefix+runID)
	if err != nil {
		return dag.WorkflowRun{}, fmt.Errorf("journal: load run %s: %w", runID, err)
	}
	var run dag.WorkflowRun
	if err := json.Unmarshal(entry.Value, &run); err != nil {
		return dag.WorkflowRun{}, fmt.Errorf("journal: decode run %s: %w", runID, err)
	}
	return run, nil
}

// readHistory reads the run's step events, keyed by step: step.started
// (the attempt the engine adopted per dispatch nonce) and the lifecycle
// events, which name the commit they carried.
func readHistory(
	ctx context.Context, js jetstream.JetStream, runID string,
) (map[string][]protocol.Event, error) {
	if js == nil {
		panic("readHistory: js must not be nil")
	}
	if runID == "" {
		panic("readHistory: runID must not be empty")
	}
	stream, err := js.Stream(ctx, historyStream)
	if err != nil {
		return nil, fmt.Errorf("journal: history stream: %w", err)
	}
	subject := "history." + runID
	var tail *jetstream.RawStreamMsg
	err = retryTransient(ctx, func() error {
		var getErr error
		tail, getErr = stream.GetLastMsgForSubject(ctx, subject)
		return getErr
	})
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return map[string][]protocol.Event{}, nil
		}
		return nil, fmt.Errorf("journal: history tail: %w", err)
	}
	var out map[string][]protocol.Event
	err = retryTransient(ctx, func() error {
		out = map[string][]protocol.Event{}
		return readHistoryEvents(ctx, js, subject, tail.Sequence, out)
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// readHistoryEvents reads the run's history up to tailSeq into out,
// grouped by step.
func readHistoryEvents(
	ctx context.Context, js jetstream.JetStream, subject string, tailSeq uint64,
	out map[string][]protocol.Event,
) error {
	if out == nil {
		panic("readHistoryEvents: out must not be nil")
	}
	if tailSeq == 0 {
		panic("readHistoryEvents: tailSeq must be positive")
	}
	return fetchUpTo(ctx, js, historyStream, subject, tailSeq, historyEventsMax,
		func(_ uint64, msg jetstream.Msg, _ *jetstream.MsgMetadata) error {
			evt, err := protocol.UnmarshalEvent(msg.Data())
			if err != nil {
				return fmt.Errorf("journal: decode history: %w", err)
			}
			switch evt.Type {
			case protocol.EventStepStarted, protocol.EventStepContinue,
				protocol.EventStepCompleted, protocol.EventStepFailed:
				out[evt.StepID] = append(out[evt.StepID], evt)
			}
			return nil
		})
}

// checkStep applies the invariants to one step's journal. history is
// the step's step.started and lifecycle events.
func checkStep(
	stepID string, state dag.StepState, records []Record, history []protocol.Event,
) StepReport {
	if stepID == "" {
		panic("checkStep: stepID must not be empty")
	}
	if len(records) > recordsMax {
		panic("checkStep: records exceed the read bound")
	}
	report := StepReport{StepID: stepID, Records: len(records)}
	commits := commitsByIteration(records)
	for _, iterationCommits := range commits {
		report.Commits += len(iterationCommits)
	}
	report.Violations = append(report.Violations,
		checkCommits(state, records, commits, history)...)
	report.Violations = append(report.Violations, checkFencing(records)...)
	report.Violations = append(report.Violations,
		checkToolPairs(records, state.Status == dag.StepStatusFailed)...)
	report.Violations = append(report.Violations, checkPlacedInputs(records)...)
	return report
}

// commitsByIteration groups commit records by iteration.
func commitsByIteration(records []Record) map[int][]Record {
	if records == nil {
		panic("commitsByIteration: records must not be nil")
	}
	out := map[int][]Record{}
	for _, r := range records {
		if r.Type == TypeCommit {
			out[r.Iteration] = append(out[r.Iteration], r)
		}
	}
	if len(out) > len(records) {
		panic("commitsByIteration: more groups than records")
	}
	return out
}

// checkCommits is invariants 1 and 6: every iteration the engine
// finished, and the terminal one, has exactly one commit from the
// accepted attempt with the action the engine applied; and the
// accepted continue commits number the engine's iterations.
func checkCommits(
	state dag.StepState, records []Record, commits map[int][]Record,
	history []protocol.Event,
) []string {
	if commits == nil {
		panic("checkCommits: commits must not be nil")
	}
	if state.Iterations < 0 {
		panic("checkCommits: iterations must not be negative")
	}
	var violations []string
	terminal := state.Status == dag.StepStatusCompleted || state.Status == dag.StepStatusFailed
	last := state.Iterations
	if !terminal {
		last = state.Iterations - 1
	}
	for iteration := 0; iteration <= last; iteration++ {
		accepted := acceptedCommits(iteration, records, commits, history)
		if len(accepted) != 1 {
			violations = append(violations, fmt.Sprintf(
				"iteration %d has %d commits named by its latest lifecycle event, want 1",
				iteration, len(accepted)))
			continue
		}
		want := ActionContinue
		if iteration == state.Iterations && terminal {
			want = ActionComplete
			if state.Status == dag.StepStatusFailed {
				want = ActionFail
			}
		}
		if got := commitAction(accepted[0]); got != want {
			violations = append(violations, fmt.Sprintf(
				"iteration %d commit action %q, want %q", iteration, got, want))
		}
	}
	continues := 0
	for iteration := range commits {
		for _, c := range acceptedCommits(iteration, records, commits, history) {
			if commitAction(c) == ActionContinue {
				continues++
			}
		}
	}
	if continues != state.Iterations {
		violations = append(violations, fmt.Sprintf(
			"engine iterations %d, accepted continue commits %d", state.Iterations, continues))
	}
	return violations
}

// acceptedCommits returns the iteration's commit that stands: the one
// named by journal_seq on the latest lifecycle event for the iteration
// in history order. History records what workers published, not what
// the engine applied, and an iteration can legitimately be named
// twice: a failed attempt's step.failed and then the retry's
// step.continue. The later event supersedes. When no event for the
// iteration carries journal_seq (a worker without it), it falls back
// to the commits of the attempt the engine adopted.
func acceptedCommits(
	iteration int, records []Record, commits map[int][]Record, history []protocol.Event,
) []Record {
	if commits == nil {
		panic("acceptedCommits: commits must not be nil")
	}
	if iteration < 0 {
		panic("acceptedCommits: iteration must not be negative")
	}
	var latest uint64
	for _, evt := range history {
		if evt.JournalSeq > 0 && evt.Iteration != nil && *evt.Iteration == iteration &&
			fencedLifecycle(evt.Type) {
			latest = evt.JournalSeq
		}
	}
	var accepted []Record
	if latest > 0 {
		for _, c := range commits[iteration] {
			if c.Seq == latest {
				accepted = append(accepted, c)
			}
		}
		return accepted
	}
	attempt := acceptedAttempt(iteration, records, history)
	for _, c := range commits[iteration] {
		if c.Attempt == attempt {
			accepted = append(accepted, c)
		}
	}
	return accepted
}

// fencedLifecycle reports whether t is a lifecycle event a commit
// precedes.
func fencedLifecycle(t protocol.EventType) bool {
	switch t {
	case protocol.EventStepContinue, protocol.EventStepCompleted, protocol.EventStepFailed:
		return true
	}
	return false
}

// acceptedAttempt is the fallback when no lifecycle event names a
// commit: the highest step.started attempt among the dispatch nonces
// that claimed the iteration, else the highest claim attempt.
func acceptedAttempt(iteration int, records []Record, history []protocol.Event) int {
	if iteration < 0 {
		panic("acceptedAttempt: iteration must not be negative")
	}
	if records == nil {
		panic("acceptedAttempt: records must not be nil")
	}
	nonces := map[string]bool{}
	claimMax := 0
	for _, r := range records {
		if r.Type == TypeClaim && r.Iteration == iteration {
			nonces[r.DispatchNonce] = true
			claimMax = max(claimMax, r.Attempt)
		}
	}
	startedMax := 0
	for _, evt := range history {
		if evt.Type == protocol.EventStepStarted && nonces[evt.DispatchNonce] &&
			evt.DispatchNonce != "" {
			startedMax = max(startedMax, evt.AttemptNumber)
		}
	}
	if startedMax > 0 {
		return startedMax
	}
	return claimMax
}

// commitAction decodes a commit's action; "" on a bad body.
func commitAction(r Record) string {
	if r.Type != TypeCommit {
		panic("commitAction: record must be a commit")
	}
	var c Commit
	if err := json.Unmarshal(r.Data, &c); err != nil {
		return ""
	}
	return c.Action
}

// checkFencing is invariant 2: a record must not follow a claim, for
// the same or a later iteration, that landed after its own claim.
func checkFencing(records []Record) []string {
	if records == nil {
		panic("checkFencing: records must not be nil")
	}
	if len(records) > recordsMax {
		panic("checkFencing: records exceed the read bound")
	}
	var violations []string
	var claims []Record
	for _, r := range records {
		if r.Type == TypeClaim {
			claims = append(claims, r)
			continue
		}
		var own *Record
		for i := range claims {
			if claims[i].sameClaim(taskOf(r)) {
				own = &claims[i]
			}
		}
		if own == nil {
			violations = append(violations, fmt.Sprintf(
				"seq %d (%s) has no claim", r.Seq, r.Type))
			continue
		}
		for _, c := range claims {
			if c.Seq > own.Seq && c.Seq < r.Seq && c.Iteration >= r.Iteration {
				violations = append(violations, fmt.Sprintf(
					"seq %d (%s, attempt %d) written after being fenced by claim seq %d (attempt %d)",
					r.Seq, r.Type, r.Attempt, c.Seq, c.Attempt))
				break
			}
		}
	}
	return violations
}

// taskOf is the claim identity a record carries.
func taskOf(r Record) Task {
	if r.Seq == 0 {
		panic("taskOf: record must have a sequence")
	}
	return Task{
		RunID: "-", StepID: "-", Iteration: r.Iteration, Attempt: r.Attempt,
		DispatchNonce: r.DispatchNonce, Delivered: r.Delivered,
		StreamSeq: r.StreamSeq,
	}
}

// checkToolPairs is invariant 4: within one iteration, every tool
// call (by position) has one tool.intent answered by one tool.result,
// in order. The result may come from a later claim than the intent: a
// successor answers a crashed holder's unfinished call without a
// second intent (ADR-026). An intent left unanswered is no violation
// once its attempt ended: a fail commit of the iteration follows it,
// or a higher attempt wrote its own intent for the call (the engine
// timed the attempt out; the next attempt runs a safe call afresh and
// supersedes it). The last unanswered intent is tolerated when the
// step ended failed: nothing could answer it.
func checkToolPairs(records []Record, stepFailed bool) []string {
	if records == nil {
		panic("checkToolPairs: records must not be nil")
	}
	type toolCall struct{ iteration, call int }
	open := map[toolCall]Record{}
	// lastCommit is the newest fail commit per iteration.
	lastCommit := map[int]uint64{}
	var violations []string
	for _, r := range records {
		switch r.Type {
		case TypeCommit:
			if commitAction(r) == ActionFail {
				lastCommit[r.Iteration] = r.Seq
			}
			continue
		case TypeToolIntent, TypeToolResult:
		default:
			continue
		}
		var body struct {
			Call *int `json:"call"`
		}
		if err := json.Unmarshal(r.Data, &body); err != nil || body.Call == nil {
			violations = append(violations, fmt.Sprintf("seq %d %s has no call", r.Seq, r.Type))
			continue
		}
		key := toolCall{r.Iteration, *body.Call}
		pending, unanswered := open[key]
		superseded := unanswered && (pending.Seq <= lastCommit[r.Iteration] ||
			r.Attempt > pending.Attempt)
		switch {
		case r.Type == TypeToolIntent && unanswered && !superseded:
			violations = append(violations, fmt.Sprintf(
				"iteration %d call %d: second tool.intent at seq %d while seq %d is unanswered",
				key.iteration, key.call, r.Seq, pending.Seq))
		case r.Type == TypeToolIntent:
			open[key] = r
		case !unanswered:
			violations = append(violations, fmt.Sprintf(
				"iteration %d call %d: tool.result at seq %d without a tool.intent",
				key.iteration, key.call, r.Seq))
		default:
			delete(open, key)
		}
	}
	if len(open) > len(records) {
		panic("checkToolPairs: more open calls than records")
	}
	var last uint64
	for _, pending := range open {
		last = max(last, pending.Seq)
	}
	for key, pending := range open {
		if pending.Seq <= lastCommit[key.iteration] || (stepFailed && pending.Seq == last) {
			continue
		}
		violations = append(violations, fmt.Sprintf(
			"iteration %d call %d: tool.intent at seq %d has no tool.result",
			key.iteration, key.call, pending.Seq))
	}
	sort.Strings(violations)
	return violations
}

// checkPlacedInputs is the journal half of invariant 5: each placed
// input's request_id appears once.
func checkPlacedInputs(records []Record) []string {
	if records == nil {
		panic("checkPlacedInputs: records must not be nil")
	}
	seen := map[string]uint64{}
	var violations []string
	for _, r := range records {
		if r.Type != TypeInputPlaced {
			continue
		}
		var placed struct {
			RequestID string `json:"request_id"`
		}
		if err := json.Unmarshal(r.Data, &placed); err != nil || placed.RequestID == "" {
			violations = append(violations, fmt.Sprintf(
				"seq %d input.placed has no request_id", r.Seq))
			continue
		}
		if first, dup := seen[placed.RequestID]; dup {
			violations = append(violations, fmt.Sprintf(
				"input %s placed twice (seq %d and %d)", placed.RequestID, first, r.Seq))
			continue
		}
		seen[placed.RequestID] = r.Seq
	}
	if len(seen) > len(records) {
		panic("checkPlacedInputs: more inputs than records")
	}
	return violations
}
