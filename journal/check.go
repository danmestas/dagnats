package journal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
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
	// submissionsBucket holds input outcomes (ADR-027); absent until
	// that lands, in which case invariant 5 is skipped.
	submissionsBucket = "submissions"
)

// Check audits a run's journals against its history and snapshot:
// each iteration has exactly one commit from the attempt the engine
// accepted (1); no record follows the claim that fenced its writer
// (2); every tool.intent has one tool.result (4); every placed input
// appears once (5); the engine's iteration count equals the accepted
// continue commits (6). Invariant 3 needs a side-effect counter
// outside the journal and is reported as skipped, as is 5 while the
// submissions bucket does not exist.
func Check(ctx context.Context, js jetstream.JetStream, runID string) (Report, error) {
	if ctx == nil {
		panic("journal.Check: ctx must not be nil")
	}
	if runID == "" {
		panic("journal.Check: runID must not be empty")
	}
	run, err := engine.NewSnapshotStore(js).Load(ctx, runID)
	if err != nil {
		return Report{}, fmt.Errorf("journal: load run %s: %w", runID, err)
	}
	startedByStep, err := readStarted(ctx, js, runID)
	if err != nil {
		return Report{}, err
	}
	report := Report{RunID: runID, Skipped: []string{
		"invariant 3 (unsafe tool executed at most once) needs the test side-effect counter",
	}}
	if _, err := js.KeyValue(ctx, submissionsBucket); err != nil {
		if !errors.Is(err, jetstream.ErrBucketNotFound) {
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
		report.Steps = append(report.Steps,
			checkStep(stepID, run.Steps[stepID], records, startedByStep[stepID]))
	}
	return report, nil
}

// readStarted reads the run's step.started events, keyed by step: the
// attempt the engine adopted for each dispatch nonce.
func readStarted(
	ctx context.Context, js jetstream.JetStream, runID string,
) (map[string][]protocol.Event, error) {
	if js == nil {
		panic("readStarted: js must not be nil")
	}
	if runID == "" {
		panic("readStarted: runID must not be empty")
	}
	consumer, err := natsutil.OpenConsumer(ctx, js, "WORKFLOW_HISTORY",
		natsutil.OrderedConsumerSpec{
			FilterSubjects: []string{"history." + runID},
			DeliverPolicy:  jetstream.DeliverAllPolicy,
		})
	if err != nil {
		return nil, fmt.Errorf("journal: open history: %w", err)
	}
	out := map[string][]protocol.Event{}
	for read := 0; read < historyEventsMax; {
		batch, err := consumer.Fetch(readBatch, jetstream.FetchMaxWait(readWait))
		if err != nil {
			return nil, fmt.Errorf("journal: fetch history: %w", err)
		}
		received := 0
		for msg := range batch.Messages() {
			received++
			evt, err := protocol.UnmarshalEvent(msg.Data())
			if err != nil {
				return nil, fmt.Errorf("journal: decode history: %w", err)
			}
			if evt.Type == protocol.EventStepStarted {
				out[evt.StepID] = append(out[evt.StepID], evt)
			}
		}
		if err := batch.Error(); err != nil {
			return nil, fmt.Errorf("journal: fetch history: %w", err)
		}
		if received == 0 {
			return out, nil
		}
		read += received
	}
	return nil, fmt.Errorf("journal: more than %d history events on %s", historyEventsMax, runID)
}

// checkStep applies the invariants to one step's journal.
func checkStep(
	stepID string, state dag.StepState, records []Record, started []protocol.Event,
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
		checkCommits(state, records, commits, started)...)
	report.Violations = append(report.Violations, checkFencing(records)...)
	report.Violations = append(report.Violations, checkToolPairs(records)...)
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
	started []protocol.Event,
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
		accepted := acceptedCommits(iteration, records, commits, started)
		if len(accepted) != 1 {
			violations = append(violations, fmt.Sprintf(
				"iteration %d has %d commits from accepted attempt %d, want 1",
				iteration, len(accepted), acceptedAttempt(iteration, records, started)))
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
		for _, c := range acceptedCommits(iteration, records, commits, started) {
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

// acceptedCommits returns the iteration's commits from the attempt the
// engine adopted.
func acceptedCommits(
	iteration int, records []Record, commits map[int][]Record, started []protocol.Event,
) []Record {
	if commits == nil {
		panic("acceptedCommits: commits must not be nil")
	}
	if iteration < 0 {
		panic("acceptedCommits: iteration must not be negative")
	}
	attempt := acceptedAttempt(iteration, records, started)
	var accepted []Record
	for _, c := range commits[iteration] {
		if c.Attempt == attempt {
			accepted = append(accepted, c)
		}
	}
	return accepted
}

// acceptedAttempt is the attempt the engine adopted for iteration: the
// highest step.started attempt among the dispatch nonces that claimed
// the iteration, else the highest claim attempt when history carries
// no matching step.started.
func acceptedAttempt(iteration int, records []Record, started []protocol.Event) int {
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
	for _, evt := range started {
		if evt.DispatchNonce != "" && nonces[evt.DispatchNonce] {
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
		DispatchNonce: r.DispatchNonce,
	}
}

// checkToolPairs is invariant 4: within one claim, every tool.intent
// has exactly one tool.result.
func checkToolPairs(records []Record) []string {
	if records == nil {
		panic("checkToolPairs: records must not be nil")
	}
	intents := map[Task]int{}
	results := map[Task]int{}
	for _, r := range records {
		switch r.Type {
		case TypeToolIntent:
			intents[taskOf(r)]++
		case TypeToolResult:
			results[taskOf(r)]++
		}
	}
	if len(results) > len(records) {
		panic("checkToolPairs: more groups than records")
	}
	var violations []string
	for claim, n := range intents {
		if results[claim] != n {
			violations = append(violations, fmt.Sprintf(
				"iteration %d attempt %d: %d tool.intent, %d tool.result",
				claim.Iteration, claim.Attempt, n, results[claim]))
		}
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
