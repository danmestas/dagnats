// api/runs_envelope_test.go
// Tests for the #452 run-list honesty surface: ListRunsEnvelope
// (runs + total + returned + truncated), CountRuns, and the --since
// filter shared by both.
// Methodology: real embedded NATS server, one per test (no sharing).
// We submit runs through the live service, wait for them to surface,
// then assert the aggregate/envelope contract with bounded timeouts.
package api

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
)

// fakeRunReader is a deterministic runReader stub: ListRecent returns
// a fixed slice (already newest-first), CountAll returns countAll. It
// lets the unfiltered-total path be tested without seeding 10k+ real
// runs — the case the #452 headline ("showing 1000 of 146046")
// depends on. countAll is intentionally larger than len(runs) to
// simulate a population beyond MaxRunsLimitCeiling.
type fakeRunReader struct {
	runs     []dag.WorkflowRun
	countAll int
}

func (f fakeRunReader) ListRecent(
	_ context.Context, limit int,
) ([]dag.WorkflowRun, error) {
	if limit <= 0 {
		panic("fakeRunReader.ListRecent: limit must be positive")
	}
	if len(f.runs) > limit {
		return f.runs[:limit], nil
	}
	return f.runs, nil
}

func (f fakeRunReader) CountAll(_ context.Context) (int, error) {
	return f.countAll, nil
}

// TestEnvelopeUnfilteredTotalIsFullPopulation proves the unfiltered
// envelope total reflects the TRUE population (via CountAll), not the
// length of the capped ListRecent window. This is the #452 headline:
// "showing <returned> of <real total>", e.g. 1000 of 146046.
func TestEnvelopeUnfilteredTotalIsFullPopulation(t *testing.T) {
	const fullPopulation = 146046
	window := make([]dag.WorkflowRun, MaxRunsLimitCeiling)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range window {
		window[i] = dag.WorkflowRun{
			RunID:      "r",
			Status:     dag.RunStatusCompleted,
			CreatedAt:  base.Add(time.Duration(i) * time.Second),
			WorkflowID: "wf",
		}
	}
	store := fakeRunReader{runs: window, countAll: fullPopulation}

	const limit = 1000
	env, err := listRunsEnvelopeFrom(
		context.Background(), store, RunsFilter{}, limit,
	)
	if err != nil {
		t.Fatalf("listRunsEnvelopeFrom: %v", err)
	}
	// Positive: total is the full population, not the 10k window.
	if env.Total != fullPopulation {
		t.Fatalf("Total = %d, want %d (full population, not window)",
			env.Total, fullPopulation)
	}
	if env.Returned != limit || len(env.Runs) != limit {
		t.Fatalf("Returned=%d len=%d, want %d",
			env.Returned, len(env.Runs), limit)
	}
	// Negative: must be flagged truncated, and Total must NOT have
	// collapsed to the ceiling.
	if !env.Truncated {
		t.Fatal("Truncated must be true (146046 > 1000)")
	}
	if env.Total == MaxRunsLimitCeiling {
		t.Fatal("Total must not saturate at MaxRunsLimitCeiling")
	}
}

// TestCountUnfilteredUsesCountAll proves the unfiltered count returns
// the exact population from CountAll, beyond the ceiling.
func TestCountUnfilteredUsesCountAll(t *testing.T) {
	const fullPopulation = 146046
	store := fakeRunReader{
		runs:     []dag.WorkflowRun{{RunID: "a"}},
		countAll: fullPopulation,
	}
	got, err := countRunsFrom(
		context.Background(), store, RunsFilter{},
	)
	if err != nil {
		t.Fatalf("countRunsFrom: %v", err)
	}
	// Positive: exact population.
	if got != fullPopulation {
		t.Fatalf("count = %d, want %d", got, fullPopulation)
	}
	// Negative: a workflow filter falls back to the window scan, so
	// it counts matches in ListRecent (here: zero matches for "nope").
	none, err := countRunsFrom(
		context.Background(), store, RunsFilter{Workflow: "nope"},
	)
	if err != nil {
		t.Fatalf("countRunsFrom(filtered): %v", err)
	}
	if none != 0 {
		t.Fatalf("filtered count = %d, want 0", none)
	}
}

// newRunsSvc spins an embedded server + orchestrator + service and
// registers a single-step workflow named wfName.
func newRunsSvc(t *testing.T, wfName string) *Service {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(orch.Stop)
	svc := NewService(nc)
	wb := dag.NewWorkflow(wfName)
	wb.Task("a", "task-a")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := svc.RegisterWorkflow(context.Background(), def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	return svc
}

// waitForRunCount submits nothing; it polls until the store reports at
// least want runs or the bounded deadline elapses.
func waitForRunCount(t *testing.T, svc *Service, want int) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		got, err := svc.CountRuns(context.Background(), RunsFilter{})
		if err != nil {
			t.Fatalf("CountRuns: %v", err)
		}
		if got >= want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("only %d/%d runs surfaced before timeout", got, want)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// TestListRunsEnvelopeTruncation proves the envelope reports the true
// total and a truncated flag when limit < total.
func TestListRunsEnvelopeTruncation(t *testing.T) {
	svc := newRunsSvc(t, "env-wf")
	const submitted = 5
	for i := 0; i < submitted; i++ {
		if _, err := svc.StartRun(
			context.Background(), "env-wf", nil,
		); err != nil {
			t.Fatalf("StartRun %d: %v", i, err)
		}
	}
	waitForRunCount(t, svc, submitted)

	env, err := svc.ListRunsEnvelope(
		context.Background(), RunsFilter{}, 2,
	)
	if err != nil {
		t.Fatalf("ListRunsEnvelope: %v", err)
	}
	// Positive: total reflects the full population, returned the cap.
	if env.Total != submitted {
		t.Fatalf("Total = %d, want %d", env.Total, submitted)
	}
	if env.Returned != 2 || len(env.Runs) != 2 {
		t.Fatalf("Returned = %d / len = %d, want 2",
			env.Returned, len(env.Runs))
	}
	if !env.Truncated {
		t.Fatal("Truncated must be true when total > returned")
	}
	// Negative: no truncation when limit covers the whole population.
	full, err := svc.ListRunsEnvelope(
		context.Background(), RunsFilter{}, 100,
	)
	if err != nil {
		t.Fatalf("ListRunsEnvelope(full): %v", err)
	}
	if full.Truncated {
		t.Fatal("Truncated must be false when limit >= total")
	}
	if full.Total != full.Returned {
		t.Fatalf("Total %d != Returned %d when not truncated",
			full.Total, full.Returned)
	}
}

// TestCountRunsRespectsWorkflowFilter proves CountRuns honors the
// workflow filter and returns aggregate counts without rows.
func TestCountRunsRespectsWorkflowFilter(t *testing.T) {
	svc := newRunsSvc(t, "count-a")
	wb := dag.NewWorkflow("count-b")
	wb.Task("b", "task-b")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build b: %v", err)
	}
	if err := svc.RegisterWorkflow(context.Background(), def); err != nil {
		t.Fatalf("RegisterWorkflow b: %v", err)
	}
	if _, err := svc.StartRun(context.Background(), "count-a", nil); err != nil {
		t.Fatalf("StartRun a: %v", err)
	}
	if _, err := svc.StartRun(context.Background(), "count-a", nil); err != nil {
		t.Fatalf("StartRun a2: %v", err)
	}
	if _, err := svc.StartRun(context.Background(), "count-b", nil); err != nil {
		t.Fatalf("StartRun b: %v", err)
	}
	waitForRunCount(t, svc, 3)

	onlyA, err := svc.CountRuns(
		context.Background(), RunsFilter{Workflow: "count-a"},
	)
	if err != nil {
		t.Fatalf("CountRuns(a): %v", err)
	}
	// Positive: exactly the two count-a runs.
	if onlyA != 2 {
		t.Fatalf("CountRuns(count-a) = %d, want 2", onlyA)
	}
	// Negative: total across both workflows is strictly larger.
	all, err := svc.CountRuns(context.Background(), RunsFilter{})
	if err != nil {
		t.Fatalf("CountRuns(all): %v", err)
	}
	if all <= onlyA {
		t.Fatalf("CountRuns(all)=%d must exceed filtered %d", all, onlyA)
	}
}

// TestRunsFilterSinceExcludesOlder proves the Since filter drops runs
// created strictly before the cutoff.
func TestRunsFilterSinceExcludesOlder(t *testing.T) {
	svc := newRunsSvc(t, "since-wf")
	if _, err := svc.StartRun(
		context.Background(), "since-wf", nil,
	); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	waitForRunCount(t, svc, 1)

	// Positive: a cutoff in the past keeps the run.
	past := time.Now().Add(-1 * time.Hour)
	keep, err := svc.CountRuns(
		context.Background(), RunsFilter{Since: past},
	)
	if err != nil {
		t.Fatalf("CountRuns(past): %v", err)
	}
	if keep < 1 {
		t.Fatalf("past cutoff dropped runs: got %d", keep)
	}
	// Negative: a cutoff in the future excludes every existing run.
	future := time.Now().Add(1 * time.Hour)
	dropped, err := svc.CountRuns(
		context.Background(), RunsFilter{Since: future},
	)
	if err != nil {
		t.Fatalf("CountRuns(future): %v", err)
	}
	if dropped != 0 {
		t.Fatalf("future cutoff kept %d runs, want 0", dropped)
	}
}
