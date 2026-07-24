// api/runs_envelope_test.go
// Tests for the #452 run-list honesty surface: ListRuns
// (runs + total + returned + truncated envelope), CountRuns, and the
// --since filter shared by both.
// Methodology: real embedded NATS server, one per test (no sharing).
// We submit runs through the live service and drive them to a terminal
// Completed state (a fixture worker), then assert the aggregate/envelope
// contract. Waiting for terminal drains the orchestrator's async snapshot
// pipeline so the keys-only count scans read a quiescent store (#570).
package api

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/worker"
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
// registers a single-step workflow named wfName. It also starts a worker
// that immediately completes the task types these tests use (task-a and
// the count-b fixture's task-b), so runs reach a terminal Completed state
// rather than sitting Queued forever. That terminal state is the settled
// signal the count assertions rely on — see waitAllComplete (#570).
func newRunsSvc(t *testing.T, wfName string) *Service {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(orch.Stop)
	w := worker.NewWorker(nc)
	complete := func(ctx worker.TaskContext) error { return ctx.Complete(nil) }
	w.Handle("task-a", complete)
	w.Handle("task-b", complete)
	w.Start()
	t.Cleanup(w.Stop)
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

// waitAllComplete blocks until every run in ids reaches a terminal
// Completed state — the settled signal these count assertions require.
//
// A run is NOT done writing when StartRun returns. The orchestrator
// drives an ASYNC snapshot pipeline per run: create (steps Pending) →
// enqueueReady re-Puts the key (step Queued) → the orchestrator consumes
// its own step.queued event and re-Puts AGAIN → step.completed →
// completion re-Puts once more with status Completed. Every one of those
// Puts, under the History:1 workflow_runs bucket, is a delete-old +
// add-new. CountAll / ListRuns count via a keys-only kv.Keys() scan
// (a DeliverLastPerSubject ordered consumer), which transiently drops a
// key that is mid-Put — so an exact total asserted before the pipeline
// drains flakes low (e.g. "Total = 4, want 5", #570). Counting or
// step-state waits cannot fix this: those intermediate Puts leave the key
// count and the step status unchanged, so they are invisible to the very
// scans that race them. Completed is the LAST write and is observed via
// GetRun — a single-key Load, not a Keys() scan — so once every run is
// Completed the store is quiescent and the count scans are race-free.
func waitAllComplete(t *testing.T, svc *Service, ids []string) {
	t.Helper()
	if svc == nil {
		panic("waitAllComplete: svc must not be nil")
	}
	if len(ids) == 0 {
		panic("waitAllComplete: ids must not be empty")
	}
	for _, id := range ids {
		waitRunStatus(t, svc, id, dag.RunStatusCompleted)
	}
}

// startRuns starts count runs of wfName and returns their run IDs.
func startRuns(
	t *testing.T, svc *Service, wfName string, count int,
) []string {
	t.Helper()
	if wfName == "" {
		panic("startRuns: wfName must not be empty")
	}
	if count <= 0 {
		panic("startRuns: count must be positive")
	}
	ids := make([]string, 0, count)
	for i := 0; i < count; i++ {
		id, err := svc.StartRun(context.Background(), wfName, nil)
		if err != nil {
			t.Fatalf("StartRun %s #%d: %v", wfName, i, err)
		}
		ids = append(ids, id)
	}
	return ids
}

// TestListRunsTruncation proves the envelope reports the true
// total and a truncated flag when limit < total.
func TestListRunsTruncation(t *testing.T) {
	svc := newRunsSvc(t, "env-wf")
	const submitted = 5
	ids := startRuns(t, svc, "env-wf", submitted)
	waitAllComplete(t, svc, ids)

	env, err := svc.ListRuns(
		context.Background(), RunsFilter{}, 2,
	)
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
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
	full, err := svc.ListRuns(
		context.Background(), RunsFilter{}, 100,
	)
	if err != nil {
		t.Fatalf("ListRuns(full): %v", err)
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
	ids := startRuns(t, svc, "count-a", 2)
	ids = append(ids, startRuns(t, svc, "count-b", 1)...)
	waitAllComplete(t, svc, ids)

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
	waitAllComplete(t, svc, startRuns(t, svc, "since-wf", 1))

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
