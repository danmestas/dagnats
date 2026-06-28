// runs_duration_test.go covers the real-duration wiring for the runs
// list rows and the run-detail header (console-fidelity B5). Before
// this pass a terminal run rendered a hardcoded "n/a"; now the detail
// page derives the terminal time from the last history event the
// console already reads, and in-flight runs render a clearly-labelled
// elapsed value.
//
// Methodology:
//   - Pure-function unit tests for runRowFromRun (no NATS): assert the
//     real value AND that the retired "n/a" stub is gone.
//   - httptest.Recorder against console.Mount with the in-memory fake
//     for the run-detail page, feeding history events with timestamps.
//   - Min two assertions per test (positive value + negative space).
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
)

// TestRunRow_inFlightShowsLabelledElapsed asserts an in-flight run's
// list row renders a real elapsed value, labelled so it can't be
// confused with a final duration.
func TestRunRow_inFlightShowsLabelledElapsed(t *testing.T) {
	run := dag.WorkflowRun{
		RunID: "run-live", WorkflowID: "alpha",
		Status:    dag.RunStatusRunning,
		CreatedAt: time.Now().Add(-3 * time.Second),
	}
	row := runRowFromRun(run)
	if !strings.Contains(row.Duration, "elapsed") {
		t.Fatalf("in-flight duration not labelled elapsed: %q", row.Duration)
	}
	if row.Duration == "n/a" {
		t.Fatalf("in-flight run still renders n/a stub")
	}
}

// TestRunRow_terminalWithoutEndTimestampIsHonest asserts a terminal
// run's list row — where the snapshot carries no terminal timestamp —
// falls back to the honest "—" placeholder, NOT the retired "n/a".
func TestRunRow_terminalWithoutEndTimestampIsHonest(t *testing.T) {
	run := dag.WorkflowRun{
		RunID: "run-done", WorkflowID: "alpha",
		Status:    dag.RunStatusCompleted,
		CreatedAt: time.Now().Add(-10 * time.Second),
	}
	row := runRowFromRun(run)
	if row.Duration == "n/a" {
		t.Fatalf("terminal list row still renders n/a stub: %q", row.Duration)
	}
	if row.Duration != "—" {
		t.Fatalf("terminal list row duration = %q, want — (data absent)", row.Duration)
	}
}

// TestRunRow_terminalWithCompletedAt asserts a terminal run whose
// snapshot now carries CompletedAt renders the real wall-clock
// duration (CompletedAt − CreatedAt) instead of the honest "—"
// fallback. The engine stamps CompletedAt on every terminal path, so
// the list can show the final duration without per-run history.
func TestRunRow_terminalWithCompletedAt(t *testing.T) {
	created := time.Now().Add(-time.Minute)
	completed := created.Add(5 * time.Second)
	run := dag.WorkflowRun{
		RunID: "run-done", WorkflowID: "alpha",
		Status:      dag.RunStatusCompleted,
		CreatedAt:   created,
		CompletedAt: &completed,
	}
	row := runRowFromRun(run)
	if row.Duration == "—" {
		t.Fatalf("terminal run with CompletedAt still renders — placeholder")
	}
	if row.Duration != "5s" {
		t.Fatalf("terminal list row duration = %q, want 5s", row.Duration)
	}
}

// TestRunDetail_terminalDurationFromEvents drives the run-detail page
// for a completed run with a terminal history event 5s after creation
// and asserts the page renders a real 5s duration — not the "n/a" stub.
func TestRunDetail_terminalDurationFromEvents(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("alpha")}
	created := time.Now().Add(-time.Minute)
	fake.runs = []dag.WorkflowRun{{
		RunID: "run-term", WorkflowID: "alpha",
		Status: dag.RunStatusCompleted, CreatedAt: created,
	}}
	fake.events["run-term"] = []api.RunEvent{
		{Type: "workflow.started", RunID: "run-term", Timestamp: created},
		{
			Type: "workflow.completed", RunID: "run-term",
			Timestamp: created.Add(5 * time.Second),
		},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/runs/run-term", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	// Positive: the real 5s terminal duration reaches the header.
	if !strings.Contains(body, "5s") {
		t.Fatalf("missing real 5s duration in run detail: %s", body)
	}
	// Negative space: the retired stub is gone.
	if strings.Contains(body, ">n/a<") {
		t.Fatalf("run detail still renders n/a stub")
	}
}
