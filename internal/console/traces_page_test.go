// traces_page_test.go exercises the cross-run Traces page: the list at
// /console/traces (one row per run, projected from the runs the console
// already reads), the per-trace detail at /console/traces/<runID> (which
// reuses the shared trace-tree component), the logs trace-id linkage, and
// the Traces nav item.
//
// Methodology:
//   - Pure handler tests against fakeDataSource (no NATS).
//   - Each subtest builds its own fake; tests never share state.
//   - Assertions look for stable substrings (labels, hrefs) so cosmetic
//     tweaks don't break the contract.
//   - Every test asserts positive space (the datum is present) AND
//     negative space (an empty/absent datum is honestly omitted, never
//     fabricated).
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
)

// seedTraceRuns returns two runs the traces list projects into rows. One
// completed, one failed, so the status pills exercise both branches.
func seedTraceRuns() []dag.WorkflowRun {
	return []dag.WorkflowRun{
		{
			RunID:      "run-aaaaaaaaaaaa-1",
			WorkflowID: "image-pipeline",
			Status:     dag.RunStatusCompleted,
			Steps:      map[string]dag.StepState{"a": {}, "b": {}},
			CreatedAt:  time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		},
		{
			RunID:      "run-bbbbbbbbbbbb-2",
			WorkflowID: "nightly-report",
			Status:     dag.RunStatusFailed,
			Steps:      map[string]dag.StepState{"x": {}},
			CreatedAt:  time.Date(2026, 1, 2, 3, 5, 5, 0, time.UTC),
		},
	}
}

func TestTracesListRendersRows(t *testing.T) {
	fake := newFakeDS()
	fake.runs = seedTraceRuns()
	h := mountWithFakeRO(t, fake, false)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(
		http.MethodGet, "/console/traces", nil,
	))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	// Positive space: both runs render with workflow + a row link keyed
	// on the runID (the join key the detail page resolves).
	for _, want := range []string{
		"image-pipeline", "nightly-report",
		`href="/console/traces/run-aaaaaaaaaaaa-1"`,
		`href="/console/traces/run-bbbbbbbbbbbb-2"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("body missing %q", want)
		}
	}
	// Both status branches render their pills.
	if !strings.Contains(body, "status-ok") {
		t.Errorf("body missing status-ok pill for completed run")
	}
	if !strings.Contains(body, "status-failed") {
		t.Errorf("body missing status-failed pill for failed run")
	}
}

func TestTracesListEmptyState(t *testing.T) {
	fake := newFakeDS()
	// No runs seeded.
	h := mountWithFakeRO(t, fake, false)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(
		http.MethodGet, "/console/traces", nil,
	))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	// Negative space: no fabricated trace rows, an honest empty message.
	if strings.Contains(body, `href="/console/traces/run-`) {
		t.Errorf("empty state must not fabricate trace rows")
	}
	if !strings.Contains(body, "No traces") {
		t.Errorf("body missing honest empty-state copy; got:\n%s", body)
	}
}

func TestTracesListStatusFilter(t *testing.T) {
	fake := newFakeDS()
	fake.runs = seedTraceRuns()
	h := mountWithFakeRO(t, fake, false)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(
		http.MethodGet, "/console/traces?status=failed", nil,
	))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	// Positive: the failed run survives the filter.
	if !strings.Contains(body, "run-bbbbbbbbbbbb-2") {
		t.Errorf("status=failed dropped the failed run")
	}
	// Negative: the completed run is filtered out.
	if strings.Contains(body, "run-aaaaaaaaaaaa-1") {
		t.Errorf("status=failed leaked the completed run")
	}
}

func TestTraceDetailReusesTreeComponent(t *testing.T) {
	fake := newFakeDS()
	fake.runs = seedTraceRuns()
	fake.runTrace = []TraceRow{
		{Depth: 0, Name: "image-pipeline", DurationMs: 120, Status: "ok", SpanID: "s1"},
		{Depth: 1, Name: "resize", DurationMs: 40, Status: "ok", SpanID: "s2"},
	}
	h := mountWithFakeRO(t, fake, false)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(
		http.MethodGet, "/console/traces/run-aaaaaaaaaaaa-1", nil,
	))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	// Positive: the shared trace-tree renders the span names + durations.
	for _, want := range []string{
		"resize", "120ms", "40ms",
		"image-pipeline",                          // workflow in header
		`run-trace-row-s1`,                        // trace-tree row id contract
		`href="/console/runs/run-aaaaaaaaaaaa-1"`, // cross-link to the run
	} {
		if !strings.Contains(body, want) {
			t.Errorf("detail body missing %q", want)
		}
	}
	// Negative: no fabricated Gantt geometry attributes.
	if strings.Contains(body, "offsetPct") || strings.Contains(body, "widthPct") {
		t.Errorf("detail must not fabricate waterfall bar geometry")
	}
}

func TestTraceDetailReadErrorDegrades(t *testing.T) {
	fake := newFakeDS()
	fake.runs = seedTraceRuns()
	fake.runTraceErr = errNotFound("trace", "boom")
	h := mountWithFakeRO(t, fake, false)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(
		http.MethodGet, "/console/traces/run-aaaaaaaaaaaa-1", nil,
	))
	// Positive: degrade to 200 + Note, never 500.
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (degrade-to-note)", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "Trace read failed") {
		t.Errorf("read error must surface an honest Note; got:\n%s", body)
	}
}

func TestTraceDetailNotFound(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFakeRO(t, fake, false)
	for _, path := range []string{"/console/traces/", "/console/traces/a/b"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusNotFound {
			t.Errorf("GET %s status = %d, want 404", path, rec.Code)
		}
	}
}

func TestLogsTraceIDLink(t *testing.T) {
	// A row carrying a trace id renders a link into the traces lookup.
	withID := renderLogRowHTML(LogRow{
		ID: "log-row-1", Level: "info", LevelText: "INFO",
		Message: "did a thing", Source: "engine",
		TraceID: "abc123def456",
	})
	if !strings.Contains(withID, `href="/console/traces?trace_id=abc123def456"`) {
		t.Errorf("trace-id row missing traces link; got:\n%s", withID)
	}
	// Negative: a row with no trace id renders no link.
	noID := renderLogRowHTML(LogRow{
		ID: "log-row-2", Level: "info", LevelText: "INFO",
		Message: "no trace", Source: "engine",
	})
	if strings.Contains(noID, "/console/traces") {
		t.Errorf("row without trace id must not render a traces link")
	}
}

func TestTracesListNoDeadFindAffordance(t *testing.T) {
	// The trace-id -> run reverse index does not exist, so the list
	// cannot find a run by trace id. Honesty rule: no Find control may
	// imply a lookup the code can't perform. The only honest trace-id
	// entry path is the inbound Logs deep-link, which is surfaced as a
	// read-only context callout (no input field, no Find button).
	fake := newFakeDS()
	fake.runs = seedTraceRuns()
	h := mountWithFakeRO(t, fake, false)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(
		http.MethodGet, "/console/traces", nil,
	))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	// Negative: no actionable trace-id Find control (input or button).
	for _, banned := range []string{
		`name="trace_id"`,  // the dead input
		`>Find<`,           // the dead button label
		`Find by trace id`, // the dead field label
	} {
		if strings.Contains(body, banned) {
			t.Errorf("traces list still ships a dead Find affordance: %q", banned)
		}
	}
	// Positive: the working status filter survives.
	if !strings.Contains(body, `name="status"`) {
		t.Errorf("status filter (the one backed control) is missing")
	}
}

func TestTracesListInboundLookupContextChip(t *testing.T) {
	// Arriving from a Logs trace-id link must render the trace id as
	// honest read-only context (not as a populated filter input), with
	// copy that points the operator at opening a run for the span tree.
	fake := newFakeDS()
	fake.runs = seedTraceRuns()
	h := mountWithFakeRO(t, fake, false)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(
		http.MethodGet, "/console/traces?trace_id=abc123def456", nil,
	))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	// Positive: the trace id is echoed as context.
	if !strings.Contains(body, "abc123def456") {
		t.Errorf("inbound trace id not surfaced as context; got:\n%s", body)
	}
	// Negative: it must NOT be echoed into an editable input value.
	if strings.Contains(body, `value="abc123def456"`) {
		t.Errorf("inbound trace id must be context, not a populated input")
	}
}

func TestTracesNavItemPresent(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/console/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `href="/console/traces"`) {
		t.Errorf("layout missing Traces nav link")
	}
	// Negative: no traces nav-count badge (no cheap honest count exists).
	if strings.Contains(body, `data-nav-count="traces"`) {
		t.Errorf("Traces nav must not carry a count badge")
	}
}
