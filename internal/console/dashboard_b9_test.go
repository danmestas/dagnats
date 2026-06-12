// dashboard_b9_test.go covers the Batch-9 dashboard remediation:
//   - ITEM 1: the topbar tile strip is compact (number + label, no
//     "click to drill" hint prose), with the 4th SUCCESS·24H tile
//     gated honestly — present only when MetricsSource has run data,
//     omitted (never a fabricated %) when it doesn't.
//   - ITEM 3: the Recent failures panel surfaces real failed runs from
//     the data source (Run id, Workflow, Error red mono snippet), and
//     an honest empty state when there are none.
//
// Methodology: pure-handler renders against newFakeDS() (+ optional
// fakeMetricsSource), asserting structural HTML facts. Min 2 assertions
// per test (positive + negative space).
package console

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
)

// TestDashboardTilesAreCompact asserts the always-on tiles dropped the
// "click to drill" hint prose (compact form) but remain clickable links
// to their drill-down lists. Positive: the failed-1h tile links to the
// filtered runs list. Negative: no "click to drill" text anywhere.
func TestDashboardTilesAreCompact(t *testing.T) {
	fake := newFakeDS()
	cfg := dashTestCfg(t, fake, nil)
	rec := dashGet(t, cfg, "/console/")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `href="/console/runs?status=failed&amp;range=1h"`) {
		t.Errorf("failed-1h tile must stay linked to its drill-down list")
	}
	if strings.Contains(body, "click to drill") {
		t.Errorf("compact tiles must drop the 'click to drill' hint prose")
	}
}

// TestDashboardSuccessTilePresentWithMetrics asserts the SUCCESS·24H
// tile (tile-success-rate) renders when the metrics source carries
// completed/failed run counts. Positive: the tile id is present.
// Negative: it is NOT marked empty/placeholder.
func TestDashboardSuccessTilePresentWithMetrics(t *testing.T) {
	src := newFakeMetricsSource()
	now := time.Now()
	src.addCounter("workflow.runs.completed", 90, now)
	src.addCounter("workflow.runs.failed", 1, now)
	fake := newFakeDS()
	cfg := dashTestCfg(t, fake, src)
	rec := dashGet(t, cfg, "/console/")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `id="tile-success-rate"`) {
		t.Errorf("success-rate tile must render when metrics have data")
	}
	if strings.Contains(body, "telemetry pending") {
		t.Errorf("success tile must not show a placeholder when it has data")
	}
}

// TestDashboardSuccessTileOmittedWithoutMetrics asserts the honest gate:
// with no MetricsSource the SUCCESS·24H tile is omitted entirely rather
// than rendering a fabricated %. Positive: the three always-on tiles
// render. Negative: no success-rate tile, no fake percentage.
func TestDashboardSuccessTileOmittedWithoutMetrics(t *testing.T) {
	fake := newFakeDS()
	cfg := dashTestCfg(t, fake, nil)
	rec := dashGet(t, cfg, "/console/")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `id="tile-failed-1h"`) {
		t.Errorf("always-on failed-1h tile must render")
	}
	if strings.Contains(body, `id="tile-success-rate"`) {
		t.Errorf("success tile must be omitted when metrics are unwired")
	}
}

// TestDashboardRecentFailuresRendersRealRows asserts the Recent failures
// panel surfaces real failed runs from the data source — the run id, the
// workflow, and the step error in the red mono snippet. Positive: the
// failed run's id + workflow + error appear. Negative: a completed run
// does NOT appear in the failures panel.
func TestDashboardRecentFailuresRendersRealRows(t *testing.T) {
	now := time.Now()
	fake := newFakeDS()
	fake.runs = []dag.WorkflowRun{
		{RunID: "fail-abc123", WorkflowID: "retry-errors",
			Status: dag.RunStatusFailed, CreatedAt: now,
			Steps: map[string]dag.StepState{
				"s1": {Status: dag.StepStatusFailed,
					Error: "dial tcp: connection refused"},
			}},
		{RunID: "ok-def456", WorkflowID: "hello-world",
			Status: dag.RunStatusCompleted, CreatedAt: now},
	}
	cfg := dashTestCfg(t, fake, nil)
	rec := dashGet(t, cfg, "/console/")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	failuresPanel := sliceAfter(body, `id="recent-failures"`)
	for _, want := range []string{
		"retry-errors", "dial tcp: connection refused",
	} {
		if !strings.Contains(failuresPanel, want) {
			t.Errorf("recent-failures panel missing %q", want)
		}
	}
	if strings.Contains(failuresPanel, "ok-def456") {
		t.Errorf("completed run must not appear in the failures panel")
	}
}

// TestDashboardRecentFailuresHonestEmpty asserts that with no failed
// runs the panel renders the honest empty state, not a fabricated row.
func TestDashboardRecentFailuresHonestEmpty(t *testing.T) {
	now := time.Now()
	fake := newFakeDS()
	fake.runs = []dag.WorkflowRun{
		{RunID: "ok-1", WorkflowID: "hello-world",
			Status: dag.RunStatusCompleted, CreatedAt: now},
	}
	cfg := dashTestCfg(t, fake, nil)
	rec := dashGet(t, cfg, "/console/")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	failuresPanel := sliceAfter(body, `id="recent-failures"`)
	if !strings.Contains(failuresPanel, "No failed runs") {
		t.Errorf("recent-failures must show the honest empty state")
	}
	if strings.Contains(failuresPanel, "dashboard-recent-item") {
		t.Errorf("empty failures panel must not render a row item")
	}
}

// sliceAfter returns the substring of body starting at marker, up to the
// next recent-actions panel marker, so a failures-panel assertion never
// matches text that belongs to the operator-actions panel below it.
func sliceAfter(body, marker string) string {
	start := strings.Index(body, marker)
	if start < 0 {
		return ""
	}
	rest := body[start:]
	if end := strings.Index(rest, `id="recent-actions"`); end > 0 {
		return rest[:end]
	}
	return rest
}
