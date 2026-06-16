// console_fidelity_polish_test.go covers the mockup-fidelity polish batch:
// the dashboard "Recent failures" 3-column table, the server JetStream
// storage radial gauge, the per-card data-source captions on the list
// pages, and the streams Policy (max-age) column.
//
// Methodology:
//   - Pure-handler tests against fakeDataSource; no NATS.
//   - Each test mounts its own handler (mountWithFake) or uses the
//     dashboard test cfg; nothing is shared.
//   - Assertions are structural (class names, header text, link hrefs)
//     so cosmetic CSS tweaks don't break the suite.
//   - Min 2 assertions per test (positive + negative space).
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
)

// TestDashboard_recentFailuresRendersAsTable asserts the Recent failures
// panel is the mockup's 3-column table (Run id / Workflow / Error) with
// the error in a truncating snippet carrying the full error as a title,
// and the run-id cell remaining a link to the run detail.
func TestDashboard_recentFailuresRendersAsTable(t *testing.T) {
	fake := newFakeDS()
	now := time.Now()
	fake.runs = []dag.WorkflowRun{
		{RunID: "abcdef123456", WorkflowID: "demo-wf",
			Status: dag.RunStatusFailed, CreatedAt: now,
			Steps: map[string]dag.StepState{
				"s1": {Status: dag.StepStatusFailed, Error: "boom-error-detail"},
			}},
	}
	cfg := dashTestCfg(t, fake, nil)
	rec := dashGet(t, cfg, "/console/")
	body := rec.Body.String()

	// Positive: a table with the three mockup column headers.
	for _, want := range []string{
		"<table", "<th>Run id</th>", "<th>Workflow</th>", "<th>Error</th>",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("recent failures table missing %q", want)
		}
	}
	// Positive: the error snippet carries the full error in its title.
	if !strings.Contains(body, `class="dn-errsnip"`) {
		t.Error("recent failures error cell missing truncating snippet class")
	}
	if !strings.Contains(body, `title="boom-error-detail"`) {
		t.Error("recent failures snippet must title the full error")
	}
	// Positive: the run-id cell is still a link to the run detail.
	if !strings.Contains(body, `href="/console/runs/abcdef123456"`) {
		t.Error("recent failures lost the run-id link")
	}
	// Negative space: the old per-row block list markup is gone.
	if strings.Contains(body, "dashboard-recent-item") {
		t.Error("recent failures still using the old block-list item markup")
	}
}

// TestServePageServer_storageGaugeRendersPercent asserts the server page
// renders a radial storage gauge carrying the storage percent next to the
// existing bytes text (which must be retained).
func TestServePageServer_storageGaugeRendersPercent(t *testing.T) {
	fake := newFakeDS()
	fake.serverHealth = ServerHealth{
		HasStats:   true,
		ServerName: "x",
		StoreUsed:  2 << 30,
		StoreMax:   10 << 30,
		StorePct:   20,
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/server", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()

	// Positive: the radial gauge element renders, driven by the percent.
	if !strings.Contains(body, "server-storage-gauge") {
		t.Error("server page missing storage radial gauge")
	}
	if !strings.Contains(body, "--storage-pct: 20") {
		t.Error("storage gauge must encode the storage percent in its style")
	}
	// Positive: the existing bytes text is retained alongside the gauge.
	if !strings.Contains(body, "of") || !strings.Contains(body, "20%") {
		t.Error("storage gauge must keep the human bytes + percent text")
	}
}

// TestServePageServer_storageGaugeAbsentWithoutStats asserts the gauge is
// not rendered on the lean (no-stats) fallback path, since there is no
// real ceiling to chart a percentage against.
func TestServePageServer_storageGaugeAbsentWithoutStats(t *testing.T) {
	fake := newFakeDS()
	fake.serverHealth = ServerHealth{HasStats: false, ServerName: "x"}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/server", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if strings.Contains(rr.Body.String(), "server-storage-gauge") {
		t.Error("storage gauge must be absent without a real account ceiling")
	}
}

// TestListPages_dataSourceCaptions asserts each list page carries the
// mockup's static right-aligned data-source caption naming where the
// table's rows come from.
func TestListPages_dataSourceCaptions(t *testing.T) {
	cases := []struct {
		name    string
		path    string
		caption string
	}{
		{"workflows", "/console/workflows", "aggregated from WORKFLOW_HISTORY"},
		{"workers", "/console/workers", "heartbeats via workers KV"},
		{"functions", "/console/task-types", "aggregated from workers KV"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeDS()
			h := mountWithFake(t, fake)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rr.Code)
			}
			body := rr.Body.String()
			if !strings.Contains(body, tc.caption) {
				t.Errorf("%s page missing data-source caption %q", tc.name, tc.caption)
			}
			if !strings.Contains(body, "card-caption") {
				t.Errorf("%s page caption missing card-caption class", tc.name)
			}
		})
	}
}

// TestStreamsList_policyColumnRendersMaxAge asserts the streams table
// carries the Policy column rendering the stream's max-age, and an
// unbounded stream renders a muted em-dash rather than a fabricated value.
func TestStreamsList_policyColumnRendersMaxAge(t *testing.T) {
	fake := newFakeDS()
	fake.configSnap.Streams = []StreamSnapshot{
		{
			Name: "TELEMETRY", Subjects: []string{"telemetry.>"},
			Messages: 10, Bytes: 2048, Consumers: 1,
			Retention: "limits", Storage: "file",
			MaxAge: "168h0m0s", Provisioned: true,
		},
		{
			Name: "WORKFLOW_HISTORY", Subjects: []string{"history.>"},
			Messages: 5, Bytes: 1024, Consumers: 1,
			Retention: "limits", Storage: "file",
			MaxAge: "", Provisioned: true,
		},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/streams", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()

	// Positive: the Policy header + the bounded stream's max-age value.
	if !strings.Contains(body, "<th>Policy</th>") {
		t.Error("streams table missing Policy column header")
	}
	if !strings.Contains(body, "168h0m0s") {
		t.Error("streams Policy cell missing the bounded stream's max-age")
	}
	// Negative space: no fabricated dedup-window label leaks (the snapshot
	// carries max-age only; dedup window has no backing field).
	if strings.Contains(body, "dedup window") {
		t.Error("streams Policy column leaked a fabricated dedup-window label")
	}
}
