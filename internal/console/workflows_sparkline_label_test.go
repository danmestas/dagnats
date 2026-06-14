// workflows_sparkline_label_test.go pins the visible header label of the
// per-workflow activity sparkline column in the workflows list table.
//
// Context: the column renders a real 24h sparkline of
// workflow.runs.completed points (when the MetricsSource is wired). The
// mockup (ConsoleViewsObserve.tsx) labels this column "24h trend"; the
// template previously read "Activity (24h)". This is a label-fidelity
// fix — the backing data, canvas, and empty-state contract are unchanged
// (sparkline_render_test.go pins the honest empty cell).
//
// Methodology: httptest + newFakeDS/mountWithFake GET of /console/workflows.
// Positive: the workflows table header reads "24h trend". Negative space:
// the stale "Activity (24h)" label is gone from the workflows page.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/dag"
)

// TestWorkflowsList_sparklineColumnLabel asserts the activity sparkline
// column header matches the mockup label "24h trend" and the stale
// "Activity (24h)" label no longer appears on the workflows page.
func TestWorkflowsList_sparklineColumnLabel(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("alpha")}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/workflows", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()

	if !strings.Contains(body, ">24h trend<") {
		t.Errorf("workflows page missing the %q sparkline column header",
			"24h trend")
	}
	// Negative space: the pre-relabel header must be gone.
	if strings.Contains(body, "Activity (24h)") {
		t.Errorf("workflows page still shows stale %q header",
			"Activity (24h)")
	}
}
