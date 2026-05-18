// side_sheet_test.go covers the Phase 2 T12 side-sheet endpoints +
// layout wiring.
//
// Methodology:
//   - Pure handler tests against fakeDataSource (no NATS).
//   - Each test mounts its own console.Mount.
//   - Assertions check the rendered fragment shape (sidesheet root,
//     section content, full-page href) and the layout outlet contract
//     (#sheet-outlet present on every page).
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

// TestSideSheet_dlqSheetFragmentRenders asserts /console/api/dlq/<seq>/sheet
// renders the side-sheet shell with the DLQ entry detail inside the
// body partial.
func TestSideSheet_dlqSheetFragmentRenders(t *testing.T) {
	fake := newFakeDS()
	dl := sampleDeadLetter(42, "timeout: deadline exceeded")
	dl.Task = "task.demo.first"
	dl.RunID = "run-abc-7777"
	fake.deadLetters = []api.DeadLetterView{dl}
	h := mountWithFake(t, fake)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet,
		"/console/api/dlq/42/sheet", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q",
			rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, `class="sidesheet"`) {
		t.Errorf("missing sidesheet root class\n--body--\n%s", body)
	}
	if !strings.Contains(body, "timeout") {
		t.Errorf("missing DLQ entry detail in sheet")
	}
	if !strings.Contains(body, `href="/console/dlq/42"`) {
		t.Errorf("missing Open-in-full-page link to /console/dlq/42")
	}
}

// TestSideSheet_runSheetFragmentRenders asserts /console/api/runs/<id>/sheet
// renders the side-sheet shell with the run detail inside the body
// partial.
func TestSideSheet_runSheetFragmentRenders(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("demo")}
	fake.runs = []dag.WorkflowRun{
		{
			RunID:      "run-xyz-1234",
			WorkflowID: "demo",
			Status:     dag.RunStatusCompleted,
			CreatedAt:  time.Now().Add(-2 * time.Minute),
		},
	}
	h := mountWithFake(t, fake)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet,
		"/console/api/runs/run-xyz-1234/sheet", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q",
			rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, `class="sidesheet"`) {
		t.Errorf("missing sidesheet root class")
	}
	if !strings.Contains(body, "demo") {
		t.Errorf("missing workflow id in sheet body")
	}
	if !strings.Contains(body, `href="/console/runs/run-xyz-1234"`) {
		t.Errorf("missing Open-in-full-page link")
	}
}

// TestSideSheet_includesFullPageLink covers both endpoints in one
// pass — every sheet must surface the escape hatch to the full page.
func TestSideSheet_includesFullPageLink(t *testing.T) {
	fake := newFakeDS()
	fake.deadLetters = []api.DeadLetterView{sampleDeadLetter(99, "panic")}
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("demo")}
	fake.runs = []dag.WorkflowRun{{
		RunID: "r-1", WorkflowID: "demo",
		Status: dag.RunStatusFailed, CreatedAt: time.Now(),
	}}
	h := mountWithFake(t, fake)
	cases := []struct {
		path string
		href string
	}{
		{"/console/api/dlq/99/sheet", `/console/dlq/99`},
		{"/console/api/runs/r-1/sheet", `/console/runs/r-1`},
	}
	for _, tc := range cases {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if rec.Code != http.StatusOK {
			t.Errorf("%s status = %d, want 200", tc.path, rec.Code)
			continue
		}
		if !strings.Contains(rec.Body.String(), tc.href) {
			t.Errorf("%s missing full-page href %q", tc.path, tc.href)
		}
		if !strings.Contains(rec.Body.String(), "Open in full page") {
			t.Errorf("%s missing Open-in-full-page button text", tc.path)
		}
	}
}

// TestSheetOutletMountedInLayout asserts every console page renders the
// outlet div so the patched sheet has a stable target. The outlet must
// live on every page that might surface the Inspect / row trigger.
func TestSheetOutletMountedInLayout(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("demo")}
	fake.runs = []dag.WorkflowRun{
		{
			RunID: "r-1", WorkflowID: "demo",
			Status: dag.RunStatusCompleted, CreatedAt: time.Now(),
		},
	}
	h := mountWithFake(t, fake)
	pages := []string{
		"/console/",
		"/console/workflows",
		"/console/runs",
		"/console/triggers",
		"/console/dlq",
	}
	for _, p := range pages {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, p, nil))
		if rec.Code != http.StatusOK {
			t.Errorf("%s status = %d", p, rec.Code)
			continue
		}
		if !strings.Contains(rec.Body.String(), `id="sheet-outlet"`) {
			t.Errorf("%s missing #sheet-outlet div", p)
		}
	}
}
