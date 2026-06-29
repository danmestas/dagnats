// jump_to_step_test.go pins the "Jump to step" control on a failed
// run's detail page to an actually-working jump, not a dead anchor.
//
// Methodology:
//   - The failed-run banner previously rendered a bare
//     `<a href="#step-row-{{.FailedStepID}}">` that targeted an id no
//     element carried, inside the (hidden) Timeline panel — so clicking
//     it did nothing. The fix turns it into a `.run-error-jump` button
//     carrying `data-jump-step=<stepid>`, gives the timeline step rows a
//     stable `id="step-row-<id>"`, and wires an inline handler that
//     activates the Timeline tab and scrolls/highlights the row.
//   - Render a real failed run through the handler and assert the new
//     wire: control carries the step id, the dead anchor is gone, the
//     timeline rows are addressable, the handler ships in the page, and
//     the highlight CSS rule exists.
//   - Own Mount per assertion; nothing shared.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
)

// failedRunFixture builds a fake data source with a single failed run
// whose "transform" step failed, so the banner + timeline rows render.
func failedRunFixture(t *testing.T) *fakeDataSource {
	t.Helper()
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{{
		Name:    "demo",
		Version: "v1",
		Steps: []dag.StepDef{
			{ID: "fetch", Task: "echo", Timeout: time.Minute},
			{ID: "transform", Task: "echo", Timeout: time.Minute},
		},
	}}
	fake.runs = []dag.WorkflowRun{
		runWithSteps("run-failed", "demo", dag.RunStatusFailed,
			map[string]dag.StepState{
				"fetch": {Status: dag.StepStatusCompleted, Attempts: 1},
				"transform": {Status: dag.StepStatusFailed,
					Attempts: 3, Error: "boom"},
			}, time.Now().Add(-time.Minute)),
	}
	return fake
}

// TestJumpToStep_controlCarriesStepID asserts the failed-run banner
// renders a `.run-error-jump` control carrying the failed step id via
// `data-jump-step`, and NOT the old dead `href="#step-row-..."` anchor.
// RED on the pre-fix bare-anchor markup.
func TestJumpToStep_controlCarriesStepID(t *testing.T) {
	body := getPage(t, failedRunFixture(t), "/console/runs/run-failed")

	if !strings.Contains(body, `class="btn btn-outline run-error-jump"`) {
		t.Errorf("jump control missing .run-error-jump button class")
	}
	if !strings.Contains(body, `data-jump-step="transform"`) {
		t.Errorf("jump control must carry data-jump-step=transform")
	}
	// Negative space: the dead anchor to a non-existent id must be gone.
	if strings.Contains(body, `href="#step-row-transform"`) {
		t.Errorf("dead jump anchor href=#step-row-transform must be removed")
	}
}

// TestJumpToStep_timelineRowsAreAddressable asserts the timeline step
// rows render a stable `id="step-row-<id>"` so the jump can target them,
// while keeping `data-step-id`. RED on the pre-fix rows (no id).
func TestJumpToStep_timelineRowsAreAddressable(t *testing.T) {
	body := getPage(t, failedRunFixture(t), "/console/runs/run-failed")

	if !strings.Contains(body, `id="step-row-transform"`) {
		t.Errorf("failed timeline row must carry id=step-row-transform")
	}
	if !strings.Contains(body, `id="step-row-fetch"`) {
		t.Errorf("timeline rows must each carry id=step-row-<id>")
	}
	// data-step-id is preserved (the SSE row-update path keys on it).
	if !strings.Contains(body, `data-step-id="transform"`) {
		t.Errorf("timeline row must keep data-step-id alongside the id")
	}
}

// TestJumpToStep_handlerShipsInPage asserts the inline click handler
// that activates the Timeline tab and scrolls/highlights the row ships
// in the run-detail page. RED before the script is added.
func TestJumpToStep_handlerShipsInPage(t *testing.T) {
	body := getPage(t, failedRunFixture(t), "/console/runs/run-failed")

	if !strings.Contains(body, "run-error-jump") {
		t.Fatalf("page missing run-error-jump wiring")
	}
	for _, sub := range []string{
		"data-jump-step",
		"scrollIntoView",
		"step-row-highlight",
	} {
		if !strings.Contains(body, sub) {
			t.Errorf("jump handler missing %q", sub)
		}
	}
}

// TestJumpToStep_highlightCSSExists asserts the served app.css carries
// the .step-row-highlight rule the handler toggles. RED before the rule
// is added.
func TestJumpToStep_highlightCSSExists(t *testing.T) {
	fake := failedRunFixture(t)
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/assets/app.css", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET app.css: status = %d, want 200", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), ".step-row-highlight") {
		t.Errorf("app.css missing .step-row-highlight rule")
	}
}
