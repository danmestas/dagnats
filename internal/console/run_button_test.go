// run_button_test.go pins the inline Run button affordance on the
// workflows list (#329, parent #274 R8 Tier 2). The button must:
//
//  1. Render for workflows with no required input (no InputSchema or
//     an empty `{}` schema).
//  2. Render disabled with a tooltip when the workflow declares a
//     required input schema — typed-input forms are out of R8 scope.
//  3. Render disabled with a tooltip referencing CONSOLE_READ_ONLY
//     when the console is in read-only mode.
//  4. POST /console/workflows/<name>/run → 200, calls StartRun on the
//     DataSource exactly once with the workflow name and an empty
//     input payload, emits an audit event with outcome=success.
//  5. POST under read-only → 405 + console_read_only body, no StartRun
//     call, audit row with outcome=denied.
//  6. POST under a CSRF-protected auth mode without a valid token →
//     403 from the CSRF middleware, no StartRun call.
//
// The DLQ visual-weight bump is a markup-shape regression test: the
// inline Retry / Discard buttons must carry the bumped classname so the
// CSS rule that thickens the border / padding fires.
//
// Methodology:
//   - Use the project's fakeDataSource shape (extended with StartRun
//     tracking + an optional error). One assertion per behaviour facet
//     (success body + audit call) where the test is asserting a pair.
//   - mountWithFakeRO / mountWithFakeAuth replicate the production
//     middleware stack so we exercise CSRF + read-only end-to-end.
//   - Two assertions per test minimum (positive + negative space).
package console

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/dag"
)

// runnableWorkflow builds a one-step demo workflow with no input
// schema. Matches the shape `dagnats demo seed` registers and is the
// canonical "fire from console" target.
func runnableWorkflow(name string) dag.WorkflowDef {
	return dag.WorkflowDef{
		Name:    name,
		Version: "v1",
		Steps:   []dag.StepDef{{ID: "noop", Type: dag.StepTypeNormal, Task: "noop"}},
	}
}

// inputRequiredWorkflow builds a workflow that declares an input
// schema with at least one required property. The Run button must
// hide / disable on these — clicking with an empty payload would
// fail validation at the engine boundary.
func inputRequiredWorkflow(name string) dag.WorkflowDef {
	return dag.WorkflowDef{
		Name:    name,
		Version: "v1",
		Steps:   []dag.StepDef{{ID: "noop", Type: dag.StepTypeNormal, Task: "noop"}},
		InputSchema: []byte(
			`{"type":"object","properties":{"prompt":{"type":"string"}},` +
				`"required":["prompt"]}`,
		),
	}
}

// TestRunButton_RendersForNoInputWorkflow pins the positive branch on
// the workflows fragment: a Run button on a runnable row.
func TestRunButton_RendersForNoInputWorkflow(t *testing.T) {
	set, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	view := WorkflowsListView{
		CSRFToken: "tok",
		ReadOnly:  false,
		Rows: []WorkflowRow{{
			Name:     "demo-noop",
			Version:  "v1",
			Runnable: true,
		}},
	}
	html, err := renderFragment(set.base, "workflows-tbody", view)
	if err != nil {
		t.Fatalf("renderFragment workflows-tbody: %v", err)
	}
	if !strings.Contains(html,
		`data-action-confirm="run"`) {
		t.Errorf("expected Run button hook on runnable row; got:\n%s", html)
	}
	if !strings.Contains(html,
		`action="/console/workflows/demo-noop/run"`) {
		t.Errorf("expected per-row POST form for run; got:\n%s", html)
	}
}

// TestRunButton_HiddenForWorkflowsWithInput pins the negative branch:
// non-runnable rows render a disabled affordance with a tooltip that
// names the reason (required input).
func TestRunButton_HiddenForWorkflowsWithInput(t *testing.T) {
	set, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	view := WorkflowsListView{
		CSRFToken: "tok",
		ReadOnly:  false,
		Rows: []WorkflowRow{{
			Name:     "needs-input",
			Version:  "v1",
			Runnable: false,
		}},
	}
	html, err := renderFragment(set.base, "workflows-tbody", view)
	if err != nil {
		t.Fatalf("renderFragment workflows-tbody: %v", err)
	}
	if strings.Contains(html, `data-action-confirm="run"`) {
		t.Errorf("non-runnable row must NOT carry the run confirm hook; got:\n%s", html)
	}
	if !strings.Contains(html, `aria-disabled="true"`) {
		t.Errorf("non-runnable row must render a disabled affordance; got:\n%s", html)
	}
	if !strings.Contains(html, "required input") {
		t.Errorf("disabled tooltip must explain the reason; got:\n%s", html)
	}
}

// TestRunButton_ReadOnly pins the read-only branch: runnable rows render
// the Run cell disabled and the tooltip names the env var.
func TestRunButton_ReadOnly(t *testing.T) {
	set, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	view := WorkflowsListView{
		CSRFToken: "tok",
		ReadOnly:  true,
		Rows: []WorkflowRow{{
			Name:     "demo-noop",
			Version:  "v1",
			Runnable: true,
		}},
	}
	html, err := renderFragment(set.base, "workflows-tbody", view)
	if err != nil {
		t.Fatalf("renderFragment workflows-tbody: %v", err)
	}
	if strings.Contains(html, `data-action-confirm="run"`) {
		t.Errorf("read-only row must NOT carry the run confirm hook; got:\n%s", html)
	}
	if !strings.Contains(html, "CONSOLE_READ_ONLY") {
		t.Errorf("read-only tooltip must reference the env var; got:\n%s", html)
	}
}

// TestRunButton_StartsRun exercises the happy path: POST returns 200,
// StartRun called once with the workflow name, audit row outcome=success.
func TestRunButton_StartsRun(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{runnableWorkflow("demo-noop")}
	fake.startRunID = "run-xyz"
	h := mountWithFakeRO(t, fake, false)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost,
		"/console/workflows/demo-noop/run", nil)
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	if len(fake.startRunCalls) != 1 ||
		fake.startRunCalls[0].Workflow != "demo-noop" {
		t.Errorf("StartRun calls = %v, want one with demo-noop",
			fake.startRunCalls)
	}
	if len(fake.auditEvents) != 1 ||
		fake.auditEvents[0].Outcome != "success" {
		t.Errorf("expected one success audit; got %+v", fake.auditEvents)
	}
	body := rr.Body.String()
	if !strings.Contains(body, `"run_id":"run-xyz"`) {
		t.Errorf("response body must echo the new run id; got %q", body)
	}
}

// TestRunButton_ReadOnly_RejectsPOST asserts the mutation refusal path
// at the handler boundary: 405 + console_read_only body, no StartRun
// call, audit row with denied outcome.
func TestRunButton_ReadOnly_RejectsPOST(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{runnableWorkflow("demo-noop")}
	h := mountWithFakeRO(t, fake, true)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
		"/console/workflows/demo-noop/run", nil))
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "console_read_only") {
		t.Errorf("expected console_read_only body; got %q", body)
	}
	if len(fake.startRunCalls) != 0 {
		t.Errorf("StartRun must not be called in read-only mode; got %v",
			fake.startRunCalls)
	}
	if len(fake.auditEvents) != 1 ||
		fake.auditEvents[0].Outcome != "denied" {
		t.Errorf("expected one denied audit; got %+v", fake.auditEvents)
	}
}

// TestRunButton_StartFailure asserts the engine-failure path: StartRun
// returns error → 500, audit outcome=failed, no run id in body.
func TestRunButton_StartFailure(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{runnableWorkflow("demo-noop")}
	fake.startRunErr = errors.New("simulated engine failure")
	h := mountWithFakeRO(t, fake, false)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
		"/console/workflows/demo-noop/run", nil))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rr.Code)
	}
	if len(fake.auditEvents) != 1 ||
		fake.auditEvents[0].Outcome != "failed" {
		t.Errorf("expected one failed audit; got %+v", fake.auditEvents)
	}
}

// TestRunButton_CSRFRequired exercises the CSRF guard. Under a non-
// loopback auth mode, a POST without a token must be rejected with 403
// before the handler runs.
func TestRunButton_CSRFRequired(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{runnableWorkflow("demo-noop")}
	h := mountWithFakeAuth(t, fake, AuthForwarded)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost,
		"/console/workflows/demo-noop/run", nil)
	req.Header.Set("X-Forwarded-User", "alice")
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rr.Code)
	}
	if len(fake.startRunCalls) != 0 {
		t.Errorf("StartRun must not be called without CSRF; got %v",
			fake.startRunCalls)
	}
}

// TestRunButton_UnknownWorkflow asserts the missing-workflow path: a
// POST to /console/workflows/<unknown>/run returns 404 and StartRun
// is not invoked.
func TestRunButton_UnknownWorkflow(t *testing.T) {
	fake := newFakeDS()
	// No workflows registered.
	h := mountWithFakeRO(t, fake, false)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
		"/console/workflows/missing/run", nil))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rr.Code)
	}
	if len(fake.startRunCalls) != 0 {
		t.Errorf("StartRun must not be called for missing workflow; got %v",
			fake.startRunCalls)
	}
}

// TestRunButton_NonRunnableRejectedAtAPI guards the server-side
// re-check: even if a client forges a POST against a workflow that
// requires input, the handler rejects the request with 400 and audits
// the denied outcome. The template hides the button, but defence-in-
// depth lives at the handler.
func TestRunButton_NonRunnableRejectedAtAPI(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{inputRequiredWorkflow("needs-input")}
	h := mountWithFakeRO(t, fake, false)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
		"/console/workflows/needs-input/run", nil))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	if len(fake.startRunCalls) != 0 {
		t.Errorf("StartRun must not be called for input-required workflow; got %v",
			fake.startRunCalls)
	}
}

// TestDLQActions_VisualWeightBumped pins the cosmetic bump on the DLQ
// row Retry / Discard buttons. The dlq-tbody fragment must carry the
// bumped classname so the CSS rule fires; absent the marker class, the
// pills fall back to the default visual weight.
func TestDLQActions_VisualWeightBumped(t *testing.T) {
	set, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	view := DLQListView{
		ReadOnly:  false,
		CSRFToken: "tok",
		Rows: []DLQRow{{
			Sequence:      99,
			ReasonShort:   "timeout",
			ReasonFull:    "deadline exceeded",
			Workflow:      "alpha",
			BodyPreserved: true,
		}},
	}
	html, err := renderFragment(set.base, "dlq-tbody", view)
	if err != nil {
		t.Fatalf("renderFragment dlq-tbody: %v", err)
	}
	// The bumped class marker carries the new visual weight; the
	// "btn-heavy" classname is the contract the CSS rule keys on.
	if !strings.Contains(html, "btn-heavy") {
		t.Errorf("dlq retry/discard must carry the bumped btn-heavy class; got:\n%s", html)
	}
	// Positive control — the underlying buttons must still be wired
	// the same so we didn't regress the existing behaviour.
	if !strings.Contains(html, `data-action-confirm="retry"`) {
		t.Errorf("dlq retry action hook missing; got:\n%s", html)
	}
}

// TestRunConfirmModal_Defined pins the new modal partial. The shape
// borrows from dlq-action-modal minus the typed-confirm input; tests
// guard against a regression where the partial drops back to typed
// confirm (R8 explicitly drops that affordance for Run).
func TestRunConfirmModal_Defined(t *testing.T) {
	set, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	html, err := renderFragment(set.base, "run-confirm-modal", nil)
	if err != nil {
		t.Fatalf("renderFragment run-confirm-modal: %v", err)
	}
	if !strings.Contains(html, `id="run-confirm-modal"`) {
		t.Errorf("run-confirm-modal must define the modal root id; got:\n%s", html)
	}
	// Quick confirm — no typed input field. Negative-space check that
	// catches a regression to typed-confirm.
	if strings.Contains(html, `id="run-confirm-input"`) {
		t.Errorf("run-confirm-modal must not include a typed-confirm input; got:\n%s", html)
	}
}
