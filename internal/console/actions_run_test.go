package console

// actions_run_test.go covers the POST /console/runs/{id}/cancel and
// /console/runs/{id}/signal mutation handlers (Run Signal / Cancel,
// wired to the existing engine api.Service.CancelRun / SendSignal).
//
// Methodology: pure handler tests against the in-memory fakeDataSource
// via mountWithFakeRO (AuthLoopback, so CSRF is bypassed) and httptest.
// Each case asserts both positive space (status + side-effect recorded)
// and negative space (the engine method was NOT called when the request
// was denied / invalid, or the audit outcome is denied / failed). One
// CSRF case uses mountWithFakeAuth(AuthForwarded) because the loopback
// harness cannot exercise the CSRF middleware that gates real
// deployments — a prod-only 403 would otherwise pass every test here.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/dag"
)

// runningRun returns a non-terminal run snapshot the cancel/signal
// handlers will accept.
func runningRun(id string) dag.WorkflowRun {
	return dag.WorkflowRun{
		RunID:      id,
		WorkflowID: "wf",
		Status:     dag.RunStatusRunning,
	}
}

func TestRunCancel_success(t *testing.T) {
	fake := newFakeDS()
	fake.runs = []dag.WorkflowRun{runningRun("run-1")}
	h := mountWithFakeRO(t, fake, false)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost,
		"/console/runs/run-1/cancel", nil)
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	if len(fake.cancelRunCalls) != 1 || fake.cancelRunCalls[0] != "run-1" {
		t.Fatalf("cancelRunCalls = %v, want [run-1]", fake.cancelRunCalls)
	}
	if !strings.Contains(rr.Body.String(), `"action":"cancel"`) {
		t.Errorf("body missing action=cancel; body=%s", rr.Body.String())
	}
	if len(fake.auditEvents) == 0 ||
		fake.auditEvents[0].Outcome != OutcomeSuccess.String() {
		t.Errorf("want success audit row; got %v", fake.auditEvents)
	}
}

func TestRunCancel_terminalRejected(t *testing.T) {
	fake := newFakeDS()
	done := runningRun("run-1")
	done.Status = dag.RunStatusCompleted
	fake.runs = []dag.WorkflowRun{done}
	h := mountWithFakeRO(t, fake, false)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost,
		"/console/runs/run-1/cancel", nil)
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rr.Code)
	}
	if len(fake.cancelRunCalls) != 0 {
		t.Errorf("CancelRun must not be called on terminal run; got %v",
			fake.cancelRunCalls)
	}
	if len(fake.auditEvents) == 0 ||
		fake.auditEvents[0].Data["reason"] != "terminal" {
		t.Errorf("want denied reason=terminal audit; got %v", fake.auditEvents)
	}
}

func TestRunCancel_readOnly(t *testing.T) {
	fake := newFakeDS()
	fake.runs = []dag.WorkflowRun{runningRun("run-1")}
	h := mountWithFakeRO(t, fake, true)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost,
		"/console/runs/run-1/cancel", nil)
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "console_read_only") {
		t.Errorf("body missing console_read_only; body=%s", rr.Body.String())
	}
	if len(fake.cancelRunCalls) != 0 {
		t.Errorf("CancelRun must not be called in read-only; got %v",
			fake.cancelRunCalls)
	}
	if len(fake.auditEvents) == 0 ||
		fake.auditEvents[0].Data["reason"] != "read_only" {
		t.Errorf("want denied reason=read_only audit; got %v", fake.auditEvents)
	}
}

func TestRunCancel_unknownRun(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFakeRO(t, fake, false)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost,
		"/console/runs/ghost/cancel", nil)
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rr.Code)
	}
	if len(fake.cancelRunCalls) != 0 {
		t.Errorf("CancelRun must not be called for unknown run; got %v",
			fake.cancelRunCalls)
	}
	if len(fake.auditEvents) == 0 ||
		fake.auditEvents[0].Data["reason"] != "not_found" {
		t.Errorf("want failed reason=not_found audit; got %v", fake.auditEvents)
	}
}

func TestRunCancel_methodNotAllowed(t *testing.T) {
	fake := newFakeDS()
	fake.runs = []dag.WorkflowRun{runningRun("run-1")}
	h := mountWithFakeRO(t, fake, false)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet,
		"/console/runs/run-1/cancel", nil)
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rr.Code)
	}
	if rr.Header().Get("Allow") != "POST" {
		t.Errorf("Allow = %q, want POST", rr.Header().Get("Allow"))
	}
	if len(fake.cancelRunCalls) != 0 {
		t.Errorf("CancelRun must not be called on GET; got %v",
			fake.cancelRunCalls)
	}
}

func TestRunCancel_engineError(t *testing.T) {
	fake := newFakeDS()
	fake.runs = []dag.WorkflowRun{runningRun("run-1")}
	fake.cancelRunErr = errNotFound("publish", "boom")
	h := mountWithFakeRO(t, fake, false)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost,
		"/console/runs/run-1/cancel", nil)
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rr.Code)
	}
	if len(fake.auditEvents) == 0 ||
		fake.auditEvents[0].Outcome != OutcomeFailed.String() {
		t.Errorf("want failed audit row; got %v", fake.auditEvents)
	}
}

func TestRunSignal_success(t *testing.T) {
	fake := newFakeDS()
	fake.runs = []dag.WorkflowRun{runningRun("run-1")}
	h := mountWithFakeRO(t, fake, false)

	body := strings.NewReader("name=approve&data=" +
		`{"ok":true}`)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost,
		"/console/runs/run-1/signal", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	if len(fake.signalCalls) != 1 {
		t.Fatalf("signalCalls = %v, want 1", fake.signalCalls)
	}
	got := fake.signalCalls[0]
	if got.RunID != "run-1" || got.Name != "approve" {
		t.Errorf("signal call = %+v, want run-1/approve", got)
	}
	if string(got.Data) != `{"ok":true}` {
		t.Errorf("signal data = %q, want JSON payload", string(got.Data))
	}
	if len(fake.auditEvents) == 0 ||
		fake.auditEvents[0].Data["signal_name"] != "approve" {
		t.Errorf("want success signal_name audit; got %v", fake.auditEvents)
	}
}

func TestRunSignal_missingName(t *testing.T) {
	fake := newFakeDS()
	fake.runs = []dag.WorkflowRun{runningRun("run-1")}
	h := mountWithFakeRO(t, fake, false)

	body := strings.NewReader("name=&data=")
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost,
		"/console/runs/run-1/signal", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	if len(fake.signalCalls) != 0 {
		t.Errorf("SendSignal must not be called without name; got %v",
			fake.signalCalls)
	}
}

func TestRunSignal_invalidJSON(t *testing.T) {
	fake := newFakeDS()
	fake.runs = []dag.WorkflowRun{runningRun("run-1")}
	h := mountWithFakeRO(t, fake, false)

	body := strings.NewReader("name=go&data=not-json{")
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost,
		"/console/runs/run-1/signal", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	if len(fake.signalCalls) != 0 {
		t.Errorf("SendSignal must not be called with bad JSON; got %v",
			fake.signalCalls)
	}
	if len(fake.auditEvents) == 0 ||
		fake.auditEvents[0].Data["reason"] != "invalid_json" {
		t.Errorf("want failed reason=invalid_json audit; got %v",
			fake.auditEvents)
	}
}

func TestRunSignal_terminalRejected(t *testing.T) {
	fake := newFakeDS()
	done := runningRun("run-1")
	done.Status = dag.RunStatusCompleted
	fake.runs = []dag.WorkflowRun{done}
	h := mountWithFakeRO(t, fake, false)

	body := strings.NewReader("name=approve&data=")
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost,
		"/console/runs/run-1/signal", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rr.Code)
	}
	if len(fake.signalCalls) != 0 {
		t.Errorf("SendSignal must not be called on terminal run; got %v",
			fake.signalCalls)
	}
}

func TestRunSignal_readOnly(t *testing.T) {
	fake := newFakeDS()
	fake.runs = []dag.WorkflowRun{runningRun("run-1")}
	h := mountWithFakeRO(t, fake, true)

	body := strings.NewReader("name=approve&data=")
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost,
		"/console/runs/run-1/signal", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rr.Code)
	}
	if len(fake.signalCalls) != 0 {
		t.Errorf("SendSignal must not be called in read-only; got %v",
			fake.signalCalls)
	}
	if len(fake.auditEvents) == 0 ||
		fake.auditEvents[0].Data["reason"] != "read_only" {
		t.Errorf("want denied reason=read_only audit; got %v", fake.auditEvents)
	}
}

// TestRunActions_jsonInjectionHardening proves the marshalled-body path:
// a run id / signal name carrying a double-quote and backslash must not
// corrupt the JSON envelope. If the handler fmt.Sprintf'd these into a
// template the body would no longer parse.
func TestRunActions_jsonInjectionHardening(t *testing.T) {
	nasty := `run"\x`
	fake := newFakeDS()
	fake.runs = []dag.WorkflowRun{runningRun(nasty)}
	h := mountWithFakeRO(t, fake, false)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost,
		"/console/runs/"+nasty+"/cancel", nil)
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	var parsed map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("cancel body not valid JSON: %v; body=%s",
			err, rr.Body.String())
	}
	if parsed["action"] != "cancel" {
		t.Errorf("parsed action = %v, want cancel", parsed["action"])
	}
}

// renderRunDetail GETs the run-detail page for a seeded run and returns
// the rendered body.
func renderRunDetail(
	t *testing.T, fake *fakeDataSource, readOnly bool, runID string,
) string {
	t.Helper()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("wf")}
	h := mountWithFakeRO(t, fake, readOnly)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/runs/"+runID, nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	return rr.Body.String()
}

func TestRunDetail_rendersSignalAndCancelForRunningRun(t *testing.T) {
	fake := newFakeDS()
	fake.runs = []dag.WorkflowRun{runningRun("run-1")}
	body := renderRunDetail(t, fake, false, "run-1")

	for _, sub := range []string{
		`id="run-cancel-btn"`,
		`id="run-signal-btn"`,
		`href="/console/runs/run-1/trace"`,
	} {
		if !strings.Contains(body, sub) {
			t.Errorf("running run detail missing %q", sub)
		}
	}
	// Negative space: a running run's controls must not be disabled.
	if strings.Contains(body, "aria-disabled=\"true\"") {
		t.Errorf("running run controls wrongly disabled; body=%s", body)
	}
}

func TestRunDetail_omitsCancelForTerminalRun(t *testing.T) {
	fake := newFakeDS()
	done := runningRun("run-1")
	done.Status = dag.RunStatusCompleted
	fake.runs = []dag.WorkflowRun{done}
	body := renderRunDetail(t, fake, false, "run-1")

	// Honesty: no dead affordance on a run that can't act on them.
	if strings.Contains(body, `id="run-cancel-btn"`) {
		t.Errorf("terminal run wrongly renders Cancel button")
	}
	if strings.Contains(body, `id="run-signal-btn"`) {
		t.Errorf("terminal run wrongly renders Signal button")
	}
	// The trace link stays — it's a real, always-available read surface.
	if !strings.Contains(body, `href="/console/runs/run-1/trace"`) {
		t.Errorf("terminal run missing trace link")
	}
}

func TestRunDetail_readOnlyDisablesControls(t *testing.T) {
	fake := newFakeDS()
	fake.runs = []dag.WorkflowRun{runningRun("run-1")}
	body := renderRunDetail(t, fake, true, "run-1")

	if !strings.Contains(body, `id="run-cancel-btn"`) ||
		!strings.Contains(body, `id="run-signal-btn"`) {
		t.Fatalf("read-only run detail missing the controls; body=%s", body)
	}
	if strings.Count(body, `aria-disabled="true"`) < 2 {
		t.Errorf("read-only controls not disabled; body=%s", body)
	}
}

// TestRunSignal_csrfForwardAuth exercises the CSRF middleware that the
// loopback harness bypasses: a forwarded-auth signal POST must 403
// without a token and 200 with the correct token sent as the
// X-CSRF-Token header (the same transport the signal form uses).
func TestRunSignal_csrfForwardAuth(t *testing.T) {
	fake := newFakeDS()
	fake.runs = []dag.WorkflowRun{runningRun("run-1")}
	h := mountWithFakeAuth(t, fake, AuthForwarded)

	// Missing token -> 403, no engine call.
	rrNo := httptest.NewRecorder()
	reqNo := httptest.NewRequest(http.MethodPost,
		"/console/runs/run-1/signal",
		strings.NewReader("name=approve&data="))
	reqNo.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqNo.Header.Set("X-Forwarded-User", "alice")
	h.ServeHTTP(rrNo, reqNo)
	if rrNo.Code != http.StatusForbidden {
		t.Fatalf("missing-token status = %d, want 403", rrNo.Code)
	}
	if len(fake.signalCalls) != 0 {
		t.Errorf("SendSignal must not be called without CSRF token; got %v",
			fake.signalCalls)
	}

	// Valid token -> 200, engine call recorded.
	good := CSRFTokenForActor(Actor{User: "alice", Source: AuthForwarded})
	rrOK := httptest.NewRecorder()
	reqOK := httptest.NewRequest(http.MethodPost,
		"/console/runs/run-1/signal",
		strings.NewReader("name=approve&data="))
	reqOK.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqOK.Header.Set("X-Forwarded-User", "alice")
	reqOK.Header.Set("X-CSRF-Token", good)
	h.ServeHTTP(rrOK, reqOK)
	if rrOK.Code != http.StatusOK {
		t.Fatalf("valid-token status = %d, want 200; body=%s",
			rrOK.Code, rrOK.Body.String())
	}
	if len(fake.signalCalls) != 1 {
		t.Errorf("want 1 SendSignal call after valid CSRF; got %v",
			fake.signalCalls)
	}
}
