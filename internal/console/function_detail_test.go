// function_detail_test.go covers the read-only function (task-type)
// detail view: GET /console/functions/{name} renders the function
// identity + health pill, a PROVIDERS table (the worker(s) registered to
// handle the task type, joined to live worker status), a RECENT
// INVOCATIONS table built by cross-referencing the runs the console
// already reads against the workflow defs that reference the function,
// and CONTRACT schema cards surfaced only when exactly one workflow that
// references the function exposes a non-empty schema.
//
// Methodology:
//   - In-memory fakeDataSource feeds configSnap.Workers (drives
//     AggregateTaskTypes + ListWorkerRows), workflows (drives GetWorkflow
//   - ListWorkflows + schema), and runs (drives ListRuns). The same
//     seams the workers/task-types/runs pages read — no second adapter,
//     no fabricated data.
//   - Each test mounts its own console.Mount via mountWithFake; nothing
//     is shared.
//   - Positive value: a function with a live provider renders its real
//     worker id + healthy pill; a referenced run appears in invocations;
//     a single-workflow schema renders. Negative space: an unknown name
//     renders an honest not-found (200 with chrome, no providers table);
//     an unrelated run is absent from invocations; the page never renders
//     the mockup's unbacked features (Invoke, telemetry tiles, sparkline,
//     in-flight column, Duration column).
package console

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/worker"
)

// seedFunctionFake returns a fake with one fresh worker registered to
// handle "billing::charge" so the providers table has a live provider.
func seedFunctionFake() *fakeDataSource {
	fake := newFakeDS()
	fake.configSnap.Workers = []worker.WorkerRegistration{
		{
			WorkerID:  "worker-alpha",
			TaskTypes: []string{"billing::charge"},
			Language:  "go",
			Transport: "nats",
			MaxTasks:  4,
			Hostname:  "host-01",
			LastSeen:  time.Now(),
		},
	}
	return fake
}

// TestFunctionDetail_rendersProviders asserts a function served by a live
// worker renders its name, the provider worker id, and a healthy pill.
func TestFunctionDetail_rendersProviders(t *testing.T) {
	fake := seedFunctionFake()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/functions/billing::charge", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, want := range []string{
		"billing::charge", "worker-alpha", "healthy", "Providers",
		`data-component="page-header"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("function detail missing %q: %s", want, truncBody(body))
		}
	}
}

// TestFunctionDetail_noWorkerAdvisory asserts a function whose owner
// worker is absent from the live worker rows renders the "no worker" pill
// plus an advisory banner rather than a healthy provider.
func TestFunctionDetail_noWorkerAdvisory(t *testing.T) {
	fake := newFakeDS()
	// Owner id references a worker that does NOT appear in the worker
	// rows (configSnap.Workers is empty), so the join finds no live
	// status — the function is registered but unserved.
	fake.taskTypeRows = []TaskTypeRow{
		{
			TaskType:       "orphan::task",
			Service:        "orphan",
			OwnerWorkerIDs: []string{"ghost-worker"},
		},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/functions/orphan::task", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "no worker") {
		t.Errorf("orphan function missing no-worker pill: %s", truncBody(body))
	}
	if !strings.Contains(body, "alert") {
		t.Errorf("orphan function missing advisory banner: %s", truncBody(body))
	}
}

// TestFunctionDetail_recentInvocations asserts a run whose workflow has a
// step referencing the function appears in the invocations table, while
// an unrelated run does not.
func TestFunctionDetail_recentInvocations(t *testing.T) {
	fake := seedFunctionFake()
	fake.workflows = []dag.WorkflowDef{
		{
			Name:  "billing-flow",
			Steps: []dag.StepDef{{ID: "s1", Task: "billing::charge"}},
		},
		{
			Name:  "other-flow",
			Steps: []dag.StepDef{{ID: "s1", Task: "email::send"}},
		},
	}
	fake.runs = []dag.WorkflowRun{
		{
			RunID:      "run-matches-0001",
			WorkflowID: "billing-flow",
			Status:     dag.RunStatusCompleted,
			CreatedAt:  time.Now(),
		},
		{
			RunID:      "run-unrelated-9999",
			WorkflowID: "other-flow",
			Status:     dag.RunStatusCompleted,
			CreatedAt:  time.Now(),
		},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/functions/billing::charge", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "run-matches") {
		t.Errorf("invocations missing matching run: %s", truncBody(body))
	}
	if strings.Contains(body, "run-unrelated") {
		t.Errorf("invocations leaked unrelated run: %s", truncBody(body))
	}
}

// TestFunctionDetail_invocationsEmptyState asserts a function with no
// matching runs renders an honest empty-state row rather than fabricated
// invocations.
func TestFunctionDetail_invocationsEmptyState(t *testing.T) {
	fake := seedFunctionFake()
	// No runs seeded; the invocations section must show empty state.
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/functions/billing::charge", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "Recent invocations") {
		t.Errorf("missing invocations section: %s", truncBody(body))
	}
	if !strings.Contains(body, "console-empty") {
		t.Errorf("missing invocations empty state: %s", truncBody(body))
	}
}

// TestFunctionDetail_schemaFromSingleWorkflow asserts a function
// referenced by exactly one workflow that exposes an InputSchema renders
// the schema, attributed to its source workflow.
func TestFunctionDetail_schemaFromSingleWorkflow(t *testing.T) {
	fake := seedFunctionFake()
	fake.workflows = []dag.WorkflowDef{
		{
			Name:        "billing-flow",
			Steps:       []dag.StepDef{{ID: "s1", Task: "billing::charge"}},
			InputSchema: json.RawMessage(`{"type":"object","title":"ChargeInput"}`),
		},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/functions/billing::charge", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "ChargeInput") {
		t.Errorf("schema not rendered: %s", truncBody(body))
	}
	if !strings.Contains(body, "billing-flow") {
		t.Errorf("schema not attributed to source workflow: %s", truncBody(body))
	}
}

// TestFunctionDetail_noSchema asserts a function with no schema-bearing
// workflow renders the honest "No schema registered" placeholder.
func TestFunctionDetail_noSchema(t *testing.T) {
	fake := seedFunctionFake()
	fake.workflows = []dag.WorkflowDef{
		{
			Name:  "billing-flow",
			Steps: []dag.StepDef{{ID: "s1", Task: "billing::charge"}},
		},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/functions/billing::charge", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "No schema registered") {
		t.Errorf("missing no-schema placeholder: %s", truncBody(body))
	}
}

// TestFunctionDetail_unknownFunctionNotFound asserts an unregistered name
// renders the honest not-found state (200 with chrome) and never a
// providers table.
func TestFunctionDetail_unknownFunctionNotFound(t *testing.T) {
	fake := seedFunctionFake()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/functions/does::not-exist", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "No function named") {
		t.Errorf("unknown function missing honest not-found: %s", truncBody(body))
	}
	if strings.Contains(body, "Providers") {
		t.Errorf("unknown function rendered a providers table")
	}
}

// TestFunctionDetail_emptyPathDispatch asserts the bare trailing-slash
// path (empty name) does not 500 — it routes to not-found rather than the
// detail view.
func TestFunctionDetail_emptyPathDispatch(t *testing.T) {
	fake := seedFunctionFake()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/functions/", nil))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("empty name status = %d, want 404", rr.Code)
	}
}

// TestFunctionDetail_omitsUnbackedFeatures guards the honesty contract:
// invoke modal, per-function telemetry tiles, sparkline, the in-flight
// column, and the dead Duration column have no backing data or mutation,
// so they must not render.
func TestFunctionDetail_omitsUnbackedFeatures(t *testing.T) {
	fake := seedFunctionFake()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/functions/billing::charge", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, banned := range []string{
		"Invoke", "Pending", "fail rate", "Fail %", "Avg duration",
		"sparkline", "In-flight", "Duration",
	} {
		if strings.Contains(body, banned) {
			t.Errorf("function detail leaked unbacked feature %q", banned)
		}
	}
}

// TestFunctionsList_rowLinksToDetail asserts the functions list rows are
// clickable and link to the per-function detail route with a chevron.
func TestFunctionsList_rowLinksToDetail(t *testing.T) {
	fake := seedFunctionFake()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/functions", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "/console/functions/billing::charge") {
		t.Errorf("functions list row not linked to detail route: %s",
			truncBody(body))
	}
	if !strings.Contains(body, "row-chevron") {
		t.Errorf("functions list missing chevron affordance")
	}
}

// TestFunctionProvidersFrom drives the pure provider-join helper with
// fixed inputs. Positive: an owner present in the rows map carries its
// live status. Negative: an owner absent from the map falls back to "no
// worker".
func TestFunctionProvidersFrom(t *testing.T) {
	owners := []string{"w-live", "w-ghost"}
	statusByID := map[string]string{"w-live": "active"}
	got := functionProvidersFrom(owners, statusByID)
	if len(got) != 2 {
		t.Fatalf("provider count = %d, want 2", len(got))
	}
	if got[0].WorkerID != "w-live" || got[0].Status != "active" {
		t.Errorf("live provider = %+v, want w-live/active", got[0])
	}
	if got[1].Status != "no worker" {
		t.Errorf("ghost provider status = %q, want no worker", got[1].Status)
	}
}

// TestInvocationsForFunction drives the cross-reference helper with fixed
// inputs. Positive: a run whose workflow references the function is kept.
// Negative: an unrelated run and a run whose workflow lookup misses are
// dropped.
func TestInvocationsForFunction(t *testing.T) {
	defs := map[string]dag.WorkflowDef{
		"wf-a": {Name: "wf-a", Steps: []dag.StepDef{{Task: "fn"}}},
		"wf-b": {Name: "wf-b", Steps: []dag.StepDef{{Task: "other"}}},
	}
	runs := []dag.WorkflowRun{
		{RunID: "r1", WorkflowID: "wf-a", Status: dag.RunStatusCompleted},
		{RunID: "r2", WorkflowID: "wf-b", Status: dag.RunStatusCompleted},
		{RunID: "r3", WorkflowID: "wf-missing", Status: dag.RunStatusCompleted},
	}
	got := invocationsForFunction(runs, defs, "fn")
	if len(got) != 1 {
		t.Fatalf("invocation count = %d, want 1", len(got))
	}
	if got[0].RunID != "r1" {
		t.Errorf("kept run = %q, want r1", got[0].RunID)
	}
}

// TestFunctionSchemaFor drives the schema-gating helper. Positive: a
// single workflow with a non-empty InputSchema sets the schema + source.
// Negative: two referencing workflows leave the schema empty (ambiguous
// attribution), and a single workflow with no schema leaves it empty.
func TestFunctionSchemaFor(t *testing.T) {
	single := []dag.WorkflowDef{
		{
			Name:        "wf-a",
			Steps:       []dag.StepDef{{Task: "fn"}},
			InputSchema: json.RawMessage(`{"x":1}`),
		},
	}
	in, out, src := functionSchemaFor(single, "fn")
	if in == "" || src != "wf-a" {
		t.Errorf("single-workflow schema = (%q,%q,%q), want non-empty/wf-a",
			in, out, src)
	}
	multi := []dag.WorkflowDef{
		{Name: "wf-a", Steps: []dag.StepDef{{Task: "fn"}},
			InputSchema: json.RawMessage(`{"x":1}`)},
		{Name: "wf-b", Steps: []dag.StepDef{{Task: "fn"}},
			InputSchema: json.RawMessage(`{"y":2}`)},
	}
	in2, _, src2 := functionSchemaFor(multi, "fn")
	if in2 != "" || src2 != "" {
		t.Errorf("multi-workflow schema = (%q,%q), want empty (ambiguous)",
			in2, src2)
	}
	none := []dag.WorkflowDef{
		{Name: "wf-a", Steps: []dag.StepDef{{Task: "fn"}}},
	}
	in3, _, _ := functionSchemaFor(none, "fn")
	if in3 != "" {
		t.Errorf("no-schema workflow = %q, want empty", in3)
	}
}
