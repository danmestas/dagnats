// api/labels_test.go

// Tests for run labels (#629): label filtering on GET /runs (single and
// AND-composed), malformed ?label= query rejection, invalid labels on
// POST /runs rejected with no run created, and bulk cancel scoped by
// label (dry_run and real).
// Methodology: real embedded NATS server + orchestrator, driven through
// the REST handler via httptest where the surface under test is HTTP
// query parsing, and through the Service directly where it isn't.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/worker"
)

// newLabelsRestFixture spins an embedded server + orchestrator + REST
// handler over an httptest.Server, and registers a single-step workflow
// wfName. Mirrors newRunsSvc (runs_envelope_test.go) but exposes the
// httptest.Server the label-filter tests need to hit query params.
func newLabelsRestFixture(t *testing.T, wfName string) (*Service, *httptest.Server) {
	t.Helper()
	if wfName == "" {
		panic("newLabelsRestFixture: wfName must not be empty")
	}
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(orch.Stop)
	w := worker.NewWorker(nc)
	w.Handle("task-a", func(ctx worker.TaskContext) error {
		return ctx.Complete(nil)
	})
	w.Start()
	t.Cleanup(w.Stop)
	svc := NewService(nc)
	handler := NewRESTHandler(svc)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	wb := dag.NewWorkflow(wfName)
	wb.Task("a", "task-a")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := svc.RegisterWorkflow(context.Background(), def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	return svc, server
}

// getRuns issues GET <base>/runs<query> and decodes the array response.
func getRuns(t *testing.T, base, query string) ([]dag.WorkflowRun, *http.Response) {
	t.Helper()
	resp, err := http.Get(base + "/runs" + query)
	if err != nil {
		t.Fatalf("GET /runs%s: %v", query, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, resp
	}
	var runs []dag.WorkflowRun
	if err := json.NewDecoder(resp.Body).Decode(&runs); err != nil {
		t.Fatalf("decode /runs%s: %v", query, err)
	}
	return runs, resp
}

func TestGetRunsFilterByLabel(t *testing.T) {
	svc, server := newLabelsRestFixture(t, "label-filter-wf")

	tenantA, err := svc.StartRunWithLabels(
		context.Background(), "label-filter-wf", nil,
		map[string]string{"tenant": "a"},
	)
	if err != nil {
		t.Fatalf("StartRunWithLabels(tenant=a): %v", err)
	}
	_, err = svc.StartRunWithLabels(
		context.Background(), "label-filter-wf", nil,
		map[string]string{"tenant": "b"},
	)
	if err != nil {
		t.Fatalf("StartRunWithLabels(tenant=b): %v", err)
	}
	waitRunStatus(t, svc, tenantA, dag.RunStatusCompleted)

	runs, resp := getRuns(t, server.URL, "?label=tenant=a")
	// Positive: only the tenant=a run comes back.
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(runs) != 1 || runs[0].RunID != tenantA {
		t.Fatalf("runs = %+v, want exactly [%s]", runs, tenantA)
	}
	// Negative: a non-matching label value returns nothing.
	none, _ := getRuns(t, server.URL, "?label=tenant=c")
	if len(none) != 0 {
		t.Fatalf("label=tenant=c returned %d runs, want 0", len(none))
	}
}

func TestGetRunsFilterByLabelANDSemantics(t *testing.T) {
	svc, server := newLabelsRestFixture(t, "label-and-wf")

	both, err := svc.StartRunWithLabels(
		context.Background(), "label-and-wf", nil,
		map[string]string{"tenant": "a", "region": "us"},
	)
	if err != nil {
		t.Fatalf("StartRunWithLabels(both): %v", err)
	}
	_, err = svc.StartRunWithLabels(
		context.Background(), "label-and-wf", nil,
		map[string]string{"tenant": "a", "region": "eu"},
	)
	if err != nil {
		t.Fatalf("StartRunWithLabels(region=eu): %v", err)
	}
	waitRunStatus(t, svc, both, dag.RunStatusCompleted)

	// Positive: both labels together match exactly the one run.
	runs, _ := getRuns(t, server.URL, "?label=tenant=a&label=region=us")
	if len(runs) != 1 || runs[0].RunID != both {
		t.Fatalf("AND filter = %+v, want exactly [%s]", runs, both)
	}
	// Negative: adding a third, non-matching label drops the match too.
	empty, _ := getRuns(t, server.URL,
		"?label=tenant=a&label=region=us&label=stage=prod")
	if len(empty) != 0 {
		t.Fatalf("over-constrained AND filter returned %d, want 0",
			len(empty))
	}
}

func TestGetRunsMalformedLabelParamReturns400(t *testing.T) {
	_, server := newLabelsRestFixture(t, "label-malformed-wf")

	// Positive: a label param with no "=" is rejected.
	_, resp := getRuns(t, server.URL, "?label=no-equals-sign")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	// Negative: a well-formed (if non-matching) label param is not
	// rejected.
	_, ok := getRuns(t, server.URL, "?label=k=v")
	if ok.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 for well-formed label",
			ok.StatusCode)
	}
}

func TestPostRunsInvalidLabelsReturns400AndCreatesNoRun(t *testing.T) {
	_, server := newLabelsRestFixture(t, "label-post-invalid-wf")

	body := mustMarshal(t, startRunRequest{
		Workflow: "label-post-invalid-wf",
		Labels:   map[string]string{"Bad Key!": "v"},
	})
	resp, err := http.Post(
		server.URL+"/runs", "application/json", bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST /runs: %v", err)
	}
	// Positive: invalid labels are rejected with 400.
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}

	// Negative: no run for this workflow exists afterwards.
	time.Sleep(100 * time.Millisecond)
	runs, listResp := getRuns(t, server.URL, "?workflow=label-post-invalid-wf")
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /runs status = %d, want 200", listResp.StatusCode)
	}
	if len(runs) != 0 {
		t.Fatalf("runs created despite invalid labels: %+v", runs)
	}
}

// TestBulkCancelByLabel deliberately runs no worker, matching
// TestBulkCancelByWorkflow (bulk_cancel_test.go): the "echo" task type
// is never handled, so runs stay Pending/Running (non-terminal) and
// bulk cancel has something to act on instead of racing completion.
func TestBulkCancelByLabel(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(orch.Stop)
	svc := NewService(nc)
	wb := dag.NewWorkflow("bulk-cancel-label-wf")
	wb.Task("s", "echo")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := svc.RegisterWorkflow(context.Background(), def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	target, err := svc.StartRunWithLabels(
		context.Background(), "bulk-cancel-label-wf", nil,
		map[string]string{"batch": "keep-me"},
	)
	if err != nil {
		t.Fatalf("StartRunWithLabels(target): %v", err)
	}
	other, err := svc.StartRunWithLabels(
		context.Background(), "bulk-cancel-label-wf", nil,
		map[string]string{"batch": "other"},
	)
	if err != nil {
		t.Fatalf("StartRunWithLabels(other): %v", err)
	}
	// Give the orchestrator a moment to move runs to Pending/Running so
	// bulk cancel has something non-terminal to act on.
	time.Sleep(200 * time.Millisecond)

	// dry_run: reports the match without mutating state.
	dry, err := svc.BulkCancelRuns(context.Background(), BulkCancelRequest{
		WorkflowID: "bulk-cancel-label-wf",
		Labels:     map[string]string{"batch": "keep-me"},
		DryRun:     true,
	})
	if err != nil {
		t.Fatalf("BulkCancelRuns(dry_run): %v", err)
	}
	if len(dry.Cancelled) != 1 || dry.Cancelled[0] != target {
		t.Fatalf("dry-run cancelled = %+v, want exactly [%s]",
			dry.Cancelled, target)
	}
	run, err := svc.GetRun(context.Background(), target)
	if err != nil {
		t.Fatalf("GetRun(target) after dry run: %v", err)
	}
	if run.Status == dag.RunStatusCancelled {
		t.Fatal("dry-run must not cancel the matching run")
	}

	// Real cancel: only the matching-label run is affected.
	resp, err := svc.BulkCancelRuns(context.Background(), BulkCancelRequest{
		WorkflowID: "bulk-cancel-label-wf",
		Labels:     map[string]string{"batch": "keep-me"},
	})
	if err != nil {
		t.Fatalf("BulkCancelRuns: %v", err)
	}
	if len(resp.Cancelled) != 1 || resp.Cancelled[0] != target {
		t.Fatalf("cancelled = %+v, want exactly [%s]",
			resp.Cancelled, target)
	}
	otherRun, err := svc.GetRun(context.Background(), other)
	if err != nil {
		t.Fatalf("GetRun(other): %v", err)
	}
	if otherRun.Status == dag.RunStatusCancelled {
		t.Fatal("non-matching-label run must not be cancelled")
	}
}

// TestGetRunsFilterByStatus proves ?status= narrows GET /runs to runs in
// that RunStatus, composes with ?label= (AND), and rejects an unknown
// status value with 400 (#629 review follow-up).
func TestGetRunsFilterByStatus(t *testing.T) {
	svc, server := newLabelsRestFixture(t, "status-filter-wf")

	completed, err := svc.StartRunWithLabels(
		context.Background(), "status-filter-wf", nil,
		map[string]string{"batch": "x"},
	)
	if err != nil {
		t.Fatalf("StartRunWithLabels(completed): %v", err)
	}
	waitRunStatus(t, svc, completed, dag.RunStatusCompleted)

	failed, err := svc.StartRunWithLabels(
		context.Background(), "status-filter-wf", nil,
		map[string]string{"batch": "x"},
	)
	if err != nil {
		t.Fatalf("StartRunWithLabels(failed): %v", err)
	}
	failedRun := waitRunStatus(t, svc, failed, dag.RunStatusCompleted)
	failedRun.Status = dag.RunStatusFailed
	if err := svc.store.Save(context.Background(), failedRun); err != nil {
		t.Fatalf("force failed state: %v", err)
	}

	// Positive: ?status=failed returns only the forced-failed run.
	runs, resp := getRuns(t, server.URL, "?status=failed")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(runs) != 1 || runs[0].RunID != failed {
		t.Fatalf("status=failed runs = %+v, want exactly [%s]", runs, failed)
	}

	// Positive: status= composes with label= (AND) — both runs share
	// batch=x, so status=failed&label=batch=x still narrows to one.
	both, _ := getRuns(t, server.URL, "?status=failed&label=batch=x")
	if len(both) != 1 || both[0].RunID != failed {
		t.Fatalf("status+label AND = %+v, want exactly [%s]", both, failed)
	}

	// Negative: an unknown status value is rejected with 400.
	_, bad := getRuns(t, server.URL, "?status=bogus")
	if bad.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=bogus status = %d, want 400", bad.StatusCode)
	}
}
