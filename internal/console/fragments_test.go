// fragments_test.go exercises the Datastar fragment endpoints —
// the small "return a tbody" handlers that the workflows-list and
// runs-list pages call after a filter input change.
//
// Methodology:
//   - Wire the same fake data source the pages tests use so the
//     filter behavior is identical (same buildXView path under the
//     hood — the fragment is a render thereof).
//   - Send the Datastar signal envelope via the SDK's expected
//     transport: GET with the JSON-encoded signals query parameter
//     'datastar'. The SDK and the official datastar-patterns skill
//     both treat this as the canonical client wire format.
//   - Assert the response is SSE (Content-Type: text/event-stream)
//     and that the body contains a `datastar-patch-elements` event
//     framing the expected tbody substring. Catching the SSE event
//     header guards the patch-mode contract.
//   - Pagination correctness across the fragment endpoint: page=2,
//     size=10 of 30 runs returns 10 runs from the middle slice.
package console

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
)

// callFragment encodes a Datastar signals envelope into the canonical
// `datastar` query parameter and issues the request. Returns the
// response body for assertions.
func callFragment(
	t *testing.T, h http.Handler, path string, signalsJSON string,
) *httptest.ResponseRecorder {
	t.Helper()
	if path == "" {
		t.Fatalf("callFragment: empty path")
	}
	v := url.Values{}
	if signalsJSON != "" {
		v.Set("datastar", signalsJSON)
	}
	fullPath := path
	if len(v) > 0 {
		fullPath = path + "?" + v.Encode()
	}
	req := httptest.NewRequest(http.MethodGet, fullPath, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func TestFragment_workflowsList_filterReturnsMatchingRows(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{
		sampleWorkflow("alpha"),
		sampleWorkflow("beta-prod"),
		sampleWorkflow("gamma"),
	}
	h := mountWithFake(t, fake)
	rr := callFragment(t, h,
		"/console/api/fragments/workflows-list",
		`{"workflowsFilter":"beta"}`,
	)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if got := rr.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("Content-Type = %q, want text/event-stream", got)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "datastar-patch-elements") {
		t.Errorf("fragment response missing SSE event marker")
	}
	if !strings.Contains(body, "beta-prod") {
		t.Errorf("fragment missing matching row beta-prod")
	}
	if strings.Contains(body, "alpha") || strings.Contains(body, "gamma") {
		t.Errorf("fragment leaked non-matching rows")
	}
}

func TestFragment_runsList_filterByStatus(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("alpha")}
	now := time.Now()
	fake.runs = []dag.WorkflowRun{
		{
			RunID: "run-c1", WorkflowID: "alpha", Status: dag.RunStatusCompleted,
			CreatedAt: now,
		},
		{
			RunID: "run-f1", WorkflowID: "alpha", Status: dag.RunStatusFailed,
			CreatedAt: now,
		},
	}
	h := mountWithFake(t, fake)
	rr := callFragment(t, h,
		"/console/api/fragments/runs-list",
		`{"runsStatus":"failed"}`,
	)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "datastar-patch-elements") {
		t.Errorf("fragment response missing SSE event marker")
	}
	if !strings.Contains(body, "run-f1") {
		t.Errorf("runs fragment missing matching run-f1")
	}
	if strings.Contains(body, "run-c1") {
		t.Errorf("runs fragment leaked non-failed run")
	}
}

func TestFragment_runsList_pageTwoReturnsSecondSlice(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("alpha")}
	now := time.Now()
	for i := 1; i <= 30; i++ {
		fake.runs = append(fake.runs, dag.WorkflowRun{
			RunID: runIDForIndex(i), WorkflowID: "alpha",
			Status:    dag.RunStatusCompleted,
			CreatedAt: now.Add(time.Duration(i) * time.Second),
		})
	}
	h := mountWithFake(t, fake)
	rr := callFragment(t, h,
		"/console/api/fragments/runs-list?page=2&size=10",
		"",
	)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "datastar-patch-elements") {
		t.Errorf("fragment response missing SSE event marker")
	}
	// Page 2 should hold run-20..run-11 (newest-first).
	for _, want := range []string{"run-20", "run-15", "run-11"} {
		if !strings.Contains(body, want) {
			t.Errorf("page-2 fragment missing %q", want)
		}
	}
	for _, leak := range []string{"run-30", "run-21", "run-10", "run-01"} {
		if strings.Contains(body, leak) {
			t.Errorf("page-2 fragment leaked %q", leak)
		}
	}
}

func TestFragment_emptySignalsAndQuery_returnsFirstPage(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("alpha")}
	fake.runs = []dag.WorkflowRun{{
		RunID:      "run-x",
		WorkflowID: "alpha",
		Status:     dag.RunStatusCompleted,
		CreatedAt:  time.Now(),
	}}
	h := mountWithFake(t, fake)
	rr := callFragment(t, h, "/console/api/fragments/runs-list", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "run-x") {
		t.Errorf("default fragment missing the single run row")
	}
}
