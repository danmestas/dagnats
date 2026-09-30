// internal/api/rest_ci_jobs_test.go
// HTTP tests for the jobs: spec over POST /v1/ci/{compile,validate} (#728).
//
// Methodology: real Service behind an httptest server (newCITestServer),
// driven with raw JSON bodies so task_namespace is exercised on the wire.
// Covers #728's acceptance that on: round-trips as structured JSON through
// BOTH compile and validate, plus the negative space: a jobs: spec with no
// namespace is rejected on both endpoints with no on: reported, and a
// checks: spec never grows an on: field.
package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

const ciJobsSpec = "on:\n  push:\n    branches: [trunk]\n" +
	"jobs:\n  test:\n    steps:\n      - run: go test ./...\n"

const ciJobsOnWant = `{"push":{"branches":["trunk"]}}`

func postCI(t *testing.T, url, endpoint string, body map[string]any) (int, []byte) {
	t.Helper()
	if endpoint == "" {
		t.Fatal("postCI: endpoint must not be empty")
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resp, err := http.Post(url+endpoint, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("POST %s: %v", endpoint, err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", endpoint, err)
	}
	return resp.StatusCode, out
}

// TestCIJobsSpecOnRoundTrips covers compile and validate for a valid
// jobs: spec: both report on: exactly as normalised.
func TestCIJobsSpecOnRoundTrips(t *testing.T) {
	_, server := newCITestServer(t)
	body := map[string]any{"name": "jobs-wf", "spec": ciJobsSpec, "task_namespace": "repo"}

	status, raw := postCI(t, server.URL, "/v1/ci/compile", body)
	if status != http.StatusOK {
		t.Fatalf("compile status = %d, want 200 (%s)", status, raw)
	}
	var compiled ciCompileResponse
	if err := json.Unmarshal(raw, &compiled); err != nil {
		t.Fatalf("decode compile: %v", err)
	}
	if string(compiled.On) != ciJobsOnWant {
		t.Fatalf("compile on = %s, want %s", compiled.On, ciJobsOnWant)
	}
	if got := compiled.Workflow.Steps[0].Task; got != "repo.job" {
		t.Fatalf("task = %q, want repo.job", got)
	}

	status, raw = postCI(t, server.URL, "/v1/ci/validate", body)
	if status != http.StatusOK {
		t.Fatalf("validate status = %d, want 200 (%s)", status, raw)
	}
	var validated ciDiagnosticsResponse
	if err := json.Unmarshal(raw, &validated); err != nil {
		t.Fatalf("decode validate: %v", err)
	}
	if !validated.Valid || string(validated.On) != ciJobsOnWant {
		t.Fatalf("validate = valid:%v on:%s, want valid with %s",
			validated.Valid, validated.On, ciJobsOnWant)
	}
}

// TestCIJobsSpecWithoutNamespaceRejected is the negative space: no
// namespace means a diagnostic on both endpoints and no on: at all.
func TestCIJobsSpecWithoutNamespaceRejected(t *testing.T) {
	_, server := newCITestServer(t)
	body := map[string]any{"name": "jobs-wf", "spec": ciJobsSpec}

	status, raw := postCI(t, server.URL, "/v1/ci/compile", body)
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("compile status = %d, want 422 (%s)", status, raw)
	}
	if !strings.Contains(string(raw), "task_namespace") {
		t.Fatalf("422 body lacks the namespace diagnostic: %s", raw)
	}
	if strings.Contains(string(raw), `"on"`) {
		t.Fatalf("a diagnosed spec must not report on: %s", raw)
	}

	status, raw = postCI(t, server.URL, "/v1/ci/validate", body)
	var validated ciDiagnosticsResponse
	if err := json.Unmarshal(raw, &validated); err != nil || status != http.StatusOK {
		t.Fatalf("validate status %d err %v (%s)", status, err, raw)
	}
	if validated.Valid || validated.On != nil {
		t.Fatalf("validate = valid:%v on:%s, want invalid with no on", validated.Valid, validated.On)
	}
}

// TestCIChecksSpecHasNoOn: the checks: shape is unchanged over HTTP and
// never grows an on: field.
func TestCIChecksSpecHasNoOn(t *testing.T) {
	_, server := newCITestServer(t)
	body := map[string]any{"name": "checks-wf", "spec": ciYMLValid}
	status, raw := postCI(t, server.URL, "/v1/ci/compile", body)
	if status != http.StatusOK {
		t.Fatalf("compile status = %d, want 200 (%s)", status, raw)
	}
	if strings.Contains(string(raw), `"on"`) {
		t.Fatalf("a checks: spec reported on: %s", raw)
	}
}
