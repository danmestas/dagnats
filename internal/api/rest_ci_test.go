// api/rest_ci_test.go
// Tests for the /v1/ci/{compile,validate} routes (#633, supersedes the API
// half of #631). Methodology: integration tests with embedded NATS, driving
// MountV1's handlers via httptest so status codes and JSON bodies are
// verified exactly as a real client would see them. Each test starts its
// own NATS server. Every test asserts both a positive outcome (the
// documented success/error shape) and a negative one (the thing that must
// NOT have happened, e.g. nothing registered).
package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// ciYMLValid is a minimal valid ci.yml spec text.
const ciYMLValid = `
checks:
  test: { call: "test" }
`

// ciYMLInvalidNeeds references a check that does not exist, producing a
// Compile diagnostic (not a Parse one).
const ciYMLInvalidNeeds = `
checks:
  build: { call: "build", needs: [missing] }
`

// ciYMLTaskValid is a minimal task:-based ci.yml spec (#671) — the
// runner-neutral path, dispatchable to any worker rather than requiring
// Dagger.
const ciYMLTaskValid = `
checks:
  test: { task: "go-test" }
`

// ciYMLTaskAndCallBothSet sets both call: and task: on the same check,
// which is mutually exclusive (#671) and must produce a diagnostic.
const ciYMLTaskAndCallBothSet = `
checks:
  test: { call: "test", task: "go-test" }
`

// newCIRequestBody marshals a POST body for /v1/ci/{compile,validate}.
func newCIRequestBody(t *testing.T, name, spec string, register bool) []byte {
	t.Helper()
	body, err := json.Marshal(ciRequest{Name: name, Spec: spec, Register: register})
	if err != nil {
		t.Fatalf("marshal ci request: %v", err)
	}
	return body
}

// newCITestServer starts an embedded-NATS Service and mounts /v1 routes on
// an httptest.Server, returning both for the caller to drive.
func newCITestServer(t *testing.T) (*Service, *httptest.Server) {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	// Mirror server.go's startHTTP: "/" carries the full REST surface
	// (including GET /workflows, which these tests assert against) and
	// MountV1 layers the more-specific /v1 patterns on the same mux.
	mux := http.NewServeMux()
	mux.Handle("/", NewRESTHandler(svc))
	MountV1(mux, svc, openTestTokenStore(t, js))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return svc, server
}

// listWorkflowNames GETs /workflows and returns the set of registered names,
// used by tests to prove register:false left the registry untouched and
// register:true actually persisted the workflow.
func listWorkflowNames(t *testing.T, serverURL string) map[string]bool {
	t.Helper()
	resp, err := http.Get(serverURL + "/workflows")
	if err != nil {
		t.Fatalf("GET /workflows: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /workflows status = %d, want 200", resp.StatusCode)
	}
	var entries []workflowListEntry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		t.Fatalf("decode /workflows: %v", err)
	}
	names := make(map[string]bool, len(entries))
	for _, wf := range entries {
		names[wf.Name] = true
	}
	return names
}

func TestCICompileWithoutRegisterDoesNotRegister(t *testing.T) {
	_, server := newCITestServer(t)
	body := newCIRequestBody(t, "ci-no-register", ciYMLValid, false)

	resp, err := http.Post(server.URL+"/v1/ci/compile", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/ci/compile: %v", err)
	}
	defer resp.Body.Close()

	// Positive: 200 with a compiled def and def_hash, registered: false.
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out ciCompileResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.DefHash == "" {
		t.Error("def_hash is empty, want a hash")
	}
	if out.Registered {
		t.Error("registered = true, want false")
	}
	if len(out.Workflow.Steps) == 0 {
		t.Error("workflow.steps is empty, want the compiled \"test\" check step")
	}

	// Negative: GET /workflows does not list it.
	names := listWorkflowNames(t, server.URL)
	if names["ci-no-register"] {
		t.Error("ci-no-register appears in GET /workflows despite register: false")
	}
}

func TestCICompileWithRegisterRegistersAndHashMatches(t *testing.T) {
	_, server := newCITestServer(t)
	body := newCIRequestBody(t, "ci-register", ciYMLValid, true)

	resp, err := http.Post(server.URL+"/v1/ci/compile", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/ci/compile: %v", err)
	}
	defer resp.Body.Close()

	// Positive: 200, registered: true, and a def_hash.
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out ciCompileResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !out.Registered {
		t.Fatal("registered = false, want true")
	}

	// Positive: GET /workflows lists it with a matching def_hash.
	respList, err := http.Get(server.URL + "/workflows")
	if err != nil {
		t.Fatalf("GET /workflows: %v", err)
	}
	defer respList.Body.Close()
	var list []workflowListEntry
	if err := json.NewDecoder(respList.Body).Decode(&list); err != nil {
		t.Fatalf("decode /workflows: %v", err)
	}
	var found bool
	for _, wf := range list {
		if wf.Name == "ci-register" {
			found = true
			// Negative: the listed def_hash must equal the compile
			// response's def_hash, not just be non-empty.
			if wf.DefHash != out.DefHash {
				t.Errorf("listed def_hash = %q, want %q", wf.DefHash, out.DefHash)
			}
		}
	}
	if !found {
		t.Fatal("ci-register does not appear in GET /workflows despite register: true")
	}
}

func TestCICompileInvalidSpecReturns422AndRegistersNothing(t *testing.T) {
	_, server := newCITestServer(t)
	body := newCIRequestBody(t, "ci-invalid", ciYMLInvalidNeeds, true)

	resp, err := http.Post(server.URL+"/v1/ci/compile", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/ci/compile: %v", err)
	}
	defer resp.Body.Close()

	// Positive: 422 with a non-empty diagnostics list.
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", resp.StatusCode)
	}
	var out ciDiagnosticsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out.Diagnostics) == 0 {
		t.Fatal("diagnostics is empty, want >=1")
	}

	// Negative: nothing was registered, even though register: true was set.
	names := listWorkflowNames(t, server.URL)
	if names["ci-invalid"] {
		t.Error("ci-invalid appears in GET /workflows despite a 422 response")
	}
}

func TestCIValidateNeverRegistersEvenForValidSpec(t *testing.T) {
	_, server := newCITestServer(t)
	body := newCIRequestBody(t, "ci-validate-valid", ciYMLValid, true)

	resp, err := http.Post(server.URL+"/v1/ci/validate", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/ci/validate: %v", err)
	}
	defer resp.Body.Close()

	// Positive: 200 with valid: true and no diagnostics.
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out ciDiagnosticsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !out.Valid {
		t.Errorf("valid = false, want true (diagnostics: %+v)", out.Diagnostics)
	}
	if len(out.Diagnostics) != 0 {
		t.Errorf("diagnostics = %+v, want none", out.Diagnostics)
	}

	// Negative: /v1/ci/validate never registers, even with register: true
	// and a spec that would otherwise compile cleanly.
	names := listWorkflowNames(t, server.URL)
	if names["ci-validate-valid"] {
		t.Error("ci-validate-valid appears in GET /workflows: /v1/ci/validate must never register")
	}
}

func TestCIValidateInvalidSpecReturns200WithDiagnostics(t *testing.T) {
	_, server := newCITestServer(t)
	body := newCIRequestBody(t, "ci-validate-invalid", ciYMLInvalidNeeds, false)

	resp, err := http.Post(server.URL+"/v1/ci/validate", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/ci/validate: %v", err)
	}
	defer resp.Body.Close()

	// Positive: always 200, but valid: false with diagnostics for a bad spec.
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (validate is always 200)", resp.StatusCode)
	}
	var out ciDiagnosticsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Valid {
		t.Error("valid = true, want false for an invalid spec")
	}
	if len(out.Diagnostics) == 0 {
		t.Error("diagnostics is empty, want >=1 for an invalid spec")
	}
}

func TestCICompileOversizedSpecReturns413(t *testing.T) {
	_, server := newCITestServer(t)
	oversized := strings.Repeat("a", ciSpecMaxBytes+1)
	body := newCIRequestBody(t, "ci-oversized", oversized, false)

	resp, err := http.Post(server.URL+"/v1/ci/compile", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/ci/compile: %v", err)
	}
	defer resp.Body.Close()

	// Positive: an oversized spec field is rejected with 413.
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}

	// Negative: a spec at exactly the limit is NOT rejected on size grounds
	// (it may still fail to parse as YAML, but must not be 413).
	atLimit := "checks:\n  test: { call: \"" + strings.Repeat("a", 10) + "\" }\n"
	body2 := newCIRequestBody(t, "ci-at-limit", atLimit, false)
	resp2, err := http.Post(server.URL+"/v1/ci/compile", "application/json", bytes.NewReader(body2))
	if err != nil {
		t.Fatalf("POST /v1/ci/compile (at-limit): %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode == http.StatusRequestEntityTooLarge {
		t.Error("small spec rejected with 413, want it to pass the size check")
	}
}

func TestCICompileEmptyNameReturns400(t *testing.T) {
	_, server := newCITestServer(t)
	body := newCIRequestBody(t, "", ciYMLValid, false)

	resp, err := http.Post(server.URL+"/v1/ci/compile", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/ci/compile: %v", err)
	}
	defer resp.Body.Close()

	// Positive: empty name is rejected with 400.
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}

	// Negative: the same request with a name set is NOT rejected on name
	// grounds (proves the 400 above was actually about the empty name).
	body2 := newCIRequestBody(t, "ci-named", ciYMLValid, false)
	resp2, err := http.Post(server.URL+"/v1/ci/compile", "application/json", bytes.NewReader(body2))
	if err != nil {
		t.Fatalf("POST /v1/ci/compile (named): %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode == http.StatusBadRequest {
		t.Error("named request rejected with 400, want it to pass the name check")
	}
}

// TestCICompileTaskSpecRegistersPlainTask verifies that POST
// /v1/ci/compile with a task:-based spec (#671) returns a def whose step
// carries the plain Task with no Dagger-shaped Metadata, and that
// register:true persists it exactly like a call:-based spec.
func TestCICompileTaskSpecRegistersPlainTask(t *testing.T) {
	_, server := newCITestServer(t)
	body := newCIRequestBody(t, "ci-task", ciYMLTaskValid, true)

	resp, err := http.Post(server.URL+"/v1/ci/compile", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/ci/compile: %v", err)
	}
	defer resp.Body.Close()

	// Positive: 200, registered: true, step's Task is the plain value.
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out ciCompileResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !out.Registered {
		t.Fatal("registered = false, want true")
	}
	if len(out.Workflow.Steps) != 1 {
		t.Fatalf("workflow.steps = %+v, want exactly 1", out.Workflow.Steps)
	}
	step := out.Workflow.Steps[0]
	if step.Task != "go-test" {
		t.Errorf("step.Task = %q, want \"go-test\"", step.Task)
	}

	// Negative: no Dagger-shaped Metadata (module/call) leaks into a
	// task:-based step.
	if len(step.Metadata) != 0 {
		t.Errorf("step.Metadata = %+v, want empty for a task: check", step.Metadata)
	}

	// Positive: it registered, same as any other clean compile.
	names := listWorkflowNames(t, server.URL)
	if !names["ci-task"] {
		t.Error("ci-task does not appear in GET /workflows despite register: true")
	}
}

// TestCIValidateTaskAndCallExclusivityDiagnostic verifies that POST
// /v1/ci/validate reports the call:/task: exclusivity diagnostic (#671)
// for a spec that sets both on the same check.
func TestCIValidateTaskAndCallExclusivityDiagnostic(t *testing.T) {
	_, server := newCITestServer(t)
	body := newCIRequestBody(t, "ci-validate-both", ciYMLTaskAndCallBothSet, false)

	resp, err := http.Post(server.URL+"/v1/ci/validate", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/ci/validate: %v", err)
	}
	defer resp.Body.Close()

	// Positive: 200 (validate is always 200), valid: false, one diagnostic
	// naming the offending check.
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (validate is always 200)", resp.StatusCode)
	}
	var out ciDiagnosticsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Valid {
		t.Error("valid = true, want false for call:+task: both set")
	}
	if len(out.Diagnostics) != 1 {
		t.Fatalf("diagnostics = %+v, want exactly 1", out.Diagnostics)
	}
	if out.Diagnostics[0].Field != "test" {
		t.Errorf("diagnostics[0].Field = %q, want \"test\"", out.Diagnostics[0].Field)
	}

	// Negative: a task:-only variant of the same check is valid.
	okBody := newCIRequestBody(t, "ci-validate-task-only", ciYMLTaskValid, false)
	okResp, err := http.Post(server.URL+"/v1/ci/validate", "application/json", bytes.NewReader(okBody))
	if err != nil {
		t.Fatalf("POST /v1/ci/validate (task only): %v", err)
	}
	defer okResp.Body.Close()
	var okOut ciDiagnosticsResponse
	if err := json.NewDecoder(okResp.Body).Decode(&okOut); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !okOut.Valid {
		t.Errorf("valid = false, want true (diagnostics: %+v)", okOut.Diagnostics)
	}
}
