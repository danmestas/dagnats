// cli/workflow_register_respond_test.go
// Tests that `workflow register` surfaces the same
// dag.ValidateRespondReachability warnings as `workflow validate`
// (issue #613), computed offline from the file's embedded HTTP trigger.
// Methodology: real embedded NATS via natsutil.StartTestServer; register
// an HTTP-triggered workflow through the CLI command in --json mode and
// assert the respond warnings appear in the JSON, that the workflow is
// still persisted, and that a clean workflow produces none.
package cli

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/natsutil"
)

// registerJSONResult mirrors the JSON shape `workflow register --json`
// is expected to emit once respond-reachability warnings are wired in.
// Defined locally so the test compiles against current code and fails
// red until respond_warnings exists.
type registerJSONResult struct {
	Name            string   `json:"name"`
	Action          string   `json:"action"`
	Steps           int      `json:"steps"`
	Warnings        []string `json:"warnings"`
	RespondWarnings []struct {
		Kind    string `json:"kind"`
		Message string `json:"message"`
	} `json:"respond_warnings"`
}

func registerJSON(t *testing.T, body string) registerJSONResult {
	t.Helper()
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "triggers"}),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	defer nc.Close()
	t.Setenv("NATS_URL", srv.ClientURL())

	tmpFile := writeTempWorkflow(t, body)
	out := captureOutput(func() {
		runWorkflowRegisterCmd([]string{tmpFile, "--json"})
	})
	var result registerJSONResult
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("unmarshal json: %v (output: %s)", err, out)
	}
	return result
}

func respondKinds(r registerJSONResult) []string {
	kinds := make([]string, 0, len(r.RespondWarnings))
	for _, w := range r.RespondWarnings {
		kinds = append(kinds, w.Kind)
	}
	return kinds
}

func TestRegisterJSONWarnsMissingRespond(t *testing.T) {
	r := registerJSON(t, wfHTTPMissingRespond)
	// Positive: registration still happened.
	if r.Action != "created" {
		t.Fatalf("expected action created, got %q", r.Action)
	}
	kinds := respondKinds(r)
	// Positive: missing_respond surfaced through register.
	if !hasKind(kinds, "missing_respond") {
		t.Fatalf("expected missing_respond, got kinds=%v", kinds)
	}
}

func TestRegisterJSONWarnsMissingSchemas(t *testing.T) {
	r := registerJSON(t, wfHTTPMissingSchemas)
	kinds := respondKinds(r)
	// Positive: missing_schemas surfaced through register.
	if !hasKind(kinds, "missing_schemas") {
		t.Fatalf("expected missing_schemas, got kinds=%v", kinds)
	}
	// Negative: a reachable respond exists — no missing_respond.
	if hasKind(kinds, "missing_respond") {
		t.Fatalf("unexpected missing_respond, kinds=%v", kinds)
	}
}

func TestRegisterJSONWarnsDuplicateRespond(t *testing.T) {
	r := registerJSON(t, wfHTTPDuplicateRespond)
	kinds := respondKinds(r)
	// Positive: duplicate_respond surfaced through register.
	if !hasKind(kinds, "duplicate_respond") {
		t.Fatalf("expected duplicate_respond, got kinds=%v", kinds)
	}
}

func TestRegisterJSONNoRespondWarningsCleanHTTP(t *testing.T) {
	r := registerJSON(t, wfHTTPClean)
	// Negative: a well-formed HTTP workflow must not false-positive.
	if len(r.RespondWarnings) != 0 {
		t.Fatalf("expected no respond warnings, got %v", respondKinds(r))
	}
}

// TestRegisterMissingRespondStillPersists confirms a missing_respond
// warning does not block registration — the workflow lands in KV.
func TestRegisterMissingRespondStillPersists(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "triggers"}),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	defer nc.Close()
	t.Setenv("NATS_URL", srv.ClientURL())

	tmpFile := writeTempWorkflow(t, wfHTTPMissingRespond)
	out := captureOutput(func() {
		runWorkflowRegisterCmd([]string{tmpFile})
	})
	// Positive: text mode still reports success.
	if !strings.Contains(out, "created") {
		t.Fatalf("register should report created, got: %s", out)
	}

	svc := api.NewService(nc)
	// Positive: the workflow is persisted despite the warning.
	if _, err := svc.GetWorkflow("wf-missing-respond"); err != nil {
		t.Fatalf("workflow not persisted despite non-fatal warning: %v",
			err)
	}
}
