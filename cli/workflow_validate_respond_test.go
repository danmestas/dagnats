// cli/workflow_validate_respond_test.go
// Tests that `workflow validate` surfaces dag.ValidateRespondReachability
// warnings (missing_respond, duplicate_respond, missing_schemas) for
// HTTP-triggered workflows — the CLI gap fixed in issue #613. No NATS:
// hasHTTPTrigger is derived from the file's own embedded triggers.
// Methodology: write temp JSON files, run the validate command in text
// and --json modes, and assert on the emitted warnings (or their
// absence). Warnings never change the exit code — validate stays valid.
package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// respondJSONResult mirrors the JSON shape `workflow validate --json` is
// expected to emit once respond-reachability warnings are wired in. It
// is defined locally so these tests compile against the current
// (unwired) code and fail red until the field exists.
type respondJSONResult struct {
	Valid    bool `json:"valid"`
	Warnings []struct {
		Kind    string `json:"kind"`
		Message string `json:"message"`
	} `json:"warnings"`
}

const httpTriggerBlock = `"triggers": [{
		"id": "t1",
		"enabled": true,
		"http": {
			"path": "/api/x",
			"method": "POST",
			"timeout_ms": 3000,
			"max_body_bytes": 1024
		}
	}],`

// wfHTTPMissingSchemas: HTTP trigger + reachable respond step, but no
// input_schema/output_schema → missing_schemas only.
const wfHTTPMissingSchemas = `{
	"name": "wf-missing-schemas",
	"version": "1.0",
	` + httpTriggerBlock + `
	"steps": [
		{"id": "a", "task": "task-a"},
		{"id": "r", "type": "respond", "depends_on": ["a"],
			"config": {"status": 200}}
	]
}`

// wfHTTPMissingRespond: HTTP trigger + schemas, but no respond step →
// missing_respond only.
const wfHTTPMissingRespond = `{
	"name": "wf-missing-respond",
	"version": "1.0",
	"input_schema": {"type": "object"},
	"output_schema": {"type": "object"},
	` + httpTriggerBlock + `
	"steps": [
		{"id": "a", "task": "task-a"}
	]
}`

// wfHTTPDuplicateRespond: HTTP trigger + schemas + two simultaneously
// reachable respond steps → duplicate_respond.
const wfHTTPDuplicateRespond = `{
	"name": "wf-dup-respond",
	"version": "1.0",
	"input_schema": {"type": "object"},
	"output_schema": {"type": "object"},
	` + httpTriggerBlock + `
	"steps": [
		{"id": "a", "task": "task-a"},
		{"id": "r1", "type": "respond", "depends_on": ["a"],
			"config": {"status": 200}},
		{"id": "r2", "type": "respond", "depends_on": ["a"],
			"config": {"status": 200}}
	]
}`

// wfNoHTTPClean: no HTTP trigger, no respond → no warnings.
const wfNoHTTPClean = `{
	"name": "wf-no-http",
	"version": "1.0",
	"steps": [
		{"id": "a", "task": "task-a"}
	]
}`

// wfHTTPClean: HTTP trigger + schemas + exactly one respond → no
// warnings (the well-formed case must not false-positive).
const wfHTTPClean = `{
	"name": "wf-http-clean",
	"version": "1.0",
	"input_schema": {"type": "object"},
	"output_schema": {"type": "object"},
	` + httpTriggerBlock + `
	"steps": [
		{"id": "a", "task": "task-a"},
		{"id": "r", "type": "respond", "depends_on": ["a"],
			"config": {"status": 200}}
	]
}`

func writeTempWorkflow(t *testing.T, body string) string {
	t.Helper()
	tmpFile := filepath.Join(t.TempDir(), "wf.json")
	if err := os.WriteFile(tmpFile, []byte(body), 0644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return tmpFile
}

func validateJSONWarnings(t *testing.T, body string) respondJSONResult {
	t.Helper()
	tmpFile := writeTempWorkflow(t, body)
	out := captureOutput(func() {
		runWorkflowValidateCmd([]string{tmpFile, "--json"})
	})
	var result respondJSONResult
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("unmarshal json: %v (output: %s)", err, out)
	}
	return result
}

func warningKinds(r respondJSONResult) []string {
	kinds := make([]string, 0, len(r.Warnings))
	for _, w := range r.Warnings {
		kinds = append(kinds, w.Kind)
	}
	return kinds
}

func hasKind(kinds []string, want string) bool {
	for _, k := range kinds {
		if k == want {
			return true
		}
	}
	return false
}

func TestValidateJSONWarnsMissingSchemas(t *testing.T) {
	r := validateJSONWarnings(t, wfHTTPMissingSchemas)
	// Positive: still valid — warnings never fail validation.
	if !r.Valid {
		t.Fatal("expected valid=true despite missing_schemas warning")
	}
	kinds := warningKinds(r)
	// Positive: missing_schemas surfaced.
	if !hasKind(kinds, "missing_schemas") {
		t.Fatalf("expected missing_schemas, got kinds=%v", kinds)
	}
	// Negative: a reachable respond exists — must NOT warn missing_respond.
	if hasKind(kinds, "missing_respond") {
		t.Fatalf("unexpected missing_respond, kinds=%v", kinds)
	}
}

func TestValidateJSONWarnsMissingRespond(t *testing.T) {
	r := validateJSONWarnings(t, wfHTTPMissingRespond)
	if !r.Valid {
		t.Fatal("expected valid=true despite missing_respond warning")
	}
	kinds := warningKinds(r)
	// Positive: missing_respond surfaced.
	if !hasKind(kinds, "missing_respond") {
		t.Fatalf("expected missing_respond, got kinds=%v", kinds)
	}
	// Negative: schemas are present — must NOT warn missing_schemas.
	if hasKind(kinds, "missing_schemas") {
		t.Fatalf("unexpected missing_schemas, kinds=%v", kinds)
	}
}

func TestValidateJSONWarnsDuplicateRespond(t *testing.T) {
	r := validateJSONWarnings(t, wfHTTPDuplicateRespond)
	if !r.Valid {
		t.Fatal("expected valid=true despite duplicate_respond warning")
	}
	kinds := warningKinds(r)
	// Positive: duplicate_respond surfaced.
	if !hasKind(kinds, "duplicate_respond") {
		t.Fatalf("expected duplicate_respond, got kinds=%v", kinds)
	}
	// Negative: two responds exist — must NOT warn missing_respond.
	if hasKind(kinds, "missing_respond") {
		t.Fatalf("unexpected missing_respond, kinds=%v", kinds)
	}
}

func TestValidateJSONNoWarningsNoHTTP(t *testing.T) {
	r := validateJSONWarnings(t, wfNoHTTPClean)
	if !r.Valid {
		t.Fatal("expected valid=true")
	}
	// Positive: a non-HTTP workflow must produce no warnings.
	if len(r.Warnings) != 0 {
		t.Fatalf("expected no warnings for non-HTTP wf, got %v",
			warningKinds(r))
	}
}

func TestValidateJSONNoWarningsCleanHTTP(t *testing.T) {
	r := validateJSONWarnings(t, wfHTTPClean)
	if !r.Valid {
		t.Fatal("expected valid=true")
	}
	// Positive: a well-formed HTTP workflow must not false-positive.
	if len(r.Warnings) != 0 {
		t.Fatalf("expected no warnings for clean HTTP wf, got %v",
			warningKinds(r))
	}
}

// TestValidateJSONWarningsFieldPresentWhenClean pins the acceptance
// criterion that the warnings array is present (not null) even when
// empty, so JSON consumers can rely on iterating it unconditionally.
func TestValidateJSONWarningsFieldPresentWhenClean(t *testing.T) {
	tmpFile := writeTempWorkflow(t, wfHTTPClean)
	out := captureOutput(func() {
		runWorkflowValidateCmd([]string{tmpFile, "--json"})
	})
	// Positive: the raw JSON carries a warnings key.
	if !strings.Contains(out, `"warnings"`) {
		t.Fatalf("expected warnings key in JSON, got: %s", out)
	}
	// Negative: it must not serialise as null.
	if strings.Contains(out, `"warnings":null`) ||
		strings.Contains(out, `"warnings": null`) {
		t.Fatalf("warnings must be an array, not null: %s", out)
	}
}

func TestValidateTextWarnsMissingSchemas(t *testing.T) {
	tmpFile := writeTempWorkflow(t, wfHTTPMissingSchemas)
	stderr := captureStderr(func() {
		// stdout carries the "Valid:" line; warnings go to stderr.
		captureOutput(func() {
			runWorkflowValidateCmd([]string{tmpFile})
		})
	})
	// Positive: text mode surfaces the warning kind on stderr.
	if !strings.Contains(stderr, "missing_schemas") {
		t.Fatalf("expected missing_schemas on stderr, got: %q", stderr)
	}
}

func TestValidateTextNoWarningsCleanHTTP(t *testing.T) {
	tmpFile := writeTempWorkflow(t, wfHTTPClean)
	stderr := captureStderr(func() {
		captureOutput(func() {
			runWorkflowValidateCmd([]string{tmpFile})
		})
	})
	// Negative: a clean workflow prints no warning kinds on stderr.
	for _, k := range []string{
		"missing_schemas", "missing_respond", "duplicate_respond",
	} {
		if strings.Contains(stderr, k) {
			t.Fatalf("clean wf should emit no warnings, saw %q: %q",
				k, stderr)
		}
	}
}
