// cli/workflow_validate_test.go
// Pure unit tests for workflow validate command. No NATS required --
// validates JSON files on disk against dag.Validate.
// Methodology: write temp JSON files, call validateWorkflowFile, and
// assert on the result string or error for valid and invalid cases.
package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWorkflowValidateAcceptsValidFile(t *testing.T) {
	validJSON := `{
		"name": "valid-wf",
		"version": "1.0",
		"steps": [
			{"id": "a", "task": "task-a"},
			{"id": "b", "task": "task-b", "depends_on": ["a"]}
		]
	}`
	tmpFile := filepath.Join(t.TempDir(), "valid.json")
	if err := os.WriteFile(
		tmpFile, []byte(validJSON), 0644,
	); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	result, err := validateWorkflowFile(tmpFile)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Positive: must report valid with name and step count.
	if !strings.Contains(result, "Valid: valid-wf") {
		t.Fatalf(
			"expected 'Valid: valid-wf' in result, got: %s",
			result,
		)
	}
	if !strings.Contains(result, "2 steps") {
		t.Fatalf(
			"expected '2 steps' in result, got: %s",
			result,
		)
	}

	// Negative: must not contain "Invalid".
	if strings.Contains(result, "Invalid") {
		t.Fatal("valid workflow should not say Invalid")
	}
}

func TestWorkflowValidateRejectsNoSteps(t *testing.T) {
	// A workflow with no steps violates dag.Validate.
	noStepsJSON := `{
		"name": "empty-wf",
		"version": "1.0",
		"steps": []
	}`
	tmpFile := filepath.Join(t.TempDir(), "empty.json")
	if err := os.WriteFile(
		tmpFile, []byte(noStepsJSON), 0644,
	); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	result, err := validateWorkflowFile(tmpFile)

	// Positive: must return an error for empty steps.
	if err == nil {
		t.Fatal("expected error for workflow with no steps")
	}

	// Positive: error must mention "Invalid".
	if !strings.Contains(err.Error(), "Invalid") {
		t.Fatalf(
			"expected 'Invalid' in error, got: %v", err,
		)
	}

	// Negative: result must be empty on error.
	if result != "" {
		t.Fatalf(
			"expected empty result on error, got: %s", result,
		)
	}
}

func TestWorkflowValidateRejectsMissingDependency(t *testing.T) {
	// Step "b" depends on "nonexistent" which is not defined.
	badDepJSON := `{
		"name": "bad-dep",
		"version": "1.0",
		"steps": [
			{"id": "a", "task": "task-a"},
			{
				"id": "b",
				"task": "task-b",
				"depends_on": ["nonexistent"]
			}
		]
	}`
	tmpFile := filepath.Join(t.TempDir(), "baddep.json")
	if err := os.WriteFile(
		tmpFile, []byte(badDepJSON), 0644,
	); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	result, err := validateWorkflowFile(tmpFile)

	// Positive: must return a validation error.
	if err == nil {
		t.Fatal("expected error for missing dependency")
	}

	// Positive: error must mention the missing dependency.
	if !strings.Contains(err.Error(), "nonexistent") {
		t.Fatalf(
			"expected 'nonexistent' in error, got: %v", err,
		)
	}

	// Negative: result must be empty on error.
	if result != "" {
		t.Fatalf(
			"expected empty result on error, got: %s", result,
		)
	}
}

func TestWorkflowValidateJSONValid(t *testing.T) {
	validJSON := `{
		"name": "json-valid",
		"version": "1.0",
		"steps": [
			{"id": "a", "task": "task-a"},
			{"id": "b", "task": "task-b", "depends_on": ["a"]}
		]
	}`
	tmpFile := filepath.Join(t.TempDir(), "valid.json")
	if err := os.WriteFile(
		tmpFile, []byte(validJSON), 0644,
	); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	output := captureOutput(func() {
		runWorkflowValidateCmd([]string{tmpFile, "--json"})
	})

	var result workflowValidateResult
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatalf("unmarshal json: %v (output: %s)", err, output)
	}

	// Positive: must be valid with correct name and step count.
	if !result.Valid {
		t.Fatal("expected valid=true")
	}
	if result.Name != "json-valid" {
		t.Fatalf("expected name 'json-valid', got %q", result.Name)
	}
	if result.Steps != 2 {
		t.Fatalf("expected 2 steps, got %d", result.Steps)
	}

	// Negative: error must be empty for valid workflow.
	if result.Error != "" {
		t.Fatalf("expected empty error, got %q", result.Error)
	}
}

func TestWorkflowValidateJSONInvalid(t *testing.T) {
	noStepsJSON := `{
		"name": "empty-wf",
		"version": "1.0",
		"steps": []
	}`
	tmpFile := filepath.Join(t.TempDir(), "empty.json")
	if err := os.WriteFile(
		tmpFile, []byte(noStepsJSON), 0644,
	); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	output := captureOutput(func() {
		runWorkflowValidateCmd([]string{tmpFile, "--json"})
	})

	var result workflowValidateResult
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatalf("unmarshal json: %v (output: %s)", err, output)
	}

	// Positive: must be invalid with an error message.
	if result.Valid {
		t.Fatal("expected valid=false for empty steps")
	}
	if result.Error == "" {
		t.Fatal("expected non-empty error for invalid workflow")
	}

	// Negative: name should be empty on validation failure.
	if result.Steps != 0 {
		t.Fatalf("expected 0 steps on error, got %d", result.Steps)
	}
}

func TestWorkflowValidateRejectsMalformedJSON(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(
		tmpFile, []byte("{not valid json}"), 0644,
	); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	result, err := validateWorkflowFile(tmpFile)

	// Positive: must return a parse error.
	if err == nil {
		t.Fatal("expected error for malformed JSON")
	}

	// Positive: error must mention parsing.
	if !strings.Contains(err.Error(), "parse workflow") {
		t.Fatalf(
			"expected 'parse workflow' in error, got: %v", err,
		)
	}

	// Negative: result must be empty.
	if result != "" {
		t.Fatalf(
			"expected empty result on error, got: %s", result,
		)
	}
}
