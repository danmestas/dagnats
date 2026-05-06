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

// TestWorkflowValidateAcceptsValidEmbeddedTrigger confirms that
// `workflow validate` honors and validates embedded triggers introduced
// in #171, achieving parity with `workflow register` (issue #180).
func TestWorkflowValidateAcceptsValidEmbeddedTrigger(t *testing.T) {
	good := `{
		"name": "wf-with-trigger",
		"version": "1.0",
		"triggers": [{
			"id": "t1",
			"enabled": true,
			"cron": {"expression": "*/5 * * * *", "timezone": "UTC", "backfill": false}
		}],
		"steps": [{"id": "a", "task": "task-a"}]
	}`
	tmpFile := filepath.Join(t.TempDir(), "good.json")
	if err := os.WriteFile(tmpFile, []byte(good), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	result, err := validateWorkflowFile(tmpFile)
	if err != nil {
		t.Fatalf("expected valid, got error: %v", err)
	}
	if !strings.Contains(result, "Valid: wf-with-trigger") {
		t.Fatalf("expected Valid output, got: %s", result)
	}
}

// TestWorkflowValidateRejectsBadEmbeddedCron confirms that an
// invalid cron in an embedded trigger fails validate, not just register
// (the gap that motivated #180).
func TestWorkflowValidateRejectsBadEmbeddedCron(t *testing.T) {
	bad := `{
		"name": "wf-bad",
		"version": "1.0",
		"triggers": [{
			"id": "t1",
			"enabled": true,
			"cron": {"expression": "not a cron", "timezone": "UTC"}
		}],
		"steps": [{"id": "a", "task": "task-a"}]
	}`
	tmpFile := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(tmpFile, []byte(bad), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := validateWorkflowFile(tmpFile)
	// Positive: errors when embedded cron is malformed.
	if err == nil {
		t.Fatal("expected error for bad embedded cron")
	}
	// Positive: error message mentions the trigger.
	if !strings.Contains(err.Error(), "trigger") {
		t.Fatalf("error should mention trigger, got: %v", err)
	}
}

// TestWorkflowValidateRejectsMismatchedTriggerWorkflowID mirrors the
// guard from registerWorkflowWithTriggers: a typo'd workflow_id in an
// embedded trigger is almost certainly a copy-paste error and should
// fail validation up-front rather than silently wire the trigger to a
// different workflow at register time.
func TestWorkflowValidateRejectsMismatchedTriggerWorkflowID(t *testing.T) {
	bad := `{
		"name": "wf-parent",
		"version": "1.0",
		"triggers": [{
			"id": "t1",
			"workflow_id": "different-wf",
			"enabled": true,
			"cron": {"expression": "*/5 * * * *", "timezone": "UTC"}
		}],
		"steps": [{"id": "a", "task": "task-a"}]
	}`
	tmpFile := filepath.Join(t.TempDir(), "mismatch.json")
	if err := os.WriteFile(tmpFile, []byte(bad), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := validateWorkflowFile(tmpFile)
	if err == nil {
		t.Fatal("expected error for mismatched workflow_id")
	}
	if !strings.Contains(err.Error(), "does not match parent") {
		t.Fatalf(
			"error should mention parent mismatch, got: %v", err)
	}
}

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

	// Negative: must not contain "invalid" error prefix.
	if strings.Contains(result, "invalid:") {
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

	// Positive: error must mention "invalid".
	if !strings.Contains(err.Error(), "invalid") {
		t.Fatalf(
			"expected 'invalid' in error, got: %v", err,
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
