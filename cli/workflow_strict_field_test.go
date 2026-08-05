// cli/workflow_strict_field_test.go
// Methodology: drive the pure parseWorkflowFile / validateWorkflowFile
// helpers directly (no NATS). A workflow-definition file that declares
// a top-level key the parser doesn't recognize — the singular `trigger`
// object instead of the `triggers` array, or any typo — must fail
// closed rather than silently dropping the data (#607). The strictness
// is scoped to the outermost envelope only: unknown keys nested inside a
// step config must still parse, preserving forward compatibility.
package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const workflowWithSingularTrigger = `{
	"name": "wf-singular",
	"version": "1.0",
	"trigger": {
		"id": "test-cron",
		"enabled": true,
		"cron": {
			"expression": "*/5 * * * *",
			"timezone": "UTC",
			"backfill": false
		}
	},
	"steps": [
		{"id": "a", "task": "task-a"}
	]
}`

const workflowWithBogusTopLevelField = `{
	"name": "wf-bogus",
	"version": "1.0",
	"scheudle": "*/5 * * * *",
	"steps": [
		{"id": "a", "task": "task-a"}
	]
}`

const workflowWithNestedUnknownField = `{
	"name": "wf-nested",
	"version": "1.0",
	"steps": [
		{"id": "a", "task": "task-a", "unknown_step_field": 123}
	]
}`

func TestParseWorkflowFile_RejectsSingularTrigger(t *testing.T) {
	_, err := parseWorkflowFile([]byte(workflowWithSingularTrigger))
	// Positive: the singular `trigger` key is rejected.
	if err == nil {
		t.Fatal("expected error for singular `trigger` field")
	}
	// Negative: the error names the offending field so the author can
	// find the typo.
	if !strings.Contains(err.Error(), "trigger") {
		t.Fatalf("error should name the field, got: %v", err)
	}
}

func TestParseWorkflowFile_RejectsUnknownTopLevelField(t *testing.T) {
	_, err := parseWorkflowFile([]byte(workflowWithBogusTopLevelField))
	// Positive: any unrecognized top-level key fails closed.
	if err == nil {
		t.Fatal("expected error for unknown top-level field")
	}
	// Negative: the error names the offending field.
	if !strings.Contains(err.Error(), "scheudle") {
		t.Fatalf("error should name the field, got: %v", err)
	}
}

func TestParseWorkflowFile_AcceptsKnownFields(t *testing.T) {
	wf, err := parseWorkflowFile([]byte(workflowWithEmbeddedTrigger))
	// Positive: a file whose keys are all recognized still parses.
	if err != nil {
		t.Fatalf("well-formed file should parse, got: %v", err)
	}
	// Negative: parsing preserves the payload (not a zero value).
	if wf.Name != "wf-with-trigger" {
		t.Fatalf("name = %q, want wf-with-trigger", wf.Name)
	}
	if len(wf.Triggers) != 1 {
		t.Fatalf("triggers len = %d, want 1", len(wf.Triggers))
	}
}

func TestParseWorkflowFile_IgnoresNestedUnknownFields(t *testing.T) {
	wf, err := parseWorkflowFile([]byte(workflowWithNestedUnknownField))
	// Positive: strictness must not reach into nested structs — an
	// unknown key inside a step still parses (forward compatibility).
	if err != nil {
		t.Fatalf("nested unknown field must not fail parse, got: %v", err)
	}
	// Negative: the recognized top-level fields survive.
	if wf.Name != "wf-nested" || len(wf.Steps) != 1 {
		t.Fatalf("unexpected parse result: %#v", wf)
	}
}

func TestWorkflowValidate_RejectsSingularTrigger(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "wf.json")
	if err := os.WriteFile(
		tmpFile, []byte(workflowWithSingularTrigger), 0644,
	); err != nil {
		t.Fatalf("write: %v", err)
	}

	_, err := validateWorkflowFile(tmpFile)
	// Positive: `workflow validate` applies the same rejection as
	// register — they share the parse path.
	if err == nil {
		t.Fatal("validate should reject singular `trigger` field")
	}
	// Negative: names the field.
	if !strings.Contains(err.Error(), "trigger") {
		t.Fatalf("error should name the field, got: %v", err)
	}
}
