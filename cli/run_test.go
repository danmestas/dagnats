// cli/run_test.go
// Tests for CLI output formatting: verify run status renders correctly.
// Methodology: unit test the formatting functions without HTTP calls.
package cli

import (
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
)

func TestFormatRunStatus(t *testing.T) {
	run := dag.WorkflowRun{
		RunID: "abc123", WorkflowID: "test-wf", Status: dag.RunStatusRunning,
		Steps: map[string]dag.StepState{
			"a": {Status: dag.StepStatusCompleted, Attempts: 1},
			"b": {Status: dag.StepStatusRunning, Attempts: 1},
		},
		CreatedAt: time.Date(2026, 3, 29, 12, 0, 0, 0, time.UTC),
	}
	output := FormatRunStatus(run)
	if !strings.Contains(output, "abc123") {
		t.Fatal("output should contain run ID")
	}
	if !strings.Contains(output, "running") {
		t.Fatal("output should contain status")
	}
	if !strings.Contains(output, "test-wf") {
		t.Fatal("output should contain workflow name")
	}
	if strings.Contains(output, "map[") {
		t.Fatal("output should not contain raw Go map syntax")
	}
}
