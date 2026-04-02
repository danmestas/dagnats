// cli/run_output_test.go
// Tests for the run output command formatting.
// Methodology: unit test the output formatting with synthetic run data.
package cli

import (
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
)

func TestFormatRunOutput_CompletedRun(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	run := dag.WorkflowRun{
		RunID:      "run123",
		WorkflowID: "test-wf",
		Status:     dag.RunStatusCompleted,
		Steps: map[string]dag.StepState{
			"step-a": {
				Status: dag.StepStatusCompleted,
				Output: []byte(`{"result":"hello"}`),
			},
		},
	}
	def := dag.WorkflowDef{
		Name: "test-wf",
		Steps: []dag.StepDef{
			{ID: "step-a", Task: "greet"},
		},
	}
	output := FormatRunOutput(run, def)
	// Positive: contains the step output data
	if !strings.Contains(output, `{"result":"hello"}`) {
		t.Fatal("output should contain step output data")
	}
	// Negative: does not contain error text
	if strings.Contains(output, "Error") {
		t.Fatal("output should not contain error text")
	}
}

func TestFormatRunOutput_NotCompleted(t *testing.T) {
	run := dag.WorkflowRun{
		RunID:  "run123",
		Status: dag.RunStatusRunning,
		Steps: map[string]dag.StepState{
			"a": {Status: dag.StepStatusRunning},
		},
	}
	def := dag.WorkflowDef{
		Name:  "test-wf",
		Steps: []dag.StepDef{{ID: "a", Task: "t"}},
	}
	output := FormatRunOutput(run, def)
	// Positive: warns that run is not completed
	if !strings.Contains(output, "not completed") {
		t.Fatal("should indicate run is not completed")
	}
	// Negative: no step output data shown
	if strings.Contains(output, "result") {
		t.Fatal("should not show output data for incomplete run")
	}
}

func TestFormatRunOutput_MultipleTerminals(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	run := dag.WorkflowRun{
		RunID:      "run456",
		WorkflowID: "multi-wf",
		Status:     dag.RunStatusCompleted,
		Steps: map[string]dag.StepState{
			"root":   {Status: dag.StepStatusCompleted, Output: []byte("root-out")},
			"leaf-a": {Status: dag.StepStatusCompleted, Output: []byte("out-a")},
			"leaf-b": {Status: dag.StepStatusCompleted, Output: []byte("out-b")},
		},
	}
	def := dag.WorkflowDef{
		Name: "multi-wf",
		Steps: []dag.StepDef{
			{ID: "root", Task: "t"},
			{ID: "leaf-a", Task: "t", DependsOn: []string{"root"}},
			{ID: "leaf-b", Task: "t", DependsOn: []string{"root"}},
		},
	}
	output := FormatRunOutput(run, def)
	// Positive: contains separator headers for multiple terminals
	if !strings.Contains(output, "---") {
		t.Fatal("should show separators for multiple terminal steps")
	}
	// Negative: does not contain root step output (root has dependents)
	if strings.Contains(output, "root-out") {
		t.Fatal("should not show output from non-terminal step")
	}
}

func TestRunOutputJSONOutput(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	result := runOutputResult{
		RunID:  "out-run-1",
		Status: "completed",
		Outputs: map[string]string{
			"final-step": "hello world",
		},
	}

	var buf strings.Builder
	err := FormatJSON(&buf, result)
	if err != nil {
		t.Fatalf("FormatJSON failed: %v", err)
	}
	output := buf.String()

	// Positive: output should contain expected fields
	if !strings.Contains(output, `"run_id"`) {
		t.Fatal("JSON output should contain run_id field")
	}
	if !strings.Contains(output, `"outputs"`) {
		t.Fatal("JSON output should contain outputs field")
	}
	if !strings.Contains(output, "hello world") {
		t.Fatal("JSON output should contain step output")
	}

	// Negative: should not contain human formatting
	if strings.Contains(output, "---") {
		t.Fatal("JSON output should not contain separator")
	}
}

func TestRunOutputJSONOmitsEmptyOutputs(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	result := runOutputResult{
		RunID:  "out-run-2",
		Status: "running",
	}

	var buf strings.Builder
	err := FormatJSON(&buf, result)
	if err != nil {
		t.Fatalf("FormatJSON failed: %v", err)
	}
	output := buf.String()

	// Positive: should contain run_id and status
	if !strings.Contains(output, `"run_id"`) {
		t.Fatal("JSON output should contain run_id field")
	}
	if !strings.Contains(output, "running") {
		t.Fatal("JSON output should contain status value")
	}

	// Negative: outputs should be omitted when nil
	if strings.Contains(output, `"outputs"`) {
		t.Fatal("JSON output should omit empty outputs")
	}
}

func TestBuildRunOutputResult(t *testing.T) {
	run := dag.WorkflowRun{
		RunID:      "build-out-1",
		WorkflowID: "wf-build",
		Status:     dag.RunStatusCompleted,
		Steps: map[string]dag.StepState{
			"step-a": {
				Status: dag.StepStatusCompleted, Attempts: 1,
				Output: []byte("result-a"),
			},
			"step-b": {
				Status: dag.StepStatusCompleted, Attempts: 1,
				Output: []byte("result-b"),
			},
		},
		CreatedAt: time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
	}
	def := dag.WorkflowDef{
		Name: "wf-build",
		Steps: []dag.StepDef{
			{ID: "step-a"},
			{ID: "step-b", DependsOn: []string{"step-a"}},
		},
	}

	result := buildRunOutputResult(run, def)

	// Positive: terminal step output should be present
	if result.Outputs["step-b"] != "result-b" {
		t.Fatalf(
			"expected step-b output 'result-b', got %q",
			result.Outputs["step-b"],
		)
	}

	// Negative: non-terminal step should not be in outputs
	if _, ok := result.Outputs["step-a"]; ok {
		t.Fatal("non-terminal step should not be in outputs")
	}
}
