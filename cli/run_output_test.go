// cli/run_output_test.go
// Tests for the run output command formatting.
// Methodology: unit test the output formatting with synthetic run data.
package cli

import (
	"strings"
	"testing"

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
