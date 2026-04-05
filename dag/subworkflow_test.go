// dag/subworkflow_test.go
// Tests for SubWorkflow config parsing, builder API, WithDetach modifier,
// and validation. Pure unit tests — no NATS dependency.
// Methodology: each test validates both positive (happy path) and negative
// (error/rejection) behavior to ensure invariants hold in both directions.
package dag

import (
	"encoding/json"
	"testing"
)

func TestParseSubWorkflowConfig_Valid(t *testing.T) {
	step := StepDef{
		ID:     "spawn-child",
		Task:   "child-wf",
		Type:   StepTypeSubWorkflow,
		Config: MarshalConfig(&SubWorkflowConfig{Workflow: "child-wf"}),
	}
	cfg, err := ParseSubWorkflowConfig(step)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Workflow != "child-wf" {
		t.Fatalf("Workflow = %q, want %q", cfg.Workflow, "child-wf")
	}
	// Negative: Detach defaults to false.
	if cfg.Detach {
		t.Fatal("Detach should default to false")
	}
}

func TestParseSubWorkflowConfig_WrongType(t *testing.T) {
	step := StepDef{
		ID:     "normal-step",
		Task:   "task-a",
		Type:   StepTypeNormal,
		Config: MarshalConfig(&SubWorkflowConfig{Workflow: "wf"}),
	}
	_, err := ParseSubWorkflowConfig(step)
	if err == nil {
		t.Fatal("expected error for wrong step type, got nil")
	}
	// Positive: error message mentions the expected type.
	if err.Error() == "" {
		t.Fatal("error message should not be empty")
	}
}

func TestParseSubWorkflowConfig_NilConfig(t *testing.T) {
	step := StepDef{
		ID:   "no-config",
		Task: "child-wf",
		Type: StepTypeSubWorkflow,
	}
	_, err := ParseSubWorkflowConfig(step)
	if err == nil {
		t.Fatal("expected error for nil Config, got nil")
	}
	// Positive: error references the step ID.
	if err.Error() == "" {
		t.Fatal("error message should not be empty")
	}
}

func TestParseSubWorkflowConfig_MalformedJSON(t *testing.T) {
	step := StepDef{
		ID:     "bad-json",
		Task:   "child-wf",
		Type:   StepTypeSubWorkflow,
		Config: json.RawMessage(`{invalid`),
	}
	_, err := ParseSubWorkflowConfig(step)
	if err == nil {
		t.Fatal("expected error for malformed JSON, got nil")
	}
	// Positive: error wraps the unmarshal error.
	if err.Error() == "" {
		t.Fatal("error message should not be empty")
	}
}

func TestBuilderSubWorkflow_ProducesCorrectStepDef(t *testing.T) {
	b := NewWorkflow("parent-wf")
	ref := b.SubWorkflow("spawn", "child-wf")

	// Positive: step ref has correct ID.
	if ref.ID() != "spawn" {
		t.Fatalf("ref.ID() = %q, want %q", ref.ID(), "spawn")
	}

	def, err := b.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if len(def.Steps) != 1 {
		t.Fatalf("len(Steps) = %d, want 1", len(def.Steps))
	}

	step := def.Steps[0]
	if step.Type != StepTypeSubWorkflow {
		t.Fatalf("Type = %v, want SubWorkflow", step.Type)
	}
	if step.Task != "child-wf" {
		t.Fatalf("Task = %q, want %q", step.Task, "child-wf")
	}

	// Negative: Config should be non-nil and parseable.
	cfg, err := ParseSubWorkflowConfig(step)
	if err != nil {
		t.Fatalf("ParseSubWorkflowConfig failed: %v", err)
	}
	if cfg.Workflow != "child-wf" {
		t.Fatalf(
			"cfg.Workflow = %q, want %q",
			cfg.Workflow, "child-wf",
		)
	}
}

func TestBuilderSubWorkflow_PanicsOnEmptyID(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for empty id")
		}
	}()
	b := NewWorkflow("parent-wf")
	b.SubWorkflow("", "child-wf")
}

func TestBuilderSubWorkflow_PanicsOnEmptyWorkflow(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for empty workflow")
		}
	}()
	b := NewWorkflow("parent-wf")
	b.SubWorkflow("spawn", "")
}

func TestWithDetach_SetsDetachTrue(t *testing.T) {
	b := NewWorkflow("parent-wf")
	b.SubWorkflow("spawn", "child-wf").WithDetach()

	def, err := b.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	cfg, err := ParseSubWorkflowConfig(def.Steps[0])
	if err != nil {
		t.Fatalf("ParseSubWorkflowConfig failed: %v", err)
	}
	// Positive: Detach is true.
	if !cfg.Detach {
		t.Fatal("Detach should be true after WithDetach()")
	}
	// Negative: Workflow name is preserved.
	if cfg.Workflow != "child-wf" {
		t.Fatalf(
			"cfg.Workflow = %q, want %q",
			cfg.Workflow, "child-wf",
		)
	}
}

func TestWithDetach_PanicsOnNonSubWorkflow(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for non-SubWorkflow step")
		}
	}()
	b := NewWorkflow("wf")
	ref := b.Task("t1", "task-a")
	ref.WithDetach()
}

func TestValidateSubWorkflow_RejectsMissingConfig(t *testing.T) {
	def := WorkflowDef{
		Name:    "bad-wf",
		Version: "1",
		Steps: []StepDef{
			{
				ID:   "spawn",
				Task: "child-wf",
				Type: StepTypeSubWorkflow,
				// Config intentionally nil
			},
		},
	}
	err := Validate(def)
	// Positive: validation rejects missing config.
	if err == nil {
		t.Fatal("expected validation error for nil config")
	}
	// Negative: non-empty error message.
	if err.Error() == "" {
		t.Fatal("error message should not be empty")
	}
}

func TestValidateSubWorkflow_RejectsEmptyWorkflow(t *testing.T) {
	def := WorkflowDef{
		Name:    "bad-wf",
		Version: "1",
		Steps: []StepDef{
			{
				ID:   "spawn",
				Task: "child-wf",
				Type: StepTypeSubWorkflow,
				Config: MarshalConfig(
					&SubWorkflowConfig{Workflow: ""},
				),
			},
		},
	}
	err := Validate(def)
	// Positive: validation rejects empty workflow name.
	if err == nil {
		t.Fatal("expected validation error for empty workflow")
	}
	// Negative: non-empty error message.
	if err.Error() == "" {
		t.Fatal("error message should not be empty")
	}
}

func TestValidateSubWorkflow_AcceptsValidConfig(t *testing.T) {
	def := WorkflowDef{
		Name:    "good-wf",
		Version: "1",
		Steps: []StepDef{
			{
				ID:   "spawn",
				Task: "child-wf",
				Type: StepTypeSubWorkflow,
				Config: MarshalConfig(
					&SubWorkflowConfig{Workflow: "child-wf"},
				),
			},
		},
	}
	err := Validate(def)
	// Positive: valid config passes.
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
}
