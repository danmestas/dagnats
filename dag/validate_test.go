// dag/validate_test.go

// Tests for DAG validation: duplicate step IDs, missing dependency references,
// cycle detection, empty workflows, and valid DAGs.
// Methodology: each test builds a specific invalid (or valid) DAG and asserts
// the exact error returned. Positive + negative space checked per test.
package dag

import (
	"strings"
	"testing"
)

func TestValidateDuplicateStepIDs(t *testing.T) {
	def := WorkflowDef{Name: "dup-ids", Version: "1", Steps: []StepDef{
		{ID: "a", Task: "task-a", Type: StepTypeNormal},
		{ID: "a", Task: "task-b", Type: StepTypeNormal},
	}}
	err := Validate(def)
	if err == nil { t.Fatal("expected error for duplicate step IDs, got nil") }
	if !strings.Contains(err.Error(), "duplicate") { t.Fatalf("error should mention 'duplicate', got: %v", err) }
}

func TestValidateMissingDependency(t *testing.T) {
	def := WorkflowDef{Name: "missing-dep", Version: "1", Steps: []StepDef{
		{ID: "a", Task: "task-a", DependsOn: []string{"nonexistent"}, Type: StepTypeNormal},
	}}
	err := Validate(def)
	if err == nil { t.Fatal("expected error for missing dependency, got nil") }
	if !strings.Contains(err.Error(), "nonexistent") { t.Fatalf("error should mention missing dep name, got: %v", err) }
}

func TestValidateCycleDetection(t *testing.T) {
	def := WorkflowDef{Name: "cycle", Version: "1", Steps: []StepDef{
		{ID: "a", Task: "task-a", DependsOn: []string{"c"}, Type: StepTypeNormal},
		{ID: "b", Task: "task-b", DependsOn: []string{"a"}, Type: StepTypeNormal},
		{ID: "c", Task: "task-c", DependsOn: []string{"b"}, Type: StepTypeNormal},
	}}
	err := Validate(def)
	if err == nil { t.Fatal("expected error for cycle, got nil") }
	if !strings.Contains(err.Error(), "cycle") { t.Fatalf("error should mention 'cycle', got: %v", err) }
}

func TestValidateEmptyWorkflow(t *testing.T) {
	def := WorkflowDef{Name: "empty", Version: "1", Steps: nil}
	err := Validate(def)
	if err == nil { t.Fatal("expected error for empty workflow, got nil") }
	if !strings.Contains(err.Error(), "no steps") { t.Fatalf("error should mention 'no steps', got: %v", err) }
}

func TestValidateValidDAG(t *testing.T) {
	def := WorkflowDef{Name: "valid", Version: "1", Steps: []StepDef{
		{ID: "a", Task: "task-a", Type: StepTypeNormal},
		{ID: "b", Task: "task-b", DependsOn: []string{"a"}, Type: StepTypeNormal},
		{ID: "c", Task: "task-c", DependsOn: []string{"a"}, Type: StepTypeNormal},
		{ID: "d", Task: "task-d", DependsOn: []string{"b", "c"}, Type: StepTypeNormal},
	}}
	err := Validate(def)
	if err != nil { t.Fatalf("expected valid DAG, got error: %v", err) }
}

func TestValidateAgentLoopRequiresLoopConfig(t *testing.T) {
	def := WorkflowDef{Name: "loop-no-config", Version: "1", Steps: []StepDef{
		{ID: "a", Task: "task-a", Type: StepTypeAgentLoop, Loop: nil},
	}}
	err := Validate(def)
	if err == nil { t.Fatal("expected error for agent loop without Loop config, got nil") }
	if !strings.Contains(err.Error(), "Loop") { t.Fatalf("error should mention 'Loop', got: %v", err) }
}

func TestValidateNormalStepRejectsLoopConfig(t *testing.T) {
	def := WorkflowDef{Name: "normal-with-loop", Version: "1", Steps: []StepDef{
		{ID: "a", Task: "task-a", Type: StepTypeNormal, Loop: &AgentLoopConfig{MaxIterations: 5}},
	}}
	err := Validate(def)
	if err == nil { t.Fatal("expected error for normal step with Loop config, got nil") }
	if !strings.Contains(err.Error(), "Loop") { t.Fatalf("error should mention 'Loop', got: %v", err) }
}

func TestValidateAgentStepRejectsLoopConfig(t *testing.T) {
	def := WorkflowDef{Name: "bad-agent", Version: "1", Steps: []StepDef{
		{ID: "a", Task: "llm-task", Type: StepTypeAgent,
			Loop: &AgentLoopConfig{MaxIterations: 5}},
	}}
	err := Validate(def)
	if err == nil {
		t.Fatal("expected error for agent step with loop config, got nil")
	}
	if !strings.Contains(err.Error(), "Loop") {
		t.Fatalf("error should mention Loop, got: %v", err)
	}
}

func TestValidateAgentStepValid(t *testing.T) {
	def := WorkflowDef{Name: "good-agent", Version: "1", Steps: []StepDef{
		{ID: "a", Task: "llm-task", Type: StepTypeAgent,
			Metadata: map[string]string{"role": "coder"}},
	}}
	if err := Validate(def); err != nil {
		t.Fatalf("valid agent step should pass, got: %v", err)
	}
}

func TestValidateOnFailureRefExists(t *testing.T) {
	def := WorkflowDef{
		Name: "v", Version: "1",
		Steps: []StepDef{
			{ID: "s1", Task: "t", Type: StepTypeNormal,
				OnFailure: "missing"},
		},
	}
	err := Validate(def)
	// Positive: error for missing reference
	if err == nil {
		t.Fatalf("expected error for OnFailure ref to missing step")
	}
	if !strings.Contains(err.Error(), "missing") {
		t.Fatalf("error = %q", err)
	}
}

func TestValidateCompensateRefExists(t *testing.T) {
	def := WorkflowDef{
		Name: "v", Version: "1",
		Steps: []StepDef{
			{ID: "s1", Task: "t", Type: StepTypeNormal,
				Compensate: "ghost"},
		},
	}
	err := Validate(def)
	if err == nil {
		t.Fatalf("expected error for Compensate ref to missing step")
	}
}

func TestValidateOnFailureAndCompensateValid(t *testing.T) {
	def := WorkflowDef{
		Name: "v", Version: "1",
		Steps: []StepDef{
			{ID: "deploy", Task: "t", Type: StepTypeNormal,
				OnFailure: "notify", Compensate: "rollback"},
			{ID: "notify", Task: "t", Type: StepTypeNormal},
			{ID: "rollback", Task: "t", Type: StepTypeNormal},
		},
	}
	if err := Validate(def); err != nil {
		t.Fatalf("valid def rejected: %v", err)
	}
}
