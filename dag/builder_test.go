// dag/builder_test.go

// Tests for the Graph DSL builder: fluent API for constructing WorkflowDefs.
// Methodology: build workflows via DSL, then inspect the resulting WorkflowDef
// to verify step count, dependency wiring, types, and validation integration.
package dag

import (
	"testing"
	"time"
)

func TestBuilderLinearChain(t *testing.T) {
	def, err := NewWorkflow("linear").
		Task("a", "task-a").
		Task("b", "task-b").DependsOn("a").
		Task("c", "task-c").DependsOn("b").
		Build()
	if err != nil { t.Fatalf("Build failed: %v", err) }
	if def.Name != "linear" { t.Fatalf("Name = %q, want %q", def.Name, "linear") }
	if len(def.Steps) != 3 { t.Fatalf("Steps count = %d, want 3", len(def.Steps)) }
	stepB := findStep(def, "b")
	if stepB == nil { t.Fatal("step 'b' not found") }
	if len(stepB.DependsOn) != 1 || stepB.DependsOn[0] != "a" {
		t.Fatalf("step 'b' DependsOn = %v, want [a]", stepB.DependsOn)
	}
}

func TestBuilderFanOutFanIn(t *testing.T) {
	def, err := NewWorkflow("fan").
		Task("root", "task-root").
		Task("left", "task-left").DependsOn("root").
		Task("right", "task-right").DependsOn("root").
		Task("join", "task-join").DependsOn("left", "right").
		Build()
	if err != nil { t.Fatalf("Build failed: %v", err) }
	if len(def.Steps) != 4 { t.Fatalf("Steps count = %d, want 4", len(def.Steps)) }
	join := findStep(def, "join")
	if join == nil { t.Fatal("step 'join' not found") }
	if len(join.DependsOn) != 2 { t.Fatalf("join.DependsOn count = %d, want 2", len(join.DependsOn)) }
}

func TestBuilderAgentLoop(t *testing.T) {
	def, err := NewWorkflow("with-loop").
		Task("prep", "task-prep").
		AgentLoop("fix", "task-fix").DependsOn("prep").WithMaxIterations(10).WithMaxDuration(5 * time.Minute).
		Build()
	if err != nil { t.Fatalf("Build failed: %v", err) }
	fix := findStep(def, "fix")
	if fix == nil { t.Fatal("step 'fix' not found") }
	if fix.Type != StepTypeAgentLoop { t.Fatalf("fix.Type = %v, want AgentLoop", fix.Type) }
	if fix.Loop == nil { t.Fatal("fix.Loop must not be nil") }
	if fix.Loop.MaxIterations != 10 { t.Fatalf("fix.Loop.MaxIterations = %d, want 10", fix.Loop.MaxIterations) }
}

func TestBuilderWithTimeout(t *testing.T) {
	def, err := NewWorkflow("timeouts").
		Task("a", "task-a").WithTimeout(30 * time.Second).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	step := findStep(def, "a")
	if step.Timeout != 30*time.Second {
		t.Fatalf("Timeout = %v, want 30s", step.Timeout)
	}
	if step.Loop != nil {
		t.Fatal("normal step should not have Loop config")
	}
}

func TestBuilderValidationError(t *testing.T) {
	_, err := NewWorkflow("bad").
		Task("a", "task-a").DependsOn("nonexistent").
		Build()
	if err == nil { t.Fatal("expected validation error, got nil") }
}

func findStep(def WorkflowDef, id string) *StepDef {
	for i := range def.Steps {
		if def.Steps[i].ID == id { return &def.Steps[i] }
	}
	return nil
}
