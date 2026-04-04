// dag/builder_test.go

// Tests for the Graph DSL builder: fluent API for constructing WorkflowDefs.
// Methodology: build workflows via DSL, then inspect the resulting WorkflowDef
// to verify step count, dependency wiring, types, and validation integration.
// Tests cover both the legacy string-based API and the new StepRef-based API.
package dag

import (
	"testing"
	"time"
)

func TestBuilderLinearChain(t *testing.T) {
	wf := NewWorkflow("linear")
	wf.Task("a", "task-a")
	wf.Task("b", "task-b")
	wf.DependsOn("a")
	wf.Task("c", "task-c")
	wf.DependsOn("b")

	def, err := wf.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if def.Name != "linear" {
		t.Fatalf("Name = %q, want %q", def.Name, "linear")
	}
	if len(def.Steps) != 3 {
		t.Fatalf("Steps count = %d, want 3", len(def.Steps))
	}
	stepB := findStep(def, "b")
	if stepB == nil {
		t.Fatal("step 'b' not found")
	}
	if len(stepB.DependsOn) != 1 || stepB.DependsOn[0] != "a" {
		t.Fatalf("step 'b' DependsOn = %v, want [a]", stepB.DependsOn)
	}
}

func TestBuilderFanOutFanIn(t *testing.T) {
	wf := NewWorkflow("fan")
	wf.Task("root", "task-root")
	wf.Task("left", "task-left")
	wf.DependsOn("root")
	wf.Task("right", "task-right")
	wf.DependsOn("root")
	wf.Task("join", "task-join")
	wf.DependsOn("left", "right")

	def, err := wf.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if len(def.Steps) != 4 {
		t.Fatalf("Steps count = %d, want 4", len(def.Steps))
	}
	join := findStep(def, "join")
	if join == nil {
		t.Fatal("step 'join' not found")
	}
	if len(join.DependsOn) != 2 {
		t.Fatalf("join.DependsOn count = %d, want 2", len(join.DependsOn))
	}
}

func TestBuilderAgentLoop(t *testing.T) {
	wf := NewWorkflow("with-loop")
	wf.Task("prep", "task-prep")
	wf.AgentLoop("fix", "task-fix")
	wf.DependsOn("prep")
	wf.WithMaxIterations(10)
	wf.WithMaxDuration(5 * time.Minute)

	def, err := wf.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	fix := findStep(def, "fix")
	if fix == nil {
		t.Fatal("step 'fix' not found")
	}
	if fix.Type != StepTypeAgentLoop {
		t.Fatalf("fix.Type = %v, want AgentLoop", fix.Type)
	}
	if fix.Loop == nil {
		t.Fatal("fix.Loop must not be nil")
	}
	if fix.Loop.MaxIterations != 10 {
		t.Fatalf("fix.Loop.MaxIterations = %d, want 10", fix.Loop.MaxIterations)
	}
}

func TestBuilderWithTimeout(t *testing.T) {
	wf := NewWorkflow("timeouts")
	wf.Task("a", "task-a")
	wf.WithTimeout(30 * time.Second)

	def, err := wf.Build()
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
	wf := NewWorkflow("bad")
	wf.Task("a", "task-a")
	wf.DependsOn("nonexistent")

	_, err := wf.Build()
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
}

func TestBuilderAgentStep(t *testing.T) {
	wf := NewWorkflow("agent-wf")
	plan := wf.Agent("plan", "llm-planner",
		map[string]string{"role": "planner"})
	_ = wf.Agent("code", "llm-coder",
		map[string]string{"role": "coder"}).After(plan)

	def, err := wf.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if len(def.Steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(def.Steps))
	}

	// Positive: step type and metadata
	if def.Steps[0].Type != StepTypeAgent {
		t.Fatalf("step 0 type = %v, want Agent", def.Steps[0].Type)
	}
	if def.Steps[0].Metadata["role"] != "planner" {
		t.Fatalf("step 0 role = %q, want planner",
			def.Steps[0].Metadata["role"])
	}

	// Positive: dependency wiring
	if len(def.Steps[1].DependsOn) != 1 {
		t.Fatalf("step 1 deps = %d, want 1",
			len(def.Steps[1].DependsOn))
	}
	if def.Steps[1].DependsOn[0] != "plan" {
		t.Fatalf("step 1 dep = %q, want plan",
			def.Steps[1].DependsOn[0])
	}
}

func TestBuilderVersionSetter(t *testing.T) {
	wf := NewWorkflow("versioned").Version("2.0")
	wf.Task("a", "task-a")
	def, err := wf.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	// Positive: custom version applied
	if def.Version != "2.0" {
		t.Fatalf("Version = %q, want %q", def.Version, "2.0")
	}
	// Negative: default is "1" when not overridden
	wf2 := NewWorkflow("default-ver")
	wf2.Task("b", "task-b")
	def2, _ := wf2.Build()
	if def2.Version != "1" {
		t.Fatalf("default Version = %q, want %q", def2.Version, "1")
	}
}

func TestBuilderNameAccessor(t *testing.T) {
	wf := NewWorkflow("my-workflow")
	// Positive: Name returns the configured name
	if wf.Name() != "my-workflow" {
		t.Fatalf("Name() = %q, want %q", wf.Name(), "my-workflow")
	}
	// Negative: different builder has different name
	wf2 := NewWorkflow("other")
	if wf2.Name() == wf.Name() {
		t.Fatal("different builders should have different names")
	}
}

func TestBuilderAgentEmptyIDPanics(t *testing.T) {
	wf := NewWorkflow("bad-agent")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for empty agent step ID")
		}
	}()
	wf.Agent("", "task", map[string]string{})
}

func TestBuilderAgentEmptyTaskPanics(t *testing.T) {
	wf := NewWorkflow("bad-agent")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for empty agent task")
		}
	}()
	wf.Agent("id", "", map[string]string{})
}

func TestBuilderDependsOnBeforeStepPanics(t *testing.T) {
	wf := NewWorkflow("no-step")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for DependsOn before step")
		}
	}()
	wf.DependsOn("a")
}

func TestBuilderWithTimeoutBeforeStepPanics(t *testing.T) {
	wf := NewWorkflow("no-step")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for WithTimeout before step")
		}
	}()
	wf.WithTimeout(time.Second)
}

func TestBuilderWithMaxIterationsBeforeStepPanics(t *testing.T) {
	wf := NewWorkflow("no-step")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for WithMaxIterations before step")
		}
	}()
	wf.WithMaxIterations(5)
}

func TestBuilderWithMaxDurationBeforeStepPanics(t *testing.T) {
	wf := NewWorkflow("no-step")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for WithMaxDuration before step")
		}
	}()
	wf.WithMaxDuration(time.Minute)
}

func TestBuilderWithMaxIterationsOnNormalPanics(t *testing.T) {
	wf := NewWorkflow("normal")
	wf.Task("a", "task-a")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal(
				"expected panic for WithMaxIterations on normal",
			)
		}
	}()
	wf.WithMaxIterations(5)
}

func TestBuilderWithMaxDurationOnNormalPanics(t *testing.T) {
	wf := NewWorkflow("normal")
	wf.Task("a", "task-a")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal(
				"expected panic for WithMaxDuration on normal",
			)
		}
	}()
	wf.WithMaxDuration(time.Minute)
}

func TestBuilderMap(t *testing.T) {
	wf := NewWorkflow("map-wf")
	input := wf.Task("input", "task-input")
	_ = wf.Map("map-step", "task-map").After(input)

	def, err := wf.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Positive: step created with correct type
	step := findStep(def, "map-step")
	if step == nil {
		t.Fatal("map-step not found")
	}
	if step.Type != StepTypeMap {
		t.Fatalf("Type = %v, want Map", step.Type)
	}

	// Positive: Map config initialized with default MaxItems
	if step.Map == nil {
		t.Fatal("Map config must not be nil")
	}
	if step.Map.MaxItems != 1000 {
		t.Fatalf("Map.MaxItems = %d, want 1000", step.Map.MaxItems)
	}
}

func TestBuilderMapEmptyIDPanics(t *testing.T) {
	wf := NewWorkflow("bad-map")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for empty map step ID")
		}
	}()
	wf.Map("", "task")
}

func TestBuilderMapEmptyTaskPanics(t *testing.T) {
	wf := NewWorkflow("bad-map")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for empty map task")
		}
	}()
	wf.Map("id", "")
}

func findStep(def WorkflowDef, id string) *StepDef {
	for i := range def.Steps {
		if def.Steps[i].ID == id {
			return &def.Steps[i]
		}
	}
	return nil
}

func TestBuildPopulatesAuxSteps(t *testing.T) {
	wb := NewWorkflow("aux-test")
	main := wb.Task("main", "risky")
	fallback := wb.Task("fallback", "recover")
	rollback := wb.Task("rollback", "undo")
	main.OnFailure(fallback)
	main.Compensate(rollback)
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	// Positive: fallback is an aux step
	if !def.AuxSteps["fallback"] {
		t.Fatal("expected fallback in AuxSteps")
	}
	// Positive: rollback is an aux step
	if !def.AuxSteps["rollback"] {
		t.Fatal("expected rollback in AuxSteps")
	}
	// Negative: main is not an aux step
	if def.AuxSteps["main"] {
		t.Fatal("main should not be in AuxSteps")
	}
}
