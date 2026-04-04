// dag/planner_test.go
// Unit tests for PlannerConfig parsing, validation, and builder.
// Methodology: test valid configs, boundary violations, and builder
// panics to ensure planner steps are structurally sound at build time.
package dag

import (
	"testing"
)

func TestParsePlannerConfig_Valid(t *testing.T) {
	step := StepDef{
		ID:   "plan",
		Task: "generate-plan",
		Type: StepTypePlanner,
		Config: MarshalConfig(&PlannerConfig{
			MaxSteps:     10,
			MaxDepth:     5,
			AllowedTasks: []string{"build", "test"},
		}),
	}
	cfg, err := ParsePlannerConfig(step)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if cfg.MaxSteps != 10 {
		t.Errorf("MaxSteps = %d, want 10", cfg.MaxSteps)
	}
	if cfg.MaxDepth != 5 {
		t.Errorf("MaxDepth = %d, want 5", cfg.MaxDepth)
	}
	if len(cfg.AllowedTasks) != 2 {
		t.Errorf(
			"AllowedTasks len = %d, want 2",
			len(cfg.AllowedTasks),
		)
	}
}

func TestParsePlannerConfig_WrongType(t *testing.T) {
	step := StepDef{
		ID:   "plan",
		Task: "task",
		Type: StepTypeNormal,
		Config: MarshalConfig(&PlannerConfig{
			MaxSteps: 10,
		}),
	}
	_, err := ParsePlannerConfig(step)
	if err == nil {
		t.Fatal("expected error for wrong type, got nil")
	}
}

func TestParsePlannerConfig_NilConfig(t *testing.T) {
	step := StepDef{
		ID:   "plan",
		Task: "task",
		Type: StepTypePlanner,
	}
	_, err := ParsePlannerConfig(step)
	if err == nil {
		t.Fatal("expected error for nil config, got nil")
	}
}

func TestValidatePlannerConfig_MaxStepsBounds(t *testing.T) {
	// MaxSteps = 0 should fail.
	step := StepDef{
		ID:   "plan",
		Task: "task",
		Type: StepTypePlanner,
		Config: MarshalConfig(&PlannerConfig{
			MaxSteps: 0,
		}),
	}
	err := validatePlannerConfig(step)
	if err == nil {
		t.Fatal("expected error for MaxSteps=0, got nil")
	}

	// MaxSteps = 101 should fail.
	step.Config = MarshalConfig(&PlannerConfig{
		MaxSteps: 101,
	})
	err = validatePlannerConfig(step)
	if err == nil {
		t.Fatal("expected error for MaxSteps=101, got nil")
	}

	// MaxSteps = 50 should pass.
	step.Config = MarshalConfig(&PlannerConfig{
		MaxSteps: 50,
	})
	err = validatePlannerConfig(step)
	if err != nil {
		t.Fatalf("expected no error for MaxSteps=50, got %v", err)
	}
}

func TestValidatePlannerConfig_MaxDepthBounds(t *testing.T) {
	// MaxDepth = -1 should fail.
	step := StepDef{
		ID:   "plan",
		Task: "task",
		Type: StepTypePlanner,
		Config: MarshalConfig(&PlannerConfig{
			MaxSteps: 10,
			MaxDepth: -1,
		}),
	}
	err := validatePlannerConfig(step)
	if err == nil {
		t.Fatal("expected error for MaxDepth=-1, got nil")
	}

	// MaxDepth = 11 should fail.
	step.Config = MarshalConfig(&PlannerConfig{
		MaxSteps: 10,
		MaxDepth: 11,
	})
	err = validatePlannerConfig(step)
	if err == nil {
		t.Fatal("expected error for MaxDepth=11, got nil")
	}
}

func TestValidatePlannerConfig_SkipsNonPlanner(t *testing.T) {
	step := StepDef{
		ID:   "normal",
		Task: "task",
		Type: StepTypeNormal,
	}
	err := validatePlannerConfig(step)
	if err != nil {
		t.Fatalf("expected nil for non-planner, got %v", err)
	}
}

func TestBuilderPlanner_Valid(t *testing.T) {
	wb := NewWorkflow("test-wf")
	wb.Planner("plan", "generate", PlannerConfig{
		MaxSteps: 10,
	})
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if len(def.Steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(def.Steps))
	}
	if def.Steps[0].Type != StepTypePlanner {
		t.Errorf(
			"expected StepTypePlanner, got %s",
			def.Steps[0].Type,
		)
	}
}

func TestBuilderPlanner_PanicsOnEmptyID(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on empty id")
		}
	}()
	wb := NewWorkflow("test-wf")
	wb.Planner("", "generate", PlannerConfig{MaxSteps: 10})
}

func TestBuilderPlanner_PanicsOnEmptyTask(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on empty task")
		}
	}()
	wb := NewWorkflow("test-wf")
	wb.Planner("plan", "", PlannerConfig{MaxSteps: 10})
}

func TestBuilderPlanner_PanicsOnZeroMaxSteps(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on zero MaxSteps")
		}
	}()
	wb := NewWorkflow("test-wf")
	wb.Planner("plan", "gen", PlannerConfig{MaxSteps: 0})
}
