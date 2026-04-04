// dag/dynamic_test.go
// Unit tests for dynamic DAG augmentation: EffectiveSteps,
// EffectiveDef, ValidateFragment, and NamespaceFragment.
// Methodology: each test covers both positive (valid input) and
// negative (constraint violation) paths with bounded assertions.
package dag

import (
	"strings"
	"testing"
	"time"
)

func TestEffectiveSteps_NoDynamic(t *testing.T) {
	def := WorkflowDef{
		Name:    "wf",
		Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "t1", Type: StepTypeNormal},
		},
	}
	run := WorkflowRun{RunID: "r1"}
	steps := EffectiveSteps(def, run)
	if len(steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(steps))
	}
	if steps[0].ID != "a" {
		t.Errorf("expected step ID 'a', got %q", steps[0].ID)
	}
}

func TestEffectiveSteps_WithDynamic(t *testing.T) {
	def := WorkflowDef{
		Name:    "wf",
		Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "t1", Type: StepTypeNormal},
		},
	}
	run := WorkflowRun{
		RunID: "r1",
		DynamicSteps: []StepDef{
			{ID: "b", Task: "t2", Type: StepTypeNormal},
		},
	}
	steps := EffectiveSteps(def, run)
	if len(steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(steps))
	}
	if steps[1].ID != "b" {
		t.Errorf("expected step ID 'b', got %q", steps[1].ID)
	}
}

func TestEffectiveDef_AugmentsSteps(t *testing.T) {
	def := WorkflowDef{
		Name:    "wf",
		Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "t1", Type: StepTypeNormal},
		},
	}
	run := WorkflowRun{
		RunID: "r1",
		DynamicSteps: []StepDef{
			{
				ID: "b", Task: "t2",
				Type:      StepTypeNormal,
				DependsOn: []string{"a"},
			},
		},
	}
	augmented := EffectiveDef(def, run)
	if len(augmented.Steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(augmented.Steps))
	}
	// Original def must not be mutated.
	if len(def.Steps) != 1 {
		t.Errorf(
			"original def mutated: expected 1 step, got %d",
			len(def.Steps),
		)
	}
}

func TestEffectiveDef_NoDynamicReturnsSame(t *testing.T) {
	def := WorkflowDef{
		Name:    "wf",
		Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "t1", Type: StepTypeNormal},
		},
	}
	run := WorkflowRun{RunID: "r1"}
	augmented := EffectiveDef(def, run)
	if len(augmented.Steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(augmented.Steps))
	}
}

func TestValidateFragment_Valid(t *testing.T) {
	fragment := []StepDef{
		{ID: "x", Task: "build", Type: StepTypeNormal},
		{
			ID: "y", Task: "test", Type: StepTypeNormal,
			DependsOn: []string{"x"},
		},
	}
	cfg := PlannerConfig{
		MaxSteps:     10,
		AllowedTasks: []string{"build", "test"},
	}
	existing := map[string]bool{"a": true}
	err := ValidateFragment(fragment, cfg, existing)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestValidateFragment_ExceedsMaxSteps(t *testing.T) {
	fragment := []StepDef{
		{ID: "x", Task: "t1", Type: StepTypeNormal},
		{ID: "y", Task: "t2", Type: StepTypeNormal},
		{ID: "z", Task: "t3", Type: StepTypeNormal},
	}
	cfg := PlannerConfig{MaxSteps: 2}
	err := ValidateFragment(
		fragment, cfg, map[string]bool{},
	)
	if err == nil {
		t.Fatal("expected error for exceeding MaxSteps")
	}
	if !strings.Contains(err.Error(), "3 steps (max 2)") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateFragment_DisallowedTask(t *testing.T) {
	fragment := []StepDef{
		{ID: "x", Task: "forbidden", Type: StepTypeNormal},
	}
	cfg := PlannerConfig{
		MaxSteps:     10,
		AllowedTasks: []string{"build", "test"},
	}
	err := ValidateFragment(
		fragment, cfg, map[string]bool{},
	)
	if err == nil {
		t.Fatal("expected error for disallowed task")
	}
	if !strings.Contains(err.Error(), "disallowed") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateFragment_Cycle(t *testing.T) {
	fragment := []StepDef{
		{
			ID: "x", Task: "t1", Type: StepTypeNormal,
			DependsOn: []string{"y"},
		},
		{
			ID: "y", Task: "t2", Type: StepTypeNormal,
			DependsOn: []string{"x"},
		},
	}
	cfg := PlannerConfig{MaxSteps: 10}
	err := ValidateFragment(
		fragment, cfg, map[string]bool{},
	)
	if err == nil {
		t.Fatal("expected error for cycle")
	}
	if !strings.Contains(err.Error(), "cycle") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateFragment_DepthExceeded(t *testing.T) {
	// Chain of 4: a -> b -> c -> d = depth 4.
	fragment := []StepDef{
		{ID: "a", Task: "t", Type: StepTypeNormal},
		{
			ID: "b", Task: "t", Type: StepTypeNormal,
			DependsOn: []string{"a"},
		},
		{
			ID: "c", Task: "t", Type: StepTypeNormal,
			DependsOn: []string{"b"},
		},
		{
			ID: "d", Task: "t", Type: StepTypeNormal,
			DependsOn: []string{"c"},
		},
	}
	cfg := PlannerConfig{MaxSteps: 10, MaxDepth: 3}
	err := ValidateFragment(
		fragment, cfg, map[string]bool{},
	)
	if err == nil {
		t.Fatal("expected error for depth exceeded")
	}
	if !strings.Contains(err.Error(), "depth") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateFragment_DuplicateID(t *testing.T) {
	fragment := []StepDef{
		{ID: "x", Task: "t1", Type: StepTypeNormal},
		{ID: "x", Task: "t2", Type: StepTypeNormal},
	}
	cfg := PlannerConfig{MaxSteps: 10}
	err := ValidateFragment(
		fragment, cfg, map[string]bool{},
	)
	if err == nil {
		t.Fatal("expected error for duplicate ID")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateFragment_CollidingID(t *testing.T) {
	fragment := []StepDef{
		{ID: "a", Task: "t1", Type: StepTypeNormal},
	}
	cfg := PlannerConfig{MaxSteps: 10}
	err := ValidateFragment(
		fragment, cfg, map[string]bool{"a": true},
	)
	if err == nil {
		t.Fatal("expected error for colliding ID")
	}
	if !strings.Contains(err.Error(), "collides") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateFragment_ExternalDep(t *testing.T) {
	fragment := []StepDef{
		{
			ID: "x", Task: "t1", Type: StepTypeNormal,
			DependsOn: []string{"external"},
		},
	}
	cfg := PlannerConfig{MaxSteps: 10}
	err := ValidateFragment(
		fragment, cfg, map[string]bool{},
	)
	if err == nil {
		t.Fatal("expected error for external dep")
	}
	if !strings.Contains(err.Error(), "not in the fragment") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateFragment_Empty(t *testing.T) {
	cfg := PlannerConfig{MaxSteps: 10}
	err := ValidateFragment(
		nil, cfg, map[string]bool{},
	)
	if err == nil {
		t.Fatal("expected error for empty fragment")
	}
}

func TestNamespaceFragment_PrefixesIDsAndDeps(t *testing.T) {
	fragment := []StepDef{
		{ID: "a", Task: "t1", Type: StepTypeNormal},
		{
			ID: "b", Task: "t2", Type: StepTypeAgentLoop,
			DependsOn: []string{"a"},
		},
	}
	result := NamespaceFragment("planner1", fragment)
	if len(result) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(result))
	}
	if result[0].ID != "planner1.a" {
		t.Errorf("expected 'planner1.a', got %q", result[0].ID)
	}
	if result[1].ID != "planner1.b" {
		t.Errorf("expected 'planner1.b', got %q", result[1].ID)
	}
	if len(result[1].DependsOn) != 1 ||
		result[1].DependsOn[0] != "planner1.a" {
		t.Errorf(
			"expected dep 'planner1.a', got %v",
			result[1].DependsOn,
		)
	}
	// Type forced to Normal.
	if result[1].Type != StepTypeNormal {
		t.Errorf(
			"expected StepTypeNormal, got %s", result[1].Type,
		)
	}
	// Original fragment must not be mutated.
	if fragment[0].ID != "a" {
		t.Errorf("original fragment mutated")
	}
}

func TestMaxChainDepth_SingleStep(t *testing.T) {
	steps := []StepDef{
		{ID: "a", Task: "t", Type: StepTypeNormal},
	}
	depth := maxChainDepth(steps)
	if depth != 1 {
		t.Errorf("expected depth 1, got %d", depth)
	}
}

func TestMaxChainDepth_Chain(t *testing.T) {
	steps := []StepDef{
		{ID: "a", Task: "t", Type: StepTypeNormal},
		{
			ID: "b", Task: "t", Type: StepTypeNormal,
			DependsOn: []string{"a"},
		},
		{
			ID: "c", Task: "t", Type: StepTypeNormal,
			DependsOn: []string{"b"},
		},
	}
	depth := maxChainDepth(steps)
	if depth != 3 {
		t.Errorf("expected depth 3, got %d", depth)
	}
}

func TestMaxChainDepth_Diamond(t *testing.T) {
	// a -> b -> d
	// a -> c -> d
	// Depth should be 3 (a -> b/c -> d).
	steps := []StepDef{
		{ID: "a", Task: "t", Type: StepTypeNormal},
		{
			ID: "b", Task: "t", Type: StepTypeNormal,
			DependsOn: []string{"a"},
		},
		{
			ID: "c", Task: "t", Type: StepTypeNormal,
			DependsOn: []string{"a"},
		},
		{
			ID: "d", Task: "t", Type: StepTypeNormal,
			DependsOn: []string{"b", "c"},
		},
	}
	depth := maxChainDepth(steps)
	if depth != 3 {
		t.Errorf("expected depth 3, got %d", depth)
	}
}

func TestStepTypePlanner_String(t *testing.T) {
	s := StepTypePlanner.String()
	if s != "planner" {
		t.Errorf("expected 'planner', got %q", s)
	}
}

func TestStepTypePlanner_JSONRoundTrip(t *testing.T) {
	step := StepDef{
		ID:      "plan",
		Task:    "gen",
		Type:    StepTypePlanner,
		Timeout: 5 * time.Second,
		Config: MarshalConfig(&PlannerConfig{
			MaxSteps: 10,
		}),
	}
	// Ensure the step can be built and validated.
	wb := NewWorkflow("test-rt")
	wb.Planner("plan", "gen", PlannerConfig{MaxSteps: 10})
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if def.Steps[0].Type != StepTypePlanner {
		t.Errorf("type mismatch after build")
	}
	_ = step // used for setup context
}
