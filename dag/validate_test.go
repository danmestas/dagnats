// dag/validate_test.go

// Tests for DAG validation: duplicate step IDs, missing dependency references,
// cycle detection, empty workflows, and valid DAGs.
// Methodology: each test builds a specific invalid (or valid) DAG and asserts
// the exact error returned. Positive + negative space checked per test.
package dag

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestValidateDuplicateStepIDs(t *testing.T) {
	def := WorkflowDef{Name: "dup-ids", Version: "1", Steps: []StepDef{
		{ID: "a", Task: "task-a", Type: StepTypeNormal},
		{ID: "a", Task: "task-b", Type: StepTypeNormal},
	}}
	err := Validate(def)
	if err == nil {
		t.Fatal("expected error for duplicate step IDs, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("error should mention 'duplicate', got: %v", err)
	}
}

func TestValidateMissingDependency(t *testing.T) {
	def := WorkflowDef{Name: "missing-dep", Version: "1", Steps: []StepDef{
		{ID: "a", Task: "task-a", DependsOn: []string{"nonexistent"}, Type: StepTypeNormal},
	}}
	err := Validate(def)
	if err == nil {
		t.Fatal("expected error for missing dependency, got nil")
	}
	if !strings.Contains(err.Error(), "nonexistent") {
		t.Fatalf("error should mention missing dep name, got: %v", err)
	}
}

func TestValidateCycleDetection(t *testing.T) {
	def := WorkflowDef{Name: "cycle", Version: "1", Steps: []StepDef{
		{ID: "a", Task: "task-a", DependsOn: []string{"c"}, Type: StepTypeNormal},
		{ID: "b", Task: "task-b", DependsOn: []string{"a"}, Type: StepTypeNormal},
		{ID: "c", Task: "task-c", DependsOn: []string{"b"}, Type: StepTypeNormal},
	}}
	err := Validate(def)
	if err == nil {
		t.Fatal("expected error for cycle, got nil")
	}
	if !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("error should mention 'cycle', got: %v", err)
	}
}

func TestValidateEmptyWorkflow(t *testing.T) {
	def := WorkflowDef{Name: "empty", Version: "1", Steps: nil}
	err := Validate(def)
	if err == nil {
		t.Fatal("expected error for empty workflow, got nil")
	}
	if !strings.Contains(err.Error(), "no steps") {
		t.Fatalf("error should mention 'no steps', got: %v", err)
	}
}

func TestValidateValidDAG(t *testing.T) {
	def := WorkflowDef{Name: "valid", Version: "1", Steps: []StepDef{
		{ID: "a", Task: "task-a", Type: StepTypeNormal},
		{ID: "b", Task: "task-b", DependsOn: []string{"a"}, Type: StepTypeNormal},
		{ID: "c", Task: "task-c", DependsOn: []string{"a"}, Type: StepTypeNormal},
		{ID: "d", Task: "task-d", DependsOn: []string{"b", "c"}, Type: StepTypeNormal},
	}}
	err := Validate(def)
	if err != nil {
		t.Fatalf("expected valid DAG, got error: %v", err)
	}
}

func TestValidateAgentLoopRequiresLoopConfig(t *testing.T) {
	def := WorkflowDef{Name: "loop-no-config", Version: "1", Steps: []StepDef{
		{ID: "a", Task: "task-a", Type: StepTypeAgentLoop, Loop: nil},
	}}
	err := Validate(def)
	if err == nil {
		t.Fatal("expected error for agent loop without Loop config, got nil")
	}
	if !strings.Contains(err.Error(), "Loop") {
		t.Fatalf("error should mention 'Loop', got: %v", err)
	}
}

func TestValidateNormalStepRejectsLoopConfig(t *testing.T) {
	def := WorkflowDef{Name: "normal-with-loop", Version: "1", Steps: []StepDef{
		{ID: "a", Task: "task-a", Type: StepTypeNormal, Loop: &AgentLoopConfig{MaxIterations: 5}},
	}}
	err := Validate(def)
	if err == nil {
		t.Fatal("expected error for normal step with Loop config, got nil")
	}
	if !strings.Contains(err.Error(), "Loop") {
		t.Fatalf("error should mention 'Loop', got: %v", err)
	}
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

func TestValidateOnFailureTargetNoDeps(t *testing.T) {
	def := WorkflowDef{
		Name: "v", Version: "1",
		Steps: []StepDef{
			{ID: "main", Task: "t", Type: StepTypeNormal,
				OnFailure: "fallback"},
			{ID: "fallback", Task: "t", Type: StepTypeNormal,
				DependsOn: []string{"main"}},
		},
	}
	err := Validate(def)
	// Positive: rejects OnFailure target with DependsOn
	if err == nil {
		t.Fatal("expected error: OnFailure target has DependsOn")
	}
}

func TestValidateCompensateTargetNoDeps(t *testing.T) {
	def := WorkflowDef{
		Name: "v", Version: "1",
		Steps: []StepDef{
			{ID: "main", Task: "t", Type: StepTypeNormal,
				Compensate: "undo"},
			{ID: "undo", Task: "t", Type: StepTypeNormal,
				DependsOn: []string{"main"}},
		},
	}
	err := Validate(def)
	// Positive: rejects Compensate target with DependsOn
	if err == nil {
		t.Fatal("expected error: Compensate target has DependsOn")
	}
}

func TestValidateOnFailureSelfReference(t *testing.T) {
	def := WorkflowDef{
		Name: "v", Version: "1",
		Steps: []StepDef{
			{ID: "main", Task: "t", Type: StepTypeNormal,
				OnFailure: "main"},
		},
	}
	err := Validate(def)
	// Positive: rejects self-referencing OnFailure
	if err == nil {
		t.Fatal("expected error: OnFailure self-reference")
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

func TestValidateMapStepOneDep(t *testing.T) {
	// Positive: Map step with exactly one dependency is valid
	def := WorkflowDef{
		Name: "v", Version: "1",
		Steps: []StepDef{
			{ID: "input", Task: "t", Type: StepTypeNormal},
			{ID: "m", Task: "t", Type: StepTypeMap,
				Map: &MapConfig{MaxItems: 100}, DependsOn: []string{"input"}},
		},
	}
	if err := Validate(def); err != nil {
		t.Fatalf("valid map step rejected: %v", err)
	}

	// Negative: Map step with zero dependencies fails
	def2 := WorkflowDef{
		Name: "v", Version: "1",
		Steps: []StepDef{
			{ID: "m", Task: "t", Type: StepTypeMap,
				Map: &MapConfig{MaxItems: 100}},
		},
	}
	err := Validate(def2)
	if err == nil {
		t.Fatal("expected error for Map step with no dependencies")
	}
	if !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("error = %q", err)
	}
}

func TestValidateMapStepMultipleDepsRejected(t *testing.T) {
	def := WorkflowDef{
		Name: "v", Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "t", Type: StepTypeNormal},
			{ID: "b", Task: "t", Type: StepTypeNormal},
			{ID: "m", Task: "t", Type: StepTypeMap,
				Map: &MapConfig{MaxItems: 100}, DependsOn: []string{"a", "b"}},
		},
	}
	err := Validate(def)
	if err == nil {
		t.Fatal("expected error for Map step with multiple dependencies")
	}
}

func TestValidateMapStepMaxItems(t *testing.T) {
	// Positive: MaxItems in valid range
	def := WorkflowDef{
		Name: "v", Version: "1",
		Steps: []StepDef{
			{ID: "input", Task: "t", Type: StepTypeNormal},
			{ID: "m", Task: "t", Type: StepTypeMap,
				Map: &MapConfig{MaxItems: 5000}, DependsOn: []string{"input"}},
		},
	}
	if err := Validate(def); err != nil {
		t.Fatalf("valid MaxItems rejected: %v", err)
	}

	// Negative: MaxItems zero fails
	def2 := WorkflowDef{
		Name: "v", Version: "1",
		Steps: []StepDef{
			{ID: "input", Task: "t", Type: StepTypeNormal},
			{ID: "m", Task: "t", Type: StepTypeMap,
				Map: &MapConfig{MaxItems: 0}, DependsOn: []string{"input"}},
		},
	}
	err := Validate(def2)
	if err == nil {
		t.Fatal("expected error for MaxItems = 0")
	}

	// Negative: MaxItems > 10,000 fails
	def3 := WorkflowDef{
		Name: "v", Version: "1",
		Steps: []StepDef{
			{ID: "input", Task: "t", Type: StepTypeNormal},
			{ID: "m", Task: "t", Type: StepTypeMap,
				Map: &MapConfig{MaxItems: 10001}, DependsOn: []string{"input"}},
		},
	}
	err = Validate(def3)
	if err == nil {
		t.Fatal("expected error for MaxItems > 10000")
	}
}

func TestValidateMapNoNesting(t *testing.T) {
	def := WorkflowDef{
		Name: "v", Version: "1",
		Steps: []StepDef{
			{ID: "input", Task: "t", Type: StepTypeNormal},
			{ID: "m1", Task: "t", Type: StepTypeMap,
				Map: &MapConfig{MaxItems: 100}, DependsOn: []string{"input"}},
			{ID: "m2", Task: "t", Type: StepTypeMap,
				Map: &MapConfig{MaxItems: 50}, DependsOn: []string{"m1"}},
		},
	}
	err := Validate(def)
	// Positive: error for nested Map step
	if err == nil {
		t.Fatal("expected error for Map step depending on Map step")
	}
	if !strings.Contains(err.Error(), "Map") && !strings.Contains(err.Error(), "nest") {
		t.Fatalf("error = %q", err)
	}

	// Negative: Map depending on normal step is valid
	def2 := WorkflowDef{
		Name: "v", Version: "1",
		Steps: []StepDef{
			{ID: "input", Task: "t", Type: StepTypeNormal},
			{ID: "m", Task: "t", Type: StepTypeMap,
				Map: &MapConfig{MaxItems: 100}, DependsOn: []string{"input"}},
		},
	}
	if err := Validate(def2); err != nil {
		t.Fatalf("valid map should pass, got: %v", err)
	}
}

func TestValidateMapStepRequiresMapConfig(t *testing.T) {
	def := WorkflowDef{
		Name: "v", Version: "1",
		Steps: []StepDef{
			{ID: "input", Task: "t", Type: StepTypeNormal},
			{ID: "m", Task: "t", Type: StepTypeMap,
				Map: nil, DependsOn: []string{"input"}},
		},
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for Map step without Map config")
		}
	}()
	Validate(def)
}

func TestValidateEmptyTaskForTaskRequiringTypes(t *testing.T) {
	// Test that step types requiring a Task field panic when Task is empty.
	// This preserves existing behavior for Normal, AgentLoop, SubWorkflow, Agent, Map.
	taskRequiringTypes := []struct {
		name     string
		stepType StepType
		loop     *AgentLoopConfig
		mapCfg   *MapConfig
		deps     []string
	}{
		{"Normal", StepTypeNormal, nil, nil, nil},
		{"AgentLoop", StepTypeAgentLoop, &AgentLoopConfig{MaxIterations: 5}, nil, nil},
		{"SubWorkflow", StepTypeSubWorkflow, nil, nil, nil},
		{"Agent", StepTypeAgent, nil, nil, nil},
		{"Map", StepTypeMap, nil, &MapConfig{MaxItems: 100}, []string{"input"}},
	}

	for _, tc := range taskRequiringTypes {
		t.Run(tc.name, func(t *testing.T) {
			steps := []StepDef{
				{ID: "input", Task: "t", Type: StepTypeNormal},
				{
					ID:        "test",
					Task:      "",
					Type:      tc.stepType,
					Loop:      tc.loop,
					Map:       tc.mapCfg,
					DependsOn: tc.deps,
				},
			}
			if tc.stepType == StepTypeMap {
				// Map needs a parent step
				steps[1].DependsOn = []string{"input"}
			} else {
				steps = []StepDef{steps[1]}
			}

			def := WorkflowDef{Name: "test", Version: "1", Steps: steps}

			defer func() {
				r := recover()
				if r == nil {
					t.Fatalf(
						"expected panic for %s step with empty Task",
						tc.name,
					)
				}
				// Positive assertion: panic message contains expected content
				panicMsg := fmt.Sprint(r)
				if !strings.Contains(panicMsg, "task is empty") {
					t.Fatalf("panic message = %q", panicMsg)
				}
			}()
			Validate(def)
		})
	}
}

func TestStepRequiresTask(t *testing.T) {
	// Positive: known task-requiring types return true
	taskTypes := []StepType{
		StepTypeNormal,
		StepTypeAgentLoop,
		StepTypeSubWorkflow,
		StepTypeAgent,
		StepTypeMap,
	}
	for _, st := range taskTypes {
		if !stepRequiresTask(st) {
			t.Errorf("stepRequiresTask(%s) = false, want true", st)
		}
	}

	// Negative: invalid/future step types return false (graceful degradation)
	// This will be the behavior for StepTypeSleep and StepTypeWaitForEvent.
	invalidType := StepType(999)
	if stepRequiresTask(invalidType) {
		t.Errorf("stepRequiresTask(999) = true, want false")
	}
}

func TestValidateSleepDuration365DayMax(t *testing.T) {
	b := NewWorkflow("test")
	b.Sleep("too-long", 366*24*time.Hour)
	_, err := b.Build()
	// Positive: error for >365 day sleep
	if err == nil {
		t.Fatal("expected error for >365 day sleep")
	}
	if !strings.Contains(err.Error(), "exceeds max") {
		t.Fatalf("expected 'exceeds max' in error, got: %v", err)
	}
}
