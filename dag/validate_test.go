// dag/validate_test.go

// Tests for DAG validation: duplicate step IDs, missing dependency references,
// cycle detection, empty workflows, and valid DAGs.
// Methodology: each test builds a specific invalid (or valid) DAG and asserts
// the exact error returned. Positive + negative space checked per test.
package dag

import (
	"encoding/json"
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
		{ID: "a", Task: "task-a", Type: StepTypeAgentLoop, Config: nil},
	}}
	err := Validate(def)
	if err == nil {
		t.Fatal("expected error for agent loop without Config, got nil")
	}
	if !strings.Contains(err.Error(), "AgentLoop") {
		t.Fatalf("error should mention 'AgentLoop', got: %v", err)
	}
}

// Normal steps no longer carry a separate Loop field, so there is
// no way to mis-assign loop config to a Normal step via the struct.
// This test is replaced by TestParseAgentLoopConfigWrongType.

// Agent steps no longer carry a separate Loop field, so there is
// no way to mis-assign loop config to an Agent step via the struct.
// This test is replaced by TestParseAgentLoopConfigWrongType.

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
				Config: MarshalConfig(&MapConfig{MaxItems: 100}), DependsOn: []string{"input"}},
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
				Config: MarshalConfig(&MapConfig{MaxItems: 100})},
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
				Config: MarshalConfig(&MapConfig{MaxItems: 100}), DependsOn: []string{"a", "b"}},
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
				Config: MarshalConfig(&MapConfig{MaxItems: 5000}), DependsOn: []string{"input"}},
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
				Config: MarshalConfig(&MapConfig{MaxItems: 0}), DependsOn: []string{"input"}},
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
				Config: MarshalConfig(&MapConfig{MaxItems: 10001}), DependsOn: []string{"input"}},
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
				Config: MarshalConfig(&MapConfig{MaxItems: 100}), DependsOn: []string{"input"}},
			{ID: "m2", Task: "t", Type: StepTypeMap,
				Config: MarshalConfig(&MapConfig{MaxItems: 50}), DependsOn: []string{"m1"}},
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
				Config: MarshalConfig(&MapConfig{MaxItems: 100}), DependsOn: []string{"input"}},
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
				Config: nil, DependsOn: []string{"input"}},
		},
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for Map step without Config")
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
		config   json.RawMessage
		deps     []string
	}{
		{"Normal", StepTypeNormal, nil, nil},
		{"AgentLoop", StepTypeAgentLoop,
			MarshalConfig(&AgentLoopConfig{MaxIterations: 5}), nil},
		{"SubWorkflow", StepTypeSubWorkflow, nil, nil},
		{"Agent", StepTypeAgent, nil, nil},
		{"Map", StepTypeMap,
			MarshalConfig(&MapConfig{MaxItems: 100}),
			[]string{"input"}},
	}

	for _, tc := range taskRequiringTypes {
		t.Run(tc.name, func(t *testing.T) {
			steps := []StepDef{
				{ID: "input", Task: "t", Type: StepTypeNormal},
				{
					ID:        "test",
					Task:      "",
					Type:      tc.stepType,
					Config:    tc.config,
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

func TestValidateMaxTaskConcurrencyBounds(t *testing.T) {
	// Negative: exceeds upper bound
	def := WorkflowDef{
		Name:    "bad-tc",
		Version: "1",
		Steps: []StepDef{
			{
				ID: "a", Task: "task-a",
				Type:               StepTypeNormal,
				MaxTaskConcurrency: 1001,
			},
		},
	}
	err := Validate(def)
	if err == nil {
		t.Fatal("expected error for MaxTaskConcurrency > 1000")
	}
	if !strings.Contains(err.Error(), "MaxTaskConcurrency") {
		t.Fatalf("error should mention MaxTaskConcurrency: %v", err)
	}

	// Positive: valid value passes
	def.Steps[0].MaxTaskConcurrency = 100
	if err := Validate(def); err != nil {
		t.Fatalf("valid MaxTaskConcurrency should pass: %v", err)
	}
}

func TestValidateMaxTaskConcurrencyNegative(t *testing.T) {
	def := WorkflowDef{
		Name:    "neg-tc",
		Version: "1",
		Steps: []StepDef{
			{
				ID: "a", Task: "task-a",
				Type:               StepTypeNormal,
				MaxTaskConcurrency: -1,
			},
		},
	}
	// Positive: negative values rejected
	err := Validate(def)
	if err == nil {
		t.Fatal("expected error for negative MaxTaskConcurrency")
	}
	// Negative: zero is valid (means unlimited)
	def.Steps[0].MaxTaskConcurrency = 0
	if err := Validate(def); err != nil {
		t.Fatalf("zero MaxTaskConcurrency should pass: %v", err)
	}
}

func TestValidateConcurrencyMaxStepsBounds(t *testing.T) {
	def := WorkflowDef{
		Name:    "bad-ms",
		Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "task-a", Type: StepTypeNormal},
		},
		Concurrency: &ConcurrencyLimit{MaxSteps: 1001},
	}
	// Positive: exceeds bound
	err := Validate(def)
	if err == nil {
		t.Fatal("expected error for MaxSteps > 1000")
	}
	if !strings.Contains(err.Error(), "MaxSteps") {
		t.Fatalf("error should mention MaxSteps: %v", err)
	}

	// Negative: valid value passes
	def.Concurrency.MaxSteps = 10
	if err := Validate(def); err != nil {
		t.Fatalf("valid MaxSteps should pass: %v", err)
	}
}

func TestBuilderWithConcurrency(t *testing.T) {
	wf := NewWorkflow("concur-wf")
	wf.WithConcurrency(3, 5)
	wf.Task("a", "task-a")
	def, err := wf.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	// Positive: concurrency set
	if def.Concurrency == nil {
		t.Fatal("Concurrency should not be nil")
	}
	if def.Concurrency.MaxRuns != 3 {
		t.Fatalf("MaxRuns = %d, want 3", def.Concurrency.MaxRuns)
	}
	if def.Concurrency.MaxSteps != 5 {
		t.Fatalf("MaxSteps = %d, want 5", def.Concurrency.MaxSteps)
	}
	// Negative: builder without WithConcurrency has nil
	wf2 := NewWorkflow("no-concur")
	wf2.Task("b", "task-b")
	def2, _ := wf2.Build()
	if def2.Concurrency != nil {
		t.Fatal("Concurrency should be nil when not set")
	}
}

func TestValidateIdempotencyKeyValid(t *testing.T) {
	wb := NewWorkflow("idemp")
	wb.Task("a", "t")
	wb.WithIdempotencyKey("data.request_id")
	// Positive: valid dot-path accepted
	if _, err := wb.Build(); err != nil {
		t.Fatalf("valid key rejected: %v", err)
	}
}

func TestValidateIdempotencyKeyLeadingDot(t *testing.T) {
	err := validateIdempotencyKey(".bad")
	// Positive: leading dot rejected
	if err == nil {
		t.Fatal("expected error for leading dot")
	}
}

func TestValidateIdempotencyKeyTrailingDot(t *testing.T) {
	err := validateIdempotencyKey("bad.")
	if err == nil {
		t.Fatal("expected error for trailing dot")
	}
}

func TestValidateIdempotencyKeyEmptySegment(t *testing.T) {
	err := validateIdempotencyKey("a..b")
	if err == nil {
		t.Fatal("expected error for empty segment")
	}
}
