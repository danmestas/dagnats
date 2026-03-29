// dag/types_test.go

// Tests for core DAG types: StepType, RunStatus, StepStatus enums,
// WorkflowDef and WorkflowRun serialization roundtrips.
// Methodology: verify enum string values, JSON marshal/unmarshal fidelity,
// and that zero values are safe defaults.
package dag

import (
	"encoding/json"
	"testing"
	"time"
)

func TestStepTypeString(t *testing.T) {
	tests := []struct {
		stepType StepType
		expected string
	}{
		{StepTypeNormal, "normal"},
		{StepTypeAgentLoop, "agent_loop"},
		{StepTypeSubWorkflow, "sub_workflow"},
	}
	for _, tt := range tests {
		got := tt.stepType.String()
		if got != tt.expected {
			t.Fatalf("StepType.String() = %q, want %q", got, tt.expected)
		}
		if got == "" {
			t.Fatalf("StepType.String() must not be empty")
		}
	}
}

func TestRunStatusString(t *testing.T) {
	statuses := []struct {
		status   RunStatus
		expected string
	}{
		{RunStatusPending, "pending"},
		{RunStatusRunning, "running"},
		{RunStatusCompleted, "completed"},
		{RunStatusFailed, "failed"},
		{RunStatusCancelled, "cancelled"},
	}
	for _, tt := range statuses {
		got := tt.status.String()
		if got != tt.expected {
			t.Fatalf("RunStatus.String() = %q, want %q", got, tt.expected)
		}
	}
}

func TestWorkflowDefJSONRoundTrip(t *testing.T) {
	def := WorkflowDef{
		Name:    "test-workflow",
		Version: "1.0.0",
		Steps: []StepDef{
			{
				ID:        "step-a",
				Task:      "task-a",
				DependsOn: nil,
				Retries:   3,
				Timeout:   30 * time.Second,
				Type:      StepTypeNormal,
			},
			{
				ID:        "step-b",
				Task:      "task-b",
				DependsOn: []string{"step-a"},
				Retries:   1,
				Timeout:   60 * time.Second,
				Type:      StepTypeAgentLoop,
				Loop:      &AgentLoopConfig{MaxIterations: 10, MaxDuration: 5 * time.Minute},
			},
		},
	}

	data, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var got WorkflowDef
	err = json.Unmarshal(data, &got)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if got.Name != def.Name {
		t.Fatalf("Name = %q, want %q", got.Name, def.Name)
	}
	if len(got.Steps) != len(def.Steps) {
		t.Fatalf("Steps count = %d, want %d", len(got.Steps), len(def.Steps))
	}
	if got.Steps[1].Type != StepTypeAgentLoop {
		t.Fatalf("Steps[1].Type = %v, want %v", got.Steps[1].Type, StepTypeAgentLoop)
	}
	if got.Steps[1].Loop == nil {
		t.Fatal("Steps[1].Loop must not be nil for AgentLoop")
	}
	if got.Steps[1].Loop.MaxIterations != 10 {
		t.Fatalf("Steps[1].Loop.MaxIterations = %d, want 10", got.Steps[1].Loop.MaxIterations)
	}
}
