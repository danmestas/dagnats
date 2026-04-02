// dag/types_test.go

// Tests for core DAG types: StepType, RunStatus, StepStatus enums,
// WorkflowDef and WorkflowRun serialization roundtrips.
// Methodology: verify enum string values, JSON marshal/unmarshal fidelity,
// and that zero values are safe defaults.
package dag

import (
	"bytes"
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

func TestRunStatusJSONRoundTrip(t *testing.T) {
	original := RunStatusRunning

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Verify JSON contains string "running", not integer 1.
	if string(data) != `"running"` {
		t.Fatalf("marshaled RunStatus = %s, want %q", data, "running")
	}

	var got RunStatus
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if got != original {
		t.Fatalf("roundtrip RunStatus = %v, want %v", got, original)
	}
}

func TestStepStatusJSONRoundTrip(t *testing.T) {
	original := StepStatusCompleted

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Verify JSON contains string "completed", not integer 3.
	if string(data) != `"completed"` {
		t.Fatalf("marshaled StepStatus = %s, want %q", data, "completed")
	}

	var got StepStatus
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if got != original {
		t.Fatalf("roundtrip StepStatus = %v, want %v", got, original)
	}
}

func TestWorkflowDefJSONRoundTrip(t *testing.T) {
	def := WorkflowDef{
		Name:    "test-workflow",
		Version: "1.0.0",
		Steps: []StepDef{
			{
				ID:      "step-a",
				Task:    "task-a",
				Timeout: 30 * time.Second,
				Type:    StepTypeNormal,
			},
			{
				ID:        "step-b",
				Task:      "task-b",
				DependsOn: []string{"step-a"},
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

func TestStepTypeAgentStringAndJSON(t *testing.T) {
	// Positive: string representation
	if got := StepTypeAgent.String(); got != "agent" {
		t.Fatalf("StepTypeAgent.String() = %q, want %q", got, "agent")
	}

	// Positive: JSON round-trip
	data, err := json.Marshal(StepTypeAgent)
	if err != nil {
		t.Fatalf("Marshal StepTypeAgent: %v", err)
	}
	if string(data) != `"agent"` {
		t.Fatalf("Marshal StepTypeAgent = %s, want %q", data, "agent")
	}

	var got StepType
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal StepTypeAgent: %v", err)
	}
	if got != StepTypeAgent {
		t.Fatalf("Unmarshal StepTypeAgent = %v, want %v", got, StepTypeAgent)
	}
}

func TestStepDefMetadataJSON(t *testing.T) {
	step := StepDef{
		ID:       "code",
		Task:     "llm-coder",
		Type:     StepTypeAgent,
		Metadata: map[string]string{"role": "coder"},
	}

	data, err := json.Marshal(step)
	if err != nil {
		t.Fatalf("Marshal StepDef with metadata: %v", err)
	}

	var got StepDef
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal StepDef with metadata: %v", err)
	}
	if got.Metadata["role"] != "coder" {
		t.Fatalf("Metadata[role] = %q, want %q", got.Metadata["role"], "coder")
	}

	// Negative: nil metadata omitted from JSON
	step2 := StepDef{ID: "plain", Task: "task", Type: StepTypeNormal}
	data2, _ := json.Marshal(step2)
	if bytes.Contains(data2, []byte("metadata")) {
		t.Fatalf("nil Metadata should be omitted from JSON, got %s", data2)
	}
}

func TestWorkflowRunParentFieldsJSON(t *testing.T) {
	run := WorkflowRun{
		RunID:        "child-1",
		WorkflowID:   "wf-1",
		Status:       RunStatusRunning,
		Steps:        map[string]StepState{},
		CreatedAt:    time.Now(),
		ParentRunID:  "parent-1",
		ParentStepID: "step-a",
	}

	data, err := json.Marshal(run)
	if err != nil {
		t.Fatalf("Marshal WorkflowRun with parent: %v", err)
	}

	var got WorkflowRun
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal WorkflowRun with parent: %v", err)
	}
	if got.ParentRunID != "parent-1" {
		t.Fatalf("ParentRunID = %q, want %q", got.ParentRunID, "parent-1")
	}
	if got.ParentStepID != "step-a" {
		t.Fatalf("ParentStepID = %q, want %q", got.ParentStepID, "step-a")
	}

	// Negative: empty parent fields omitted
	run2 := WorkflowRun{RunID: "top", WorkflowID: "wf", Status: RunStatusPending,
		Steps: map[string]StepState{}, CreatedAt: time.Now()}
	data2, _ := json.Marshal(run2)
	if bytes.Contains(data2, []byte("parent_run_id")) {
		t.Fatalf("empty ParentRunID should be omitted, got %s", data2)
	}
}

func TestStepDefCompensationFieldsJSON(t *testing.T) {
	step := StepDef{
		ID:          "deploy",
		Task:        "deploy-task",
		Type:        StepTypeNormal,
		WorkerGroup: "gpu",
		OnFailure:   "notify",
		Compensate:  "rollback",
	}
	data, err := json.Marshal(step)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got StepDef
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Positive: WorkerGroup round-trips
	if got.WorkerGroup != "gpu" {
		t.Fatalf("WorkerGroup = %q, want gpu", got.WorkerGroup)
	}
	// Positive: OnFailure round-trips
	if got.OnFailure != "notify" {
		t.Fatalf("OnFailure = %q, want notify", got.OnFailure)
	}
	// Positive: Compensate round-trips
	if got.Compensate != "rollback" {
		t.Fatalf("Compensate = %q, want rollback", got.Compensate)
	}
}

func TestWorkflowDefTimeoutAndSchemaJSON(t *testing.T) {
	wf := WorkflowDef{
		Name:         "test",
		Version:      "1",
		Steps:        []StepDef{{ID: "s1", Task: "t", Type: StepTypeNormal}},
		Timeout:      30 * time.Minute,
		InputSchema:  json.RawMessage(`{"type":"object"}`),
		OutputSchema: json.RawMessage(`{"type":"string"}`),
	}
	data, err := json.Marshal(wf)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got WorkflowDef
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Positive: Timeout round-trips
	if got.Timeout != 30*time.Minute {
		t.Fatalf("Timeout = %v, want 30m", got.Timeout)
	}
	// Positive: InputSchema round-trips
	if string(got.InputSchema) != `{"type":"object"}` {
		t.Fatalf("InputSchema = %s", got.InputSchema)
	}
}

func TestNewWorkflowRunInitialization(t *testing.T) {
	def := WorkflowDef{
		Name: "test", Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "t", Type: StepTypeNormal},
			{ID: "b", Task: "t", Type: StepTypeNormal},
		},
	}
	run := NewWorkflowRun(def, "run-123")
	// Positive: all steps initialized to pending
	if len(run.Steps) != 2 {
		t.Fatalf("Steps count = %d, want 2", len(run.Steps))
	}
	if run.Steps["a"].Status != StepStatusPending {
		t.Fatalf("step a status = %v, want pending",
			run.Steps["a"].Status)
	}
	// Positive: run metadata set
	if run.RunID != "run-123" {
		t.Fatalf("RunID = %q, want run-123", run.RunID)
	}
	if run.Status != RunStatusPending {
		t.Fatalf("Status = %v, want pending", run.Status)
	}
}

func TestNewWorkflowRunEmptyRunIDPanics(t *testing.T) {
	def := WorkflowDef{
		Name: "t", Version: "1",
		Steps: []StepDef{{ID: "a", Task: "t", Type: StepTypeNormal}},
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for empty runID")
		}
	}()
	NewWorkflowRun(def, "")
}

func TestNewWorkflowRunNoStepsPanics(t *testing.T) {
	def := WorkflowDef{Name: "t", Version: "1", Steps: []StepDef{}}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for empty steps")
		}
	}()
	NewWorkflowRun(def, "run-1")
}

func TestStepStatusStringAll(t *testing.T) {
	statuses := []struct {
		s    StepStatus
		want string
	}{
		{StepStatusPending, "pending"},
		{StepStatusQueued, "queued"},
		{StepStatusRunning, "running"},
		{StepStatusCompleted, "completed"},
		{StepStatusFailed, "failed"},
		{StepStatusSkipped, "skipped"},
		{StepStatusCancelled, "cancelled"},
	}
	for _, tt := range statuses {
		if got := tt.s.String(); got != tt.want {
			t.Fatalf("StepStatus(%d).String() = %q, want %q",
				tt.s, got, tt.want)
		}
	}
}

func TestRunStatusUnmarshalJSONInvalid(t *testing.T) {
	var r RunStatus
	err := r.UnmarshalJSON([]byte(`"bogus"`))
	// Positive: error for unknown
	if err == nil {
		t.Fatal("expected error for unknown RunStatus")
	}
	// Negative: valid string works
	err = r.UnmarshalJSON([]byte(`"running"`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStepStatusUnmarshalJSONInvalid(t *testing.T) {
	var s StepStatus
	err := s.UnmarshalJSON([]byte(`"bogus"`))
	// Positive: error for unknown
	if err == nil {
		t.Fatal("expected error for unknown StepStatus")
	}
	// Negative: valid string works
	err = s.UnmarshalJSON([]byte(`"queued"`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWorkflowRunDeadlineJSON(t *testing.T) {
	deadline := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	run := WorkflowRun{
		RunID: "r1", WorkflowID: "wf", Status: RunStatusRunning,
		Steps: map[string]StepState{}, CreatedAt: time.Now(),
		Deadline: &deadline,
	}
	data, err := json.Marshal(run)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got WorkflowRun
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Positive: Deadline round-trips
	if got.Deadline == nil || !got.Deadline.Equal(deadline) {
		t.Fatalf("Deadline = %v, want %v", got.Deadline, deadline)
	}
	// Positive: nil deadline omitted
	run2 := WorkflowRun{RunID: "r2", WorkflowID: "wf",
		Status: RunStatusPending, Steps: map[string]StepState{},
		CreatedAt: time.Now()}
	data2, _ := json.Marshal(run2)
	if bytes.Contains(data2, []byte(`"deadline"`)) {
		t.Fatalf("nil Deadline should be omitted")
	}
}
