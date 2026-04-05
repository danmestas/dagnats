// dag/types_test.go

// Tests for core DAG types: StepType, RunStatus, StepStatus enums,
// WorkflowDef and WorkflowRun serialization roundtrips.
// Methodology: verify enum string values, JSON marshal/unmarshal fidelity,
// and that zero values are safe defaults.
package dag

import (
	"bytes"
	"encoding/json"
	"strings"
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
				Config:    MarshalConfig(&AgentLoopConfig{MaxIterations: 10, MaxDuration: 5 * time.Minute}),
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
	loopCfg, loopErr := ParseAgentLoopConfig(got.Steps[1])
	if loopErr != nil {
		t.Fatalf("ParseAgentLoopConfig: %v", loopErr)
	}
	if loopCfg.MaxIterations != 10 {
		t.Fatalf("MaxIterations = %d, want 10", loopCfg.MaxIterations)
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

func TestStepTypeMapStringAndJSON(t *testing.T) {
	// Positive: string representation
	if got := StepTypeMap.String(); got != "map" {
		t.Fatalf("StepTypeMap.String() = %q, want %q", got, "map")
	}

	// Positive: JSON round-trip
	data, err := json.Marshal(StepTypeMap)
	if err != nil {
		t.Fatalf("Marshal StepTypeMap: %v", err)
	}
	if string(data) != `"map"` {
		t.Fatalf("Marshal StepTypeMap = %s, want %q", data, "map")
	}

	var got StepType
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal StepTypeMap: %v", err)
	}
	if got != StepTypeMap {
		t.Fatalf("Unmarshal StepTypeMap = %v, want %v", got, StepTypeMap)
	}
}

func TestMapConfigJSON(t *testing.T) {
	cfg := MapConfig{MaxItems: 500}

	// Positive: JSON round-trip
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Marshal MapConfig: %v", err)
	}

	var got MapConfig
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal MapConfig: %v", err)
	}
	if got.MaxItems != 500 {
		t.Fatalf("MaxItems = %d, want 500", got.MaxItems)
	}

	// Negative: zero value has zero MaxItems
	zero := MapConfig{}
	dataZero, _ := json.Marshal(zero)
	var gotZero MapConfig
	json.Unmarshal(dataZero, &gotZero)
	if gotZero.MaxItems != 0 {
		t.Fatalf("zero MapConfig MaxItems = %d, want 0", gotZero.MaxItems)
	}
}

func TestMapInstanceStateJSON(t *testing.T) {
	state := MapInstanceState{
		Status: StepStatusCompleted,
		Output: json.RawMessage(`{"result":"success"}`),
		Error:  "",
	}

	// Positive: JSON round-trip
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("Marshal MapInstanceState: %v", err)
	}

	var got MapInstanceState
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal MapInstanceState: %v", err)
	}
	if got.Status != StepStatusCompleted {
		t.Fatalf("Status = %v, want completed", got.Status)
	}
	if string(got.Output) != `{"result":"success"}` {
		t.Fatalf("Output = %s", got.Output)
	}

	// Negative: omitempty fields omitted when empty
	state2 := MapInstanceState{Status: StepStatusRunning}
	data2, _ := json.Marshal(state2)
	if bytes.Contains(data2, []byte("output")) {
		t.Fatalf("empty Output should be omitted, got %s", data2)
	}
	if bytes.Contains(data2, []byte("error")) {
		t.Fatalf("empty Error should be omitted, got %s", data2)
	}
}

func TestStepStateMapInstancesJSON(t *testing.T) {
	state := StepState{
		Status: StepStatusRunning,
		MapInstances: []MapInstanceState{
			{Status: StepStatusCompleted, Output: json.RawMessage(`{"a":1}`)},
			{Status: StepStatusRunning},
		},
	}

	// Positive: JSON round-trip
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("Marshal StepState with MapInstances: %v", err)
	}

	var got StepState
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal StepState with MapInstances: %v", err)
	}
	if len(got.MapInstances) != 2 {
		t.Fatalf("MapInstances count = %d, want 2", len(got.MapInstances))
	}
	if got.MapInstances[0].Status != StepStatusCompleted {
		t.Fatalf("MapInstances[0].Status = %v, want completed",
			got.MapInstances[0].Status)
	}

	// Negative: nil MapInstances omitted
	state2 := StepState{Status: StepStatusPending}
	data2, _ := json.Marshal(state2)
	if bytes.Contains(data2, []byte("map_instances")) {
		t.Fatalf("nil MapInstances should be omitted, got %s", data2)
	}
}

func TestStepStatusRecoveredRoundTrip(t *testing.T) {
	// Positive: Recovered serializes to "recovered"
	data, err := json.Marshal(StepStatusRecovered)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(data) != `"recovered"` {
		t.Fatalf("got %s, want \"recovered\"", data)
	}

	// Positive: "recovered" deserializes back
	var s StepStatus
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if s != StepStatusRecovered {
		t.Fatalf("got %v, want StepStatusRecovered", s)
	}
}

func TestRunStatusCompensatedRoundTrip(t *testing.T) {
	data, err := json.Marshal(RunStatusCompensated)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	// Positive: serializes to "compensated"
	if string(data) != `"compensated"` {
		t.Fatalf("got %s, want \"compensated\"", data)
	}
	var r RunStatus
	if err := json.Unmarshal(data, &r); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	// Positive: round-trips correctly
	if r != RunStatusCompensated {
		t.Fatalf("got %v, want RunStatusCompensated", r)
	}
}

func TestRunStatusCompensateFailedRoundTrip(t *testing.T) {
	data, err := json.Marshal(RunStatusCompensateFailed)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(data) != `"compensate_failed"` {
		t.Fatalf("got %s, want \"compensate_failed\"", data)
	}
	var r RunStatus
	if err := json.Unmarshal(data, &r); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if r != RunStatusCompensateFailed {
		t.Fatalf("got %v, want RunStatusCompensateFailed", r)
	}
}

func TestStepTypeSleepString(t *testing.T) {
	// Positive: string representation
	if StepTypeSleep.String() != "sleep" {
		t.Fatalf("expected 'sleep', got '%s'", StepTypeSleep.String())
	}

	// Positive: JSON unmarshal
	var st StepType
	err := json.Unmarshal([]byte(`"sleep"`), &st)
	if err != nil {
		t.Fatalf("unmarshal must succeed: %v", err)
	}
	if st != StepTypeSleep {
		t.Fatalf("expected StepTypeSleep, got %v", st)
	}
}

func TestRunStatusIsTerminal(t *testing.T) {
	terminals := []RunStatus{
		RunStatusCompleted, RunStatusFailed,
		RunStatusCancelled, RunStatusCompensated,
		RunStatusCompensateFailed,
	}
	for _, s := range terminals {
		if !s.IsTerminal() {
			t.Fatalf("%s should be terminal", s)
		}
	}

	nonTerminals := []RunStatus{
		RunStatusPending, RunStatusRunning,
	}
	for _, s := range nonTerminals {
		if s.IsTerminal() {
			t.Fatalf("%s should not be terminal", s)
		}
	}
}

func TestStepDef_SingletonJSON(t *testing.T) {
	step := StepDef{ID: "x", Task: "t", Singleton: true}
	data, err := json.Marshal(step)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got StepDef
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	// Positive: Singleton survives round-trip
	if !got.Singleton {
		t.Error("Singleton lost in round-trip")
	}
	// Negative: non-singleton omits field from JSON
	step2 := StepDef{ID: "y", Task: "t"}
	data2, _ := json.Marshal(step2)
	if strings.Contains(string(data2), "singleton") {
		t.Error("non-singleton should omit field")
	}
}

func TestCancelOnBuilderAndJSON(t *testing.T) {
	wb := NewWorkflow("cancel-test")
	wb.Task("s", "echo")
	wb.CancelOn("task.done",
		Match{Left: "data.task_id", Op: MatchOpEq,
			Right: "input.task_id"})

	def, err := wb.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(def.CancelOn) != 1 {
		t.Fatalf("CancelOn = %d, want 1", len(def.CancelOn))
	}
	if def.CancelOn[0].Event != "task.done" {
		t.Fatalf("event = %q", def.CancelOn[0].Event)
	}

	data, _ := json.Marshal(def)
	var got WorkflowDef
	json.Unmarshal(data, &got)
	if len(got.CancelOn) != 1 {
		t.Fatalf("roundtrip: CancelOn = %d", len(got.CancelOn))
	}
}

func TestCancelOnWithTimeout(t *testing.T) {
	wb := NewWorkflow("cancel-timeout")
	wb.Task("s", "echo")
	wb.CancelOnWithTimeout("deploy.rollback",
		Match{Left: "data.env", Op: MatchOpEq, Right: "input.env"},
		1*time.Hour)
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if def.CancelOn[0].Timeout != 1*time.Hour {
		t.Fatalf("timeout = %v, want 1h",
			def.CancelOn[0].Timeout)
	}
}

func TestSingletonModeRoundTrip(t *testing.T) {
	cfg := SingletonConfig{
		Mode: SingletonModeCancel,
		Key:  "data.env",
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got SingletonConfig
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Mode != SingletonModeCancel {
		t.Fatalf("mode = %v, want cancel", got.Mode)
	}
	if got.Key != "data.env" {
		t.Fatalf("key = %q, want data.env", got.Key)
	}
}

func TestSingletonOnWorkflowDef(t *testing.T) {
	wb := NewWorkflow("test-singleton")
	wb.Task("s", "echo")
	wb.WithSingleton(SingletonModeSkip)
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if def.Singleton == nil {
		t.Fatal("singleton should not be nil")
	}
	if def.Singleton.Mode != SingletonModeSkip {
		t.Fatalf("mode = %v, want skip", def.Singleton.Mode)
	}
}
