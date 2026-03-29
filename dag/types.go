package dag

import (
	"encoding/json"
	"fmt"
	"time"
)

// StepType distinguishes execution semantics — normal tasks run once, agent loops
// iterate until a termination signal, and sub-workflows delegate to a nested DAG.
type StepType int

const (
	StepTypeNormal      StepType = iota
	StepTypeAgentLoop
	StepTypeSubWorkflow
)

var stepTypeStrings = [...]string{"normal", "agent_loop", "sub_workflow"}

func (s StepType) String() string {
	if int(s) < len(stepTypeStrings) {
		return stepTypeStrings[s]
	}
	panic("unknown StepType")
}

func (s StepType) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

func (s *StepType) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	for i, v := range stepTypeStrings {
		if v == str {
			*s = StepType(i)
			return nil
		}
	}
	panic("unknown StepType string: " + str)
}

// RunStatus tracks the lifecycle of a workflow run. The zero value (pending)
// is a safe default — a newly created run has not yet been claimed by the engine.
type RunStatus int

const (
	RunStatusPending   RunStatus = iota
	RunStatusRunning
	RunStatusCompleted
	RunStatusFailed
	RunStatusCancelled
)

var runStatusStrings = [...]string{
	"pending", "running", "completed", "failed", "cancelled",
}

func (r RunStatus) String() string {
	if int(r) < len(runStatusStrings) {
		return runStatusStrings[r]
	}
	panic("unknown RunStatus")
}

func (r RunStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.String())
}

func (r *RunStatus) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	for i, v := range runStatusStrings {
		if v == str {
			*r = RunStatus(i)
			return nil
		}
	}
	return fmt.Errorf("unknown RunStatus string: %q", str)
}

// StepStatus tracks the lifecycle of a single step within a run. Queued means
// the step has been dispatched to NATS but not yet claimed by a worker.
type StepStatus int

const (
	StepStatusPending   StepStatus = iota
	StepStatusQueued
	StepStatusRunning
	StepStatusCompleted
	StepStatusFailed
	StepStatusSkipped
)

var stepStatusStrings = [...]string{
	"pending", "queued", "running", "completed", "failed", "skipped",
}

func (s StepStatus) String() string {
	if int(s) < len(stepStatusStrings) {
		return stepStatusStrings[s]
	}
	panic("unknown StepStatus")
}

func (s StepStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

func (s *StepStatus) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	for i, v := range stepStatusStrings {
		if v == str {
			*s = StepStatus(i)
			return nil
		}
	}
	return fmt.Errorf("unknown StepStatus string: %q", str)
}

// AgentLoopConfig bounds the iterative behavior of an agent-loop step.
// Both limits are enforced: whichever fires first terminates the loop.
type AgentLoopConfig struct {
	MaxIterations int           `json:"max_iterations"`
	MaxDuration   time.Duration `json:"max_duration,omitempty"`
}

// StepDef is the immutable declaration of a single step within a WorkflowDef.
// DependsOn lists step IDs that must complete before this step is queued.
type StepDef struct {
	ID        string           `json:"id"`
	Task      string           `json:"task"`
	DependsOn []string         `json:"depends_on,omitempty"`
	Retries   int              `json:"retries"`
	Timeout   time.Duration    `json:"timeout"`
	Type      StepType         `json:"type"`
	Loop      *AgentLoopConfig `json:"loop,omitempty"`
}

// WorkflowDef is the immutable schema for a workflow. Stored once, referenced
// by many runs. Version allows schema evolution without breaking existing runs.
type WorkflowDef struct {
	Name    string    `json:"name"`
	Version string    `json:"version"`
	Steps   []StepDef `json:"steps"`
}

// StepState captures mutable runtime state for one step in a run.
// Output is kept as raw bytes to remain payload-agnostic.
// Iterations tracks how many agent-loop Continue cycles have completed;
// used to generate unique dedup IDs for each re-enqueue.
// LoopStartedAt records when the first iteration began, for MaxDuration enforcement.
type StepState struct {
	Status        StepStatus `json:"status"`
	Attempts      int        `json:"attempts"`
	Iterations    int        `json:"iterations,omitempty"`
	LoopStartedAt time.Time  `json:"loop_started_at,omitempty"`
	Output        []byte     `json:"output,omitempty"`
	Error         string     `json:"error,omitempty"`
}

// WorkflowRun holds live state for a single execution of a WorkflowDef.
// Steps maps step ID to its current StepState; initialized to pending for all steps.
type WorkflowRun struct {
	RunID      string               `json:"run_id"`
	WorkflowID string               `json:"workflow_id"`
	Status     RunStatus            `json:"status"`
	Steps      map[string]StepState `json:"steps"`
	CreatedAt  time.Time            `json:"created_at"`
}

// NewWorkflowRun constructs a WorkflowRun with all steps initialized to pending.
// runID must be non-empty — callers are responsible for providing a unique ID
// (e.g. nuid.Next()) before calling this constructor.
func NewWorkflowRun(def WorkflowDef, runID string) WorkflowRun {
	if runID == "" {
		panic("NewWorkflowRun: runID must not be empty")
	}
	if len(def.Steps) == 0 {
		panic("NewWorkflowRun: WorkflowDef must have at least one step")
	}
	steps := make(map[string]StepState, len(def.Steps))
	for _, step := range def.Steps {
		steps[step.ID] = StepState{Status: StepStatusPending}
	}
	return WorkflowRun{
		RunID:      runID,
		WorkflowID: def.Name,
		Status:     RunStatusPending,
		Steps:      steps,
		CreatedAt:  time.Now().UTC(),
	}
}
