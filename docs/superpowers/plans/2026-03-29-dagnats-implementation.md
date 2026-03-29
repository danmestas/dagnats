# DagNats Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a DAG-based workflow engine on NATS with a Graph DSL, thin orchestrator, deep worker interface, agent loop support, and provider-agnostic observability.

**Architecture:** Thin orchestrator consumes event history, resolves DAG dependencies, enqueues ready tasks. Workers pull tasks via JetStream, execute handlers, publish results. All state lives in NATS (streams + KV). Observability via provider-agnostic interfaces.

**Tech Stack:** Go, NATS JetStream (streams, KV, object store), nats.go client library, embedded nats-server for tests.

---

## File Map

```
dagnats/
├── go.mod
├── go.sum
├── dag/
│   ├── types.go          # WorkflowDef, StepDef, StepType, RunStatus, StepStatus
│   ├── builder.go        # Graph DSL: NewWorkflow, Task, DependsOn, AgentLoop
│   ├── resolve.go        # DAG resolution: topological sort, ResolveReady
│   ├── validate.go       # DAG validation: cycles, missing deps, duplicate IDs
│   ├── types_test.go     # Type serialization tests
│   ├── builder_test.go   # DSL construction tests
│   ├── resolve_test.go   # Resolution + topological sort tests
│   └── validate_test.go  # Validation tests
├── observe/
│   ├── observe.go        # Logger, Metrics, ErrorReporter interfaces + Field type
│   ├── noop.go           # No-op implementations
│   └── observe_test.go   # Interface compliance tests
├── natsutil/
│   ├── conn.go           # Connection helpers, stream/KV/consumer setup
│   ├── testserver.go     # Embedded NATS server helper for tests
│   └── conn_test.go      # Connection + setup tests
├── engine/
│   ├── events.go         # Event types, serialization, history helpers
│   ├── snapshot.go       # KV snapshot read/write, state reconstruction
│   ├── orchestrator.go   # Core loop: consume events, resolve DAG, enqueue tasks
│   ├── events_test.go    # Event serialization tests
│   ├── snapshot_test.go  # Snapshot tests (real NATS)
│   └── orchestrator_test.go # Orchestrator integration tests (real NATS)
├── worker/
│   ├── context.go        # TaskContext implementation
│   ├── worker.go         # Worker: handler registration, consumer pull loop
│   ├── context_test.go   # TaskContext tests (real NATS)
│   └── worker_test.go    # Worker integration tests (real NATS)
├── api/
│   ├── service.go        # Core service logic (shared by REST + NATS)
│   ├── rest.go           # REST HTTP handlers
│   ├── natsapi.go        # NATS request/reply handlers
│   ├── service_test.go   # Service logic tests (real NATS)
│   └── rest_test.go      # REST endpoint tests
├── cli/
│   ├── root.go           # CLI root command
│   ├── workflow.go       # workflow list/register subcommands
│   ├── run.go            # run start/status/history/retry subcommands
│   └── run_test.go       # CLI output formatting tests
└── cmd/
    ├── dagnats-engine/
    │   └── main.go       # Orchestrator binary
    ├── dagnats-api/
    │   └── main.go       # Control plane binary
    └── dagnats/
        └── main.go       # CLI binary
```

---

### Task 1: Go Module Init + Project Scaffolding

**Files:**
- Create: `go.mod`

- [ ] **Step 1: Initialize Go module**

```bash
cd /Users/dmestas/projects/dagnats
go mod init github.com/danmestas/dagnats
```

- [ ] **Step 2: Add core dependencies**

```bash
go get github.com/nats-io/nats.go@latest
go get github.com/nats-io/nats-server/v2@latest
```

- [ ] **Step 3: Verify module**

Run: `go mod tidy && cat go.mod`
Expected: Module `github.com/danmestas/dagnats` with nats.go and nats-server dependencies.

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum
git commit -m "feat: initialize Go module with NATS dependencies"
```

---

### Task 2: Core Types (`dag/types.go`)

**Files:**
- Create: `dag/types.go`
- Create: `dag/types_test.go`

- [ ] **Step 1: Write failing test for StepType constants and RunStatus/StepStatus**

```go
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
		// Negative: no other value should match
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
				ID:            "step-b",
				Task:          "task-b",
				DependsOn:     []string{"step-a"},
				Retries:       1,
				Timeout:       60 * time.Second,
				Type:          StepTypeAgentLoop,
				MaxIterations: 10,
				MaxDuration:   5 * time.Minute,
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
	if got.Steps[1].MaxIterations != 10 {
		t.Fatalf("Steps[1].MaxIterations = %d, want 10", got.Steps[1].MaxIterations)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./dag/ -v`
Expected: FAIL — types not defined.

- [ ] **Step 3: Implement types**

```go
// dag/types.go
package dag

import (
	"encoding/json"
	"time"
)

// StepType distinguishes normal steps from agent loops and sub-workflows.
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

// RunStatus tracks the lifecycle of a workflow run.
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

// StepStatus tracks the lifecycle of a single step within a run.
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

// StepDef defines a single step in a workflow DAG.
type StepDef struct {
	ID            string        `json:"id"`
	Task          string        `json:"task"`
	DependsOn     []string      `json:"depends_on,omitempty"`
	Retries       int           `json:"retries"`
	Timeout       time.Duration `json:"timeout"`
	Type          StepType      `json:"type"`
	MaxIterations int           `json:"max_iterations,omitempty"`
	MaxDuration   time.Duration `json:"max_duration,omitempty"`
}

// WorkflowDef is the static DAG template for a workflow.
type WorkflowDef struct {
	Name    string    `json:"name"`
	Version string    `json:"version"`
	Steps   []StepDef `json:"steps"`
}

// StepState tracks the runtime state of a step within a run.
type StepState struct {
	Status   StepStatus `json:"status"`
	Attempts int        `json:"attempts"`
	Output   []byte     `json:"output,omitempty"`
	Error    string     `json:"error,omitempty"`
}

// WorkflowRun represents a live execution of a workflow.
type WorkflowRun struct {
	RunID      string               `json:"run_id"`
	WorkflowID string               `json:"workflow_id"`
	Status     RunStatus            `json:"status"`
	Steps      map[string]StepState `json:"steps"`
	CreatedAt  time.Time            `json:"created_at"`
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats && go test ./dag/ -v`
Expected: PASS — all three tests.

- [ ] **Step 5: Commit**

```bash
git add dag/types.go dag/types_test.go
git commit -m "feat(dag): add core types — WorkflowDef, StepDef, WorkflowRun, enums"
```

---

### Task 3: DAG Validation (`dag/validate.go`)

**Files:**
- Create: `dag/validate.go`
- Create: `dag/validate_test.go`

- [ ] **Step 1: Write failing tests for validation**

```go
// dag/validate_test.go

// Tests for DAG validation: duplicate step IDs, missing dependency references,
// cycle detection, empty workflows, and valid DAGs.
// Methodology: each test builds a specific invalid (or valid) DAG and asserts
// the exact error returned. Positive + negative space checked per test.
package dag

import (
	"strings"
	"testing"
)

func TestValidateDuplicateStepIDs(t *testing.T) {
	def := WorkflowDef{
		Name:    "dup-ids",
		Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "task-a", Type: StepTypeNormal},
			{ID: "a", Task: "task-b", Type: StepTypeNormal},
		},
	}
	err := Validate(def)
	if err == nil {
		t.Fatal("expected error for duplicate step IDs, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("error should mention 'duplicate', got: %v", err)
	}
}

func TestValidateMissingDependency(t *testing.T) {
	def := WorkflowDef{
		Name:    "missing-dep",
		Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "task-a", DependsOn: []string{"nonexistent"}, Type: StepTypeNormal},
		},
	}
	err := Validate(def)
	if err == nil {
		t.Fatal("expected error for missing dependency, got nil")
	}
	if !strings.Contains(err.Error(), "nonexistent") {
		t.Fatalf("error should mention missing dep name, got: %v", err)
	}
}

func TestValidateCycleDetection(t *testing.T) {
	def := WorkflowDef{
		Name:    "cycle",
		Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "task-a", DependsOn: []string{"c"}, Type: StepTypeNormal},
			{ID: "b", Task: "task-b", DependsOn: []string{"a"}, Type: StepTypeNormal},
			{ID: "c", Task: "task-c", DependsOn: []string{"b"}, Type: StepTypeNormal},
		},
	}
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
	def := WorkflowDef{
		Name:    "valid",
		Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "task-a", Type: StepTypeNormal},
			{ID: "b", Task: "task-b", DependsOn: []string{"a"}, Type: StepTypeNormal},
			{ID: "c", Task: "task-c", DependsOn: []string{"a"}, Type: StepTypeNormal},
			{ID: "d", Task: "task-d", DependsOn: []string{"b", "c"}, Type: StepTypeNormal},
		},
	}
	err := Validate(def)
	if err != nil {
		t.Fatalf("expected valid DAG, got error: %v", err)
	}
}

func TestValidateAgentLoopRequiresMaxIterations(t *testing.T) {
	def := WorkflowDef{
		Name:    "loop-no-max",
		Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "task-a", Type: StepTypeAgentLoop, MaxIterations: 0},
		},
	}
	err := Validate(def)
	if err == nil {
		t.Fatal("expected error for agent loop without MaxIterations, got nil")
	}
	if !strings.Contains(err.Error(), "MaxIterations") {
		t.Fatalf("error should mention 'MaxIterations', got: %v", err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/dmestas/projects/dagnats && go test ./dag/ -run TestValidate -v`
Expected: FAIL — `Validate` not defined.

- [ ] **Step 3: Implement validation**

```go
// dag/validate.go
package dag

import "fmt"

// Validate checks a WorkflowDef for structural errors:
// duplicate IDs, missing dependencies, cycles, and agent loop bounds.
func Validate(def WorkflowDef) error {
	if len(def.Steps) == 0 {
		return fmt.Errorf("workflow %q has no steps", def.Name)
	}

	ids := make(map[string]bool, len(def.Steps))
	for _, s := range def.Steps {
		if ids[s.ID] {
			return fmt.Errorf("duplicate step ID %q", s.ID)
		}
		ids[s.ID] = true
	}

	for _, s := range def.Steps {
		for _, dep := range s.DependsOn {
			if !ids[dep] {
				return fmt.Errorf(
					"step %q depends on %q which does not exist",
					s.ID, dep,
				)
			}
		}
		if s.Type == StepTypeAgentLoop && s.MaxIterations <= 0 {
			return fmt.Errorf(
				"step %q is AgentLoop but MaxIterations is %d (must be > 0)",
				s.ID, s.MaxIterations,
			)
		}
	}

	return detectCycle(def.Steps)
}

// detectCycle uses iterative Kahn's algorithm (no recursion per TigerStyle).
func detectCycle(steps []StepDef) error {
	inDegree := make(map[string]int, len(steps))
	dependents := make(map[string][]string, len(steps))

	for _, s := range steps {
		if _, ok := inDegree[s.ID]; !ok {
			inDegree[s.ID] = 0
		}
		for _, dep := range s.DependsOn {
			inDegree[s.ID]++
			dependents[dep] = append(dependents[dep], s.ID)
		}
	}

	queue := make([]string, 0, len(steps))
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}

	visited := 0
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		visited++
		for _, dep := range dependents[node] {
			inDegree[dep]--
			if inDegree[dep] == 0 {
				queue = append(queue, dep)
			}
		}
	}

	if visited != len(steps) {
		return fmt.Errorf("workflow contains a cycle (%d of %d steps reachable)", visited, len(steps))
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats && go test ./dag/ -run TestValidate -v`
Expected: PASS — all six validation tests.

- [ ] **Step 5: Commit**

```bash
git add dag/validate.go dag/validate_test.go
git commit -m "feat(dag): add DAG validation — duplicates, missing deps, cycles, agent loop bounds"
```

---

### Task 4: Graph DSL Builder (`dag/builder.go`)

**Files:**
- Create: `dag/builder.go`
- Create: `dag/builder_test.go`

- [ ] **Step 1: Write failing tests for the builder DSL**

```go
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

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if def.Name != "linear" {
		t.Fatalf("Name = %q, want %q", def.Name, "linear")
	}
	if len(def.Steps) != 3 {
		t.Fatalf("Steps count = %d, want 3", len(def.Steps))
	}

	// Verify dependency wiring
	stepB := findStep(def, "b")
	if stepB == nil {
		t.Fatal("step 'b' not found")
	}
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
	def, err := NewWorkflow("with-loop").
		Task("prep", "task-prep").
		AgentLoop("fix", "task-fix").
			DependsOn("prep").
			WithMaxIterations(10).
			WithMaxDuration(5 * time.Minute).
		Build()

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
	if fix.MaxIterations != 10 {
		t.Fatalf("fix.MaxIterations = %d, want 10", fix.MaxIterations)
	}
}

func TestBuilderWithRetries(t *testing.T) {
	def, err := NewWorkflow("retries").
		Task("a", "task-a").WithRetries(5).WithTimeout(30 * time.Second).
		Build()

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	step := findStep(def, "a")
	if step.Retries != 5 {
		t.Fatalf("Retries = %d, want 5", step.Retries)
	}
	if step.Timeout != 30*time.Second {
		t.Fatalf("Timeout = %v, want 30s", step.Timeout)
	}
}

func TestBuilderValidationError(t *testing.T) {
	_, err := NewWorkflow("bad").
		Task("a", "task-a").DependsOn("nonexistent").
		Build()

	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
}

func findStep(def WorkflowDef, id string) *StepDef {
	for i := range def.Steps {
		if def.Steps[i].ID == id {
			return &def.Steps[i]
		}
	}
	return nil
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/dmestas/projects/dagnats && go test ./dag/ -run TestBuilder -v`
Expected: FAIL — `NewWorkflow` not defined.

- [ ] **Step 3: Implement the builder**

```go
// dag/builder.go
package dag

import "time"

// WorkflowBuilder constructs a WorkflowDef via fluent API.
type WorkflowBuilder struct {
	name    string
	version string
	steps   []StepDef
	current int // index of the step being configured
}

// NewWorkflow starts building a workflow with the given name.
func NewWorkflow(name string) *WorkflowBuilder {
	return &WorkflowBuilder{
		name:    name,
		version: "1",
		current: -1,
	}
}

// Version sets the workflow version.
func (b *WorkflowBuilder) Version(v string) *WorkflowBuilder {
	b.version = v
	return b
}

// Task adds a normal step to the workflow.
func (b *WorkflowBuilder) Task(id, task string) *WorkflowBuilder {
	b.steps = append(b.steps, StepDef{
		ID:   id,
		Task: task,
		Type: StepTypeNormal,
	})
	b.current = len(b.steps) - 1
	return b
}

// AgentLoop adds an agent loop step to the workflow.
func (b *WorkflowBuilder) AgentLoop(id, task string) *WorkflowBuilder {
	b.steps = append(b.steps, StepDef{
		ID:   id,
		Task: task,
		Type: StepTypeAgentLoop,
	})
	b.current = len(b.steps) - 1
	return b
}

// DependsOn sets dependencies for the current step.
func (b *WorkflowBuilder) DependsOn(ids ...string) *WorkflowBuilder {
	if b.current < 0 {
		panic("DependsOn called before adding a step")
	}
	b.steps[b.current].DependsOn = append(
		b.steps[b.current].DependsOn, ids...,
	)
	return b
}

// WithRetries sets the retry count for the current step.
func (b *WorkflowBuilder) WithRetries(n int) *WorkflowBuilder {
	if b.current < 0 {
		panic("WithRetries called before adding a step")
	}
	b.steps[b.current].Retries = n
	return b
}

// WithTimeout sets the timeout for the current step.
func (b *WorkflowBuilder) WithTimeout(d time.Duration) *WorkflowBuilder {
	if b.current < 0 {
		panic("WithTimeout called before adding a step")
	}
	b.steps[b.current].Timeout = d
	return b
}

// WithMaxIterations sets the iteration cap for an AgentLoop step.
func (b *WorkflowBuilder) WithMaxIterations(n int) *WorkflowBuilder {
	if b.current < 0 {
		panic("WithMaxIterations called before adding a step")
	}
	b.steps[b.current].MaxIterations = n
	return b
}

// WithMaxDuration sets the total duration cap for an AgentLoop step.
func (b *WorkflowBuilder) WithMaxDuration(d time.Duration) *WorkflowBuilder {
	if b.current < 0 {
		panic("WithMaxDuration called before adding a step")
	}
	b.steps[b.current].MaxDuration = d
	return b
}

// Build validates and returns the WorkflowDef.
func (b *WorkflowBuilder) Build() (WorkflowDef, error) {
	def := WorkflowDef{
		Name:    b.name,
		Version: b.version,
		Steps:   b.steps,
	}
	if err := Validate(def); err != nil {
		return WorkflowDef{}, err
	}
	return def, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats && go test ./dag/ -run TestBuilder -v`
Expected: PASS — all five builder tests.

- [ ] **Step 5: Commit**

```bash
git add dag/builder.go dag/builder_test.go
git commit -m "feat(dag): add Graph DSL builder — fluent API for workflow construction"
```

---

### Task 5: DAG Resolution (`dag/resolve.go`)

**Files:**
- Create: `dag/resolve.go`
- Create: `dag/resolve_test.go`

- [ ] **Step 1: Write failing tests for DAG resolution**

```go
// dag/resolve_test.go

// Tests for DAG resolution: given a set of completed steps, determine which
// steps are ready to execute. Uses topological ordering for deterministic results.
// Methodology: build DAGs of varying shapes, mark subsets as completed, and
// verify exactly which steps become ready. Check both inclusion and exclusion.
package dag

import (
	"sort"
	"testing"
)

func TestResolveReadyFirstSteps(t *testing.T) {
	def := WorkflowDef{
		Name:    "test",
		Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "t-a", Type: StepTypeNormal},
			{ID: "b", Task: "t-b", DependsOn: []string{"a"}, Type: StepTypeNormal},
		},
	}

	ready := ResolveReady(def, map[string]bool{}, map[string]bool{})
	if len(ready) != 1 {
		t.Fatalf("expected 1 ready step, got %d", len(ready))
	}
	if ready[0].ID != "a" {
		t.Fatalf("expected step 'a', got %q", ready[0].ID)
	}
}

func TestResolveReadyAfterCompletion(t *testing.T) {
	def := WorkflowDef{
		Name:    "test",
		Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "t-a", Type: StepTypeNormal},
			{ID: "b", Task: "t-b", DependsOn: []string{"a"}, Type: StepTypeNormal},
			{ID: "c", Task: "t-c", DependsOn: []string{"a"}, Type: StepTypeNormal},
		},
	}

	completed := map[string]bool{"a": true}
	ready := ResolveReady(def, completed, map[string]bool{})

	ids := readyIDs(ready)
	sort.Strings(ids)
	if len(ids) != 2 || ids[0] != "b" || ids[1] != "c" {
		t.Fatalf("expected [b, c], got %v", ids)
	}

	// Negative: completed step must not appear
	for _, s := range ready {
		if completed[s.ID] {
			t.Fatalf("completed step %q appeared in ready list", s.ID)
		}
	}
}

func TestResolveReadyFanIn(t *testing.T) {
	def := WorkflowDef{
		Name:    "test",
		Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "t-a", Type: StepTypeNormal},
			{ID: "b", Task: "t-b", Type: StepTypeNormal},
			{ID: "c", Task: "t-c", DependsOn: []string{"a", "b"}, Type: StepTypeNormal},
		},
	}

	// Only 'a' completed — 'c' should NOT be ready
	ready := ResolveReady(def, map[string]bool{"a": true}, map[string]bool{})
	for _, s := range ready {
		if s.ID == "c" {
			t.Fatal("step 'c' should not be ready — 'b' not completed")
		}
	}

	// Both completed — 'c' should be ready
	ready = ResolveReady(def, map[string]bool{"a": true, "b": true}, map[string]bool{})
	if len(ready) != 1 || ready[0].ID != "c" {
		t.Fatalf("expected [c], got %v", readyIDs(ready))
	}
}

func TestResolveReadySkipsQueued(t *testing.T) {
	def := WorkflowDef{
		Name:    "test",
		Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "t-a", Type: StepTypeNormal},
		},
	}

	// 'a' already queued — should not appear again
	ready := ResolveReady(def, map[string]bool{}, map[string]bool{"a": true})
	if len(ready) != 0 {
		t.Fatalf("expected 0 ready steps (already queued), got %d", len(ready))
	}
}

func TestResolveReadyAllCompleted(t *testing.T) {
	def := WorkflowDef{
		Name:    "test",
		Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "t-a", Type: StepTypeNormal},
			{ID: "b", Task: "t-b", DependsOn: []string{"a"}, Type: StepTypeNormal},
		},
	}

	ready := ResolveReady(def, map[string]bool{"a": true, "b": true}, map[string]bool{})
	if len(ready) != 0 {
		t.Fatalf("expected 0 ready steps (all completed), got %d", len(ready))
	}
}

func readyIDs(steps []StepDef) []string {
	ids := make([]string, len(steps))
	for i, s := range steps {
		ids[i] = s.ID
	}
	return ids
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/dmestas/projects/dagnats && go test ./dag/ -run TestResolveReady -v`
Expected: FAIL — `ResolveReady` not defined.

- [ ] **Step 3: Implement resolution**

```go
// dag/resolve.go
package dag

// ResolveReady returns steps whose dependencies are all completed,
// that are not already completed or queued.
// completed: set of step IDs that have finished successfully.
// queued: set of step IDs that are already enqueued or in-progress.
func ResolveReady(
	def WorkflowDef,
	completed map[string]bool,
	queued map[string]bool,
) []StepDef {
	ready := make([]StepDef, 0, len(def.Steps))

	for _, step := range def.Steps {
		if completed[step.ID] || queued[step.ID] {
			continue
		}
		if allDepsCompleted(step.DependsOn, completed) {
			ready = append(ready, step)
		}
	}

	return ready
}

// IsComplete returns true when every step in the workflow has completed.
func IsComplete(def WorkflowDef, completed map[string]bool) bool {
	for _, step := range def.Steps {
		if !completed[step.ID] {
			return false
		}
	}
	return true
}

func allDepsCompleted(deps []string, completed map[string]bool) bool {
	for _, dep := range deps {
		if !completed[dep] {
			return false
		}
	}
	return true
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats && go test ./dag/ -run TestResolveReady -v`
Expected: PASS — all five resolution tests.

- [ ] **Step 5: Commit**

```bash
git add dag/resolve.go dag/resolve_test.go
git commit -m "feat(dag): add DAG resolution — find ready steps given completed set"
```

---

### Task 6: Observability Interfaces (`observe/`)

**Files:**
- Create: `observe/observe.go`
- Create: `observe/noop.go`
- Create: `observe/observe_test.go`

- [ ] **Step 1: Write failing tests for observability interfaces and noop implementations**

```go
// observe/observe_test.go

// Tests for observability interfaces and noop implementations.
// Methodology: verify noop implementations satisfy interfaces at compile time,
// that they don't panic when called, and that Logger.With returns a usable Logger.
package observe

import (
	"context"
	"testing"
)

func TestNoopLoggerSatisfiesInterface(t *testing.T) {
	var logger Logger = NewNoopLogger()
	if logger == nil {
		t.Fatal("NewNoopLogger returned nil")
	}

	// Must not panic
	logger.Info("test message", String("key", "val"))
	logger.Error("test error", nil, String("key", "val"))

	child := logger.With(String("component", "test"))
	if child == nil {
		t.Fatal("Logger.With returned nil")
	}
}

func TestNoopErrorReporterSatisfiesInterface(t *testing.T) {
	var reporter ErrorReporter = NewNoopErrorReporter()
	if reporter == nil {
		t.Fatal("NewNoopErrorReporter returned nil")
	}

	// Must not panic
	ctx := context.Background()
	reporter.CaptureError(ctx, nil, nil)
	reporter.CaptureMessage(ctx, "test", LevelInfo)
}

func TestNoopMetricsSatisfiesInterface(t *testing.T) {
	var metrics Metrics = NewNoopMetrics()
	if metrics == nil {
		t.Fatal("NewNoopMetrics returned nil")
	}

	counter := metrics.Counter("test_counter", nil)
	if counter == nil {
		t.Fatal("Counter returned nil")
	}
	counter.Inc() // must not panic

	histogram := metrics.Histogram("test_hist", nil)
	if histogram == nil {
		t.Fatal("Histogram returned nil")
	}
	histogram.Observe(1.5) // must not panic

	gauge := metrics.Gauge("test_gauge", nil)
	if gauge == nil {
		t.Fatal("Gauge returned nil")
	}
	gauge.Set(42.0) // must not panic
}

func TestLevelString(t *testing.T) {
	tests := []struct {
		level    Level
		expected string
	}{
		{LevelDebug, "debug"},
		{LevelInfo, "info"},
		{LevelWarn, "warn"},
		{LevelError, "error"},
	}
	for _, tt := range tests {
		got := tt.level.String()
		if got != tt.expected {
			t.Fatalf("Level.String() = %q, want %q", got, tt.expected)
		}
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/dmestas/projects/dagnats && go test ./observe/ -v`
Expected: FAIL — package not found.

- [ ] **Step 3: Implement interfaces**

```go
// observe/observe.go
package observe

import "context"

// Level represents log severity.
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

var levelStrings = [...]string{"debug", "info", "warn", "error"}

func (l Level) String() string {
	if int(l) < len(levelStrings) {
		return levelStrings[l]
	}
	panic("unknown Level")
}

// Field is a structured log field.
type Field struct {
	Key   string
	Value interface{}
}

// String creates a string-typed field.
func String(key, val string) Field {
	return Field{Key: key, Value: val}
}

// Int creates an int-typed field.
func Int(key string, val int) Field {
	return Field{Key: key, Value: val}
}

// Err creates an error-typed field.
func Err(err error) Field {
	return Field{Key: "error", Value: err}
}

// Logger provides structured logging. Provider-agnostic.
type Logger interface {
	Info(msg string, fields ...Field)
	Error(msg string, err error, fields ...Field)
	With(fields ...Field) Logger
}

// ErrorReporter captures errors for external reporting. Provider-agnostic.
type ErrorReporter interface {
	CaptureError(ctx context.Context, err error, tags map[string]string)
	CaptureMessage(ctx context.Context, msg string, level Level)
}

// Counter is an incrementable metric.
type Counter interface {
	Inc()
	Add(delta float64)
}

// Histogram records value distributions.
type Histogram interface {
	Observe(value float64)
}

// Gauge tracks a value that goes up and down.
type Gauge interface {
	Set(value float64)
	Inc()
	Dec()
}

// Metrics provides metric instruments. Provider-agnostic.
type Metrics interface {
	Counter(name string, tags map[string]string) Counter
	Histogram(name string, tags map[string]string) Histogram
	Gauge(name string, tags map[string]string) Gauge
}
```

```go
// observe/noop.go
package observe

import "context"

// --- Noop Logger ---

type noopLogger struct{}

func NewNoopLogger() Logger                              { return &noopLogger{} }
func (n *noopLogger) Info(msg string, fields ...Field)   {}
func (n *noopLogger) Error(msg string, err error, fields ...Field) {}
func (n *noopLogger) With(fields ...Field) Logger        { return n }

// --- Noop ErrorReporter ---

type noopErrorReporter struct{}

func NewNoopErrorReporter() ErrorReporter { return &noopErrorReporter{} }
func (n *noopErrorReporter) CaptureError(ctx context.Context, err error, tags map[string]string) {}
func (n *noopErrorReporter) CaptureMessage(ctx context.Context, msg string, level Level) {}

// --- Noop Metrics ---

type noopMetrics struct{}
type noopCounter struct{}
type noopHistogram struct{}
type noopGauge struct{}

func NewNoopMetrics() Metrics { return &noopMetrics{} }

func (n *noopMetrics) Counter(name string, tags map[string]string) Counter     { return &noopCounter{} }
func (n *noopMetrics) Histogram(name string, tags map[string]string) Histogram { return &noopHistogram{} }
func (n *noopMetrics) Gauge(name string, tags map[string]string) Gauge         { return &noopGauge{} }

func (n *noopCounter) Inc()              {}
func (n *noopCounter) Add(delta float64) {}
func (n *noopHistogram) Observe(float64) {}
func (n *noopGauge) Set(float64)         {}
func (n *noopGauge) Inc()                {}
func (n *noopGauge) Dec()                {}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats && go test ./observe/ -v`
Expected: PASS — all four tests.

- [ ] **Step 5: Commit**

```bash
git add observe/observe.go observe/noop.go observe/observe_test.go
git commit -m "feat(observe): add provider-agnostic observability interfaces + noop defaults"
```

---

### Task 7: NATS Utilities + Test Server (`natsutil/`)

**Files:**
- Create: `natsutil/conn.go`
- Create: `natsutil/testserver.go`
- Create: `natsutil/conn_test.go`

- [ ] **Step 1: Write failing tests for NATS connection and stream setup**

```go
// natsutil/conn_test.go

// Tests for NATS utility functions: connection, stream creation, KV bucket setup.
// Methodology: each test starts an embedded NATS server, calls the utility,
// then verifies the resource was created via NATS JetStream API.
// Bounded 5-second timeout on all operations.
package natsutil

import (
	"testing"
	"time"
)

func TestStartTestServer(t *testing.T) {
	ns, nc := StartTestServer(t)
	if ns == nil {
		t.Fatal("test server is nil")
	}
	if nc == nil {
		t.Fatal("nats connection is nil")
	}
	if !nc.IsConnected() {
		t.Fatal("nats connection is not connected")
	}
}

func TestSetupStreams(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	err = SetupStreams(js)
	if err != nil {
		t.Fatalf("SetupStreams failed: %v", err)
	}

	// Verify WORKFLOW_HISTORY stream exists
	info, err := js.StreamInfo("WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("StreamInfo(WORKFLOW_HISTORY) failed: %v", err)
	}
	if info == nil {
		t.Fatal("WORKFLOW_HISTORY stream not found")
	}

	// Verify TASK_QUEUES stream exists
	info, err = js.StreamInfo("TASK_QUEUES")
	if err != nil {
		t.Fatalf("StreamInfo(TASK_QUEUES) failed: %v", err)
	}
	if info == nil {
		t.Fatal("TASK_QUEUES stream not found")
	}

	// Verify EVENTS stream exists
	info, err = js.StreamInfo("EVENTS")
	if err != nil {
		t.Fatalf("StreamInfo(EVENTS) failed: %v", err)
	}
	if info == nil {
		t.Fatal("EVENTS stream not found")
	}
}

func TestSetupKVBuckets(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	err = SetupKVBuckets(js)
	if err != nil {
		t.Fatalf("SetupKVBuckets failed: %v", err)
	}

	// Verify workflow_defs bucket
	kv, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("KeyValue(workflow_defs) failed: %v", err)
	}

	// Write and read back to verify it works
	_, err = kv.PutString("test-key", "test-value")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	entry, err := kv.Get("test-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(entry.Value()) != "test-value" {
		t.Fatalf("value = %q, want %q", string(entry.Value()), "test-value")
	}

	// Verify workflow_runs bucket exists
	_, err = js.KeyValue("workflow_runs")
	if err != nil {
		t.Fatalf("KeyValue(workflow_runs) failed: %v", err)
	}
}

func TestSetupAll(t *testing.T) {
	_, nc := StartTestServer(t)

	done := make(chan error, 1)
	go func() { done <- SetupAll(nc) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SetupAll failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SetupAll timed out after 5s")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/dmestas/projects/dagnats && go test ./natsutil/ -v`
Expected: FAIL — package not found.

- [ ] **Step 3: Implement test server helper**

```go
// natsutil/testserver.go
package natsutil

import (
	"testing"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// StartTestServer launches an embedded NATS server with JetStream enabled.
// The server is automatically shut down when the test completes.
// Returns the server and a connected client.
func StartTestServer(t *testing.T) (*natsserver.Server, *nats.Conn) {
	t.Helper()

	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1, // random available port
		JetStream: true,
		StoreDir:  t.TempDir(),
	}

	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("failed to create test NATS server: %v", err)
	}

	ns.Start()
	if !ns.ReadyForConnections(5_000_000_000) { // 5 seconds in nanoseconds
		t.Fatal("NATS server not ready after 5s")
	}

	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("failed to connect to test NATS server: %v", err)
	}
	t.Cleanup(func() { nc.Close() })

	return ns, nc
}
```

- [ ] **Step 4: Implement connection and setup helpers**

```go
// natsutil/conn.go
package natsutil

import (
	"github.com/nats-io/nats.go"
)

// SetupStreams creates the core JetStream streams for DagNats.
func SetupStreams(js nats.JetStreamContext) error {
	streams := []nats.StreamConfig{
		{
			Name:       "WORKFLOW_HISTORY",
			Subjects:   []string{"history.>"},
			Retention:  nats.LimitsPolicy,
			Storage:    nats.FileStorage,
			Duplicates: 5_000_000_000, // 5 second dedup window (nanoseconds)
		},
		{
			Name:      "TASK_QUEUES",
			Subjects:  []string{"task.>"},
			Retention: nats.WorkQueuePolicy,
			Storage:   nats.FileStorage,
		},
		{
			Name:      "EVENTS",
			Subjects:  []string{"event.>"},
			Retention: nats.LimitsPolicy,
			Storage:   nats.FileStorage,
		},
	}

	for _, cfg := range streams {
		_, err := js.AddStream(&cfg)
		if err != nil {
			return err
		}
	}
	return nil
}

// SetupKVBuckets creates the KV buckets for workflow defs and run state.
func SetupKVBuckets(js nats.JetStreamContext) error {
	buckets := []nats.KeyValueConfig{
		{Bucket: "workflow_defs"},
		{Bucket: "workflow_runs"},
	}

	for _, cfg := range buckets {
		_, err := js.CreateKeyValue(&cfg)
		if err != nil {
			return err
		}
	}
	return nil
}

// SetupAll initializes all NATS resources (streams + KV buckets).
func SetupAll(nc *nats.Conn) error {
	js, err := nc.JetStream()
	if err != nil {
		return err
	}

	if err := SetupStreams(js); err != nil {
		return err
	}
	return SetupKVBuckets(js)
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats && go test ./natsutil/ -v -timeout 30s`
Expected: PASS — all four tests.

- [ ] **Step 6: Commit**

```bash
git add natsutil/conn.go natsutil/testserver.go natsutil/conn_test.go
git commit -m "feat(natsutil): add NATS connection helpers, stream/KV setup, test server"
```

---

### Task 8: Event Types and Serialization (`engine/events.go`)

**Files:**
- Create: `engine/events.go`
- Create: `engine/events_test.go`

- [ ] **Step 1: Write failing tests for event types**

```go
// engine/events_test.go

// Tests for workflow event types: serialization roundtrips, required fields,
// and event type classification.
// Methodology: construct events, marshal to JSON, unmarshal back, and verify
// all fields survive the roundtrip. Check required field validation.
package engine

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEventJSONRoundTrip(t *testing.T) {
	original := Event{
		Type:      EventStepCompleted,
		RunID:     "run-123",
		StepID:    "step-a",
		Timestamp: time.Now().UTC().Truncate(time.Millisecond),
		Payload:   json.RawMessage(`{"result":"ok"}`),
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded Event
	err = json.Unmarshal(data, &decoded)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if decoded.Type != original.Type {
		t.Fatalf("Type = %q, want %q", decoded.Type, original.Type)
	}
	if decoded.RunID != original.RunID {
		t.Fatalf("RunID = %q, want %q", decoded.RunID, original.RunID)
	}
	if decoded.StepID != original.StepID {
		t.Fatalf("StepID = %q, want %q", decoded.StepID, original.StepID)
	}
	if !decoded.Timestamp.Equal(original.Timestamp) {
		t.Fatalf("Timestamp = %v, want %v", decoded.Timestamp, original.Timestamp)
	}
}

func TestEventTypeConstants(t *testing.T) {
	types := []EventType{
		EventWorkflowStarted,
		EventStepQueued,
		EventStepStarted,
		EventStepCompleted,
		EventStepFailed,
		EventStepContinue,
		EventAgentLoopIteration,
		EventWorkflowCompleted,
		EventWorkflowFailed,
	}

	seen := make(map[EventType]bool, len(types))
	for _, et := range types {
		if et == "" {
			t.Fatal("EventType must not be empty")
		}
		if seen[et] {
			t.Fatalf("duplicate EventType: %q", et)
		}
		seen[et] = true
	}
}

func TestNewStepCompletedEvent(t *testing.T) {
	evt := NewStepEvent(EventStepCompleted, "run-1", "step-a", []byte(`"output"`))

	if evt.Type != EventStepCompleted {
		t.Fatalf("Type = %q, want %q", evt.Type, EventStepCompleted)
	}
	if evt.RunID != "run-1" {
		t.Fatalf("RunID = %q, want %q", evt.RunID, "run-1")
	}
	if evt.StepID != "step-a" {
		t.Fatalf("StepID = %q, want %q", evt.StepID, "step-a")
	}
	if evt.Timestamp.IsZero() {
		t.Fatal("Timestamp must not be zero")
	}
	if evt.Payload == nil {
		t.Fatal("Payload must not be nil")
	}
}

func TestNATSSubjectForEvent(t *testing.T) {
	evt := Event{RunID: "run-abc", Type: EventStepCompleted}
	subject := evt.NATSSubject()

	expected := "history.run-abc"
	if subject != expected {
		t.Fatalf("NATSSubject() = %q, want %q", subject, expected)
	}

	// Negative: empty RunID should panic
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for empty RunID")
		}
	}()
	empty := Event{RunID: "", Type: EventStepCompleted}
	empty.NATSSubject()
}

func TestNATSMsgID(t *testing.T) {
	evt := Event{
		RunID:  "run-1",
		StepID: "step-a",
		Type:   EventStepCompleted,
	}
	msgID := evt.NATSMsgID()
	if msgID != "run-1.step-a.step.completed" {
		t.Fatalf("NATSMsgID() = %q, want %q", msgID, "run-1.step-a.step.completed")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/dmestas/projects/dagnats && go test ./engine/ -v`
Expected: FAIL — package not found.

- [ ] **Step 3: Implement event types**

```go
// engine/events.go
package engine

import (
	"encoding/json"
	"time"
)

// EventType identifies the kind of workflow event.
type EventType string

const (
	EventWorkflowStarted    EventType = "workflow.started"
	EventStepQueued         EventType = "step.queued"
	EventStepStarted        EventType = "step.started"
	EventStepCompleted      EventType = "step.completed"
	EventStepFailed         EventType = "step.failed"
	EventStepContinue       EventType = "step.continue"
	EventAgentLoopIteration EventType = "agent.loop.iteration"
	EventWorkflowCompleted  EventType = "workflow.completed"
	EventWorkflowFailed     EventType = "workflow.failed"
)

// Event is a single immutable entry in the workflow history stream.
type Event struct {
	Type      EventType       `json:"type"`
	RunID     string          `json:"run_id"`
	StepID    string          `json:"step_id,omitempty"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

// NewStepEvent creates an event for a specific step.
func NewStepEvent(
	eventType EventType,
	runID string,
	stepID string,
	payload []byte,
) Event {
	if runID == "" {
		panic("NewStepEvent: runID must not be empty")
	}
	if stepID == "" {
		panic("NewStepEvent: stepID must not be empty")
	}
	return Event{
		Type:      eventType,
		RunID:     runID,
		StepID:    stepID,
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}
}

// NewWorkflowEvent creates a workflow-level event (no step).
func NewWorkflowEvent(eventType EventType, runID string, payload []byte) Event {
	if runID == "" {
		panic("NewWorkflowEvent: runID must not be empty")
	}
	return Event{
		Type:      eventType,
		RunID:     runID,
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}
}

// NATSSubject returns the JetStream subject for this event.
func (e Event) NATSSubject() string {
	if e.RunID == "" {
		panic("Event.NATSSubject: RunID must not be empty")
	}
	return "history." + e.RunID
}

// NATSMsgID returns a dedup key for JetStream exactly-once delivery.
// Format: {run_id}.{step_id}.{event_type}
func (e Event) NATSMsgID() string {
	return e.RunID + "." + e.StepID + "." + string(e.Type)
}

// Marshal serializes the event to JSON.
func (e Event) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

// UnmarshalEvent deserializes an event from JSON.
func UnmarshalEvent(data []byte) (Event, error) {
	var evt Event
	err := json.Unmarshal(data, &evt)
	return evt, err
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats && go test ./engine/ -v`
Expected: PASS — all five tests.

- [ ] **Step 5: Commit**

```bash
git add engine/events.go engine/events_test.go
git commit -m "feat(engine): add event types, serialization, NATS subject/msgID helpers"
```

---

### Task 9: KV Snapshot Read/Write (`engine/snapshot.go`)

**Files:**
- Create: `engine/snapshot.go`
- Create: `engine/snapshot_test.go`

- [ ] **Step 1: Write failing tests for snapshot operations**

```go
// engine/snapshot_test.go

// Tests for KV snapshot operations: store and retrieve WorkflowRun state.
// Methodology: uses real embedded NATS server. Each test gets its own server.
// Tests write snapshots, read them back, verify field fidelity, and check
// missing key behavior.
package engine

import (
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/natsutil"
)

func TestSnapshotWriteAndRead(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	err = natsutil.SetupKVBuckets(js)
	if err != nil {
		t.Fatalf("SetupKVBuckets failed: %v", err)
	}

	store := NewSnapshotStore(js)

	run := dag.WorkflowRun{
		RunID:      "run-123",
		WorkflowID: "test-wf",
		Status:     dag.RunStatusRunning,
		Steps: map[string]dag.StepState{
			"step-a": {Status: dag.StepStatusCompleted, Attempts: 1, Output: []byte(`"ok"`)},
			"step-b": {Status: dag.StepStatusPending},
		},
		CreatedAt: time.Now().UTC().Truncate(time.Millisecond),
	}

	err = store.Save(run)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	got, err := store.Load(run.RunID)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if got.RunID != run.RunID {
		t.Fatalf("RunID = %q, want %q", got.RunID, run.RunID)
	}
	if got.Status != dag.RunStatusRunning {
		t.Fatalf("Status = %v, want Running", got.Status)
	}
	if len(got.Steps) != 2 {
		t.Fatalf("Steps count = %d, want 2", len(got.Steps))
	}
	if got.Steps["step-a"].Status != dag.StepStatusCompleted {
		t.Fatalf("step-a Status = %v, want Completed", got.Steps["step-a"].Status)
	}
}

func TestSnapshotLoadNotFound(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	err = natsutil.SetupKVBuckets(js)
	if err != nil {
		t.Fatalf("SetupKVBuckets failed: %v", err)
	}

	store := NewSnapshotStore(js)
	_, err = store.Load("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent run, got nil")
	}
	if err != ErrRunNotFound {
		t.Fatalf("expected ErrRunNotFound, got: %v", err)
	}
}

func TestSnapshotUpdate(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	err = natsutil.SetupKVBuckets(js)
	if err != nil {
		t.Fatalf("SetupKVBuckets failed: %v", err)
	}

	store := NewSnapshotStore(js)

	run := dag.WorkflowRun{
		RunID:      "run-456",
		WorkflowID: "test-wf",
		Status:     dag.RunStatusRunning,
		Steps:      map[string]dag.StepState{"a": {Status: dag.StepStatusPending}},
		CreatedAt:  time.Now().UTC().Truncate(time.Millisecond),
	}

	err = store.Save(run)
	if err != nil {
		t.Fatalf("first Save failed: %v", err)
	}

	// Update the run
	run.Steps["a"] = dag.StepState{Status: dag.StepStatusCompleted, Attempts: 1}
	run.Status = dag.RunStatusCompleted

	err = store.Save(run)
	if err != nil {
		t.Fatalf("second Save failed: %v", err)
	}

	got, err := store.Load("run-456")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if got.Status != dag.RunStatusCompleted {
		t.Fatalf("Status = %v, want Completed", got.Status)
	}
	if got.Steps["a"].Status != dag.StepStatusCompleted {
		t.Fatalf("step-a Status = %v, want Completed", got.Steps["a"].Status)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/dmestas/projects/dagnats && go test ./engine/ -run TestSnapshot -v -timeout 30s`
Expected: FAIL — `NewSnapshotStore` not defined.

- [ ] **Step 3: Implement snapshot store**

```go
// engine/snapshot.go
package engine

import (
	"encoding/json"
	"errors"

	"github.com/danmestas/dagnats/dag"
	"github.com/nats-io/nats.go"
)

// ErrRunNotFound is returned when a workflow run does not exist in KV.
var ErrRunNotFound = errors.New("workflow run not found")

// SnapshotStore reads and writes WorkflowRun snapshots to NATS KV.
type SnapshotStore struct {
	kv nats.KeyValue
}

// NewSnapshotStore creates a SnapshotStore backed by the workflow_runs KV bucket.
func NewSnapshotStore(js nats.JetStreamContext) *SnapshotStore {
	kv, err := js.KeyValue("workflow_runs")
	if err != nil {
		panic("NewSnapshotStore: workflow_runs bucket not found: " + err.Error())
	}
	return &SnapshotStore{kv: kv}
}

// Save writes or updates a WorkflowRun snapshot in KV.
func (s *SnapshotStore) Save(run dag.WorkflowRun) error {
	if run.RunID == "" {
		panic("SnapshotStore.Save: RunID must not be empty")
	}
	data, err := json.Marshal(run)
	if err != nil {
		return err
	}
	_, err = s.kv.Put("run."+run.RunID, data)
	return err
}

// Load retrieves a WorkflowRun snapshot from KV.
// Returns ErrRunNotFound if the run does not exist.
func (s *SnapshotStore) Load(runID string) (dag.WorkflowRun, error) {
	if runID == "" {
		panic("SnapshotStore.Load: runID must not be empty")
	}
	entry, err := s.kv.Get("run." + runID)
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			return dag.WorkflowRun{}, ErrRunNotFound
		}
		return dag.WorkflowRun{}, err
	}

	var run dag.WorkflowRun
	err = json.Unmarshal(entry.Value(), &run)
	return run, err
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats && go test ./engine/ -run TestSnapshot -v -timeout 30s`
Expected: PASS — all three snapshot tests.

- [ ] **Step 5: Commit**

```bash
git add engine/snapshot.go engine/snapshot_test.go
git commit -m "feat(engine): add KV snapshot store for workflow run state"
```

---

### Task 10: Orchestrator Core Loop (`engine/orchestrator.go`)

**Files:**
- Create: `engine/orchestrator.go`
- Create: `engine/orchestrator_test.go`

- [ ] **Step 1: Write failing tests for orchestrator event processing**

```go
// engine/orchestrator_test.go

// Tests for the orchestrator core loop: consuming history events, resolving
// ready steps, and publishing task messages. Uses real embedded NATS server.
// Methodology: publish events to history stream, let orchestrator process them,
// then verify tasks appear on the correct subjects and KV state is updated.
package engine

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
)

func TestOrchestratorStartsFirstStep(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name:    "test-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
			{ID: "b", Task: "task-b", DependsOn: []string{"a"}, Type: dag.StepTypeNormal},
		},
	}

	// Store workflow def
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("KeyValue(workflow_defs) failed: %v", err)
	}
	defData, err := json.Marshal(wfDef)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	_, err = defKV.Put(wfDef.Name, defData)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	orch := NewOrchestrator(nc, observe.NewNoopLogger(), observe.NewNoopMetrics())
	orch.Start()
	defer orch.Stop()

	// Publish workflow.started event
	evt := NewWorkflowEvent(EventWorkflowStarted, "run-1", defData)
	evtData, err := evt.Marshal()
	if err != nil {
		t.Fatalf("Marshal event failed: %v", err)
	}
	_, err = js.Publish(evt.NATSSubject(), evtData, nats.MsgId(evt.NATSMsgID()))
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Subscribe to task queue and wait for task-a to be enqueued
	sub, err := js.PullSubscribe("task.task-a.*", "", nats.BindStream("TASK_QUEUES"))
	if err != nil {
		t.Fatalf("PullSubscribe failed: %v", err)
	}

	msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch task failed (timeout?): %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 task message, got %d", len(msgs))
	}

	// Negative: task-b should NOT be enqueued yet
	subB, err := js.PullSubscribe("task.task-b.*", "", nats.BindStream("TASK_QUEUES"))
	if err != nil {
		t.Fatalf("PullSubscribe for task-b failed: %v", err)
	}
	msgsB, _ := subB.Fetch(1, nats.MaxWait(500*time.Millisecond))
	if len(msgsB) > 0 {
		t.Fatal("task-b should not be enqueued before task-a completes")
	}
}

func TestOrchestratorAdvancesAfterStepCompleted(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name:    "test-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
			{ID: "b", Task: "task-b", DependsOn: []string{"a"}, Type: dag.StepTypeNormal},
		},
	}

	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("KeyValue failed: %v", err)
	}
	defData, err := json.Marshal(wfDef)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	_, err = defKV.Put(wfDef.Name, defData)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	orch := NewOrchestrator(nc, observe.NewNoopLogger(), observe.NewNoopMetrics())
	orch.Start()
	defer orch.Stop()

	// Publish workflow.started
	startEvt := NewWorkflowEvent(EventWorkflowStarted, "run-2", defData)
	startData, _ := startEvt.Marshal()
	js.Publish(startEvt.NATSSubject(), startData, nats.MsgId(startEvt.NATSMsgID()))

	// Wait for task-a to be enqueued, then ack it
	subA, _ := js.PullSubscribe("task.task-a.*", "", nats.BindStream("TASK_QUEUES"))
	msgsA, err := subA.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch task-a failed: %v", err)
	}
	msgsA[0].Ack()

	// Publish step.completed for step-a
	compEvt := NewStepEvent(EventStepCompleted, "run-2", "a", []byte(`"done"`))
	compData, _ := compEvt.Marshal()
	js.Publish(compEvt.NATSSubject(), compData, nats.MsgId(compEvt.NATSMsgID()))

	// Now task-b should be enqueued
	subB, _ := js.PullSubscribe("task.task-b.*", "", nats.BindStream("TASK_QUEUES"))
	msgsB, err := subB.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch task-b failed (timeout?): %v", err)
	}
	if len(msgsB) != 1 {
		t.Fatalf("expected 1 task-b message, got %d", len(msgsB))
	}
}

func TestOrchestratorCompletesWorkflow(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name:    "single-step",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}

	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopLogger(), observe.NewNoopMetrics())
	orch.Start()
	defer orch.Stop()

	// Start workflow
	startEvt := NewWorkflowEvent(EventWorkflowStarted, "run-3", defData)
	startData, _ := startEvt.Marshal()
	js.Publish(startEvt.NATSSubject(), startData, nats.MsgId(startEvt.NATSMsgID()))

	// Complete step-a
	time.Sleep(200 * time.Millisecond) // let orchestrator process start
	compEvt := NewStepEvent(EventStepCompleted, "run-3", "a", []byte(`"done"`))
	compData, _ := compEvt.Marshal()
	js.Publish(compEvt.NATSSubject(), compData, nats.MsgId(compEvt.NATSMsgID()))

	// Verify workflow is completed in KV
	time.Sleep(500 * time.Millisecond) // let orchestrator process completion
	store := NewSnapshotStore(js)
	run, err := store.Load("run-3")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if run.Status != dag.RunStatusCompleted {
		t.Fatalf("Status = %v, want Completed", run.Status)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/dmestas/projects/dagnats && go test ./engine/ -run TestOrchestrator -v -timeout 30s`
Expected: FAIL — `NewOrchestrator` not defined.

- [ ] **Step 3: Implement orchestrator**

```go
// engine/orchestrator.go
package engine

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
)

// Orchestrator consumes workflow history events, resolves DAG dependencies,
// and enqueues ready tasks. It is stateless — all state lives in NATS.
type Orchestrator struct {
	nc      *nats.Conn
	js      nats.JetStreamContext
	store   *SnapshotStore
	defKV   nats.KeyValue
	logger  observe.Logger
	metrics observe.Metrics
	sub     *nats.Subscription
	stop    chan struct{}
	wg      sync.WaitGroup
}

// NewOrchestrator creates an orchestrator connected to NATS.
func NewOrchestrator(
	nc *nats.Conn,
	logger observe.Logger,
	metrics observe.Metrics,
) *Orchestrator {
	js, err := nc.JetStream()
	if err != nil {
		panic("NewOrchestrator: JetStream init failed: " + err.Error())
	}

	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		panic("NewOrchestrator: workflow_defs bucket not found: " + err.Error())
	}

	return &Orchestrator{
		nc:      nc,
		js:      js,
		store:   NewSnapshotStore(js),
		defKV:   defKV,
		logger:  logger,
		metrics: metrics,
		stop:    make(chan struct{}),
	}
}

// Start begins consuming history events.
func (o *Orchestrator) Start() {
	sub, err := o.js.Subscribe(
		"history.>",
		o.handleEvent,
		nats.DeliverNew(),
		nats.AckExplicit(),
	)
	if err != nil {
		panic("Orchestrator.Start: Subscribe failed: " + err.Error())
	}
	o.sub = sub
}

// Stop gracefully shuts down the orchestrator.
func (o *Orchestrator) Stop() {
	close(o.stop)
	if o.sub != nil {
		o.sub.Unsubscribe()
	}
	o.wg.Wait()
}

func (o *Orchestrator) handleEvent(msg *nats.Msg) {
	evt, err := UnmarshalEvent(msg.Data)
	if err != nil {
		o.logger.Error("failed to unmarshal event", err)
		msg.Ack()
		return
	}

	o.logger.Info("processing event",
		observe.String("type", string(evt.Type)),
		observe.String("run_id", evt.RunID),
		observe.String("step_id", evt.StepID),
	)

	switch evt.Type {
	case EventWorkflowStarted:
		o.handleWorkflowStarted(evt)
	case EventStepCompleted:
		o.handleStepCompleted(evt)
	case EventStepContinue:
		o.handleStepContinue(evt)
	case EventStepFailed:
		o.handleStepFailed(evt)
	}

	msg.Ack()
}

func (o *Orchestrator) handleWorkflowStarted(evt Event) {
	var wfDef dag.WorkflowDef
	err := json.Unmarshal(evt.Payload, &wfDef)
	if err != nil {
		o.logger.Error("failed to unmarshal workflow def from event", err)
		return
	}

	run := dag.WorkflowRun{
		RunID:      evt.RunID,
		WorkflowID: wfDef.Name,
		Status:     dag.RunStatusRunning,
		Steps:      make(map[string]dag.StepState, len(wfDef.Steps)),
		CreatedAt:  evt.Timestamp,
	}
	for _, step := range wfDef.Steps {
		run.Steps[step.ID] = dag.StepState{Status: dag.StepStatusPending}
	}

	o.store.Save(run)
	o.enqueueReady(wfDef, run)
}

func (o *Orchestrator) handleStepCompleted(evt Event) {
	run, wfDef, err := o.loadRunAndDef(evt.RunID)
	if err != nil {
		o.logger.Error("failed to load run/def", err,
			observe.String("run_id", evt.RunID))
		return
	}

	state := run.Steps[evt.StepID]
	state.Status = dag.StepStatusCompleted
	state.Output = evt.Payload
	state.Attempts++
	run.Steps[evt.StepID] = state

	completed := o.completedSet(run)
	if dag.IsComplete(wfDef, completed) {
		run.Status = dag.RunStatusCompleted
		o.store.Save(run)
		wfEvt := NewWorkflowEvent(EventWorkflowCompleted, evt.RunID, nil)
		data, _ := wfEvt.Marshal()
		o.js.Publish(wfEvt.NATSSubject(), data,
			nats.MsgId(wfEvt.NATSMsgID()))
		return
	}

	o.store.Save(run)
	o.enqueueReady(wfDef, run)
}

func (o *Orchestrator) handleStepContinue(evt Event) {
	run, _, err := o.loadRunAndDef(evt.RunID)
	if err != nil {
		o.logger.Error("failed to load run/def", err,
			observe.String("run_id", evt.RunID))
		return
	}

	state := run.Steps[evt.StepID]
	state.Attempts++
	run.Steps[evt.StepID] = state
	o.store.Save(run)

	// Re-enqueue the same step with new input
	subject := "task." + evt.StepID + "." + evt.RunID
	o.js.Publish(subject, evt.Payload)
}

func (o *Orchestrator) handleStepFailed(evt Event) {
	run, wfDef, err := o.loadRunAndDef(evt.RunID)
	if err != nil {
		o.logger.Error("failed to load run/def", err,
			observe.String("run_id", evt.RunID))
		return
	}

	state := run.Steps[evt.StepID]
	state.Attempts++
	state.Error = string(evt.Payload)

	// Find the step def for retry config
	var stepDef dag.StepDef
	for _, s := range wfDef.Steps {
		if s.ID == evt.StepID {
			stepDef = s
			break
		}
	}

	if state.Attempts >= stepDef.Retries+1 {
		state.Status = dag.StepStatusFailed
		run.Steps[evt.StepID] = state
		run.Status = dag.RunStatusFailed
		o.store.Save(run)

		wfEvt := NewWorkflowEvent(EventWorkflowFailed, evt.RunID, nil)
		data, _ := wfEvt.Marshal()
		o.js.Publish(wfEvt.NATSSubject(), data,
			nats.MsgId(wfEvt.NATSMsgID()))
		return
	}

	run.Steps[evt.StepID] = state
	o.store.Save(run)
	// JetStream NakWithDelay handles redelivery — orchestrator does not re-enqueue
}

func (o *Orchestrator) enqueueReady(wfDef dag.WorkflowDef, run dag.WorkflowRun) {
	completed := o.completedSet(run)
	queued := o.queuedSet(run)
	ready := dag.ResolveReady(wfDef, completed, queued)

	for _, step := range ready {
		subject := "task." + step.Task + "." + run.RunID

		// Build task payload with run context
		payload := TaskPayload{
			RunID:  run.RunID,
			StepID: step.ID,
			Input:  run.Steps[step.ID].Output, // previous step output or nil
		}

		// For first steps with no deps, use workflow start payload
		if len(step.DependsOn) == 0 && payload.Input == nil {
			// Input comes from workflow start event, already in run state
		}

		// Find input from completed dependencies
		if len(step.DependsOn) == 1 {
			depState := run.Steps[step.DependsOn[0]]
			payload.Input = depState.Output
		}

		data, _ := json.Marshal(payload)
		msgID := run.RunID + "." + step.ID + ".queued"
		o.js.Publish(subject, data, nats.MsgId(msgID))

		state := run.Steps[step.ID]
		state.Status = dag.StepStatusQueued
		run.Steps[step.ID] = state

		o.logger.Info("enqueued task",
			observe.String("run_id", run.RunID),
			observe.String("step_id", step.ID),
			observe.String("subject", subject),
		)
	}

	o.store.Save(run)
}

func (o *Orchestrator) loadRunAndDef(
	runID string,
) (dag.WorkflowRun, dag.WorkflowDef, error) {
	run, err := o.store.Load(runID)
	if err != nil {
		return dag.WorkflowRun{}, dag.WorkflowDef{}, err
	}

	entry, err := o.defKV.Get(run.WorkflowID)
	if err != nil {
		return dag.WorkflowRun{}, dag.WorkflowDef{}, err
	}

	var wfDef dag.WorkflowDef
	err = json.Unmarshal(entry.Value(), &wfDef)
	return run, wfDef, err
}

func (o *Orchestrator) completedSet(run dag.WorkflowRun) map[string]bool {
	set := make(map[string]bool, len(run.Steps))
	for id, state := range run.Steps {
		if state.Status == dag.StepStatusCompleted {
			set[id] = true
		}
	}
	return set
}

func (o *Orchestrator) queuedSet(run dag.WorkflowRun) map[string]bool {
	set := make(map[string]bool, len(run.Steps))
	for id, state := range run.Steps {
		if state.Status == dag.StepStatusQueued ||
			state.Status == dag.StepStatusRunning {
			set[id] = true
		}
	}
	return set
}

// TaskPayload is the message sent to workers via TASK_QUEUES.
type TaskPayload struct {
	RunID  string          `json:"run_id"`
	StepID string          `json:"step_id"`
	Input  json.RawMessage `json:"input,omitempty"`
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats && go test ./engine/ -run TestOrchestrator -v -timeout 30s`
Expected: PASS — all three orchestrator tests.

- [ ] **Step 5: Commit**

```bash
git add engine/orchestrator.go engine/orchestrator_test.go
git commit -m "feat(engine): add orchestrator core loop — event consumption, DAG resolution, task enqueue"
```

---

### Task 11: Worker Framework (`worker/`)

**Files:**
- Create: `worker/context.go`
- Create: `worker/worker.go`
- Create: `worker/context_test.go`
- Create: `worker/worker_test.go`

- [ ] **Step 1: Write failing tests for TaskContext**

```go
// worker/context_test.go

// Tests for TaskContext: the deep interface workers use to report results.
// Methodology: create a TaskContext with a real NATS connection, call Complete/Fail/Continue,
// and verify the correct events appear on the history stream.
package worker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/nats-io/nats.go"
)

func TestTaskContextComplete(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	// Subscribe to history to verify events
	sub, err := js.SubscribeSync("history.run-1", nats.DeliverAll())
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	ctx := newTaskContext(js, "run-1", "step-a", []byte(`"input"`))

	err = ctx.Complete([]byte(`"output"`))
	if err != nil {
		t.Fatalf("Complete failed: %v", err)
	}

	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg timeout: %v", err)
	}

	var evt engine.Event
	err = json.Unmarshal(msg.Data, &evt)
	if err != nil {
		t.Fatalf("Unmarshal event failed: %v", err)
	}

	if evt.Type != engine.EventStepCompleted {
		t.Fatalf("event type = %q, want %q", evt.Type, engine.EventStepCompleted)
	}
	if evt.RunID != "run-1" {
		t.Fatalf("RunID = %q, want %q", evt.RunID, "run-1")
	}
	if evt.StepID != "step-a" {
		t.Fatalf("StepID = %q, want %q", evt.StepID, "step-a")
	}
}

func TestTaskContextFail(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	sub, err := js.SubscribeSync("history.run-2", nats.DeliverAll())
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	ctx := newTaskContext(js, "run-2", "step-b", nil)

	err = ctx.Fail(fmt.Errorf("something broke"))
	if err != nil {
		t.Fatalf("Fail failed: %v", err)
	}

	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg timeout: %v", err)
	}

	var evt engine.Event
	json.Unmarshal(msg.Data, &evt)
	if evt.Type != engine.EventStepFailed {
		t.Fatalf("event type = %q, want %q", evt.Type, engine.EventStepFailed)
	}
}

func TestTaskContextContinue(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	sub, err := js.SubscribeSync("history.run-3", nats.DeliverAll())
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	ctx := newTaskContext(js, "run-3", "step-c", nil)

	err = ctx.Continue([]byte(`"next input"`))
	if err != nil {
		t.Fatalf("Continue failed: %v", err)
	}

	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg timeout: %v", err)
	}

	var evt engine.Event
	json.Unmarshal(msg.Data, &evt)
	if evt.Type != engine.EventStepContinue {
		t.Fatalf("event type = %q, want %q", evt.Type, engine.EventStepContinue)
	}
}

func TestTaskContextInput(t *testing.T) {
	js := nats.JetStreamContext(nil) // not needed for Input()
	ctx := newTaskContext(nil, "run-4", "step-d", []byte(`"hello"`))
	_ = js // silence unused

	got := ctx.Input()
	if string(got) != `"hello"` {
		t.Fatalf("Input() = %q, want %q", string(got), `"hello"`)
	}
	if ctx.RunID() != "run-4" {
		t.Fatalf("RunID() = %q, want %q", ctx.RunID(), "run-4")
	}
	if ctx.StepID() != "step-d" {
		t.Fatalf("StepID() = %q, want %q", ctx.StepID(), "step-d")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/dmestas/projects/dagnats && go test ./worker/ -v -timeout 30s`
Expected: FAIL — package not found.

- [ ] **Step 3: Implement TaskContext**

```go
// worker/context.go
package worker

import (
	"fmt"

	"github.com/danmestas/dagnats/engine"
	"github.com/nats-io/nats.go"
)

// taskContext implements the deep worker interface.
// Workers call Complete, Fail, or Continue — nothing else.
type taskContext struct {
	js     nats.JetStreamContext
	runID  string
	stepID string
	input  []byte
}

func newTaskContext(
	js nats.JetStreamContext,
	runID string,
	stepID string,
	input []byte,
) *taskContext {
	return &taskContext{
		js:     js,
		runID:  runID,
		stepID: stepID,
		input:  input,
	}
}

func (c *taskContext) Input() []byte  { return c.input }
func (c *taskContext) RunID() string  { return c.runID }
func (c *taskContext) StepID() string { return c.stepID }

func (c *taskContext) Complete(output []byte) error {
	return c.publishEvent(engine.EventStepCompleted, output)
}

func (c *taskContext) Fail(err error) error {
	payload := []byte(fmt.Sprintf("%q", err.Error()))
	return c.publishEvent(engine.EventStepFailed, payload)
}

func (c *taskContext) Continue(output []byte) error {
	return c.publishEvent(engine.EventStepContinue, output)
}

func (c *taskContext) publishEvent(eventType engine.EventType, payload []byte) error {
	evt := engine.NewStepEvent(eventType, c.runID, c.stepID, payload)
	data, err := evt.Marshal()
	if err != nil {
		return err
	}
	_, err = c.js.Publish(
		evt.NATSSubject(),
		data,
		nats.MsgId(evt.NATSMsgID()),
	)
	return err
}
```

- [ ] **Step 4: Run tests to verify they pass**

Note: `TestTaskContextFail` needs `fmt` imported in the test file. Add `"fmt"` to the imports in `context_test.go`.

Run: `cd /Users/dmestas/projects/dagnats && go test ./worker/ -run TestTaskContext -v -timeout 30s`
Expected: PASS — all four context tests.

- [ ] **Step 5: Write failing tests for Worker**

```go
// worker/worker_test.go

// Tests for the Worker: handler registration and task consumption.
// Methodology: start embedded NATS, register a handler, publish a task message,
// verify the handler executes and a completion event appears on history.
package worker

import (
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
)

func TestWorkerHandlesTask(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	var called atomic.Bool
	w := NewWorker(nc, observe.NewNoopLogger())
	w.Handle("echo", func(ctx TaskContext) error {
		called.Store(true)
		return ctx.Complete(ctx.Input())
	})
	w.Start()
	defer w.Stop()

	// Publish a task
	payload := engine.TaskPayload{
		RunID:  "run-1",
		StepID: "step-a",
		Input:  json.RawMessage(`"hello"`),
	}
	data, _ := json.Marshal(payload)
	_, err = js.Publish("task.echo.run-1", data)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Wait for handler to be called
	deadline := time.After(5 * time.Second)
	for !called.Load() {
		select {
		case <-deadline:
			t.Fatal("handler not called within 5s")
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Verify completion event
	sub, _ := js.SubscribeSync("history.run-1", nats.DeliverAll())
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg timeout: %v", err)
	}
	var evt engine.Event
	json.Unmarshal(msg.Data, &evt)
	if evt.Type != engine.EventStepCompleted {
		t.Fatalf("event type = %q, want %q", evt.Type, engine.EventStepCompleted)
	}
}
```

- [ ] **Step 6: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./worker/ -run TestWorker -v -timeout 30s`
Expected: FAIL — `NewWorker` not defined.

- [ ] **Step 7: Implement Worker**

```go
// worker/worker.go
package worker

import (
	"encoding/json"

	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
)

// TaskContext is the interface workers use to interact with the engine.
type TaskContext interface {
	Input() []byte
	RunID() string
	StepID() string
	Complete(output []byte) error
	Fail(err error) error
	Continue(output []byte) error
}

// HandlerFunc is the function signature for task handlers.
type HandlerFunc func(ctx TaskContext) error

// Worker pulls tasks from JetStream and executes registered handlers.
type Worker struct {
	nc       *nats.Conn
	js       nats.JetStreamContext
	logger   observe.Logger
	handlers map[string]HandlerFunc
	subs     []*nats.Subscription
}

// NewWorker creates a worker connected to NATS.
func NewWorker(nc *nats.Conn, logger observe.Logger) *Worker {
	js, err := nc.JetStream()
	if err != nil {
		panic("NewWorker: JetStream init failed: " + err.Error())
	}
	return &Worker{
		nc:       nc,
		js:       js,
		logger:   logger,
		handlers: make(map[string]HandlerFunc),
	}
}

// Handle registers a handler for a task type.
func (w *Worker) Handle(taskType string, handler HandlerFunc) {
	if taskType == "" {
		panic("Worker.Handle: taskType must not be empty")
	}
	if handler == nil {
		panic("Worker.Handle: handler must not be nil")
	}
	w.handlers[taskType] = handler
}

// Start begins pulling tasks for all registered handlers.
func (w *Worker) Start() {
	for taskType, handler := range w.handlers {
		subject := "task." + taskType + ".>"
		h := handler // capture for closure
		tt := taskType

		sub, err := w.js.Subscribe(subject, func(msg *nats.Msg) {
			w.handleMessage(tt, h, msg)
		}, nats.AckExplicit(), nats.DeliverAll())
		if err != nil {
			panic("Worker.Start: Subscribe failed for " + taskType + ": " + err.Error())
		}
		w.subs = append(w.subs, sub)
	}
}

// Stop unsubscribes from all task subjects.
func (w *Worker) Stop() {
	for _, sub := range w.subs {
		sub.Unsubscribe()
	}
}

func (w *Worker) handleMessage(
	taskType string,
	handler HandlerFunc,
	msg *nats.Msg,
) {
	var payload engine.TaskPayload
	err := json.Unmarshal(msg.Data, &payload)
	if err != nil {
		w.logger.Error("failed to unmarshal task payload", err,
			observe.String("task_type", taskType))
		msg.Ack()
		return
	}

	ctx := newTaskContext(w.js, payload.RunID, payload.StepID, payload.Input)

	w.logger.Info("executing task",
		observe.String("task_type", taskType),
		observe.String("run_id", payload.RunID),
		observe.String("step_id", payload.StepID),
	)

	err = handler(ctx)
	if err != nil {
		w.logger.Error("task handler returned error", err,
			observe.String("task_type", taskType),
			observe.String("run_id", payload.RunID),
		)
		// Handler should have called Fail() — if not, do it now
		ctx.Fail(err)
	}

	msg.Ack()
}
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats && go test ./worker/ -v -timeout 30s`
Expected: PASS — all five worker tests.

- [ ] **Step 9: Commit**

```bash
git add worker/context.go worker/worker.go worker/context_test.go worker/worker_test.go
git commit -m "feat(worker): add worker framework — TaskContext, handler registration, task consumption"
```

---

### Task 12: Control Plane Service (`api/service.go`)

**Files:**
- Create: `api/service.go`
- Create: `api/service_test.go`

- [ ] **Step 1: Write failing tests for the service layer**

```go
// api/service_test.go

// Tests for the control plane service: register workflows, start runs, get status.
// Methodology: real embedded NATS. Verify KV state after each operation.
package api

import (
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
)

func TestServiceRegisterWorkflow(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	svc := NewService(nc, observe.NewNoopLogger())

	wfDef, err := dag.NewWorkflow("test-wf").
		Task("a", "task-a").
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	err = svc.RegisterWorkflow(wfDef)
	if err != nil {
		t.Fatalf("RegisterWorkflow failed: %v", err)
	}

	// Verify it's stored
	got, err := svc.GetWorkflow("test-wf")
	if err != nil {
		t.Fatalf("GetWorkflow failed: %v", err)
	}
	if got.Name != "test-wf" {
		t.Fatalf("Name = %q, want %q", got.Name, "test-wf")
	}
}

func TestServiceStartRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	svc := NewService(nc, observe.NewNoopLogger())

	wfDef, _ := dag.NewWorkflow("test-wf").
		Task("a", "task-a").
		Build()
	svc.RegisterWorkflow(wfDef)

	runID, err := svc.StartRun("test-wf", []byte(`"input"`))
	if err != nil {
		t.Fatalf("StartRun failed: %v", err)
	}
	if runID == "" {
		t.Fatal("runID must not be empty")
	}
}

func TestServiceGetRunStatus(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	svc := NewService(nc, observe.NewNoopLogger())

	wfDef, _ := dag.NewWorkflow("test-wf").
		Task("a", "task-a").
		Build()
	svc.RegisterWorkflow(wfDef)

	runID, _ := svc.StartRun("test-wf", nil)

	run, err := svc.GetRun(runID)
	if err != nil {
		t.Fatalf("GetRun failed: %v", err)
	}
	if run.RunID != runID {
		t.Fatalf("RunID = %q, want %q", run.RunID, runID)
	}
	// Run was just created — status should be pending or running
	if run.Status != dag.RunStatusPending && run.Status != dag.RunStatusRunning {
		t.Fatalf("Status = %v, want Pending or Running", run.Status)
	}
}

func TestServiceGetRunNotFound(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	svc := NewService(nc, observe.NewNoopLogger())
	_, err = svc.GetRun("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent run")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/dmestas/projects/dagnats && go test ./api/ -v -timeout 30s`
Expected: FAIL — package not found.

- [ ] **Step 3: Implement service**

```go
// api/service.go
package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
)

// Service implements the core control plane logic, shared by REST and NATS APIs.
type Service struct {
	nc     *nats.Conn
	js     nats.JetStreamContext
	defKV  nats.KeyValue
	store  *engine.SnapshotStore
	logger observe.Logger
}

// NewService creates a control plane service.
func NewService(nc *nats.Conn, logger observe.Logger) *Service {
	js, err := nc.JetStream()
	if err != nil {
		panic("NewService: JetStream init failed: " + err.Error())
	}
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		panic("NewService: workflow_defs bucket not found: " + err.Error())
	}
	return &Service{
		nc:     nc,
		js:     js,
		defKV:  defKV,
		store:  engine.NewSnapshotStore(js),
		logger: logger,
	}
}

// RegisterWorkflow stores a workflow definition in KV.
func (s *Service) RegisterWorkflow(def dag.WorkflowDef) error {
	if err := dag.Validate(def); err != nil {
		return fmt.Errorf("invalid workflow: %w", err)
	}
	data, err := json.Marshal(def)
	if err != nil {
		return err
	}
	_, err = s.defKV.Put(def.Name, data)
	return err
}

// GetWorkflow retrieves a workflow definition by name.
func (s *Service) GetWorkflow(name string) (dag.WorkflowDef, error) {
	entry, err := s.defKV.Get(name)
	if err != nil {
		return dag.WorkflowDef{}, err
	}
	var def dag.WorkflowDef
	err = json.Unmarshal(entry.Value(), &def)
	return def, err
}

// StartRun creates a new workflow run and publishes the start event.
func (s *Service) StartRun(workflowName string, input []byte) (string, error) {
	entry, err := s.defKV.Get(workflowName)
	if err != nil {
		return "", fmt.Errorf("workflow %q not found: %w", workflowName, err)
	}

	runID := generateRunID()

	// Publish workflow.started with the full def as payload
	evt := engine.NewWorkflowEvent(engine.EventWorkflowStarted, runID, entry.Value())
	data, err := evt.Marshal()
	if err != nil {
		return "", err
	}
	_, err = s.js.Publish(evt.NATSSubject(), data, nats.MsgId(evt.NATSMsgID()))
	if err != nil {
		return "", err
	}

	// Save initial snapshot so GetRun works immediately
	var def dag.WorkflowDef
	json.Unmarshal(entry.Value(), &def)

	run := dag.WorkflowRun{
		RunID:      runID,
		WorkflowID: workflowName,
		Status:     dag.RunStatusPending,
		Steps:      make(map[string]dag.StepState, len(def.Steps)),
		CreatedAt:  evt.Timestamp,
	}
	for _, step := range def.Steps {
		run.Steps[step.ID] = dag.StepState{Status: dag.StepStatusPending}
	}
	s.store.Save(run)

	s.logger.Info("started run",
		observe.String("run_id", runID),
		observe.String("workflow", workflowName),
	)
	return runID, nil
}

// GetRun retrieves the current state of a workflow run.
func (s *Service) GetRun(runID string) (dag.WorkflowRun, error) {
	return s.store.Load(runID)
}

func generateRunID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		panic("generateRunID: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats && go test ./api/ -v -timeout 30s`
Expected: PASS — all four service tests.

- [ ] **Step 5: Commit**

```bash
git add api/service.go api/service_test.go
git commit -m "feat(api): add control plane service — register, start, get workflows and runs"
```

---

### Task 13: REST API (`api/rest.go`)

**Files:**
- Create: `api/rest.go`
- Create: `api/rest_test.go`

- [ ] **Step 1: Write failing tests for REST handlers**

```go
// api/rest_test.go

// Tests for REST API endpoints using net/http/httptest.
// Methodology: create a test service with real NATS, make HTTP requests via
// httptest.Server, verify response codes and JSON bodies.
package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
)

func TestRESTRegisterWorkflow(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc, observe.NewNoopLogger())
	handler := NewRESTHandler(svc)
	server := httptest.NewServer(handler)
	defer server.Close()

	wfDef, _ := dag.NewWorkflow("rest-test").
		Task("a", "task-a").
		Build()
	body, _ := json.Marshal(wfDef)

	resp, err := http.Post(server.URL+"/workflows", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
}

func TestRESTStartRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc, observe.NewNoopLogger())
	handler := NewRESTHandler(svc)
	server := httptest.NewServer(handler)
	defer server.Close()

	// Register workflow first
	wfDef, _ := dag.NewWorkflow("rest-run").Task("a", "task-a").Build()
	svc.RegisterWorkflow(wfDef)

	body := []byte(`{"workflow": "rest-run", "input": "test"}`)
	resp, err := http.Post(server.URL+"/runs", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}

	var result map[string]string
	json.NewDecoder(resp.Body).Decode(&result)
	if result["run_id"] == "" {
		t.Fatal("response missing run_id")
	}
}

func TestRESTGetRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc, observe.NewNoopLogger())
	handler := NewRESTHandler(svc)
	server := httptest.NewServer(handler)
	defer server.Close()

	wfDef, _ := dag.NewWorkflow("rest-get").Task("a", "task-a").Build()
	svc.RegisterWorkflow(wfDef)
	runID, _ := svc.StartRun("rest-get", nil)

	resp, err := http.Get(server.URL + "/runs/" + runID)
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var run dag.WorkflowRun
	json.NewDecoder(resp.Body).Decode(&run)
	if run.RunID != runID {
		t.Fatalf("RunID = %q, want %q", run.RunID, runID)
	}
}

func TestRESTGetRunNotFound(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc, observe.NewNoopLogger())
	handler := NewRESTHandler(svc)
	server := httptest.NewServer(handler)
	defer server.Close()

	resp, err := http.Get(server.URL + "/runs/nonexistent")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/dmestas/projects/dagnats && go test ./api/ -run TestREST -v -timeout 30s`
Expected: FAIL — `NewRESTHandler` not defined.

- [ ] **Step 3: Implement REST handler**

```go
// api/rest.go
package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/engine"
)

// NewRESTHandler returns an http.Handler for the DagNats REST API.
// Uses only stdlib — no router dependency.
func NewRESTHandler(svc *Service) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/workflows", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handleRegisterWorkflow(svc, w, r)
	})

	mux.HandleFunc("/runs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handleStartRun(svc, w, r)
	})

	mux.HandleFunc("/runs/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handleGetRun(svc, w, r)
	})

	return mux
}

func handleRegisterWorkflow(svc *Service, w http.ResponseWriter, r *http.Request) {
	var def dag.WorkflowDef
	err := json.NewDecoder(r.Body).Decode(&def)
	if err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	err = svc.RegisterWorkflow(def)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "registered", "name": def.Name})
}

type startRunRequest struct {
	Workflow string          `json:"workflow"`
	Input    json.RawMessage `json:"input,omitempty"`
}

func handleStartRun(svc *Service, w http.ResponseWriter, r *http.Request) {
	var req startRunRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	runID, err := svc.StartRun(req.Workflow, req.Input)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"run_id": runID})
}

func handleGetRun(svc *Service, w http.ResponseWriter, r *http.Request) {
	// Extract run ID from /runs/{id}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/runs/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "missing run ID", http.StatusBadRequest)
		return
	}
	runID := parts[0]

	run, err := svc.GetRun(runID)
	if err != nil {
		if errors.Is(err, engine.ErrRunNotFound) {
			http.Error(w, "run not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(run)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats && go test ./api/ -v -timeout 30s`
Expected: PASS — all eight api tests.

- [ ] **Step 5: Commit**

```bash
git add api/rest.go api/rest_test.go
git commit -m "feat(api): add REST API handlers — register workflows, start/get runs"
```

---

### Task 14: End-to-End Integration Test

**Files:**
- Create: `e2e_test.go` (root package)

- [ ] **Step 1: Write failing E2E test**

```go
// e2e_test.go

// End-to-end test: register a workflow, start a run, workers execute all steps,
// verify workflow completes with correct state in KV and event history.
// Methodology: real NATS server, real orchestrator, real workers. No mocks.
package dagnats_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/api"
	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/worker"
)

func TestE2ELinearWorkflow(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	// Start orchestrator
	orch := engine.NewOrchestrator(nc, observe.NewNoopLogger(), observe.NewNoopMetrics())
	orch.Start()
	defer orch.Stop()

	// Register workers
	w := worker.NewWorker(nc, observe.NewNoopLogger())
	w.Handle("task-a", func(ctx worker.TaskContext) error {
		return ctx.Complete([]byte(`"a-output"`))
	})
	w.Handle("task-b", func(ctx worker.TaskContext) error {
		return ctx.Complete([]byte(`"b-output"`))
	})
	w.Start()
	defer w.Stop()

	// Register workflow and start run via service
	svc := api.NewService(nc, observe.NewNoopLogger())
	wfDef, err := dag.NewWorkflow("e2e-linear").
		Task("a", "task-a").
		Task("b", "task-b").DependsOn("a").
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	svc.RegisterWorkflow(wfDef)

	runID, err := svc.StartRun("e2e-linear", nil)
	if err != nil {
		t.Fatalf("StartRun failed: %v", err)
	}

	// Poll for workflow completion (bounded timeout)
	deadline := time.After(10 * time.Second)
	for {
		run, err := svc.GetRun(runID)
		if err != nil {
			t.Fatalf("GetRun failed: %v", err)
		}
		if run.Status == dag.RunStatusCompleted {
			// Verify step states
			if run.Steps["a"].Status != dag.StepStatusCompleted {
				t.Fatalf("step-a status = %v, want Completed", run.Steps["a"].Status)
			}
			if run.Steps["b"].Status != dag.StepStatusCompleted {
				t.Fatalf("step-b status = %v, want Completed", run.Steps["b"].Status)
			}
			break
		}
		if run.Status == dag.RunStatusFailed {
			t.Fatalf("workflow failed unexpectedly")
		}
		select {
		case <-deadline:
			t.Fatalf("workflow did not complete within 10s, status: %v", run.Status)
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Paired assertion: also verify history stream has correct events
	js, _ := nc.JetStream()
	sub, _ := js.SubscribeSync("history."+runID, nats.DeliverAll())
	var eventTypes []string
	for {
		msg, err := sub.NextMsg(1 * time.Second)
		if err != nil {
			break
		}
		var evt engine.Event
		json.Unmarshal(msg.Data, &evt)
		eventTypes = append(eventTypes, string(evt.Type))
	}

	// Should contain at minimum: workflow.started, step.completed (x2), workflow.completed
	foundStart := false
	foundEnd := false
	completedCount := 0
	for _, et := range eventTypes {
		if et == "workflow.started" {
			foundStart = true
		}
		if et == "workflow.completed" {
			foundEnd = true
		}
		if et == "step.completed" {
			completedCount++
		}
	}
	if !foundStart {
		t.Fatal("history missing workflow.started event")
	}
	if !foundEnd {
		t.Fatal("history missing workflow.completed event")
	}
	if completedCount < 2 {
		t.Fatalf("expected at least 2 step.completed events, got %d", completedCount)
	}
}
```

Note: Add `nats "github.com/nats-io/nats.go"` to the imports.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test -run TestE2E -v -timeout 30s`
Expected: FAIL (or pass if all prior tasks are implemented correctly — this validates the integration).

- [ ] **Step 3: Fix any integration issues discovered**

If the E2E test fails, debug and fix the root cause in the relevant package. Common issues:
- Subject naming mismatches between orchestrator publish and worker subscribe
- Race conditions in event delivery timing
- KV snapshot consistency

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test -run TestE2E -v -timeout 30s`
Expected: PASS

- [ ] **Step 5: Run all tests**

Run: `cd /Users/dmestas/projects/dagnats && go test ./... -v -timeout 60s`
Expected: All tests PASS across all packages.

- [ ] **Step 6: Commit**

```bash
git add e2e_test.go
git commit -m "test: add end-to-end integration test for linear workflow lifecycle"
```

---

### Task 15: CLI Scaffolding (`cli/` + `cmd/`)

**Files:**
- Create: `cli/root.go`
- Create: `cli/workflow.go`
- Create: `cli/run.go`
- Create: `cmd/dagnats/main.go`
- Create: `cmd/dagnats-engine/main.go`
- Create: `cmd/dagnats-api/main.go`

- [ ] **Step 1: Write failing test for CLI output formatting**

```go
// cli/run_test.go

// Tests for CLI output formatting: verify run status renders correctly.
// Methodology: unit test the formatting functions without HTTP calls.
package cli

import (
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
)

func TestFormatRunStatus(t *testing.T) {
	run := dag.WorkflowRun{
		RunID:      "abc123",
		WorkflowID: "test-wf",
		Status:     dag.RunStatusRunning,
		Steps: map[string]dag.StepState{
			"a": {Status: dag.StepStatusCompleted, Attempts: 1},
			"b": {Status: dag.StepStatusRunning, Attempts: 1},
		},
		CreatedAt: time.Date(2026, 3, 29, 12, 0, 0, 0, time.UTC),
	}

	output := FormatRunStatus(run)
	if !strings.Contains(output, "abc123") {
		t.Fatal("output should contain run ID")
	}
	if !strings.Contains(output, "running") {
		t.Fatal("output should contain status")
	}
	if !strings.Contains(output, "test-wf") {
		t.Fatal("output should contain workflow name")
	}

	// Negative: should not contain raw Go struct syntax
	if strings.Contains(output, "map[") {
		t.Fatal("output should not contain raw Go map syntax")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -v`
Expected: FAIL — package not found.

- [ ] **Step 3: Implement CLI packages**

```go
// cli/root.go
package cli

import (
	"fmt"
	"os"
)

// Run parses args and executes the appropriate subcommand.
func Run(args []string) {
	if len(args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch args[1] {
	case "workflow":
		runWorkflowCmd(args[2:])
	case "run":
		runRunCmd(args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage: dagnats <command> [args]")
	fmt.Fprintln(os.Stderr, "Commands:")
	fmt.Fprintln(os.Stderr, "  workflow  list, register workflows")
	fmt.Fprintln(os.Stderr, "  run       start, status, history, retry runs")
}
```

```go
// cli/workflow.go
package cli

import "fmt"

func runWorkflowCmd(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: dagnats workflow <list|register>")
		return
	}
	switch args[0] {
	case "list":
		fmt.Println("(workflow list not yet implemented)")
	case "register":
		fmt.Println("(workflow register not yet implemented)")
	default:
		fmt.Printf("unknown workflow subcommand: %s\n", args[0])
	}
}
```

```go
// cli/run.go
package cli

import (
	"fmt"
	"strings"

	"github.com/danmestas/dagnats/dag"
)

func runRunCmd(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: dagnats run <start|status|history|retry>")
		return
	}
	switch args[0] {
	case "start":
		fmt.Println("(run start not yet implemented)")
	case "status":
		fmt.Println("(run status not yet implemented)")
	case "history":
		fmt.Println("(run history not yet implemented)")
	case "retry":
		fmt.Println("(run retry not yet implemented)")
	default:
		fmt.Printf("unknown run subcommand: %s\n", args[0])
	}
}

// FormatRunStatus formats a WorkflowRun for terminal display.
func FormatRunStatus(run dag.WorkflowRun) string {
	var b strings.Builder

	fmt.Fprintf(&b, "Run:      %s\n", run.RunID)
	fmt.Fprintf(&b, "Workflow: %s\n", run.WorkflowID)
	fmt.Fprintf(&b, "Status:   %s\n", run.Status.String())
	fmt.Fprintf(&b, "Created:  %s\n", run.CreatedAt.Format("2006-01-02 15:04:05 UTC"))
	fmt.Fprintf(&b, "\nSteps:\n")

	for id, state := range run.Steps {
		fmt.Fprintf(&b, "  %-20s %s (attempts: %d)\n",
			id, state.Status.String(), state.Attempts)
	}

	return b.String()
}
```

```go
// cmd/dagnats/main.go
package main

import (
	"os"

	"github.com/danmestas/dagnats/cli"
)

func main() {
	cli.Run(os.Args)
}
```

```go
// cmd/dagnats-engine/main.go
package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
)

func main() {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, err := nats.Connect(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to NATS: %v\n", err)
		os.Exit(1)
	}
	defer nc.Close()

	err = natsutil.SetupAll(nc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to setup NATS resources: %v\n", err)
		os.Exit(1)
	}

	logger := observe.NewNoopLogger()
	metrics := observe.NewNoopMetrics()

	orch := engine.NewOrchestrator(nc, logger, metrics)
	orch.Start()
	fmt.Println("dagnats-engine started")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	fmt.Println("shutting down...")
	orch.Stop()
}
```

```go
// cmd/dagnats-api/main.go
package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/danmestas/dagnats/api"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
)

func main() {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, err := nats.Connect(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to NATS: %v\n", err)
		os.Exit(1)
	}
	defer nc.Close()

	err = natsutil.SetupAll(nc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to setup NATS resources: %v\n", err)
		os.Exit(1)
	}

	svc := api.NewService(nc, observe.NewNoopLogger())
	handler := api.NewRESTHandler(svc)

	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	fmt.Printf("dagnats-api listening on %s\n", addr)
	if err := http.ListenAndServe(addr, handler); err != nil {
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -v`
Expected: PASS

- [ ] **Step 5: Verify binaries compile**

Run: `cd /Users/dmestas/projects/dagnats && go build ./cmd/dagnats && go build ./cmd/dagnats-engine && go build ./cmd/dagnats-api`
Expected: No errors. Three binaries produced.

- [ ] **Step 6: Clean up binaries and commit**

```bash
rm -f dagnats dagnats-engine dagnats-api
git add cli/ cmd/
git commit -m "feat(cli): add CLI scaffolding and binary entry points"
```

---

### Task 16: NATS Request/Reply API (`api/natsapi.go`)

**Files:**
- Create: `api/natsapi.go`
- Create: `api/natsapi_test.go`

- [ ] **Step 1: Write failing tests for NATS request/reply API**

```go
// api/natsapi_test.go

// Tests for NATS request/reply control plane API.
// Methodology: real NATS, send request messages, verify reply payloads.
package api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
)

func TestNATSAPIRegisterAndStartRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc, observe.NewNoopLogger())
	natsAPI := NewNATSAPI(svc, nc)
	natsAPI.Start()
	defer natsAPI.Stop()

	// Register workflow via NATS request
	wfDef, _ := dag.NewWorkflow("nats-test").Task("a", "task-a").Build()
	reqData, _ := json.Marshal(wfDef)
	reply, err := nc.Request("api.workflows.register", reqData, 5*time.Second)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	var regResp map[string]string
	json.Unmarshal(reply.Data, &regResp)
	if regResp["status"] != "registered" {
		t.Fatalf("status = %q, want 'registered'", regResp["status"])
	}

	// Start run via NATS request
	startReq, _ := json.Marshal(startRunRequest{Workflow: "nats-test"})
	reply, err = nc.Request("api.runs.start", startReq, 5*time.Second)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	var startResp map[string]string
	json.Unmarshal(reply.Data, &startResp)
	if startResp["run_id"] == "" {
		t.Fatal("response missing run_id")
	}

	// Get run status via NATS request
	reply, err = nc.Request("api.runs.get", []byte(startResp["run_id"]), 5*time.Second)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	var run dag.WorkflowRun
	json.Unmarshal(reply.Data, &run)
	if run.RunID != startResp["run_id"] {
		t.Fatalf("RunID = %q, want %q", run.RunID, startResp["run_id"])
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./api/ -run TestNATSAPI -v -timeout 30s`
Expected: FAIL — `NewNATSAPI` not defined.

- [ ] **Step 3: Implement NATS request/reply API**

```go
// api/natsapi.go
package api

import (
	"encoding/json"

	"github.com/danmestas/dagnats/dag"
	"github.com/nats-io/nats.go"
)

// NATSAPI handles NATS request/reply for the control plane.
type NATSAPI struct {
	svc  *Service
	nc   *nats.Conn
	subs []*nats.Subscription
}

// NewNATSAPI creates a NATS request/reply API handler.
func NewNATSAPI(svc *Service, nc *nats.Conn) *NATSAPI {
	return &NATSAPI{svc: svc, nc: nc}
}

// Start subscribes to API subjects.
func (n *NATSAPI) Start() {
	handlers := map[string]nats.MsgHandler{
		"api.workflows.register": n.handleRegister,
		"api.runs.start":         n.handleStartRun,
		"api.runs.get":           n.handleGetRun,
	}
	for subject, handler := range handlers {
		sub, err := n.nc.Subscribe(subject, handler)
		if err != nil {
			panic("NATSAPI.Start: Subscribe failed for " + subject + ": " + err.Error())
		}
		n.subs = append(n.subs, sub)
	}
}

// Stop unsubscribes from all API subjects.
func (n *NATSAPI) Stop() {
	for _, sub := range n.subs {
		sub.Unsubscribe()
	}
}

func (n *NATSAPI) handleRegister(msg *nats.Msg) {
	var def dag.WorkflowDef
	if err := json.Unmarshal(msg.Data, &def); err != nil {
		n.reply(msg, map[string]string{"error": err.Error()})
		return
	}
	if err := n.svc.RegisterWorkflow(def); err != nil {
		n.reply(msg, map[string]string{"error": err.Error()})
		return
	}
	n.reply(msg, map[string]string{"status": "registered", "name": def.Name})
}

func (n *NATSAPI) handleStartRun(msg *nats.Msg) {
	var req startRunRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		n.reply(msg, map[string]string{"error": err.Error()})
		return
	}
	runID, err := n.svc.StartRun(req.Workflow, req.Input)
	if err != nil {
		n.reply(msg, map[string]string{"error": err.Error()})
		return
	}
	n.reply(msg, map[string]string{"run_id": runID})
}

func (n *NATSAPI) handleGetRun(msg *nats.Msg) {
	runID := string(msg.Data)
	run, err := n.svc.GetRun(runID)
	if err != nil {
		n.reply(msg, map[string]string{"error": err.Error()})
		return
	}
	data, _ := json.Marshal(run)
	msg.Respond(data)
}

func (n *NATSAPI) reply(msg *nats.Msg, payload interface{}) {
	data, _ := json.Marshal(payload)
	msg.Respond(data)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats && go test ./api/ -run TestNATSAPI -v -timeout 30s`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add api/natsapi.go api/natsapi_test.go
git commit -m "feat(api): add NATS request/reply API for internal service communication"
```

---

### Task 17: Agent Loop E2E Test

**Files:**
- Modify: `e2e_test.go`

- [ ] **Step 1: Write failing agent loop E2E test**

Add to `e2e_test.go`:

```go
func TestE2EAgentLoop(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	js, _ := nc.JetStream()

	orch := engine.NewOrchestrator(nc, observe.NewNoopLogger(), observe.NewNoopMetrics())
	orch.Start()
	defer orch.Stop()

	// Worker that loops 3 times then completes
	iteration := 0
	w := worker.NewWorker(nc, observe.NewNoopLogger())
	w.Handle("looper", func(ctx worker.TaskContext) error {
		iteration++
		if iteration < 3 {
			return ctx.Continue([]byte(fmt.Sprintf(`"iteration-%d"`, iteration)))
		}
		return ctx.Complete([]byte(`"done after 3"`))
	})
	w.Start()
	defer w.Stop()

	svc := api.NewService(nc, observe.NewNoopLogger())
	wfDef, _ := dag.NewWorkflow("e2e-loop").
		AgentLoop("loop", "looper").WithMaxIterations(10).
		Build()
	svc.RegisterWorkflow(wfDef)

	runID, _ := svc.StartRun("e2e-loop", nil)

	deadline := time.After(10 * time.Second)
	for {
		run, _ := svc.GetRun(runID)
		if run.Status == dag.RunStatusCompleted {
			break
		}
		if run.Status == dag.RunStatusFailed {
			t.Fatal("workflow failed unexpectedly")
		}
		select {
		case <-deadline:
			run, _ := svc.GetRun(runID)
			t.Fatalf("agent loop did not complete within 10s, status: %v", run.Status)
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Verify history contains continue events
	sub, _ := js.SubscribeSync("history."+runID, nats.DeliverAll())
	continueCount := 0
	for {
		msg, err := sub.NextMsg(1 * time.Second)
		if err != nil {
			break
		}
		var evt engine.Event
		json.Unmarshal(msg.Data, &evt)
		if evt.Type == engine.EventStepContinue {
			continueCount++
		}
	}
	if continueCount < 2 {
		t.Fatalf("expected at least 2 continue events, got %d", continueCount)
	}
}
```

Note: Add `"fmt"` to imports in `e2e_test.go`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test -run TestE2EAgentLoop -v -timeout 30s`
Expected: FAIL (likely because orchestrator's handleStepContinue needs the task type to re-enqueue correctly).

- [ ] **Step 3: Fix orchestrator's agent loop re-enqueue**

The `handleStepContinue` in `engine/orchestrator.go` currently publishes to `task.{step_id}.{run_id}` but it should publish to `task.{task_type}.{run_id}`. Fix this by looking up the step's task type from the workflow def.

Update `handleStepContinue` in `engine/orchestrator.go`:

```go
func (o *Orchestrator) handleStepContinue(evt Event) {
	run, wfDef, err := o.loadRunAndDef(evt.RunID)
	if err != nil {
		o.logger.Error("failed to load run/def", err,
			observe.String("run_id", evt.RunID))
		return
	}

	state := run.Steps[evt.StepID]
	state.Attempts++
	state.Status = dag.StepStatusQueued
	run.Steps[evt.StepID] = state
	o.store.Save(run)

	// Find the step def to get the task type
	var taskType string
	for _, s := range wfDef.Steps {
		if s.ID == evt.StepID {
			taskType = s.Task
			break
		}
	}
	if taskType == "" {
		o.logger.Error("step not found in workflow def", nil,
			observe.String("step_id", evt.StepID))
		return
	}

	subject := "task." + taskType + "." + evt.RunID
	payload := engine.TaskPayload{
		RunID:  evt.RunID,
		StepID: evt.StepID,
		Input:  evt.Payload,
	}
	data, _ := json.Marshal(payload)
	msgID := evt.RunID + "." + evt.StepID + ".continue." + fmt.Sprintf("%d", state.Attempts)
	o.js.Publish(subject, data, nats.MsgId(msgID))
}
```

Note: Add `"fmt"` to `engine/orchestrator.go` imports.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test -run TestE2EAgentLoop -v -timeout 30s`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add e2e_test.go engine/orchestrator.go
git commit -m "feat(engine): fix agent loop re-enqueue + add agent loop E2E test"
```

---

### Task 18: Final Validation

- [ ] **Step 1: Run full test suite**

Run: `cd /Users/dmestas/projects/dagnats && go test ./... -v -timeout 60s -count=1`
Expected: All tests pass across all packages.

- [ ] **Step 2: Run vet and check for issues**

Run: `cd /Users/dmestas/projects/dagnats && go vet ./...`
Expected: No issues.

- [ ] **Step 3: Verify clean git state**

Run: `cd /Users/dmestas/projects/dagnats && git status && git log --oneline`
Expected: Clean working tree with commits for each task.

- [ ] **Step 4: Push to GitHub**

```bash
git push origin main
```

---

## Deferred (not in this plan)

These spec features are intentionally deferred to a follow-up plan:
- **Child workflows** (`SpawnWorkflow`, `WaitForAll`, KV watch signaling) — requires the core engine to be stable first
- **NATS auth setup** (operator/account/user JWTs via `nsc`) — infrastructure concern, not code
- **Chaos/fault tests** — stretch goal after core is proven
- **Object Store** for large payloads — needs real usage patterns first
