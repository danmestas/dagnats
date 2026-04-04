// dag/builder.go

// WorkflowBuilder provides a fluent DSL for constructing WorkflowDefs.
// Centralizing construction here lets callers express graph topology naturally
// without touching StepDef internals — the builder enforces invariants and
// delegates final structural validation to Validate.
package dag

import "time"

// WorkflowBuilder accumulates step definitions and wires them into a WorkflowDef
// on Build(). current tracks the most recently added step so that chained
// modifier calls (DependsOn, WithTimeout, etc.) always target the right step.
type WorkflowBuilder struct {
	name        string
	version     string
	steps       []StepDef
	current     int
	concurrency *ConcurrencyLimit
}

// NewWorkflow starts a new builder for a workflow with the given name.
// Version defaults to "1" — override via Version() if needed.
func NewWorkflow(name string) *WorkflowBuilder {
	return &WorkflowBuilder{name: name, version: "1", current: -1}
}

// Name returns the workflow name. Used by higher-level packages that need
// to derive task names from the workflow identity.
func (b *WorkflowBuilder) Name() string { return b.name }

// Version overrides the default workflow version string.
func (b *WorkflowBuilder) Version(v string) *WorkflowBuilder {
	b.version = v
	return b
}

// Task appends a normal (non-looping) step and returns a StepRef for
// compile-time-safe dependency wiring and modifier chaining.
func (b *WorkflowBuilder) Task(id, task string) StepRef {
	b.steps = append(b.steps, StepDef{
		ID: id, Task: task, Type: StepTypeNormal,
	})
	b.current = len(b.steps) - 1
	return StepRef{id: id, index: b.current, builder: b}
}

// AgentLoop appends an agent-loop step with an initialised (but unconfigured)
// AgentLoopConfig. Callers must configure bounds via WithMaxIterations /
// WithMaxDuration before Build() — Validate enforces MaxIterations > 0.
func (b *WorkflowBuilder) AgentLoop(id, task string) StepRef {
	b.steps = append(b.steps, StepDef{
		ID:     id,
		Task:   task,
		Type:   StepTypeAgentLoop,
		Config: MarshalConfig(&AgentLoopConfig{}),
	})
	b.current = len(b.steps) - 1
	return StepRef{id: id, index: b.current, builder: b}
}

// Agent appends a Claude Agent SDK step. Metadata carries role and other
// agent-specific config — the core DAG package is ignorant of what it means.
func (b *WorkflowBuilder) Agent(
	id, task string, metadata map[string]string,
) StepRef {
	if id == "" {
		panic("dag: step id must not be empty")
	}
	if task == "" {
		panic("dag: step task must not be empty")
	}
	b.steps = append(b.steps, StepDef{
		ID:       id,
		Task:     task,
		Type:     StepTypeAgent,
		Metadata: metadata,
	})
	b.current = len(b.steps) - 1
	return StepRef{id: id, index: b.current, builder: b}
}

// SubWorkflow appends a sub-workflow step that spawns a child workflow
// execution. The child workflow must be registered in the workflow_defs
// KV bucket. By default the parent step blocks until the child completes;
// use WithDetach() on the returned StepRef to fire-and-forget.
func (b *WorkflowBuilder) SubWorkflow(
	id, workflow string,
) StepRef {
	if id == "" {
		panic("SubWorkflow: id must not be empty")
	}
	if workflow == "" {
		panic("SubWorkflow: workflow must not be empty")
	}
	step := StepDef{
		ID:   id,
		Task: workflow,
		Type: StepTypeSubWorkflow,
		Config: MarshalConfig(&SubWorkflowConfig{
			Workflow: workflow,
		}),
	}
	b.steps = append(b.steps, step)
	idx := len(b.steps) - 1
	b.current = idx
	return StepRef{id: id, index: idx, builder: b}
}

// Map appends a map step that fans out over an array from its dependency.
// The step will execute taskType once per item in the input array, up to
// MapConfig.MaxItems. Returns a StepRef for chaining dependency wiring
// and calling WithMaxItems to override the default bound of 1000.
func (b *WorkflowBuilder) Map(id, taskType string) StepRef {
	if id == "" {
		panic("Map: id must not be empty")
	}
	if taskType == "" {
		panic("Map: taskType must not be empty")
	}
	step := StepDef{
		ID:     id,
		Task:   taskType,
		Type:   StepTypeMap,
		Config: MarshalConfig(&MapConfig{MaxItems: 1000}),
	}
	b.steps = append(b.steps, step)
	idx := len(b.steps) - 1
	b.current = idx
	return StepRef{id: id, index: idx, builder: b}
}

// Sleep adds a durable delay step to the workflow.
// No worker is involved — the engine handles the timer.
func (b *WorkflowBuilder) Sleep(id string, duration time.Duration) StepRef {
	if id == "" {
		panic("Sleep: id must not be empty")
	}
	if duration <= 0 {
		panic("Sleep: duration must be positive")
	}
	step := StepDef{
		ID:     id,
		Type:   StepTypeSleep,
		Config: MarshalConfig(&SleepConfig{Duration: duration}),
	}
	b.steps = append(b.steps, step)
	idx := len(b.steps) - 1
	b.current = idx
	return StepRef{id: id, index: idx, builder: b}
}

// WaitForEvent adds a step that waits for an external event to match a condition.
// No worker is involved — the engine handles event matching.
func (b *WorkflowBuilder) WaitForEvent(id string, opts WaitForEventOpts) StepRef {
	if id == "" {
		panic("WaitForEvent: id must not be empty")
	}
	if opts.Event == "" {
		panic("WaitForEvent: opts.Event must not be empty")
	}
	step := StepDef{
		ID:     id,
		Type:   StepTypeWaitForEvent,
		Config: MarshalConfig(&opts),
	}
	b.steps = append(b.steps, step)
	idx := len(b.steps) - 1
	b.current = idx
	return StepRef{id: id, index: idx, builder: b}
}

// Approval adds a human approval gate step to the workflow.
// No worker is involved — the engine manages the token and timeout.
func (b *WorkflowBuilder) Approval(
	id string, cfg ApprovalConfig,
) StepRef {
	if id == "" {
		panic("Approval: id must not be empty")
	}
	if cfg.Timeout <= 0 {
		panic("Approval: Timeout must be positive")
	}
	b.steps = append(b.steps, StepDef{
		ID:     id,
		Type:   StepTypeApproval,
		Config: MarshalConfig(&cfg),
	})
	idx := len(b.steps) - 1
	b.current = idx
	return StepRef{id: id, index: idx, builder: b}
}

// Planner appends a planner step that generates a DAG fragment at
// runtime. The worker outputs JSON steps; the engine validates,
// namespaces, and materializes them into the running workflow.
func (b *WorkflowBuilder) Planner(
	id, task string, cfg PlannerConfig,
) StepRef {
	if id == "" {
		panic("Planner: id must not be empty")
	}
	if task == "" {
		panic("Planner: task must not be empty")
	}
	if cfg.MaxSteps <= 0 {
		panic("Planner: MaxSteps must be positive")
	}
	b.steps = append(b.steps, StepDef{
		ID:     id,
		Task:   task,
		Type:   StepTypePlanner,
		Config: MarshalConfig(&cfg),
	})
	idx := len(b.steps) - 1
	b.current = idx
	return StepRef{id: id, index: idx, builder: b}
}

// WithConcurrency sets workflow-level concurrency limits. MaxRuns bounds
// how many runs of this workflow execute in parallel; MaxSteps bounds
// how many steps execute concurrently within a single run.
func (b *WorkflowBuilder) WithConcurrency(
	maxRuns, maxSteps int,
) *WorkflowBuilder {
	b.concurrency = &ConcurrencyLimit{
		MaxRuns:  maxRuns,
		MaxSteps: maxSteps,
	}
	return b
}

// DependsOn declares that the active step must not start until all listed step
// IDs have completed. Kept for backward compatibility — prefer After(StepRef)
// for new code which provides compile-time safety.
func (b *WorkflowBuilder) DependsOn(ids ...string) *WorkflowBuilder {
	if b.current < 0 {
		panic("DependsOn called before adding a step")
	}
	b.steps[b.current].DependsOn = append(
		b.steps[b.current].DependsOn, ids...,
	)
	return b
}

// WithTimeout sets the per-attempt timeout on the active step.
// Kept for backward compatibility — prefer StepRef.WithTimeout for new code.
func (b *WorkflowBuilder) WithTimeout(d time.Duration) *WorkflowBuilder {
	if b.current < 0 {
		panic("WithTimeout called before adding a step")
	}
	b.steps[b.current].Timeout = d
	return b
}

// WithMaxIterations configures the iteration bound on the active AgentLoop
// step. Kept for backward compatibility — prefer StepRef.WithMaxIterations.
func (b *WorkflowBuilder) WithMaxIterations(n int) *WorkflowBuilder {
	if b.current < 0 {
		panic("WithMaxIterations called before adding a step")
	}
	if b.steps[b.current].Type != StepTypeAgentLoop {
		panic("WithMaxIterations called on non-AgentLoop step")
	}
	cfg, err := ParseAgentLoopConfig(b.steps[b.current])
	if err != nil {
		panic("WithMaxIterations: " + err.Error())
	}
	cfg.MaxIterations = n
	b.steps[b.current].Config = MarshalConfig(&cfg)
	return b
}

// WithMaxDuration configures the wall-clock bound on the active AgentLoop
// step. Kept for backward compatibility — prefer StepRef.WithMaxDuration.
func (b *WorkflowBuilder) WithMaxDuration(d time.Duration) *WorkflowBuilder {
	if b.current < 0 {
		panic("WithMaxDuration called before adding a step")
	}
	if b.steps[b.current].Type != StepTypeAgentLoop {
		panic("WithMaxDuration called on non-AgentLoop step")
	}
	cfg, err := ParseAgentLoopConfig(b.steps[b.current])
	if err != nil {
		panic("WithMaxDuration: " + err.Error())
	}
	cfg.MaxDuration = d
	b.steps[b.current].Config = MarshalConfig(&cfg)
	return b
}

// Build assembles the WorkflowDef and delegates to Validate. Any structural
// error (cycle, missing dep, etc.) is surfaced here so callers get a clean
// error value rather than a panic at execution time.
func (b *WorkflowBuilder) Build() (WorkflowDef, error) {
	def := WorkflowDef{
		Name:        b.name,
		Version:     b.version,
		Steps:       b.steps,
		Concurrency: b.concurrency,
	}
	if err := Validate(def); err != nil {
		return WorkflowDef{}, err
	}
	def.AuxSteps = buildAuxSteps(def.Steps)
	return def, nil
}

// buildAuxSteps collects step IDs referenced by OnFailure or Compensate.
// These steps are auxiliary — they don't block workflow completion
// unless explicitly triggered.
func buildAuxSteps(steps []StepDef) map[string]bool {
	aux := make(map[string]bool)
	for _, step := range steps {
		if step.OnFailure != "" {
			aux[step.OnFailure] = true
		}
		if step.Compensate != "" {
			aux[step.Compensate] = true
		}
	}
	if len(aux) == 0 {
		return nil
	}
	return aux
}
