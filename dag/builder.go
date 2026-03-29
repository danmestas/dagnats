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
	name    string
	version string
	steps   []StepDef
	current int
}

// NewWorkflow starts a new builder for a workflow with the given name.
// Version defaults to "1" — override via Version() if needed.
func NewWorkflow(name string) *WorkflowBuilder {
	return &WorkflowBuilder{name: name, version: "1", current: -1}
}

// Version overrides the default workflow version string.
func (b *WorkflowBuilder) Version(v string) *WorkflowBuilder {
	b.version = v
	return b
}

// Task appends a normal (non-looping) step and makes it the active step for
// subsequent modifier calls.
func (b *WorkflowBuilder) Task(id, task string) *WorkflowBuilder {
	b.steps = append(b.steps, StepDef{ID: id, Task: task, Type: StepTypeNormal})
	b.current = len(b.steps) - 1
	return b
}

// AgentLoop appends an agent-loop step with an initialised (but unconfigured)
// AgentLoopConfig. Callers must configure bounds via WithMaxIterations /
// WithMaxDuration before Build() — Validate enforces MaxIterations > 0.
func (b *WorkflowBuilder) AgentLoop(id, task string) *WorkflowBuilder {
	b.steps = append(b.steps, StepDef{
		ID:   id,
		Task: task,
		Type: StepTypeAgentLoop,
		Loop: &AgentLoopConfig{},
	})
	b.current = len(b.steps) - 1
	return b
}

// DependsOn declares that the active step must not start until all listed step
// IDs have completed. Panics if called before any step is added — this is a
// programmer error, not a runtime condition.
func (b *WorkflowBuilder) DependsOn(ids ...string) *WorkflowBuilder {
	if b.current < 0 {
		panic("DependsOn called before adding a step")
	}
	b.steps[b.current].DependsOn = append(b.steps[b.current].DependsOn, ids...)
	return b
}

// WithTimeout sets the per-attempt timeout on the active step.
func (b *WorkflowBuilder) WithTimeout(d time.Duration) *WorkflowBuilder {
	if b.current < 0 {
		panic("WithTimeout called before adding a step")
	}
	b.steps[b.current].Timeout = d
	return b
}

// WithMaxIterations configures the iteration bound on the active AgentLoop step.
// Panics if the active step is not an AgentLoop — calling this on a Task step
// indicates a logic error in the caller.
func (b *WorkflowBuilder) WithMaxIterations(n int) *WorkflowBuilder {
	if b.current < 0 {
		panic("WithMaxIterations called before adding a step")
	}
	if b.steps[b.current].Loop == nil {
		panic("WithMaxIterations called on non-AgentLoop step")
	}
	b.steps[b.current].Loop.MaxIterations = n
	return b
}

// WithMaxDuration configures the wall-clock bound on the active AgentLoop step.
func (b *WorkflowBuilder) WithMaxDuration(d time.Duration) *WorkflowBuilder {
	if b.current < 0 {
		panic("WithMaxDuration called before adding a step")
	}
	if b.steps[b.current].Loop == nil {
		panic("WithMaxDuration called on non-AgentLoop step")
	}
	b.steps[b.current].Loop.MaxDuration = d
	return b
}

// Build assembles the WorkflowDef and delegates to Validate. Any structural
// error (cycle, missing dep, etc.) is surfaced here so callers get a clean
// error value rather than a panic at execution time.
func (b *WorkflowBuilder) Build() (WorkflowDef, error) {
	def := WorkflowDef{Name: b.name, Version: b.version, Steps: b.steps}
	if err := Validate(def); err != nil {
		return WorkflowDef{}, err
	}
	return def, nil
}
