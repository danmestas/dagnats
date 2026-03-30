// dag/builder.go

// WorkflowBuilder provides a fluent DSL for constructing WorkflowDefs.
// Centralizing construction here lets callers express graph topology
// naturally without touching StepDef internals — the builder enforces
// invariants and delegates structural validation to Validate.
package dag

// WorkflowBuilder accumulates step definitions and wires them into a
// WorkflowDef on Build().
type WorkflowBuilder struct {
	name    string
	version string
	steps   []StepDef
}

// NewWorkflow starts a new builder for a workflow with the given name.
// Version defaults to "1" — override via Version() if needed.
func NewWorkflow(name string) *WorkflowBuilder {
	return &WorkflowBuilder{name: name, version: "1"}
}

// Name returns the workflow name.
func (b *WorkflowBuilder) Name() string { return b.name }

// Version overrides the default workflow version string.
func (b *WorkflowBuilder) Version(v string) *WorkflowBuilder {
	b.version = v
	return b
}

// Task appends a normal step and returns a StepRef for compile-time
// safe dependency wiring and modifier chaining.
func (b *WorkflowBuilder) Task(id, task string) StepRef {
	b.steps = append(b.steps, StepDef{
		ID: id, Task: task, Type: StepTypeNormal,
	})
	return StepRef{
		id: id, index: len(b.steps) - 1, builder: b,
	}
}

// AgentLoop appends an agent-loop step with an initialised (but
// unconfigured) AgentLoopConfig. Configure bounds via
// WithMaxIterations / WithMaxDuration before Build().
func (b *WorkflowBuilder) AgentLoop(id, task string) StepRef {
	b.steps = append(b.steps, StepDef{
		ID:   id,
		Task: task,
		Type: StepTypeAgentLoop,
		Loop: &AgentLoopConfig{},
	})
	return StepRef{
		id: id, index: len(b.steps) - 1, builder: b,
	}
}

// Build assembles the WorkflowDef and delegates to Validate.
func (b *WorkflowBuilder) Build() (WorkflowDef, error) {
	def := WorkflowDef{
		Name: b.name, Version: b.version, Steps: b.steps,
	}
	if err := Validate(def); err != nil {
		return WorkflowDef{}, err
	}
	return def, nil
}
