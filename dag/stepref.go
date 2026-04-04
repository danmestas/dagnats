package dag

import "time"

// StepRef is a compile-time-safe handle to a step within a WorkflowBuilder.
// Returned by Task() and AgentLoop(), it replaces string-based DependsOn
// with typed references that cannot silently miswire dependencies.
// The zero value is unusable — only the builder constructs valid StepRefs.
type StepRef struct {
	id      string
	index   int
	builder *WorkflowBuilder
}

// ID returns the step's string identifier. Useful for bridge code that still
// needs the raw ID (e.g. serialization boundaries).
func (r StepRef) ID() string { return r.id }

// After declares that this step depends on the given steps. Compile-time safe:
// passing a StepRef from a different builder panics immediately rather than
// producing a corrupt WorkflowDef discovered at validation time.
func (r StepRef) After(refs ...StepRef) StepRef {
	if r.builder == nil {
		panic("After called on zero-value StepRef")
	}
	for _, ref := range refs {
		if ref.builder != r.builder {
			panic("After: StepRef belongs to a different WorkflowBuilder")
		}
		r.builder.steps[r.index].DependsOn = append(
			r.builder.steps[r.index].DependsOn, ref.id,
		)
	}
	return r
}

// WithTimeout sets the per-attempt timeout on this step.
func (r StepRef) WithTimeout(d time.Duration) StepRef {
	if r.builder == nil {
		panic("WithTimeout called on zero-value StepRef")
	}
	r.builder.steps[r.index].Timeout = d
	return r
}

// SkipIf sets a condition that, when true, causes this step to be skipped
// instead of executed. The condition's StepID must be in DependsOn (enforced
// by Validate). Skipped steps are treated as "satisfied" for downstream deps.
func (r StepRef) SkipIf(cond *ParentCond) StepRef {
	if r.builder == nil {
		panic("SkipIf called on zero-value StepRef")
	}
	r.builder.steps[r.index].SkipIf = cond
	return r
}

// WithRetries sets the maximum number of retry attempts for this step.
// Zero means no retries — the step fails permanently on first error.
func (r StepRef) WithRetries(n int) StepRef {
	if r.builder == nil {
		panic("WithRetries called on zero-value StepRef")
	}
	r.builder.steps[r.index].Retries = n
	return r
}

// WithMaxIterations configures the iteration bound on an AgentLoop step.
// Panics if the step is not an AgentLoop — calling this on a Task step
// indicates a logic error in the caller.
func (r StepRef) WithMaxIterations(n int) StepRef {
	if r.builder == nil {
		panic("WithMaxIterations called on zero-value StepRef")
	}
	if r.builder.steps[r.index].Loop == nil {
		panic("WithMaxIterations called on non-AgentLoop step")
	}
	r.builder.steps[r.index].Loop.MaxIterations = n
	return r
}

// WithLoopDelay configures the delay between agent loop iterations.
// The orchestrator waits this duration before re-enqueuing the step.
// Useful for rate-limited APIs where you need spacing between calls.
func (r StepRef) WithLoopDelay(d time.Duration) StepRef {
	if r.builder == nil {
		panic("WithLoopDelay called on zero-value StepRef")
	}
	if r.builder.steps[r.index].Loop == nil {
		panic("WithLoopDelay called on non-AgentLoop step")
	}
	r.builder.steps[r.index].Loop.LoopDelay = d
	return r
}

// WithMaxDuration configures the wall-clock bound on an AgentLoop step.
func (r StepRef) WithMaxDuration(d time.Duration) StepRef {
	if r.builder == nil {
		panic("WithMaxDuration called on zero-value StepRef")
	}
	if r.builder.steps[r.index].Loop == nil {
		panic("WithMaxDuration called on non-AgentLoop step")
	}
	r.builder.steps[r.index].Loop.MaxDuration = d
	return r
}

// WithMaxItems configures the maximum number of items to process for a Map
// step. Calling this on a non-Map step or with n <= 0 panics — these are
// programmer errors that should be caught immediately.
func (r StepRef) WithMaxItems(n int) StepRef {
	if r.builder == nil {
		panic("WithMaxItems called on zero-value StepRef")
	}
	if r.builder.steps[r.index].Map == nil {
		panic("WithMaxItems called on non-Map step")
	}
	if n <= 0 {
		panic("WithMaxItems: n must be positive")
	}
	r.builder.steps[r.index].Map.MaxItems = n
	return r
}
