// internal/engine/run_mutator.go
// runMutator is the single port step-kind handlers use to mutate
// run lifecycle state: load, persist, complete, fail, and enqueue
// downstream steps. It replaces the multi-callback parameter lists
// that previously coupled subsystems (e.g. ApprovalGate) to four
// distinct Orchestrator methods per call.
//
// *Orchestrator implements this interface directly via its existing
// private methods — the interface is package-private so no new
// public API is added and no adapter type is needed. Handlers depend
// on runMutator + their own subsystem, never on the whole
// Orchestrator.
package engine

import (
	"context"

	"github.com/danmestas/dagnats/dag"
)

// runMutator encapsulates the run-lifecycle mutations a step-kind
// handler performs. Method signatures match the existing
// Orchestrator methods so *Orchestrator satisfies it with no
// wrapper code. Pure DAG advance (advance.go / advance_exec.go)
// intentionally stays out of this port — the mutator persists and
// routes, it does not compute the next frontier.
type runMutator interface {
	// loadRunAndDef loads the workflow definition and current run
	// snapshot for runID.
	loadRunAndDef(
		ctx context.Context, runID string,
	) (dag.WorkflowDef, dag.WorkflowRun, error)

	// saveSnapshot persists run; stepID labels the snapshot
	// duration metric (empty for workflow-scoped saves).
	saveSnapshot(
		ctx context.Context, run dag.WorkflowRun, stepID string,
	) error

	// completeWorkflow marks run terminal-completed and releases
	// its admission/sticky/concurrency resources.
	completeWorkflow(ctx context.Context, run dag.WorkflowRun) error

	// failWorkflow marks run terminal-failed. Signature matches the
	// existing method: the failing stepDef and its state drive
	// on-failure / compensate routing.
	failWorkflow(
		ctx context.Context,
		run dag.WorkflowRun,
		stepDef dag.StepDef,
		state dag.StepState,
	) error

	// enqueueReady resolves and dispatches the run's newly-ready
	// steps against wfDef.
	enqueueReady(
		ctx context.Context, wfDef dag.WorkflowDef, run dag.WorkflowRun,
	) error
}

// Compile-time assertion: *Orchestrator is the production
// runMutator. This is the whole point of the port — if any of the
// five methods drifts, the build breaks here rather than at a
// dispatch call site.
var _ runMutator = (*Orchestrator)(nil)
