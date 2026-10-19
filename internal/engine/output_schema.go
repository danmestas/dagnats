package engine

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/danmestas/dagnats/dag"
)

// outputSchemaViolation checks the run's final output against its
// workflow's OutputSchema just before completion (#784). Returns the
// offending terminal step ID and the violation, or nil when the run
// may complete. A def that cannot be loaded (for example a pinned
// version deleted with `workflow delete --force`) is not a reason to
// wedge a finished run, so it completes unvalidated.
func (o *Orchestrator) outputSchemaViolation(
	ctx context.Context, run dag.WorkflowRun,
) (string, error) {
	if run.RunID == "" {
		panic("outputSchemaViolation: RunID must not be empty")
	}
	if run.WorkflowID == "" {
		panic("outputSchemaViolation: WorkflowID must not be empty")
	}
	wfDef, err := o.loadDef(ctx, run.WorkflowID)
	if err != nil {
		slog.DebugContext(ctx,
			"output schema: def unavailable, completing unvalidated",
			"run_id", run.RunID, "error", err)
		return "", nil
	}
	return dag.ValidateRunOutput(wfDef, run.Steps)
}

// failOnOutputSchema fails a run whose final output violates the
// OutputSchema. The offending terminal step is marked Failed with the
// violation as its error -- that is where a run's failure reason lives
// -- and the run goes through failWorkflow, so terminal bookkeeping
// (workflow.failed, admission release, parent notification) matches
// every other failure.
func (o *Orchestrator) failOnOutputSchema(
	ctx context.Context,
	run dag.WorkflowRun,
	stepID string,
	violation error,
) error {
	if stepID == "" {
		panic("failOnOutputSchema: stepID must not be empty")
	}
	if violation == nil {
		panic("failOnOutputSchema: violation must not be nil")
	}
	state := run.Steps[stepID]
	state.Status = dag.StepStatusFailed
	state.Error = fmt.Sprintf(
		"output schema violation in step %q: %v", stepID, violation,
	)
	run.Steps[stepID] = state
	return o.failWorkflow(ctx, run, dag.StepDef{ID: stepID}, state)
}
