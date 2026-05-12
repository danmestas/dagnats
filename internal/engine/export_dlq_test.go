// internal/engine/export_dlq_test.go
// Test-only seam: lets engine_test drive RecoveryManager.PublishDeadLetter
// without exporting the field on Orchestrator. Lives in package `engine`
// (not engine_test) so it can reach unexported state. The seam is the
// minimum surface needed for #202's regression test.
//
// Must not import dagnatstest — would create an import cycle because
// dagnatstest imports engine. The seam takes a *Orchestrator and the
// caller extracts it from the harness.
package engine

import (
	"context"

	"github.com/danmestas/dagnats/dag"
)

// PublishDeadLetterForTest drives RecoveryManager.PublishDeadLetter
// against the given Orchestrator's recovery manager. Test-only —
// production code goes through failWorkflow / failMapStep / failAuxStep
// call sites.
func PublishDeadLetterForTest(
	ctx context.Context,
	o *Orchestrator,
	task, runID, stepID string,
	attempts int,
) {
	if ctx == nil {
		panic("PublishDeadLetterForTest: ctx must not be nil")
	}
	if o == nil {
		panic("PublishDeadLetterForTest: o must not be nil")
	}
	if o.recovery == nil {
		panic("PublishDeadLetterForTest: recovery not wired")
	}
	if runID == "" {
		panic("PublishDeadLetterForTest: runID must not be empty")
	}
	if stepID == "" {
		panic("PublishDeadLetterForTest: stepID must not be empty")
	}
	stepDef := dag.StepDef{ID: stepID, Task: task}
	state := dag.StepState{
		Status:   dag.StepStatusFailed,
		Attempts: attempts,
		Error:    "synthetic test failure",
	}
	o.recovery.PublishDeadLetter(ctx, runID, stepDef, state)
}
