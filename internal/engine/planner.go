// engine/planner.go
// Materializes planner step output into the running DAG. A planner
// worker returns a JSON fragment of steps; this code validates,
// namespaces, and appends them to the WorkflowRun's DynamicSteps.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

// maxTotalDynamicSteps is the absolute upper bound on accumulated
// dynamic steps across all planner materializations in one run.
// Prevents runaway planners from creating unbounded work.
const maxTotalDynamicSteps = 500

// plannerOutput is the expected JSON shape from a planner worker.
type plannerOutput struct {
	Steps []dag.StepDef `json:"steps"`
}

// materializePlannerOutput processes a completed planner step's
// output, validating and injecting generated steps into the run.
func (o *Orchestrator) materializePlannerOutput(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	stepDef dag.StepDef,
	payload []byte,
) error {
	if run.RunID == "" {
		panic("materializePlannerOutput: RunID must not be empty")
	}
	if stepDef.ID == "" {
		panic("materializePlannerOutput: stepDef.ID must not be empty")
	}

	cfg, err := dag.ParsePlannerConfig(stepDef)
	if err != nil {
		return o.failPlannerStep(
			ctx, &run, stepDef,
			fmt.Sprintf("parse planner config: %v", err),
		)
	}

	fragment, err := parsePlannerPayload(payload)
	if err != nil {
		return o.failPlannerStep(
			ctx, &run, stepDef,
			fmt.Sprintf("parse planner output: %v", err),
		)
	}

	// Empty plan means no dynamic steps — just proceed normally.
	if len(fragment) == 0 {
		return o.finishPlannerNoSteps(ctx, wfDef, run)
	}

	if err := checkDynamicBound(run, fragment); err != nil {
		return o.failPlannerStep(
			ctx, &run, stepDef, err.Error(),
		)
	}

	existingIDs := buildExistingIDSet(wfDef)
	namespaced := dag.NamespaceFragment(stepDef.ID, fragment)

	if err := dag.ValidateFragment(
		namespaced, cfg, existingIDs,
	); err != nil {
		return o.failPlannerStep(
			ctx, &run, stepDef,
			fmt.Sprintf("validate fragment: %v", err),
		)
	}

	wireEntrySteps(namespaced, stepDef.ID)
	return o.applyFragment(
		ctx, wfDef, &run, stepDef, namespaced,
	)
}

// parsePlannerPayload extracts step definitions from the planner
// worker's JSON output. Returns nil slice for empty/null payload.
func parsePlannerPayload(
	payload []byte,
) ([]dag.StepDef, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	var out plannerOutput
	if err := json.Unmarshal(payload, &out); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return out.Steps, nil
}

// checkDynamicBound ensures adding fragment won't exceed the
// absolute cap on dynamic steps per run.
func checkDynamicBound(
	run dag.WorkflowRun, fragment []dag.StepDef,
) error {
	if len(fragment) == 0 {
		panic("checkDynamicBound: fragment must not be empty")
	}
	total := len(run.DynamicSteps) + len(fragment)
	if total > maxTotalDynamicSteps {
		return fmt.Errorf(
			"total dynamic steps would be %d (max %d)",
			total, maxTotalDynamicSteps,
		)
	}
	return nil
}

// buildExistingIDSet collects all step IDs currently in the
// workflow definition (static + already-materialized dynamic).
func buildExistingIDSet(
	wfDef dag.WorkflowDef,
) map[string]bool {
	if len(wfDef.Steps) == 0 {
		panic("buildExistingIDSet: wfDef has no steps")
	}
	ids := make(map[string]bool, len(wfDef.Steps))
	for _, step := range wfDef.Steps {
		ids[step.ID] = true
	}
	return ids
}

// wireEntrySteps makes fragment entry points (steps with no deps)
// depend on the planner step, ensuring they run after planning.
func wireEntrySteps(
	fragment []dag.StepDef, plannerID string,
) {
	if plannerID == "" {
		panic("wireEntrySteps: plannerID must not be empty")
	}
	if len(fragment) == 0 {
		panic("wireEntrySteps: fragment must not be empty")
	}
	for i := range fragment {
		if len(fragment[i].DependsOn) == 0 {
			fragment[i].DependsOn = []string{plannerID}
		}
	}
}

// applyFragment appends validated steps to the run, initializes
// their state, saves the snapshot, and enqueues newly-ready steps.
func (o *Orchestrator) applyFragment(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	stepDef dag.StepDef,
	fragment []dag.StepDef,
) error {
	if run.RunID == "" {
		panic("applyFragment: RunID must not be empty")
	}
	if len(fragment) == 0 {
		panic("applyFragment: fragment must not be empty")
	}

	run.DynamicSteps = append(run.DynamicSteps, fragment...)
	for _, step := range fragment {
		run.Steps[step.ID] = dag.StepState{
			Status: dag.StepStatusPending,
		}
	}

	if err := o.saveSnapshot(ctx, *run); err != nil {
		return err
	}

	o.publishMaterializedEvent(ctx, run.RunID, stepDef.ID)

	augmented := dag.EffectiveDef(wfDef, *run)
	return o.enqueueReady(ctx, augmented, *run)
}

// finishPlannerNoSteps handles the case where the planner emits
// an empty plan — just check completion and enqueue as normal.
func (o *Orchestrator) finishPlannerNoSteps(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
) error {
	if run.RunID == "" {
		panic("finishPlannerNoSteps: RunID must not be empty")
	}
	completed := completedSet(run)
	if dag.IsComplete(wfDef, completed) {
		return o.completeWorkflow(ctx, run)
	}
	if err := o.saveSnapshot(ctx, run); err != nil {
		return err
	}
	return o.enqueueReady(ctx, wfDef, run)
}

// failPlannerStep marks the planner step as failed and fails the
// workflow. Planner failures are not retryable — the fragment is
// structurally invalid and retrying won't fix it.
func (o *Orchestrator) failPlannerStep(
	ctx context.Context,
	run *dag.WorkflowRun,
	stepDef dag.StepDef,
	reason string,
) error {
	if run.RunID == "" {
		panic("failPlannerStep: RunID must not be empty")
	}
	if stepDef.ID == "" {
		panic("failPlannerStep: stepDef.ID must not be empty")
	}

	slog.ErrorContext(ctx,
		"planner step failed",
		"error", fmt.Errorf("%s", reason),
		"run_id", run.RunID,
		"step_id", stepDef.ID,
	)

	state := run.Steps[stepDef.ID]
	state.Status = dag.StepStatusFailed
	state.Error = reason
	run.Steps[stepDef.ID] = state

	return o.failWorkflow(ctx, *run, stepDef, state)
}

// publishMaterializedEvent emits the planner.materialized event
// to the history stream for observability.
func (o *Orchestrator) publishMaterializedEvent(
	ctx context.Context, runID, stepID string,
) {
	if runID == "" {
		panic("publishMaterializedEvent: runID must not be empty")
	}
	if stepID == "" {
		panic(
			"publishMaterializedEvent: stepID must not be empty",
		)
	}
	evt := protocol.NewStepEvent(
		protocol.EventPlannerMaterialized, runID, stepID, nil,
	)
	data, err := evt.Marshal()
	if err != nil {
		return
	}
	_, pubErr := o.js.Publish(
		ctx, evt.NATSSubject(), data,
		jetstream.WithMsgID(evt.NATSMsgID()),
	)
	if pubErr != nil {
		slog.ErrorContext(ctx,
			"publish planner.materialized failed",
			"error", pubErr,
			"run_id", runID,
			"step_id", stepID,
		)
	}
}
