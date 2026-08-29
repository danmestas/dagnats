// internal/engine/map_handlers.go
// Map step-kind handlers, extracted from orchestrator.go per issue #599
// (item 1). These are the per-event-kind handlers and helpers for the
// fan-out/fan-in Map step: enqueue splits the upstream array into
// instances, and the per-instance completed/failed events converge the
// results. dispatchEvent in orchestrator.go remains the single switch
// that routes to them under the run lock. Methods stay on *Orchestrator
// (a same-package file split, not a behavior change).
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/runid"
	"github.com/danmestas/dagnats/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"
)

// isMapInstanceID returns true if the step ID is a map instance
// (format: "{stepID}.map.{index}").
func isMapInstanceID(stepID string) bool {
	return strings.Contains(stepID, ".map.")
}

// parseMapInstanceID splits a compound map instance ID into the
// base step ID and instance index. Panics if the format is invalid.
func parseMapInstanceID(stepID string) (string, int) {
	parts := strings.Split(stepID, ".map.")
	if len(parts) != 2 {
		panic("parseMapInstanceID: invalid format: " + stepID)
	}
	idx, err := strconv.Atoi(parts[1])
	if err != nil {
		panic("parseMapInstanceID: invalid index: " + parts[1])
	}
	return parts[0], idx
}

// mapInstanceID constructs a compound step ID for a map instance.
func mapInstanceID(stepID string, index int) string {
	return stepID + ".map." + strconv.Itoa(index)
}

// enqueueMapStep reads the upstream output as a JSON array and
// publishes one task per element. MapInstances track each item's
// state on the Map step's StepState.
func (o *Orchestrator) enqueueMapStep(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	step dag.StepDef,
) error {
	if step.Type != dag.StepTypeMap {
		panic("enqueueMapStep: step is not a Map step")
	}
	if len(step.DependsOn) != 1 {
		panic("enqueueMapStep: Map step must have exactly one dep")
	}

	// Read upstream output as JSON array.
	upstream := run.Steps[step.DependsOn[0]]
	var items []json.RawMessage
	if err := json.Unmarshal(upstream.Output, &items); err != nil {
		return fmt.Errorf(
			"map step %q: upstream output is not a JSON array: %w",
			step.ID, err,
		)
	}

	if err := o.validateAndInitMapInstances(
		ctx, run, step, items,
	); err != nil {
		return err
	}

	return o.publishMapTasks(ctx, run.RunID, wfDef.Name, step, items)
}

// validateAndInitMapInstances checks MaxItems and initializes
// the MapInstances slice on the step state.
func (o *Orchestrator) validateAndInitMapInstances(
	ctx context.Context,
	run *dag.WorkflowRun,
	step dag.StepDef,
	items []json.RawMessage,
) error {
	mapCfg, err := dag.ParseMapConfig(step)
	if err != nil {
		panic("validateAndInitMapInstances: " + err.Error())
	}
	maxItems := mapCfg.MaxItems
	if len(items) > maxItems {
		return fmt.Errorf(
			"map step %q: %d items exceeds MaxItems %d",
			step.ID, len(items), maxItems,
		)
	}

	state := run.Steps[step.ID]
	state.Status = dag.StepStatusRunning
	state.MapInstances = make(
		[]dag.MapInstanceState, len(items),
	)
	for i := range items {
		state.MapInstances[i] = dag.MapInstanceState{
			Status: dag.StepStatusQueued,
		}
	}
	run.Steps[step.ID] = state
	return o.saveSnapshot(ctx, *run, step.ID)
}

// publishMapTasks publishes one task per map item concurrently.
// workflowName is the parent run's workflow definition name (wfDef.Name),
// used ONLY for telemetry -- see the strip comment below for why passing
// the real name here is safe.
func (o *Orchestrator) publishMapTasks(
	ctx context.Context,
	runID string,
	workflowName string,
	step dag.StepDef,
	items []json.RawMessage,
) error {
	var g errgroup.Group
	for i, item := range items {
		i, item := i, item
		instanceStep := step
		instanceStep.ID = mapInstanceID(step.ID, i)
		// #513: map instances are data-parallel work items that must
		// categorically never hold a control-plane handle (#380). The STRIP
		// below -- not the workflowName value passed to Publish -- is what
		// enforces that deny-by-default: stripControlPlaneCapability removes
		// "control-plane" from this instance's RequiredCapabilities
		// unconditionally, regardless of grant policy. Because
		// effectiveCapabilities (grant_policy.go) is strip-only and
		// short-circuits when control-plane is already absent from caps, the
		// workflowName argument becomes structurally irrelevant to this
		// instance's grant decision from here on -- which is exactly what
		// makes it safe to pass the REAL workflow name through for
		// telemetry instead of forging "". Do not revert to an empty name
		// here, and do not delete the strip below: either change would
		// silently reconnect telemetry to the grant key or regrant
		// control-plane to map instances.
		instanceStep.RequiredCapabilities = stripControlPlaneCapability(
			step.RequiredCapabilities,
		)
		// A fresh nonce keeps the run-binding field populated though
		// instances do not call the control plane (#380).
		nonce := runid.New()
		g.Go(func() error {
			return o.publisher.Publish(
				ctx, runID, instanceStep, item, 0, workflowName, nonce,
			)
		})
	}
	return g.Wait()
}

// handleMapInstanceCompleted updates a single map instance's state.
// When all instances are done, collects outputs and completes the
// Map step.
func (o *Orchestrator) handleMapInstanceCompleted(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	evt protocol.Event,
) error {
	baseID, idx := parseMapInstanceID(evt.StepID)
	state := run.Steps[baseID]

	if idx < 0 || idx >= len(state.MapInstances) {
		return fmt.Errorf(
			"map instance index %d out of range for %q",
			idx, baseID,
		)
	}

	state.MapInstances[idx].Status = dag.StepStatusCompleted
	state.MapInstances[idx].Output = evt.Payload
	run.Steps[baseID] = state

	if !allMapInstancesDone(state.MapInstances) {
		return o.saveSnapshot(ctx, run, baseID)
	}

	return o.collectMapOutputs(ctx, wfDef, run, baseID, state)
}

// allMapInstancesDone returns true when every instance is completed.
func allMapInstancesDone(instances []dag.MapInstanceState) bool {
	for _, inst := range instances {
		if inst.Status != dag.StepStatusCompleted {
			return false
		}
	}
	return true
}

// collectMapOutputs gathers outputs from all instances into an
// ordered JSON array and completes the Map step.
func (o *Orchestrator) collectMapOutputs(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	baseID string,
	state dag.StepState,
) error {
	outputs := make(
		[]json.RawMessage, len(state.MapInstances),
	)
	for i, inst := range state.MapInstances {
		outputs[i] = inst.Output
	}
	collected, err := json.Marshal(outputs)
	if err != nil {
		return fmt.Errorf("marshal map outputs: %w", err)
	}

	state.Status = dag.StepStatusCompleted
	state.Output = collected
	run.Steps[baseID] = state

	completed := completedSet(run)
	if dag.IsComplete(wfDef, completed) {
		return o.completeWorkflow(ctx, run)
	}
	if err := o.saveSnapshot(ctx, run, baseID); err != nil {
		return err
	}
	return o.enqueueReady(ctx, wfDef, run)
}

// handleMapInstanceFailed marks the Map step as failed immediately
// (fail-fast). Remaining instances will expire via AckWait.
func (o *Orchestrator) handleMapInstanceFailed(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	evt protocol.Event,
) error {
	baseID, idx := parseMapInstanceID(evt.StepID)
	state := run.Steps[baseID]

	if idx < 0 || idx >= len(state.MapInstances) {
		return fmt.Errorf(
			"map instance index %d out of range for %q",
			idx, baseID,
		)
	}

	state.MapInstances[idx].Status = dag.StepStatusFailed
	if evt.Payload != nil {
		state.MapInstances[idx].Error = string(evt.Payload)
	}

	// Fail-fast: mark the Map step as failed.
	state.Status = dag.StepStatusFailed
	state.Error = fmt.Sprintf(
		"map instance %d failed: %s", idx,
		state.MapInstances[idx].Error,
	)
	run.Steps[baseID] = state

	return o.failMapStep(ctx, wfDef, run, baseID, state)
}

// failMapStep handles the on-failure handler or fails the workflow.
func (o *Orchestrator) failMapStep(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	baseID string,
	state dag.StepState,
) error {
	stepDef, _ := findStepDef(wfDef, baseID)

	// Check for on-failure handler.
	if stepDef.OnFailure != "" {
		return o.runMapOnFailure(
			ctx, wfDef, run, baseID, state, stepDef,
		)
	}

	// No on-failure — fail the workflow.
	run, err := finalizeRun(
		ctx, o.tp, o.saveSnapshot, run, dag.RunStatusFailed, baseID,
	)
	if err != nil {
		return err
	}
	wfAttr := metric.WithAttributes(
		attribute.String("workflow", run.WorkflowID),
	)
	o.metrics.runsActive.Add(ctx, -1, wfAttr)
	o.metrics.runsFailed.Add(ctx, 1, wfAttr)
	taskSubject := ""
	if stepDef.Task != "" {
		taskSubject = o.publisher.StepSubject(stepDef, run.RunID)
	}
	o.recovery.PublishDeadLetter(ctx, run, wfDef, stepDef, state,
		taskSubject)
	return o.notifyParentIfChild(
		ctx, run, fmt.Errorf("%s", state.Error),
	)
}

// runMapOnFailure enqueues the on-failure step for a failed map.
func (o *Orchestrator) runMapOnFailure(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	baseID string,
	state dag.StepState,
	stepDef dag.StepDef,
) error {
	onFailStep, found := findStepDef(
		wfDef, stepDef.OnFailure,
	)
	if !found {
		return nil
	}
	ofState := run.Steps[onFailStep.ID]
	ofState.Status = dag.StepStatusQueued
	stampDispatch(&ofState, time.Now().UTC())
	run.Steps[onFailStep.ID] = ofState
	if err := o.saveSnapshot(ctx, run, onFailStep.ID); err != nil {
		return err
	}
	errorInput := []byte(fmt.Sprintf(
		`{"failed_step":"%s","error":%q}`,
		baseID, state.Error,
	))
	return o.publisher.Publish(
		ctx, run.RunID, onFailStep, errorInput, 0,
		run.WorkflowID, ofState.DispatchNonce,
	)
}
