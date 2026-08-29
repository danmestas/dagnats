// internal/engine/spawn_handlers.go
// Sub-workflow spawn and child lifecycle handlers, extracted from
// orchestrator.go per issue #599 (item 1). enqueueSubWorkflow/spawnChild
// launch a child run; handleWorkflowSpawn creates it, and
// handleChildCompleted/handleChildFailed converge the child's terminal
// outputs back into the parent step. dispatchEvent in orchestrator.go
// remains the single switch that routes to them under the run lock. Methods
// stay on *Orchestrator (a same-package file split, not a behavior change).
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// handleWorkflowSpawn creates a child WorkflowRun from a spawn event.
// The child is linked to the parent via ParentRunID and ParentStepID.
func (o *Orchestrator) handleWorkflowSpawn(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleWorkflowSpawn: RunID must not be empty")
	}
	var payload struct {
		ChildRunID    string          `json:"child_run_id"`
		ChildWorkflow string          `json:"child_workflow"`
		ParentStepID  string          `json:"parent_step_id"`
		Input         json.RawMessage `json:"input"`
		Detach        bool            `json:"detach"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal spawn payload: %w", err)
	}
	if payload.ChildRunID == "" {
		panic("handleWorkflowSpawn: child_run_id must not be empty")
	}

	// Enforce max nesting depth by walking the parent chain.
	// The child would be at depth+1, so reject when depth+1 > max.
	depth := o.nestingDepth(ctx, evt.RunID)
	if depth+1 >= maxNestingDepth {
		slog.ErrorContext(ctx,
			"spawn rejected: max nesting depth exceeded",
			"error", fmt.Errorf(
				"depth %d >= max %d", depth, maxNestingDepth,
			),
		)
		return fmt.Errorf(
			"max nesting depth %d exceeded", maxNestingDepth,
		)
	}

	return o.createChildRun(ctx, evt.RunID, payload.ChildRunID,
		payload.ChildWorkflow, payload.ParentStepID,
		payload.Input, payload.Detach)
}

// createChildRun loads the child workflow def, creates the child run,
// and enqueues its entry-point steps. For detached children the parent
// link is omitted so they run independently.
func (o *Orchestrator) createChildRun(
	ctx context.Context,
	parentRunID string,
	childRunID string,
	childWorkflow string,
	parentStepID string,
	input json.RawMessage,
	detach bool,
) error {
	if childRunID == "" {
		panic("createChildRun: childRunID must not be empty")
	}
	if childWorkflow == "" {
		panic("createChildRun: childWorkflow must not be empty")
	}

	entry, err := o.defKV.Get(ctx, childWorkflow)
	if err != nil {
		return fmt.Errorf(
			"load child workflow def %q: %w",
			childWorkflow, err,
		)
	}
	var childDef dag.WorkflowDef
	if err := json.Unmarshal(entry.Value(), &childDef); err != nil {
		return fmt.Errorf("unmarshal child def: %w", err)
	}

	childRun := dag.NewWorkflowRun(childDef, childRunID)
	childRun.Input = input
	childRun.Status = dag.RunStatusRunning

	// Load the parent UNCONDITIONALLY, detached or not (#634 review
	// round 2, Major): RootRunID and TriggerDepth are different
	// invariants that happen to both be parent-derived. RootRunID is
	// lineage DISPLAY — "detached" deliberately starts a new visible
	// tree, so a detached child self-roots regardless of the parent.
	// TriggerDepth is a SAFETY CAP — `detach` is an author-controlled
	// workflow-definition flag, so gating the cap's inheritance on it
	// would make the cap bypassable by construction: A
	// --run_terminal--> B, B spawns a DETACHED child C, C's
	// completion triggers A again — every hop resets to depth 0 and
	// TriggerDepthMax never engages. A genuinely-missing parent
	// (ErrRunNotFound) means this child heads a new depth-0 lineage
	// regardless of detach — mirroring nestingDepth, which treats a
	// missing parent as the chain root. Only a real store fault (not
	// a miss) propagates as a wrapped error.
	parent, parentErr := o.store.Load(ctx, parentRunID)
	switch {
	case parentErr == nil:
		childRun.TriggerDepth = parent.TriggerDepth
	case errors.Is(parentErr, ErrRunNotFound):
		// childRun.TriggerDepth stays 0 (zero value).
	default:
		return fmt.Errorf(
			"load parent run %q for spawn derivation: %w",
			parentRunID, parentErr,
		)
	}

	if !detach {
		childRun.ParentRunID = parentRunID
		childRun.ParentStepID = parentStepID
		if parentErr == nil {
			childRun.RootRunID = RootRunIDOf(parent)
		} else {
			childRun.RootRunID = childRunID
		}
	} else {
		childRun.RootRunID = childRunID // detached child self-roots (#377)
	}

	if err := o.saveSnapshot(ctx, childRun, ""); err != nil {
		return err
	}

	o.metrics.runsActive.Add(ctx, 1)
	return o.enqueueReady(ctx, childDef, childRun)
}

// enqueueSubWorkflow resolves input, generates a child run ID, and
// publishes a spawn event. For detached sub-workflows the parent step
// completes immediately; otherwise it stays Running until the child
// finishes.
func (o *Orchestrator) enqueueSubWorkflow(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	step dag.StepDef,
) error {
	if step.Type != dag.StepTypeSubWorkflow {
		panic("enqueueSubWorkflow: wrong step type")
	}
	if run.RunID == "" {
		panic("enqueueSubWorkflow: RunID must not be empty")
	}

	cfg, err := dag.ParseSubWorkflowConfig(step)
	if err != nil {
		return fmt.Errorf("parse sub-workflow config: %w", err)
	}

	input, err := dag.ResolveInput(step, run.Steps, run.Input)
	if err != nil {
		return fmt.Errorf(
			"resolve input for step %q: %w", step.ID, err,
		)
	}
	childRunID := nuid.Next()

	if err := o.spawnChild(
		ctx, wfDef, run, step, cfg, input, childRunID,
	); err != nil {
		return err
	}

	// Detached sub-workflows complete the parent step immediately,
	// which may unblock downstream steps or complete the workflow.
	if cfg.Detach {
		completed := completedSet(*run)
		if dag.IsComplete(wfDef, completed) {
			return o.completeWorkflow(ctx, *run)
		}
		return o.enqueueReady(ctx, wfDef, *run)
	}
	return nil
}

// spawnChild marks the parent step state, saves the snapshot, and
// publishes the spawn event. Extracted to keep enqueueSubWorkflow
// within the 70-line limit.
func (o *Orchestrator) spawnChild(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	step dag.StepDef,
	cfg dag.SubWorkflowConfig,
	input []byte,
	childRunID string,
) error {
	if childRunID == "" {
		panic("spawnChild: childRunID must not be empty")
	}
	if step.ID == "" {
		panic("spawnChild: step.ID must not be empty")
	}

	state := run.Steps[step.ID]
	if cfg.Detach {
		state.Status = dag.StepStatusCompleted
		state.ChildRunID = childRunID
		state.Output = []byte(fmt.Sprintf(
			`{"child_run_id":%q}`, childRunID,
		))
	} else {
		state.Status = dag.StepStatusRunning
		state.ChildRunID = childRunID
	}
	run.Steps[step.ID] = state
	if err := o.saveSnapshot(ctx, *run, step.ID); err != nil {
		return err
	}

	return o.publishSpawnEvent(
		ctx, run.RunID, step.ID, cfg, input, childRunID,
	)
}

// publishSpawnEvent publishes EventWorkflowSpawn to the history
// stream with the child run metadata in the payload.
func (o *Orchestrator) publishSpawnEvent(
	ctx context.Context,
	parentRunID string,
	parentStepID string,
	cfg dag.SubWorkflowConfig,
	input []byte,
	childRunID string,
) error {
	if parentRunID == "" {
		panic("publishSpawnEvent: parentRunID must not be empty")
	}
	if parentStepID == "" {
		panic("publishSpawnEvent: parentStepID must not be empty")
	}

	payload, err := json.Marshal(map[string]interface{}{
		"child_run_id":   childRunID,
		"child_workflow": cfg.Workflow,
		"parent_step_id": parentStepID,
		"input":          json.RawMessage(input),
		"detach":         cfg.Detach,
	})
	if err != nil {
		return fmt.Errorf("marshal spawn payload: %w", err)
	}

	evt := protocol.NewStepEvent(
		protocol.EventWorkflowSpawn,
		parentRunID, parentStepID, payload,
	)
	msg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Header: nats.Header{
			"Nats-Msg-Id": {evt.NATSMsgID()},
		},
	}
	if _, err := o.tp.JSPublishMsgEvent(ctx, msg, &evt); err != nil {
		return fmt.Errorf("publish spawn event: %w", err)
	}
	return nil
}

// handleChildCompleted processes EventWorkflowChildCompleted: loads
// the child run's terminal output, marks the parent step Completed,
// and enqueues the next ready steps.
func (o *Orchestrator) handleChildCompleted(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleChildCompleted: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleChildCompleted: StepID must not be empty")
	}

	wfDef, run, err := o.loadRunAndDef(ctx, evt.RunID)
	if err != nil {
		return err
	}

	state := run.Steps[evt.StepID]
	if state.Status != dag.StepStatusRunning {
		return nil // Already handled or cancelled.
	}

	output, err := o.loadChildTerminalOutputs(ctx, state.ChildRunID)
	if err != nil {
		return fmt.Errorf("load child outputs: %w", err)
	}

	state.Status = dag.StepStatusCompleted
	state.Output = output
	run.Steps[evt.StepID] = state

	completed := completedSet(run)
	if dag.IsComplete(wfDef, completed) {
		return o.completeWorkflow(ctx, run)
	}
	if err := o.saveSnapshot(ctx, run, evt.StepID); err != nil {
		return err
	}
	return o.enqueueReady(ctx, wfDef, run)
}

// loadChildTerminalOutputs loads the child run and its workflow def,
// finds terminal steps (steps no other step depends on), and returns
// their outputs. One terminal step returns raw output; multiple
// returns a JSON map keyed by step ID.
func (o *Orchestrator) loadChildTerminalOutputs(
	ctx context.Context, childRunID string,
) ([]byte, error) {
	if childRunID == "" {
		panic("loadChildTerminalOutputs: childRunID empty")
	}
	childDef, childRun, err := o.loadRunAndDef(ctx, childRunID)
	if err != nil {
		return nil, err
	}
	return collectTerminalOutputs(childDef, childRun)
}

// collectTerminalOutputs finds steps that no other step depends on
// and returns their outputs. Single terminal returns raw output;
// multiple terminals return a JSON map keyed by step ID.
func collectTerminalOutputs(
	def dag.WorkflowDef, run dag.WorkflowRun,
) ([]byte, error) {
	if len(def.Steps) == 0 {
		panic("collectTerminalOutputs: def has no steps")
	}
	if run.Steps == nil {
		panic("collectTerminalOutputs: run.Steps is nil")
	}
	depTargets := make(map[string]bool, len(def.Steps))
	for _, step := range def.Steps {
		for _, dep := range step.DependsOn {
			depTargets[dep] = true
		}
	}
	var terminals []dag.StepDef
	const maxTerminals = 1000
	for _, step := range def.Steps {
		if !depTargets[step.ID] {
			terminals = append(terminals, step)
		}
		if len(terminals) > maxTerminals {
			break
		}
	}
	if len(terminals) == 1 {
		return run.Steps[terminals[0].ID].Output, nil
	}
	collected := make(
		map[string]json.RawMessage, len(terminals),
	)
	for _, step := range terminals {
		collected[step.ID] = run.Steps[step.ID].Output
	}
	return json.Marshal(collected)
}

// handleChildFailed processes EventWorkflowChildFailed: marks the
// parent step Failed and delegates to failWorkflow.
func (o *Orchestrator) handleChildFailed(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleChildFailed: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleChildFailed: StepID must not be empty")
	}

	wfDef, run, err := o.loadRunAndDef(ctx, evt.RunID)
	if err != nil {
		return err
	}

	state := run.Steps[evt.StepID]
	if state.Status != dag.StepStatusRunning {
		return nil // Already handled or cancelled.
	}

	var payload struct {
		Error string `json:"error"`
	}
	if evt.Payload != nil {
		if err := json.Unmarshal(
			evt.Payload, &payload,
		); err != nil {
			return fmt.Errorf(
				"unmarshal child failed payload: %w", err,
			)
		}
	}

	state.Status = dag.StepStatusFailed
	state.Error = "child workflow failed: " + payload.Error
	run.Steps[evt.StepID] = state

	stepDef, _ := findStepDef(wfDef, evt.StepID)
	return o.failWorkflow(ctx, run, stepDef, state)
}
