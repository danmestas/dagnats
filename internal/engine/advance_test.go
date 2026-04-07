// advance_test.go
// Methodology: Pure unit tests for the Advance state machine core.
// No NATS, no I/O — tests exercise only (def, run, event) → (run, effects).
// Each test creates a minimal WorkflowDef + WorkflowRun, feeds one Event,
// and asserts the resulting run state and side effects. Red-green TDD:
// tests were written first, then Advance was implemented to pass them.
package engine

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
)

// helper: build a minimal linear chain def with given step IDs.
// Each step depends on the previous one. Steps use StepTypeNormal.
func linearDef(ids ...string) dag.WorkflowDef {
	if len(ids) == 0 {
		panic("linearDef: at least one step ID required")
	}
	steps := make([]dag.StepDef, len(ids))
	for i, id := range ids {
		steps[i] = dag.StepDef{
			ID:      id,
			Task:    "task." + id,
			Timeout: 30 * time.Second,
		}
		if i > 0 {
			steps[i].DependsOn = []string{ids[i-1]}
		}
	}
	return dag.WorkflowDef{
		Name:    "test-workflow",
		Version: "1",
		Steps:   steps,
	}
}

// helper: build a run from a def with all steps Pending.
func newRun(def dag.WorkflowDef) dag.WorkflowRun {
	return dag.NewWorkflowRun(def, "run-1")
}

// helper: count side effects of a specific type.
func countEffects[T SideEffect](effects []SideEffect) int {
	n := 0
	for _, e := range effects {
		if _, ok := e.(T); ok {
			n++
		}
	}
	return n
}

// helper: find first effect of a specific type.
func findEffect[T SideEffect](effects []SideEffect) (T, bool) {
	for _, e := range effects {
		if typed, ok := e.(T); ok {
			return typed, true
		}
	}
	var zero T
	return zero, false
}

func TestAdvance_StepCompletionAdvancesDAG(t *testing.T) {
	// Two-step linear chain: a → b.
	// Complete step "a" → expect step "b" enqueued.
	def := linearDef("a", "b")
	run := newRun(def)
	run.Status = dag.RunStatusRunning

	evt := Event{
		Type:    EventStepCompleted,
		StepID:  "a",
		Payload: []byte(`"output-a"`),
	}

	result, effects := Advance(def, run, evt)

	// Positive: step "a" must be Completed.
	if result.Steps["a"].Status != dag.StepStatusCompleted {
		t.Fatalf(
			"step a status = %v, want Completed",
			result.Steps["a"].Status,
		)
	}

	// Positive: step "b" must be enqueued as a side effect.
	enqueue, found := findEffect[EnqueueTask](effects)
	if !found {
		t.Fatal("expected EnqueueTask for step b, got none")
	}
	if enqueue.Step.ID != "b" {
		t.Fatalf(
			"EnqueueTask step = %q, want %q", enqueue.Step.ID, "b",
		)
	}

	// Negative: no CompleteWorkflow — step "b" still pending.
	if countEffects[CompleteWorkflow](effects) != 0 {
		t.Fatal("unexpected CompleteWorkflow effect")
	}
}

func TestAdvance_LastStepCompletesWorkflow(t *testing.T) {
	// Single step "a", complete it → workflow completes.
	def := linearDef("a")
	run := newRun(def)
	run.Status = dag.RunStatusRunning

	evt := Event{
		Type:    EventStepCompleted,
		StepID:  "a",
		Payload: []byte(`"done"`),
	}

	result, effects := Advance(def, run, evt)

	// Positive: CompleteWorkflow side effect emitted.
	_, found := findEffect[CompleteWorkflow](effects)
	if !found {
		t.Fatal("expected CompleteWorkflow effect, got none")
	}

	// Positive: run status is Completed.
	if result.Status != dag.RunStatusCompleted {
		t.Fatalf(
			"run status = %v, want Completed", result.Status,
		)
	}

	// Negative: no EnqueueTask — nothing left to run.
	if countEffects[EnqueueTask](effects) != 0 {
		t.Fatal("unexpected EnqueueTask effect")
	}
}

func TestAdvance_NonRetriableFailureFailsWorkflow(t *testing.T) {
	// Single step "a", fail it non-retriable → workflow fails.
	def := linearDef("a")
	run := newRun(def)
	run.Status = dag.RunStatusRunning

	evt := Event{
		Type:   EventStepFailed,
		StepID: "a",
		FailPayload: FailPayload{
			Error:       "permanent error",
			FailureType: FailureTypeNonRetriable,
		},
	}

	result, effects := Advance(def, run, evt)

	// Positive: step "a" must be Failed.
	if result.Steps["a"].Status != dag.StepStatusFailed {
		t.Fatalf(
			"step a status = %v, want Failed",
			result.Steps["a"].Status,
		)
	}

	// Positive: FailWorkflow side effect emitted.
	fw, found := findEffect[FailWorkflow](effects)
	if !found {
		t.Fatal("expected FailWorkflow effect, got none")
	}
	if fw.StepID != "a" {
		t.Fatalf("FailWorkflow.StepID = %q, want %q", fw.StepID, "a")
	}

	// Negative: no EnqueueTask — nothing should run.
	if countEffects[EnqueueTask](effects) != 0 {
		t.Fatal("unexpected EnqueueTask effect")
	}
}

func TestAdvance_AgentLoopContinueIncrementsIteration(t *testing.T) {
	// AgentLoop step with MaxIterations=5.
	// Continue at iteration 0 → iteration becomes 1, re-enqueued.
	loopCfg, err := json.Marshal(dag.AgentLoopConfig{
		MaxIterations: 5,
	})
	if err != nil {
		t.Fatalf("marshal loop config: %v", err)
	}
	def := dag.WorkflowDef{
		Name:    "loop-wf",
		Version: "1",
		Steps: []dag.StepDef{{
			ID:      "a",
			Task:    "task.a",
			Type:    dag.StepTypeAgentLoop,
			Config:  loopCfg,
			Timeout: 30 * time.Second,
		}},
	}
	run := newRun(def)
	run.Status = dag.RunStatusRunning
	// Mark as Queued so it looks like it's in-flight.
	state := run.Steps["a"]
	state.Status = dag.StepStatusQueued
	run.Steps["a"] = state

	evt := Event{
		Type:   EventStepContinue,
		StepID: "a",
	}

	result, effects := Advance(def, run, evt)

	// Positive: iteration count incremented to 1.
	if result.Steps["a"].Iterations != 1 {
		t.Fatalf(
			"iterations = %d, want 1",
			result.Steps["a"].Iterations,
		)
	}

	// Positive: EnqueueTask emitted to re-run the step.
	enqueue, found := findEffect[EnqueueTask](effects)
	if !found {
		t.Fatal("expected EnqueueTask for re-enqueue, got none")
	}
	if enqueue.Step.ID != "a" {
		t.Fatalf(
			"EnqueueTask step = %q, want %q", enqueue.Step.ID, "a",
		)
	}
	if enqueue.Iteration != 1 {
		t.Fatalf(
			"EnqueueTask iteration = %d, want 1",
			enqueue.Iteration,
		)
	}

	// Negative: no CompleteWorkflow or FailWorkflow.
	if countEffects[CompleteWorkflow](effects) != 0 {
		t.Fatal("unexpected CompleteWorkflow effect")
	}
	if countEffects[FailWorkflow](effects) != 0 {
		t.Fatal("unexpected FailWorkflow effect")
	}
}

func TestAdvance_AgentLoopMaxIterationsFails(t *testing.T) {
	// Continue at iteration = maxIterations → expect FailWorkflow.
	loopCfg, err := json.Marshal(dag.AgentLoopConfig{
		MaxIterations: 3,
	})
	if err != nil {
		t.Fatalf("marshal loop config: %v", err)
	}
	def := dag.WorkflowDef{
		Name:    "loop-wf",
		Version: "1",
		Steps: []dag.StepDef{{
			ID:      "a",
			Task:    "task.a",
			Type:    dag.StepTypeAgentLoop,
			Config:  loopCfg,
			Timeout: 30 * time.Second,
		}},
	}
	run := newRun(def)
	run.Status = dag.RunStatusRunning
	// Simulate having already done 2 iterations; next Continue
	// increments to 3 which equals MaxIterations.
	state := run.Steps["a"]
	state.Status = dag.StepStatusQueued
	state.Iterations = 2
	run.Steps["a"] = state

	evt := Event{
		Type:   EventStepContinue,
		StepID: "a",
	}

	result, effects := Advance(def, run, evt)

	// Positive: FailWorkflow emitted.
	fw, found := findEffect[FailWorkflow](effects)
	if !found {
		t.Fatal("expected FailWorkflow effect, got none")
	}
	if fw.StepID != "a" {
		t.Fatalf("FailWorkflow.StepID = %q, want %q", fw.StepID, "a")
	}

	// Positive: step status is Failed.
	if result.Steps["a"].Status != dag.StepStatusFailed {
		t.Fatalf(
			"step a status = %v, want Failed",
			result.Steps["a"].Status,
		)
	}

	// Negative: no EnqueueTask — loop is done.
	if countEffects[EnqueueTask](effects) != 0 {
		t.Fatal("unexpected EnqueueTask effect")
	}
}

func TestAdvance_ParallelFanOutEnqueuesBoth(t *testing.T) {
	// a → (b, c): completing "a" should enqueue both "b" and "c".
	def := dag.WorkflowDef{
		Name:    "fanout-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task.a", Timeout: 30 * time.Second},
			{
				ID: "b", Task: "task.b",
				DependsOn: []string{"a"},
				Timeout:   30 * time.Second,
			},
			{
				ID: "c", Task: "task.c",
				DependsOn: []string{"a"},
				Timeout:   30 * time.Second,
			},
		},
	}
	run := newRun(def)
	run.Status = dag.RunStatusRunning

	evt := Event{
		Type:    EventStepCompleted,
		StepID:  "a",
		Payload: []byte(`"out-a"`),
	}

	_, effects := Advance(def, run, evt)

	// Positive: both "b" and "c" are enqueued.
	enqueued := map[string]bool{}
	for _, e := range effects {
		if eq, ok := e.(EnqueueTask); ok {
			enqueued[eq.Step.ID] = true
		}
	}
	if !enqueued["b"] {
		t.Fatal("expected EnqueueTask for step b")
	}
	if !enqueued["c"] {
		t.Fatal("expected EnqueueTask for step c")
	}

	// Negative: no CompleteWorkflow — b and c still pending.
	if countEffects[CompleteWorkflow](effects) != 0 {
		t.Fatal("unexpected CompleteWorkflow effect")
	}
}

func TestAdvance_FanInWaitsForBoth(t *testing.T) {
	// (a, b) → c: "a" completed, "b" still running → no effects.
	// Then "b" completed → "c" enqueued.
	def := dag.WorkflowDef{
		Name:    "fanin-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task.a", Timeout: 30 * time.Second},
			{ID: "b", Task: "task.b", Timeout: 30 * time.Second},
			{
				ID: "c", Task: "task.c",
				DependsOn: []string{"a", "b"},
				Timeout:   30 * time.Second,
			},
		},
	}
	run := newRun(def)
	run.Status = dag.RunStatusRunning
	// Mark "b" as Queued (in-flight).
	bState := run.Steps["b"]
	bState.Status = dag.StepStatusQueued
	run.Steps["b"] = bState

	// Step 1: "a" completes — "c" should NOT be enqueued yet.
	evt1 := Event{
		Type:    EventStepCompleted,
		StepID:  "a",
		Payload: []byte(`"out-a"`),
	}
	result1, effects1 := Advance(def, run, evt1)

	// Negative: no EnqueueTask for "c" — "b" not done.
	if countEffects[EnqueueTask](effects1) != 0 {
		t.Fatal("unexpected EnqueueTask before fan-in complete")
	}

	// Step 2: "b" completes — now "c" should be enqueued.
	evt2 := Event{
		Type:    EventStepCompleted,
		StepID:  "b",
		Payload: []byte(`"out-b"`),
	}
	_, effects2 := Advance(def, result1, evt2)

	// Positive: "c" is now enqueued.
	enqueue, found := findEffect[EnqueueTask](effects2)
	if !found {
		t.Fatal("expected EnqueueTask for step c after fan-in")
	}
	if enqueue.Step.ID != "c" {
		t.Fatalf(
			"EnqueueTask step = %q, want %q", enqueue.Step.ID, "c",
		)
	}

	// Negative: no CompleteWorkflow yet — "c" still pending.
	if countEffects[CompleteWorkflow](effects2) != 0 {
		t.Fatal("unexpected CompleteWorkflow effect")
	}
}

func TestAdvance_SkippedStepsResolved(t *testing.T) {
	// a → b (skip_if: a output "skip" == true), a → c
	// "a" completes with {"skip": true} → "b" skipped, "c" enqueued.
	// Then "c" completes → workflow completes.
	def := dag.WorkflowDef{
		Name:    "skip-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task.a", Timeout: 30 * time.Second},
			{
				ID: "b", Task: "task.b",
				DependsOn: []string{"a"},
				Timeout:   30 * time.Second,
				SkipIf: &dag.ParentCond{
					StepID: "a",
					Field:  "skip",
					Op:     "==",
					Value:  true,
				},
			},
			{
				ID: "c", Task: "task.c",
				DependsOn: []string{"a"},
				Timeout:   30 * time.Second,
			},
		},
	}
	run := newRun(def)
	run.Status = dag.RunStatusRunning

	// Step "a" completes with output that triggers skip on "b".
	evt := Event{
		Type:    EventStepCompleted,
		StepID:  "a",
		Payload: []byte(`{"skip": true}`),
	}

	result, effects := Advance(def, run, evt)

	// Positive: step "a" is Completed.
	if result.Steps["a"].Status != dag.StepStatusCompleted {
		t.Fatalf(
			"step a status = %v, want Completed",
			result.Steps["a"].Status,
		)
	}

	// Positive: SkipStep emitted for "b".
	skip, foundSkip := findEffect[SkipStep](effects)
	if !foundSkip {
		t.Fatal("expected SkipStep for step b, got none")
	}
	if skip.StepID != "b" {
		t.Fatalf("SkipStep.StepID = %q, want %q", skip.StepID, "b")
	}

	// Positive: "c" is enqueued (not skipped).
	enqueue, foundEnqueue := findEffect[EnqueueTask](effects)
	if !foundEnqueue {
		t.Fatal("expected EnqueueTask for step c, got none")
	}
	if enqueue.Step.ID != "c" {
		t.Fatalf(
			"EnqueueTask step = %q, want %q",
			enqueue.Step.ID, "c",
		)
	}

	// Negative: no CompleteWorkflow yet — "c" still pending.
	if countEffects[CompleteWorkflow](effects) != 0 {
		t.Fatal("unexpected CompleteWorkflow before c completes")
	}

	// Now complete "c" → workflow should complete since "b" is
	// skipped (counts as completed) and "c" is completed.
	evt2 := Event{
		Type:    EventStepCompleted,
		StepID:  "c",
		Payload: []byte(`"done"`),
	}
	result2, effects2 := Advance(def, result, evt2)

	// Positive: CompleteWorkflow emitted.
	_, foundComplete := findEffect[CompleteWorkflow](effects2)
	if !foundComplete {
		t.Fatal("expected CompleteWorkflow after c completes")
	}

	// Positive: run status is Completed.
	if result2.Status != dag.RunStatusCompleted {
		t.Fatalf(
			"run status = %v, want Completed", result2.Status,
		)
	}
}
