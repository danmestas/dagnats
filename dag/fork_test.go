// dag/fork_test.go
// Fork step reuse (#810): a fork reuses the parent's completed and
// skipped steps that neither are the forked step nor depend on it,
// and seeds them into the new run; the forked step, everything
// downstream of it, auxiliary handlers and steps the parent had not
// finished run afresh.
//
// Methodology: pure unit tests over a diamond-plus-tail workflow,
// a -> {s, b} -> c, plus an on-failure handler h, with a parent run in
// which every step has finished. Each case asserts what is reused and
// what is not.
package dag

import (
	"encoding/json"
	"testing"
	"time"
)

func forkDef() WorkflowDef {
	return WorkflowDef{
		Name: "wf",
		Steps: []StepDef{
			{ID: "a", Task: "a"},
			{ID: "s", Task: "s", DependsOn: []string{"a"}},
			{ID: "b", Task: "b", DependsOn: []string{"a"}},
			{ID: "c", Task: "c", DependsOn: []string{"s", "b"}},
			{ID: "h", Task: "h"},
		},
		AuxSteps: map[string]bool{"h": true},
	}
}

func finishedParent() WorkflowRun {
	run := NewWorkflowRun(forkDef(), "parent")
	done := time.Now().UTC()
	for id := range run.Steps {
		run.Steps[id] = StepState{
			Status: StepStatusCompleted, Output: []byte(`"` + id + `"`), CompletedAt: &done,
			Attempts: 1,
		}
	}
	run.Steps["b"] = StepState{Status: StepStatusSkipped}
	return run
}

func TestForkReuse_ReusesFinishedStepsOutsideTheForkedSubgraph(t *testing.T) {
	reused := ForkReuse(forkDef(), finishedParent(), "s")
	if got := reused["a"]; got.Status != StepStatusCompleted || string(got.Output) != `"a"` {
		t.Fatalf("a = %+v, want the parent's completed output", got)
	}
	if got := reused["b"]; got.Status != StepStatusSkipped {
		t.Fatalf("b = %+v, want skipped", got)
	}
	for _, id := range []string{"s", "c", "h"} {
		if _, ok := reused[id]; ok {
			t.Fatalf("%s reused; the forked step, its dependents and aux steps run afresh", id)
		}
	}
}

func TestForkReuse_UnfinishedParentStepsRunAgain(t *testing.T) {
	parent := finishedParent()
	parent.Steps["b"] = StepState{Status: StepStatusRunning}
	reused := ForkReuse(forkDef(), parent, "s")
	if _, ok := reused["b"]; ok {
		t.Fatalf("running step b reused")
	}
	if _, ok := reused["a"]; !ok {
		t.Fatalf("completed step a not reused")
	}
}

func TestApplyFork_SeedsReusedStepsAndRecordsTheOrigin(t *testing.T) {
	data, err := json.Marshal(ForkStart{
		Origin: ForkOrigin{RunID: "parent", StepID: "s", JournalSeq: 7},
		Reused: ForkReuse(forkDef(), finishedParent(), "s"),
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var start ForkStart
	if err := json.Unmarshal(data, &start); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	run := NewWorkflowRun(forkDef(), "child")
	ApplyFork(&run, start)
	if run.ForkedFrom == nil || *run.ForkedFrom != start.Origin {
		t.Fatalf("forked_from = %+v, want %+v", run.ForkedFrom, start.Origin)
	}
	if run.Steps["a"].Status != StepStatusCompleted || string(run.Steps["a"].Output) != `"a"` {
		t.Fatalf("a = %+v, want seeded completed", run.Steps["a"])
	}
	if run.Steps["s"].Status != StepStatusPending || run.Steps["c"].Status != StepStatusPending {
		t.Fatalf("s = %v, c = %v; want both pending", run.Steps["s"].Status, run.Steps["c"].Status)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(data, &wire); err != nil {
		t.Fatalf("decode wire: %v", err)
	}
	if _, ok := wire["origin"]; !ok || len(wire) != 2 {
		t.Fatalf("fork start wire = %s, want origin and reused", data)
	}
}

func TestApplyFork_IgnoresStepsTheRunDoesNotHave(t *testing.T) {
	run := NewWorkflowRun(forkDef(), "child")
	ApplyFork(&run, ForkStart{
		Origin: ForkOrigin{RunID: "parent", StepID: "s", JournalSeq: 7},
		Reused: map[string]StepState{
			"ghost": {Status: StepStatusCompleted}, "s": {Status: StepStatusCompleted},
		},
	})
	if _, ok := run.Steps["ghost"]; ok {
		t.Fatalf("a step outside the def was seeded")
	}
	if run.Steps["s"].Status != StepStatusPending {
		t.Fatalf("the forked step was seeded as %v", run.Steps["s"].Status)
	}
}
