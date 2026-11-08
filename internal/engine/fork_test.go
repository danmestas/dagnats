// internal/engine/fork_test.go
// A forked run's workflow.started payload (#810) carries a "fork"
// section; the engine seeds the reused steps into the new run and
// records its origin. A start payload without one, or one the engine
// cannot decode, starts the run as before.
//
// Methodology: pure unit tests of applyForkStart on payload bytes as
// the API publishes them; positive and negative space per case.
package engine

import (
	"encoding/json"
	"testing"

	"github.com/danmestas/dagnats/dag"
)

func forkTestDef() dag.WorkflowDef {
	return dag.WorkflowDef{Name: "wf", Steps: []dag.StepDef{
		{ID: "a", Task: "a"}, {ID: "s", Task: "s", DependsOn: []string{"a"}},
	}}
}

func TestApplyForkStart_SeedsReusedSteps(t *testing.T) {
	payload, err := json.Marshal(map[string]any{
		"workflow_def": forkTestDef(),
		"fork": dag.ForkStart{
			Origin: dag.ForkOrigin{RunID: "parent", StepID: "s", JournalSeq: 3},
			Reused: map[string]dag.StepState{
				"a": {Status: dag.StepStatusCompleted, Output: []byte(`1`)},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	run := dag.NewWorkflowRun(forkTestDef(), "child")
	applyForkStart(&run, payload)
	if run.Steps["a"].Status != dag.StepStatusCompleted || string(run.Steps["a"].Output) != "1" {
		t.Fatalf("a = %+v, want seeded", run.Steps["a"])
	}
	if run.ForkedFrom == nil || run.ForkedFrom.RunID != "parent" {
		t.Fatalf("forked_from = %+v", run.ForkedFrom)
	}
	if run.Steps["s"].Status != dag.StepStatusPending {
		t.Fatalf("s = %v, want pending", run.Steps["s"].Status)
	}
}

func TestApplyForkStart_LeavesOtherStartsAlone(t *testing.T) {
	for _, payload := range []string{`{"workflow_def":{}}`, `not json`, `{"fork":{}}`} {
		run := dag.NewWorkflowRun(forkTestDef(), "child")
		applyForkStart(&run, []byte(payload))
		if run.ForkedFrom != nil || run.Steps["a"].Status != dag.StepStatusPending {
			t.Fatalf("payload %s changed the run: %+v", payload, run)
		}
	}
}
