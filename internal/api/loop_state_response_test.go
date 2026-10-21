// internal/api/loop_state_response_test.go
// Run API responses leave out each step's committed agent-loop state
// (#801). Methodology: a run snapshot with loop_state is saved straight
// into the store, then read back through GetRunResponse and ScanRuns.
package api

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestRunResponsesOmitLoopState(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	wb := dag.NewWorkflow("loop-state-api")
	wb.AgentLoop("agent", "agent-task").WithMaxIterations(3)
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	run := dag.NewWorkflowRun(wfDef, "run-ls")
	run.Status = dag.RunStatusRunning
	run.Steps["agent"] = dag.StepState{
		Status: dag.StepStatusRunning, Attempts: 1, Iterations: 1,
		LoopState: []byte(`{"secret":"working memory"}`),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := engine.NewSnapshotStore(js).SaveInitial(ctx, run); err != nil {
		t.Fatalf("SaveInitial: %v", err)
	}
	svc := NewService(nc)

	resp, err := svc.GetRunResponse(ctx, "run-ls")
	if err != nil {
		t.Fatalf("GetRunResponse: %v", err)
	}
	body, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(body), "loop_state") || strings.Contains(string(body), "working memory") {
		t.Fatalf("GetRunResponse leaks loop state: %s", body)
	}
	if resp.Steps["agent"].Iterations != 1 {
		t.Fatalf("response lost other step fields: %+v", resp.Steps["agent"])
	}

	runs, err := svc.ScanRuns(ctx, RunsFilter{}, 10)
	if err != nil {
		t.Fatalf("ScanRuns: %v", err)
	}
	if len(runs) != 1 || len(runs[0].Steps["agent"].LoopState) != 0 {
		t.Fatalf("ScanRuns = %d runs, loop_state=%q; want 1 run with no loop state",
			len(runs), runs[0].Steps["agent"].LoopState)
	}
	// The stored snapshot still holds it.
	stored, err := engine.NewSnapshotStore(js).Load(ctx, "run-ls")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(stored.Steps["agent"].LoopState) == 0 {
		t.Fatal("stripping the response must not touch the stored snapshot")
	}
}
