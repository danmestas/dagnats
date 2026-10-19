// engine/output_schema_test.go
// Methodology: real embedded NATS server, real orchestrator. Start a
// one-step run, publish the step's completion with a chosen output, and
// assert the run's terminal state (#784 item 5): a final output that
// matches the workflow OutputSchema completes the run; a mismatch fails
// it with the violation (path + reason) as the failure reason; a workflow
// without a schema is unaffected. Each test owns its server.
package engine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

// runOneStepWithOutput registers a one-step workflow with the given
// OutputSchema (may be empty), runs it, completes step "a" with output,
// and returns the store once the run is terminal.
func runOneStepWithOutput(
	t *testing.T, schema string, output string,
) (*SnapshotStore, dag.WorkflowRun) {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	wfDef := dag.WorkflowDef{
		Name: "out-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	if schema != "" {
		wfDef.OutputSchema = json.RawMessage(schema)
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)
	mustPut(t, defKV, dag.DefVersionKey(wfDef.Name, dag.DefHash(wfDef)), defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(orch.Stop)

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "out-run",
		mustMarshal(t, map[string]any{
			"workflow_def": wfDef, "input": json.RawMessage(`{}`),
		}),
	)
	startData, _ := startEvt.Marshal()
	mustPublish(t, js, startEvt.NATSSubject(), startData)

	store := NewSnapshotStore(jsNew)
	waitForStepStatus(t, store, "out-run", "a", dag.StepStatusQueued,
		10*time.Second)
	doneEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "out-run", "a", []byte(output))
	doneData, _ := doneEvt.Marshal()
	mustPublish(t, js, doneEvt.NATSSubject(), doneData)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Load(context.Background(), "out-run")
		if err == nil && run.Status.IsTerminal() {
			return store, run
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("run did not reach a terminal status")
	return nil, dag.WorkflowRun{}
}

const outSchemaRequiresOK = `{"type":"object","required":["ok"],` +
	`"properties":{"ok":{"type":"boolean"}}}`

func TestOutputSchemaMatchCompletesRun(t *testing.T) {
	_, run := runOneStepWithOutput(t, outSchemaRequiresOK, `{"ok":true}`)
	if run.Status != dag.RunStatusCompleted {
		t.Fatalf("status = %v, want Completed", run.Status)
	}
	if run.Steps["a"].Status != dag.StepStatusCompleted {
		t.Fatalf("step a = %v, want Completed", run.Steps["a"].Status)
	}
}

func TestOutputSchemaMismatchFailsRun(t *testing.T) {
	_, run := runOneStepWithOutput(t, outSchemaRequiresOK, `{"ok":"yes"}`)
	if run.Status != dag.RunStatusFailed {
		t.Fatalf("status = %v, want Failed", run.Status)
	}
	got := run.Steps["a"].Error
	if !strings.Contains(got, "output schema violation") ||
		!strings.Contains(got, "ok") {
		t.Fatalf("error should name the violation and path, got %q", got)
	}
	if run.CompletedAt == nil {
		t.Fatal("failed run must carry CompletedAt")
	}
}

func TestOutputSchemaAbsentLeavesRunUnaffected(t *testing.T) {
	_, run := runOneStepWithOutput(t, "", `{"anything":"goes"}`)
	if run.Status != dag.RunStatusCompleted {
		t.Fatalf("status = %v, want Completed", run.Status)
	}
	if run.Steps["a"].Error != "" {
		t.Fatalf("unexpected step error %q", run.Steps["a"].Error)
	}
}
