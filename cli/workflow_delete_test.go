// cli/workflow_delete_test.go
// Methodology: exercise the guarded `workflow delete` core against a
// real embedded NATS server. Delete removes the definition only; it
// refuses when a trigger still references the workflow (#607) or when
// a run for it is still non-terminal (#682), in both cases unless
// --force is passed, and errors on an unknown name. The os.Exit-bearing
// CLI wrapper is kept thin; the decision logic lives in deleteWorkflow,
// which returns errors so the tests can assert without spawning a
// process.
package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/nats-io/nats.go/jetstream"
)

func newDeleteTestService(t *testing.T) (*api.Service, string) {
	t.Helper()
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "triggers"}),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	svc := api.NewService(nc)
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	t.Cleanup(cancel)
	def := dag.WorkflowDef{
		Name:  "wf-del",
		Steps: []dag.StepDef{{ID: "a", Task: "task-a"}},
	}
	if err := svc.RegisterWorkflow(ctx, def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	return svc, srv.ClientURL()
}

func TestDeleteWorkflow_HappyPath(t *testing.T) {
	svc, _ := newDeleteTestService(t)
	ctx := context.Background()

	if err := deleteWorkflow(ctx, svc, "wf-del", false); err != nil {
		t.Fatalf("deleteWorkflow: %v", err)
	}
	// Positive: workflow is gone.
	if _, err := svc.GetWorkflow("wf-del"); err == nil {
		t.Fatal("workflow should be deleted")
	}
	// Negative: a second delete now errors (already gone).
	if err := deleteWorkflow(ctx, svc, "wf-del", false); err == nil {
		t.Fatal("second delete should error on missing workflow")
	}
}

func TestDeleteWorkflow_RefusesWithReferencingTrigger(t *testing.T) {
	svc, _ := newDeleteTestService(t)
	ctx := context.Background()

	trig := trigger.TriggerDef{
		ID:         "trig-ref",
		WorkflowID: "wf-del",
		Enabled:    true,
		Cron:       &trigger.CronConfig{Expression: "* * * * *"},
	}
	if err := svc.CreateTrigger(ctx, trig); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}

	err := deleteWorkflow(ctx, svc, "wf-del", false)
	// Positive: delete is refused while a trigger references it.
	if err == nil {
		t.Fatal("delete should be refused with referencing trigger")
	}
	// Negative: the error names the offending trigger.
	if !strings.Contains(err.Error(), "trig-ref") {
		t.Fatalf("error should name the trigger, got: %v", err)
	}
	// Negative: the workflow must NOT have been deleted.
	if _, gerr := svc.GetWorkflow("wf-del"); gerr != nil {
		t.Fatal("workflow must survive a refused delete")
	}
}

func TestDeleteWorkflow_ForceBypassesRefusal(t *testing.T) {
	svc, _ := newDeleteTestService(t)
	ctx := context.Background()

	trig := trigger.TriggerDef{
		ID:         "trig-ref",
		WorkflowID: "wf-del",
		Enabled:    true,
		Cron:       &trigger.CronConfig{Expression: "* * * * *"},
	}
	if err := svc.CreateTrigger(ctx, trig); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}

	// Positive: --force deletes despite the referencing trigger.
	if err := deleteWorkflow(ctx, svc, "wf-del", true); err != nil {
		t.Fatalf("force delete: %v", err)
	}
	if _, gerr := svc.GetWorkflow("wf-del"); gerr == nil {
		t.Fatal("workflow should be deleted with --force")
	}
	// Negative: --force does NOT cascade-delete the trigger (out of
	// scope per #607); it only bypasses the refusal.
	defs, err := svc.ListTriggers(ctx)
	if err != nil {
		t.Fatalf("ListTriggers: %v", err)
	}
	if len(defs) != 1 {
		t.Fatalf("trigger should survive force delete, got %d", len(defs))
	}
}

// newDeleteTestServiceWithStore is newDeleteTestService plus a
// SnapshotStore over the same NATS connection, for tests that need to
// seed run snapshots (#682's non-terminal-run guard).
func newDeleteTestServiceWithStore(
	t *testing.T,
) (*api.Service, *engine.SnapshotStore) {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "triggers"}),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	svc := api.NewService(nc)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	store := engine.NewSnapshotStore(js)
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	t.Cleanup(cancel)
	def := dag.WorkflowDef{
		Name:  "wf-del",
		Steps: []dag.StepDef{{ID: "a", Task: "task-a"}},
	}
	if err := svc.RegisterWorkflow(ctx, def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	return svc, store
}

func TestDeleteWorkflow_RefusesWithNonTerminalRun(t *testing.T) {
	svc, store := newDeleteTestServiceWithStore(t)
	ctx := context.Background()

	run := dag.WorkflowRun{
		RunID: "run-active-1", WorkflowID: "wf-del",
		Status: dag.RunStatusRunning,
	}
	if err := store.SaveInitial(ctx, run); err != nil {
		t.Fatalf("SaveInitial: %v", err)
	}

	err := deleteWorkflow(ctx, svc, "wf-del", false)
	// Positive: delete is refused while a run is non-terminal.
	if err == nil {
		t.Fatal("delete should be refused with a non-terminal run")
	}
	var refusal *api.ErrWorkflowHasNonTerminalRuns
	if !errors.As(err, &refusal) {
		t.Fatalf("error should be *api.ErrWorkflowHasNonTerminalRuns, got: %v", err)
	}
	// Negative: the error names the offending run.
	if !strings.Contains(err.Error(), "run-active-1") {
		t.Fatalf("error should name the run, got: %v", err)
	}
	// Negative: the workflow must NOT have been deleted.
	if _, gerr := svc.GetWorkflow("wf-del"); gerr != nil {
		t.Fatal("workflow must survive a refused delete")
	}
}

func TestDeleteWorkflow_ForceBypassesNonTerminalRunRefusal(t *testing.T) {
	svc, store := newDeleteTestServiceWithStore(t)
	ctx := context.Background()

	run := dag.WorkflowRun{
		RunID: "run-active-1", WorkflowID: "wf-del",
		Status: dag.RunStatusRunning,
	}
	if err := store.SaveInitial(ctx, run); err != nil {
		t.Fatalf("SaveInitial: %v", err)
	}

	// Positive: --force deletes despite the non-terminal run.
	if err := deleteWorkflow(ctx, svc, "wf-del", true); err != nil {
		t.Fatalf("force delete: %v", err)
	}
	if _, gerr := svc.GetWorkflow("wf-del"); gerr == nil {
		t.Fatal("workflow should be deleted with --force")
	}
	// Negative: --force does not touch the run itself.
	got, err := store.Load(ctx, "run-active-1")
	if err != nil {
		t.Fatalf("run should survive force delete: %v", err)
	}
	if got.Status != dag.RunStatusRunning {
		t.Fatalf("run status changed by force delete: %v", got.Status)
	}
}

func TestWorkflowDelete_JSONOutput(t *testing.T) {
	svc, url := newDeleteTestService(t)
	_ = svc
	t.Setenv("NATS_URL", url)

	var buf bytes.Buffer
	runWorkflowDeleteCmdWithWriter([]string{"wf-del", "--json"}, &buf)

	// Positive: JSON mode emits a structured deletion result.
	var result workflowDeleteResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON output: %v (%s)", err, buf.String())
	}
	if result.Action != "deleted" || result.Name != "wf-del" {
		t.Fatalf("unexpected result: %#v", result)
	}
	// Negative: JSON mode must not leak human text.
	if bytes.Contains(buf.Bytes(), []byte("Workflow deleted")) {
		t.Fatal("JSON mode should not contain human text")
	}
}
