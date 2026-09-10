// api/workflow_delete_triggers_test.go
// Methodology: exercise Service.DeleteWorkflow's trigger-reference
// guard (#607) directly against a real embedded NATS server. The
// guard used to live only in the CLI (cli/workflow_delete.go); it was
// pulled down into the service so REST inherits the identical refusal
// contract (#682 review) -- this file is the direct service-level
// coverage for that move, independent of either caller.
package api

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/trigger"
)

func newTriggerGuardTestService(t *testing.T) *Service {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "triggers"}),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)
	def := dag.WorkflowDef{
		Name:  "wf-trig-guard",
		Steps: []dag.StepDef{{ID: "a", Task: "task-a"}},
	}
	if err := svc.RegisterWorkflow(context.Background(), def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	return svc
}

func TestDeleteWorkflow_RefusesWithReferencingTrigger(t *testing.T) {
	svc := newTriggerGuardTestService(t)
	ctx := context.Background()

	trig := trigger.TriggerDef{
		ID:         "trig-ref",
		WorkflowID: "wf-trig-guard",
		Enabled:    true,
		Cron:       &trigger.CronConfig{Expression: "* * * * *"},
	}
	if err := svc.CreateTrigger(ctx, trig); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}

	err := svc.DeleteWorkflow(ctx, "wf-trig-guard", false)
	// Positive: delete is refused while a trigger references it.
	if err == nil {
		t.Fatal("delete should be refused with referencing trigger")
	}
	var refusal *ErrWorkflowHasTriggers
	if !errors.As(err, &refusal) {
		t.Fatalf("error should be *ErrWorkflowHasTriggers, got: %v (%T)", err, err)
	}
	// Negative: the error names the offending trigger.
	if !strings.Contains(err.Error(), "trig-ref") {
		t.Fatalf("error should name the trigger, got: %v", err)
	}
	// Negative: the workflow must NOT have been deleted.
	if _, gerr := svc.GetWorkflow("wf-trig-guard"); gerr != nil {
		t.Fatal("workflow must survive a refused delete")
	}
}

func TestDeleteWorkflow_ForceBypassesTriggerRefusal(t *testing.T) {
	svc := newTriggerGuardTestService(t)
	ctx := context.Background()

	trig := trigger.TriggerDef{
		ID:         "trig-ref",
		WorkflowID: "wf-trig-guard",
		Enabled:    true,
		Cron:       &trigger.CronConfig{Expression: "* * * * *"},
	}
	if err := svc.CreateTrigger(ctx, trig); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}

	// Positive: force deletes despite the referencing trigger.
	if err := svc.DeleteWorkflow(ctx, "wf-trig-guard", true); err != nil {
		t.Fatalf("force delete: %v", err)
	}
	if _, gerr := svc.GetWorkflow("wf-trig-guard"); gerr == nil {
		t.Fatal("workflow should be deleted with force")
	}
	// Negative: force does NOT cascade-delete the trigger.
	defs, err := svc.ListTriggers(ctx)
	if err != nil {
		t.Fatalf("ListTriggers: %v", err)
	}
	if len(defs) != 1 {
		t.Fatalf("trigger should survive force delete, got %d", len(defs))
	}
}
