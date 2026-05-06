// cli/workflow_register_trigger_test.go
// Tests for the embedded-trigger handling on `workflow register`
// (issue #171). Methodology: write a workflow JSON file with a
// `triggers` block, run the register command, query svc.ListTriggers
// to verify the trigger landed in KV.
package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/natsutil"
)

const workflowWithEmbeddedTrigger = `{
	"name": "wf-with-trigger",
	"version": "1.0",
	"triggers": [
		{
			"id": "test-cron",
			"enabled": true,
			"cron": {
				"expression": "*/5 * * * *",
				"timezone": "UTC",
				"backfill": false
			}
		}
	],
	"steps": [
		{"id": "a", "task": "task-a"}
	]
}`

const workflowWithBadEmbeddedCron = `{
	"name": "wf-bad-cron",
	"version": "1.0",
	"triggers": [
		{
			"id": "bad-cron",
			"enabled": true,
			"cron": {
				"expression": "this is not a cron",
				"timezone": "UTC",
				"backfill": false
			}
		}
	],
	"steps": [
		{"id": "a", "task": "task-a"}
	]
}`

func TestWorkflowRegister_HonorsEmbeddedTrigger(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "triggers"}),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	defer nc.Close()
	t.Setenv("NATS_URL", srv.ClientURL())

	tmpFile := filepath.Join(t.TempDir(), "wf.json")
	if err := os.WriteFile(
		tmpFile, []byte(workflowWithEmbeddedTrigger), 0644,
	); err != nil {
		t.Fatalf("write: %v", err)
	}

	out := captureOutput(func() {
		runWorkflowRegisterCmd([]string{tmpFile})
	})
	// Positive: register succeeded.
	if !strings.Contains(out, "created") {
		t.Fatalf("register should report created, got: %s", out)
	}

	svc := api.NewService(nc)
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	defs, err := svc.ListTriggers(ctx)
	if err != nil {
		t.Fatalf("ListTriggers: %v", err)
	}
	// Positive: exactly one trigger appears.
	if len(defs) != 1 {
		t.Fatalf("expected 1 trigger, got %d: %#v", len(defs), defs)
	}
	got := defs[0]
	// Positive: trigger fields populated correctly.
	if got.ID != "test-cron" {
		t.Fatalf("trigger ID = %q, want %q", got.ID, "test-cron")
	}
	if got.WorkflowID != "wf-with-trigger" {
		t.Fatalf("workflow_id = %q, want auto-filled %q",
			got.WorkflowID, "wf-with-trigger")
	}
	if got.Cron == nil || got.Cron.Expression != "*/5 * * * *" {
		t.Fatalf("cron = %#v, want expression */5 * * * *", got.Cron)
	}
	if !got.Enabled {
		t.Fatal("trigger should be enabled")
	}
}

func TestWorkflowRegister_EmbeddedTriggerIdempotent(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "triggers"}),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	defer nc.Close()
	t.Setenv("NATS_URL", srv.ClientURL())

	tmpFile := filepath.Join(t.TempDir(), "wf.json")
	if err := os.WriteFile(
		tmpFile, []byte(workflowWithEmbeddedTrigger), 0644,
	); err != nil {
		t.Fatalf("write: %v", err)
	}

	captureOutput(func() {
		runWorkflowRegisterCmd([]string{tmpFile})
	})
	captureOutput(func() {
		runWorkflowRegisterCmd([]string{tmpFile})
	})

	svc := api.NewService(nc)
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	defs, err := svc.ListTriggers(ctx)
	if err != nil {
		t.Fatalf("ListTriggers: %v", err)
	}
	// Positive: exactly one trigger after two registrations.
	if len(defs) != 1 {
		t.Fatalf("expected 1 trigger after re-register, got %d", len(defs))
	}
}

const workflowWithMismatchedTriggerWorkflowID = `{
	"name": "wf-parent",
	"version": "1.0",
	"triggers": [
		{
			"id": "test-cron",
			"workflow_id": "different-wf",
			"enabled": true,
			"cron": {
				"expression": "*/5 * * * *",
				"timezone": "UTC",
				"backfill": false
			}
		}
	],
	"steps": [
		{"id": "a", "task": "task-a"}
	]
}`

func TestRegisterWorkflowWithTriggers_RejectsMismatchedWorkflowID(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "triggers"}),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	defer nc.Close()
	svc := api.NewService(nc)
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	wf, err := parseWorkflowFile(
		[]byte(workflowWithMismatchedTriggerWorkflowID),
	)
	if err != nil {
		t.Fatalf("parseWorkflowFile: %v", err)
	}
	err = registerWorkflowWithTriggers(ctx, svc, wf)
	// Positive: errors when workflow_id doesn't match parent.
	if err == nil {
		t.Fatal("expected error for mismatched workflow_id")
	}
	// Positive: error message mentions the mismatch.
	if !strings.Contains(err.Error(), "does not match parent") {
		t.Fatalf("error should mention mismatch, got: %v", err)
	}

	// Workflow must NOT be stored.
	if _, gerr := svc.GetWorkflow("wf-parent"); gerr == nil {
		t.Fatal("workflow should not be stored when trigger mismatches")
	}
}

// TestRegisterWorkflowWithTriggers_AtomicOnInvalidTrigger exercises the
// pure helper directly to avoid the os.Exit path in runWorkflowRegisterCmd.
// Verifies that an invalid trigger leaves NO state behind: workflow is not
// registered, triggers KV is empty.
func TestRegisterWorkflowWithTriggers_AtomicOnInvalidTrigger(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "triggers"}),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	defer nc.Close()
	svc := api.NewService(nc)
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	wf, err := parseWorkflowFile([]byte(workflowWithBadEmbeddedCron))
	if err != nil {
		t.Fatalf("parseWorkflowFile: %v", err)
	}
	err = registerWorkflowWithTriggers(ctx, svc, wf)
	// Positive: returns an error.
	if err == nil {
		t.Fatal("expected error for invalid embedded cron trigger")
	}

	// Atomicity: workflow must NOT be in the defs KV.
	if _, gerr := svc.GetWorkflow("wf-bad-cron"); gerr == nil {
		t.Fatal("workflow should not be stored when trigger is invalid")
	}

	// Atomicity: no triggers created. ListTriggers may return a
	// "no keys found" error from JetStream KV when the bucket is
	// empty — treat that as len(defs) == 0.
	defs, lerr := svc.ListTriggers(ctx)
	if lerr != nil && !strings.Contains(lerr.Error(), "no keys found") {
		t.Fatalf("ListTriggers: %v", lerr)
	}
	if len(defs) != 0 {
		t.Fatalf("expected 0 triggers after failed register, got %d", len(defs))
	}
}
