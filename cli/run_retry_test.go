// cli/run_retry_test.go
// Tests for the retry command.
// Methodology: integration tests with embedded NATS. Manually save a
// snapshot, register a workflow, then retry and verify a new run is
// created with a different ID.
package cli

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/api"
	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
)

func TestRetryCreatesNewRun(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	// Register workflow so StartRun can find the definition.
	svc := api.NewService(nc, observe.NewNoopTelemetry())
	def := dag.WorkflowDef{
		Name:    "retry-test",
		Version: "1.0",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "t1", Timeout: time.Second},
		},
	}
	if err := svc.RegisterWorkflow(
		context.Background(), def,
	); err != nil {
		t.Fatalf("register workflow: %v", err)
	}

	// Manually save a snapshot so GetRun finds the original run.
	js, _ := nc.JetStream()
	store := engine.NewSnapshotStore(js)
	originalRunID := "orig-run-001"
	run := dag.WorkflowRun{
		RunID:      originalRunID,
		WorkflowID: "retry-test",
		Status:     dag.RunStatusFailed,
		Steps: map[string]dag.StepState{
			"s1": {Status: dag.StepStatusFailed},
		},
		CreatedAt: time.Now().UTC(),
	}
	if err := store.Save(run); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	output := captureOutput(func() {
		runRetryCmd([]string{originalRunID})
	})

	// Positive: output should contain the workflow name.
	if !strings.Contains(output, "retry-test") {
		t.Fatalf("expected workflow name in output, got: %s",
			output)
	}

	// Positive: output should contain a new run ID that differs
	// from the original.
	if strings.Contains(output, originalRunID) {
		t.Fatalf("output should not echo original run ID as"+
			" new run: %s", output)
	}

	// Negative: should not contain error text.
	if strings.Contains(output, "Error") {
		t.Fatalf("unexpected error in output: %s", output)
	}
}

func TestRetryJSONOutput(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	svc := api.NewService(nc, observe.NewNoopTelemetry())
	def := dag.WorkflowDef{
		Name:    "retry-json-test",
		Version: "1.0",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "t1", Timeout: time.Second},
		},
	}
	if err := svc.RegisterWorkflow(
		context.Background(), def,
	); err != nil {
		t.Fatalf("register workflow: %v", err)
	}

	js, _ := nc.JetStream()
	store := engine.NewSnapshotStore(js)
	originalRunID := "orig-run-json-001"
	run := dag.WorkflowRun{
		RunID:      originalRunID,
		WorkflowID: "retry-json-test",
		Status:     dag.RunStatusFailed,
		Steps: map[string]dag.StepState{
			"s1": {Status: dag.StepStatusFailed},
		},
		CreatedAt: time.Now().UTC(),
	}
	if err := store.Save(run); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	output := captureOutput(func() {
		runRetryCmd([]string{originalRunID, "--json"})
	})

	// Positive: should be valid JSON with expected fields.
	var result runRetryResult
	if err := json.Unmarshal(
		[]byte(output), &result,
	); err != nil {
		t.Fatalf("output should be valid JSON: %v\n%s",
			err, output)
	}
	if result.OriginalRunID != originalRunID {
		t.Fatalf("expected original_run_id %s, got %s",
			originalRunID, result.OriginalRunID)
	}
	if result.Workflow != "retry-json-test" {
		t.Fatalf("expected workflow retry-json-test, got %s",
			result.Workflow)
	}

	// Positive: new run ID must differ from original.
	if result.NewRunID == originalRunID {
		t.Fatal("new_run_id must differ from original")
	}
	if result.NewRunID == "" {
		t.Fatal("new_run_id must not be empty")
	}

	// Negative: should not contain human-readable text.
	if strings.Contains(output, "Retrying") {
		t.Fatal("JSON output should not contain text message")
	}
}

func TestRetryNonexistentRun(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	// Override exitFunc to prevent os.Exit in tests.
	var exitCode int
	oldExit := exitFunc
	exitFunc = func(code int) { exitCode = code }
	defer func() { exitFunc = oldExit }()

	output := captureOutput(func() {
		runRetryCmd([]string{"nonexistent-run-id"})
	})

	// Positive: exit code should be 1 for missing run.
	if exitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode)
	}

	// Negative: should not contain success text.
	if strings.Contains(output, "Retrying") {
		t.Fatal("should not show success for missing run")
	}
}
