// cli/inspect_test.go
// Tests for the unified inspect command.
// Methodology: integration tests with embedded NATS. Verify inspect
// output combines status, failure events, and DLQ entries.
package cli

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"

	"github.com/danmestas/dagnats/api"
	"github.com/danmestas/dagnats/engine"
)

func TestInspectShowsStatusAndFailures(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	js, _ := nc.JetStream()

	// Create a run snapshot so GetRun works
	tel := observe.NewNoopTelemetry()
	store := engine.NewSnapshotStore(js)
	run := dag.WorkflowRun{
		RunID:      "inspect-run-1",
		WorkflowID: "test-wf",
		Status:     dag.RunStatusFailed,
		Steps: map[string]dag.StepState{
			"step-a": {
				Status:   dag.StepStatusFailed,
				Attempts: 2,
				Error:    "connection timeout",
			},
		},
		CreatedAt: time.Now().UTC(),
	}
	if err := store.Save(run); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	// Publish a step.failed event
	evt := protocol.Event{
		Type:      protocol.EventStepFailed,
		RunID:     "inspect-run-1",
		StepID:    "step-a",
		Timestamp: time.Now().UTC(),
		Payload:   json.RawMessage(`{"error":"connection timeout"}`),
	}
	evtData, _ := evt.Marshal()
	js.Publish("history.inspect-run-1", evtData)

	// Publish a dead letter for this run
	dlPayload, _ := json.Marshal(map[string]interface{}{
		"run_id":  "inspect-run-1",
		"step_id": "step-a",
	})
	js.Publish("dead.failing-task", dlPayload)

	_ = tel // silence unused

	output := captureOutput(func() {
		runInspectCmd([]string{"inspect-run-1"})
	})

	// Positive: should contain run status info
	if !strings.Contains(output, "inspect-run-1") {
		t.Fatal("output should contain run ID")
	}
	if !strings.Contains(output, "connection timeout") {
		t.Fatal("output should contain step error")
	}

	// Positive: should contain failures section
	if !strings.Contains(output, "step.failed") {
		t.Fatal("output should contain failure events")
	}

	// Negative: should not contain unrelated data
	if strings.Contains(output, "phantom") {
		t.Fatal("output should not contain phantom data")
	}
}

func TestInspectResultJSONSerialization(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	result := inspectResult{
		Run: dag.WorkflowRun{
			RunID:      "json-inspect-1",
			WorkflowID: "test-wf",
			Status:     dag.RunStatusFailed,
			Steps: map[string]dag.StepState{
				"step-a": {
					Status:   dag.StepStatusFailed,
					Attempts: 2,
					Error:    "timeout",
				},
			},
			CreatedAt: time.Date(
				2026, 4, 1, 12, 0, 0, 0, time.UTC,
			),
		},
		Failures: []api.RunEvent{
			{
				Type:   "step.failed",
				RunID:  "json-inspect-1",
				StepID: "step-a",
				Timestamp: time.Date(
					2026, 4, 1, 12, 1, 0, 0, time.UTC,
				),
				Data: "timeout",
			},
		},
		DeadLetters: []api.DeadLetter{
			{
				Sequence: 1,
				RunID:    "json-inspect-1",
				StepID:   "step-a",
				Task:     "failing-task",
				Error:    "timeout",
			},
		},
	}

	var buf strings.Builder
	err := FormatJSON(&buf, result)
	if err != nil {
		t.Fatalf("FormatJSON failed: %v", err)
	}
	output := buf.String()

	// Positive: should contain all three sections
	if !strings.Contains(output, `"run"`) {
		t.Fatal("JSON should contain run section")
	}
	if !strings.Contains(output, `"failures"`) {
		t.Fatal("JSON should contain failures section")
	}
	if !strings.Contains(output, `"dead_letters"`) {
		t.Fatal("JSON should contain dead_letters section")
	}
	if !strings.Contains(output, "json-inspect-1") {
		t.Fatal("JSON should contain run ID")
	}

	// Negative: should not contain human-readable formatting
	if strings.Contains(output, "Run:") {
		t.Fatal("JSON should not contain human format")
	}
}

func TestInspectResultOmitsEmptySections(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	result := inspectResult{
		Run: dag.WorkflowRun{
			RunID:      "clean-json-1",
			WorkflowID: "clean-wf",
			Status:     dag.RunStatusCompleted,
			Steps: map[string]dag.StepState{
				"a": {
					Status:   dag.StepStatusCompleted,
					Attempts: 1,
				},
			},
			CreatedAt: time.Date(
				2026, 4, 1, 12, 0, 0, 0, time.UTC,
			),
		},
	}

	var buf strings.Builder
	err := FormatJSON(&buf, result)
	if err != nil {
		t.Fatalf("FormatJSON failed: %v", err)
	}
	output := buf.String()

	// Positive: should contain run data
	if !strings.Contains(output, "clean-json-1") {
		t.Fatal("JSON should contain run ID")
	}

	// Negative: omitempty should exclude empty sections
	if strings.Contains(output, "failures") {
		t.Fatal("JSON should omit empty failures")
	}
	if strings.Contains(output, "dead_letters") {
		t.Fatal("JSON should omit empty dead_letters")
	}
}

func TestInspectCleanRunShowsNoFailures(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	js, _ := nc.JetStream()

	svc := api.NewService(nc, observe.NewNoopTelemetry())
	wb := dag.NewWorkflow("clean-wf")
	wb.Task("a", "task-a")
	def, _ := wb.Build()
	svc.RegisterWorkflow(context.Background(), def)

	store := engine.NewSnapshotStore(js)
	run := dag.WorkflowRun{
		RunID:      "clean-run-1",
		WorkflowID: "clean-wf",
		Status:     dag.RunStatusCompleted,
		Steps: map[string]dag.StepState{
			"a": {
				Status:   dag.StepStatusCompleted,
				Attempts: 1,
			},
		},
		CreatedAt: time.Now().UTC(),
	}
	store.Save(run)

	output := captureOutput(func() {
		runInspectCmd([]string{"clean-run-1"})
	})

	// Positive: should show completed status
	if !strings.Contains(output, "completed") {
		t.Fatal("output should contain completed status")
	}

	// Negative: should not contain Failures or Dead Letters sections
	if strings.Contains(output, "Failures:") {
		t.Fatal("clean run should not show Failures section")
	}
	if strings.Contains(output, "Dead Letters:") {
		t.Fatal("clean run should not show Dead Letters section")
	}
}
