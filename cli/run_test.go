// cli/run_test.go
// Tests for CLI output formatting and run commands.
// Methodology: unit test formatting functions, integration test commands
// with embedded NATS to verify event publishing and --output flag.
package cli

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/api"
	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestFormatRunStatus(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	run := dag.WorkflowRun{
		RunID: "abc123", WorkflowID: "test-wf", Status: dag.RunStatusRunning,
		Steps: map[string]dag.StepState{
			"a": {Status: dag.StepStatusCompleted, Attempts: 1},
			"b": {Status: dag.StepStatusRunning, Attempts: 1},
		},
		CreatedAt: time.Date(2026, 3, 29, 12, 0, 0, 0, time.UTC),
	}
	output := FormatRunStatus(run)
	if !strings.Contains(output, "abc123") {
		t.Fatal("output should contain run ID")
	}
	if !strings.Contains(output, "running") {
		t.Fatal("output should contain status")
	}
	if !strings.Contains(output, "test-wf") {
		t.Fatal("output should contain workflow name")
	}
	if strings.Contains(output, "map[") {
		t.Fatal("output should not contain raw Go map syntax")
	}
}

func TestCancelCommandPublishesEvent(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	// Set NATS_URL env var for the CLI to use
	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	js, _ := nc.JetStream()

	// Subscribe to history stream to catch cancel event
	sub, _ := js.SubscribeSync("history.>",
		nats.AckExplicit(), nats.DeliverAll())

	cancelRunID := "aabbccdd11223344aabbccdd11223344"

	// Run cancel command
	runCancelCmd([]string{cancelRunID})

	// Positive: cancel event should be published
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("cancel event not published: %v", err)
	}

	evt, err := protocol.UnmarshalEvent(msg.Data)
	if err != nil {
		t.Fatalf("unmarshal event failed: %v", err)
	}
	if evt.Type != protocol.EventWorkflowCancelled {
		t.Fatalf(
			"expected EventWorkflowCancelled, got %s", evt.Type,
		)
	}
	if evt.RunID != cancelRunID {
		t.Fatalf("expected RunID %s, got %s",
			cancelRunID, evt.RunID)
	}

	// Negative: no second event should be published
	_, err = sub.NextMsg(500 * time.Millisecond)
	if err == nil {
		t.Fatal("unexpected second event published")
	}
}

func TestFormatRunStatusShowsStepErrors(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	run := dag.WorkflowRun{
		RunID: "err-run", WorkflowID: "wf-err",
		Status: dag.RunStatusFailed,
		Steps: map[string]dag.StepState{
			"ok-step": {
				Status: dag.StepStatusCompleted, Attempts: 1,
			},
			"bad-step": {
				Status: dag.StepStatusFailed, Attempts: 3,
				Error: "connection refused",
			},
		},
		CreatedAt: time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
	}
	output := FormatRunStatus(run)

	// Positive: failed step error should be visible
	if !strings.Contains(output, "connection refused") {
		t.Fatal("output should contain step error message")
	}

	// Negative: completed step should not show error text
	if strings.Contains(output, "ok-step") &&
		strings.Contains(output, "error:") {
		// Check the ok-step line doesn't have error
		for _, line := range strings.Split(output, "\n") {
			if strings.Contains(line, "ok-step") &&
				strings.Contains(line, "error:") {
				t.Fatal("completed step should not show error")
			}
		}
	}
}

func TestFormatRunStatusShowsIterations(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	run := dag.WorkflowRun{
		RunID: "loop-run", WorkflowID: "wf-loop",
		Status: dag.RunStatusRunning,
		Steps: map[string]dag.StepState{
			"loop-step": {
				Status: dag.StepStatusRunning, Attempts: 1,
				Iterations: 5,
			},
			"plain-step": {
				Status: dag.StepStatusCompleted, Attempts: 1,
			},
		},
		CreatedAt: time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
	}
	output := FormatRunStatus(run)

	// Positive: loop step should show iteration count
	if !strings.Contains(output, "iterations: 5") {
		t.Fatal("output should contain iteration count")
	}

	// Negative: plain step should not show iterations
	for _, line := range strings.Split(output, "\n") {
		if strings.Contains(line, "plain-step") &&
			strings.Contains(line, "iterations") {
			t.Fatal("plain step should not show iterations")
		}
	}
}

func TestRunEventsTypeFilter(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	filterRunID1 := "ff110000111111112222222233333333"
	js, _ := nc.JetStream()
	publishTestEvent(t, js, filterRunID1,
		protocol.EventStepQueued, "step-a")
	publishTestEvent(t, js, filterRunID1,
		protocol.EventStepFailed, "step-a")
	publishTestEvent(t, js, filterRunID1,
		protocol.EventStepCompleted, "step-b")

	output := captureOutput(func() {
		runEventsCmd([]string{
			filterRunID1, "--type=step.failed",
		})
	})

	// Positive: should contain the filtered type
	if !strings.Contains(output, "step.failed") {
		t.Fatal("output should contain step.failed")
	}
	// Negative: should not contain other types
	if strings.Contains(output, "step.queued") {
		t.Fatal("output should not contain step.queued")
	}
	if strings.Contains(output, "step.completed") {
		t.Fatal("output should not contain step.completed")
	}
}

func TestRunEventsStepFilter(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	filterRunID2 := "ff220000111111112222222233333333"
	js, _ := nc.JetStream()
	publishTestEvent(t, js, filterRunID2,
		protocol.EventStepQueued, "step-a")
	publishTestEvent(t, js, filterRunID2,
		protocol.EventStepQueued, "step-b")

	output := captureOutput(func() {
		runEventsCmd([]string{
			filterRunID2, "--step=step-a",
		})
	})

	// Positive: should contain step-a
	if !strings.Contains(output, "step-a") {
		t.Fatal("output should contain step-a")
	}
	// Negative: should not contain step-b
	if strings.Contains(output, "step-b") {
		t.Fatal("output should not contain step-b")
	}
}

// publishTestEvent publishes a protocol.Event to the history stream.
func publishTestEvent(
	t *testing.T, js nats.JetStreamContext,
	runID string, evtType protocol.EventType, stepID string,
) {
	t.Helper()
	evt := protocol.Event{
		Type:      evtType,
		RunID:     runID,
		StepID:    stepID,
		Timestamp: time.Now().UTC(),
	}
	data, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	_, err = js.Publish("history."+runID, data)
	if err != nil {
		t.Fatalf("publish event: %v", err)
	}
}

func TestRunStatusJSONOutput(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	run := dag.WorkflowRun{
		RunID: "json-run", WorkflowID: "wf-json",
		Status: dag.RunStatusRunning,
		Steps: map[string]dag.StepState{
			"step-a": {Status: dag.StepStatusCompleted, Attempts: 1},
		},
		CreatedAt: time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
	}

	var buf strings.Builder
	err := FormatJSON(&buf, run)
	if err != nil {
		t.Fatalf("FormatJSON failed: %v", err)
	}
	output := buf.String()

	// Positive: output should be valid JSON containing run_id
	if !strings.Contains(output, `"run_id"`) {
		t.Fatal("JSON output should contain run_id field")
	}
	if !strings.Contains(output, "json-run") {
		t.Fatal("JSON output should contain run ID value")
	}

	// Negative: output should not contain table formatting
	if strings.Contains(output, "Run:") {
		t.Fatal("JSON output should not contain table format")
	}
}

func TestRunListJSONOutput(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	runs := []dag.WorkflowRun{
		{
			RunID: "list-1", WorkflowID: "wf-a",
			Status: dag.RunStatusCompleted,
			Steps:  map[string]dag.StepState{},
			CreatedAt: time.Date(
				2026, 4, 1, 12, 0, 0, 0, time.UTC,
			),
		},
		{
			RunID: "list-2", WorkflowID: "wf-b",
			Status: dag.RunStatusRunning,
			Steps:  map[string]dag.StepState{},
			CreatedAt: time.Date(
				2026, 4, 1, 13, 0, 0, 0, time.UTC,
			),
		},
	}

	var buf strings.Builder
	err := FormatJSON(&buf, runs)
	if err != nil {
		t.Fatalf("FormatJSON failed: %v", err)
	}
	output := buf.String()

	// Positive: output should contain both run IDs
	if !strings.Contains(output, "list-1") {
		t.Fatal("JSON output should contain first run ID")
	}
	if !strings.Contains(output, "list-2") {
		t.Fatal("JSON output should contain second run ID")
	}

	// Negative: output should not contain table headers
	if strings.Contains(output, "RUN_ID") {
		t.Fatal("JSON output should not contain table headers")
	}
}

func TestHasJSONFlagIntegration(t *testing.T) {
	// Positive: --json should be detected
	if !HasJSONFlag([]string{"--json", "--status=running"}) {
		t.Fatal("should detect --json flag")
	}

	// Negative: absent --json should return false
	if HasJSONFlag([]string{"--status=running"}) {
		t.Fatal("should not detect --json when absent")
	}
}

func TestStripJSONFlagPreservesOtherArgs(t *testing.T) {
	args := StripJSONFlag(
		[]string{"--json", "--workflow=wf", "--status=ok"},
	)

	// Positive: other args preserved
	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(args))
	}

	// Negative: --json should be removed
	for _, arg := range args {
		if arg == "--json" {
			t.Fatal("--json should have been stripped")
		}
	}
}

func TestRunEventsJSONOutput(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	events := []api.RunEvent{
		{
			Type:      "step.queued",
			RunID:     "evt-run-1",
			StepID:    "step-a",
			Timestamp: time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
			Data:      "queued",
		},
		{
			Type:      "step.completed",
			RunID:     "evt-run-1",
			StepID:    "step-a",
			Timestamp: time.Date(2026, 4, 1, 12, 1, 0, 0, time.UTC),
			Data:      "done",
		},
	}

	var buf strings.Builder
	err := FormatJSON(&buf, events)
	if err != nil {
		t.Fatalf("FormatJSON failed: %v", err)
	}
	output := buf.String()

	// Positive: output should contain event fields
	if !strings.Contains(output, `"type"`) {
		t.Fatal("JSON output should contain type field")
	}
	if !strings.Contains(output, "step.queued") {
		t.Fatal("JSON output should contain event type value")
	}
	if !strings.Contains(output, "step-a") {
		t.Fatal("JSON output should contain step ID")
	}

	// Negative: output should not contain table headers
	if strings.Contains(output, "TIMESTAMP") {
		t.Fatal("JSON output should not contain table headers")
	}
}

func TestRunStartJSONOutput(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	result := runStartResult{RunID: "start-abc"}

	var buf strings.Builder
	err := FormatJSON(&buf, result)
	if err != nil {
		t.Fatalf("FormatJSON failed: %v", err)
	}
	output := buf.String()

	// Positive: output should contain run_id field
	if !strings.Contains(output, `"run_id"`) {
		t.Fatal("JSON output should contain run_id field")
	}
	if !strings.Contains(output, "start-abc") {
		t.Fatal("JSON output should contain run ID value")
	}

	// Negative: should not contain human-readable prefix
	if strings.Contains(output, "Started:") {
		t.Fatal("JSON output should not contain human text")
	}
}

func TestRunCancelJSONOutput(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	result := runCancelResult{
		RunID:     "cancel-xyz",
		Cancelled: true,
	}

	var buf strings.Builder
	err := FormatJSON(&buf, result)
	if err != nil {
		t.Fatalf("FormatJSON failed: %v", err)
	}
	output := buf.String()

	// Positive: output should contain expected fields
	if !strings.Contains(output, `"run_id"`) {
		t.Fatal("JSON output should contain run_id field")
	}
	if !strings.Contains(output, `"cancelled": true`) {
		t.Fatal("JSON output should contain cancelled field")
	}

	// Negative: should not contain human text
	if strings.Contains(output, "Cancelled:") {
		t.Fatal("JSON output should not contain human text")
	}
}

func TestFormatRunStatusWithDefShowsRetryMax(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	run := dag.WorkflowRun{
		RunID: "retry-run", WorkflowID: "wf-retry",
		Status: dag.RunStatusFailed,
		Steps: map[string]dag.StepState{
			"fetch": {
				Status: dag.StepStatusFailed, Attempts: 3,
				Error: "timeout",
			},
			"parse": {
				Status: dag.StepStatusCompleted, Attempts: 1,
			},
		},
		CreatedAt: time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
	}
	def := &dag.WorkflowDef{
		Name:    "wf-retry",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "fetch",
				Task: "fetch-task",
				Retry: &dag.RetryPolicy{
					MaxAttempts: 4,
				},
			},
			{
				ID:   "parse",
				Task: "parse-task",
			},
		},
	}
	output := FormatRunStatusWithDef(run, def)

	// Positive: failed step with retry policy shows max attempts
	if !strings.Contains(output, "attempts: 3/5") {
		t.Fatalf(
			"expected 'attempts: 3/5', got:\n%s", output,
		)
	}

	// Negative: step without retry policy shows plain attempts
	for _, line := range strings.Split(output, "\n") {
		if strings.Contains(line, "parse") &&
			strings.Contains(line, "/") {
			t.Fatal("step without retry should not show max")
		}
	}
}

func TestFormatRunStatusWithDefNilDef(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	run := dag.WorkflowRun{
		RunID: "nil-def", WorkflowID: "wf-nil",
		Status: dag.RunStatusRunning,
		Steps: map[string]dag.StepState{
			"step-a": {
				Status: dag.StepStatusRunning, Attempts: 2,
			},
		},
		CreatedAt: time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
	}
	output := FormatRunStatusWithDef(run, nil)

	// Positive: should still render attempts without slash
	if !strings.Contains(output, "attempts: 2)") {
		t.Fatalf(
			"expected plain 'attempts: 2)', got:\n%s", output,
		)
	}

	// Negative: should not contain slash notation
	for _, line := range strings.Split(output, "\n") {
		if strings.Contains(line, "step-a") &&
			strings.Contains(line, "/") {
			t.Fatal("nil def should not produce slash notation")
		}
	}
}

func TestRunSignalJSONOutput(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	result := runSignalResult{
		RunID:  "sig-run",
		Signal: "approval",
		Sent:   true,
	}

	var buf strings.Builder
	err := FormatJSON(&buf, result)
	if err != nil {
		t.Fatalf("FormatJSON failed: %v", err)
	}
	output := buf.String()

	// Positive: output should contain all fields
	if !strings.Contains(output, `"run_id"`) {
		t.Fatal("JSON output should contain run_id field")
	}
	if !strings.Contains(output, `"signal"`) {
		t.Fatal("JSON output should contain signal field")
	}
	if !strings.Contains(output, `"sent": true`) {
		t.Fatal("JSON output should contain sent field")
	}

	// Negative: should not contain human text
	if strings.Contains(output, "Signal sent:") {
		t.Fatal("JSON output should not contain human text")
	}
}

func TestRunStartOutputPrintsResult(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	t.Setenv("NATS_URL", srv.ClientURL())

	tel := observe.NewNoopTelemetry()
	js, _ := nc.JetStream()

	// Register a one-step workflow definition.
	svc := api.NewService(nc, tel)
	wb := dag.NewWorkflow("output-test-wf")
	wb.Task("echo", "echo-task")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	ctx := context.Background()
	if err := svc.RegisterWorkflow(ctx, wfDef); err != nil {
		t.Fatalf("RegisterWorkflow failed: %v", err)
	}

	// Create a completed run snapshot directly in KV.
	store := engine.NewSnapshotStore(js)
	runID := "output-test-run-1"
	run := dag.WorkflowRun{
		RunID:      runID,
		WorkflowID: "output-test-wf",
		Status:     dag.RunStatusCompleted,
		Steps: map[string]dag.StepState{
			"echo": {
				Status:   dag.StepStatusCompleted,
				Attempts: 1,
				Output:   []byte("hello from echo"),
			},
		},
		CreatedAt: time.Now().UTC(),
	}
	if err := store.Save(run); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	// Publish a completed event so watchRunWithStatus exits.
	evt := protocol.Event{
		Type:      protocol.EventWorkflowCompleted,
		RunID:     runID,
		Timestamp: time.Now().UTC(),
	}
	evtData, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	_, err = js.Publish("history."+runID, evtData)
	if err != nil {
		t.Fatalf("publish event: %v", err)
	}

	// Use watchRunWithStatus + printRunOutputForStart to test
	// the --output flow end-to-end without a live orchestrator.
	var status dag.RunStatus
	output := captureOutput(func() {
		status = watchRunWithStatus(svc, runID)
		if status == dag.RunStatusCompleted {
			printRunOutputForStart(svc, runID)
		}
	})

	// Positive: output should contain the step output data.
	if !strings.Contains(output, "hello from echo") {
		t.Fatalf(
			"expected 'hello from echo' in output, got:\n%s",
			output,
		)
	}

	// Negative: should not contain JSON run_id (not JSON mode).
	if strings.Contains(output, `"run_id"`) {
		t.Fatal("output should not contain JSON run_id")
	}
}

func TestSignalCommandWritesToKV(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "signals"}))
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	// Set NATS_URL env var for the CLI to use
	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	js, _ := nc.JetStream()
	sigKV, _ := js.KeyValue("signals")

	signalRunID := "aabbccdd55667788aabbccdd55667788"

	// Send signal command
	runSignalCmd([]string{signalRunID, "approval", "approved"})

	// Positive: signal should be written to KV bucket
	entry, err := sigKV.Get(signalRunID + ".approval")
	if err != nil {
		t.Fatalf("signal not written to KV: %v", err)
	}
	if string(entry.Value()) != "approved" {
		t.Fatalf("expected payload 'approved', got %q", entry.Value())
	}

	// Negative: other keys should not exist
	_, err = sigKV.Get(signalRunID + ".other")
	if err == nil {
		t.Fatal("unexpected signal key found")
	}
}
