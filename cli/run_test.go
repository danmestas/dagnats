// cli/run_test.go
// Tests for CLI output formatting and run commands.
// Methodology: unit test formatting functions, integration test commands
// with embedded NATS to verify event publishing.
package cli

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/natsutil"
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

	// Run cancel command
	runCancelCmd([]string{"test-run-1"})

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
	if evt.RunID != "test-run-1" {
		t.Fatalf("expected RunID test-run-1, got %s", evt.RunID)
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

	js, _ := nc.JetStream()
	publishTestEvent(t, js, "test-filter-1",
		protocol.EventStepQueued, "step-a")
	publishTestEvent(t, js, "test-filter-1",
		protocol.EventStepFailed, "step-a")
	publishTestEvent(t, js, "test-filter-1",
		protocol.EventStepCompleted, "step-b")

	output := captureOutput(func() {
		runEventsCmd([]string{
			"test-filter-1", "--type=step.failed",
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

	js, _ := nc.JetStream()
	publishTestEvent(t, js, "test-filter-2",
		protocol.EventStepQueued, "step-a")
	publishTestEvent(t, js, "test-filter-2",
		protocol.EventStepQueued, "step-b")

	output := captureOutput(func() {
		runEventsCmd([]string{
			"test-filter-2", "--step=step-a",
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

	// Send signal command
	runSignalCmd([]string{"run-abc", "approval", "approved"})

	// Positive: signal should be written to KV bucket
	entry, err := sigKV.Get("run-abc.approval")
	if err != nil {
		t.Fatalf("signal not written to KV: %v", err)
	}
	if string(entry.Value()) != "approved" {
		t.Fatalf("expected payload 'approved', got %q", entry.Value())
	}

	// Negative: other keys should not exist
	_, err = sigKV.Get("run-abc.other")
	if err == nil {
		t.Fatal("unexpected signal key found")
	}
}
