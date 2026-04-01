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
