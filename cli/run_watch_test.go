// cli/run_watch_test.go
// Tests for the run watch command and watchRunWithStatus.
// Methodology: compile-time signature verification and integration test
// with embedded NATS to validate that watch attaches to an existing run
// and outputs events, and that watchRunWithStatus returns terminal status.
package cli

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

// Compile-time check: runWatchCmd must accept []string.
var _ func([]string) = runWatchCmd

func TestRunWatchPanicsOnNilArgs(t *testing.T) {
	defer func() {
		r := recover()
		// Positive: passing nil args must panic with the right message.
		if r == nil {
			t.Fatal("expected panic on nil args")
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "args must not be nil") {
			t.Fatalf("unexpected panic message: %v", r)
		}
	}()
	runWatchCmd(nil)
}

func TestRunWatchOutputsEventsForExistingRun(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	// Create a run snapshot so GetRun succeeds.
	store := engine.NewSnapshotStore(jsNew)
	run := dag.WorkflowRun{
		RunID:      "watch-run-1",
		WorkflowID: "test-wf",
		Status:     dag.RunStatusCompleted,
		Steps: map[string]dag.StepState{
			"step-a": {
				Status:   dag.StepStatusCompleted,
				Attempts: 1,
			},
		},
		CreatedAt: time.Now().UTC(),
	}
	if err := store.Save(run); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	// Publish an event so watchRun has something to print.
	evt := protocol.Event{
		Type:      protocol.EventStepCompleted,
		RunID:     "watch-run-1",
		StepID:    "step-a",
		Timestamp: time.Now().UTC(),
	}
	evtData, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	if _, err := js.Publish("history.watch-run-1", evtData); err != nil {
		t.Fatalf("publish event: %v", err)
	}

	output := captureOutput(func() {
		runWatchCmd([]string{"watch-run-1"})
	})

	// Positive: output should contain the step event.
	if !strings.Contains(output, "step-a") {
		t.Fatal("output should contain step-a event")
	}

	// Negative: output should not contain unrelated run data.
	if strings.Contains(output, "phantom") {
		t.Fatal("output should not contain phantom data")
	}
}

func TestWatchRunWithStatusReturnsTerminal(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	t.Setenv("NATS_URL", srv.ClientURL())

	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	// Create a completed run snapshot.
	store := engine.NewSnapshotStore(jsNew)
	run := dag.WorkflowRun{
		RunID:      "watch-status-1",
		WorkflowID: "test-wf",
		Status:     dag.RunStatusCompleted,
		Steps: map[string]dag.StepState{
			"step-a": {
				Status:   dag.StepStatusCompleted,
				Attempts: 1,
			},
		},
		CreatedAt: time.Now().UTC(),
	}
	if err := store.Save(run); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	// Publish a completed event so watchRunWithStatus sees it.
	evt := protocol.Event{
		Type:      protocol.EventWorkflowCompleted,
		RunID:     "watch-status-1",
		Timestamp: time.Now().UTC(),
	}
	evtData, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	_, err = js.Publish(
		"history.watch-status-1", evtData,
	)
	if err != nil {
		t.Fatalf("publish event: %v", err)
	}

	svc := api.NewService(nc, observe.NewNoopTelemetry())

	var status dag.RunStatus
	captureOutput(func() {
		status = watchRunWithStatus(svc, "watch-status-1")
	})

	// Positive: status should be completed.
	if status != dag.RunStatusCompleted {
		t.Fatalf(
			"expected completed, got %s", status.String(),
		)
	}

	// Negative: status should not be pending.
	if status == dag.RunStatusPending {
		t.Fatal("status should not be pending")
	}
}
