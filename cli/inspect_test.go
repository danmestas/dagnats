// cli/inspect_test.go
// Tests for the unified inspect command with cross-referenced output.
// Methodology: integration tests with embedded NATS verify inspect
// output inlines failure events and DLQ entries under failed steps.
// Unit tests verify collectStepContexts and sortedStepIDs helpers.
package cli

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

	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
)

func TestInspectShowsInlineFailuresAndDLQ(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	t.Setenv("NATS_URL", srv.ClientURL())

	js, err2 := nc.JetStream()
	if err2 != nil {
		t.Fatalf("JetStream: %v", err2)
	}
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	store := engine.NewSnapshotStore(jsNew)
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
	if err := store.Save(context.Background(), run); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	// Publish a step.failed event
	evt := protocol.Event{
		Type:      protocol.EventStepFailed,
		RunID:     "inspect-run-1",
		StepID:    "step-a",
		Timestamp: time.Now().UTC(),
		Payload: json.RawMessage(
			`{"error":"connection timeout"}`,
		),
	}
	evtData, err3 := evt.Marshal()
	if err3 != nil {
		t.Fatalf("Marshal event: %v", err3)
	}
	if _, err4 := js.Publish(
		"history.inspect-run-1", evtData,
	); err4 != nil {
		t.Fatalf("Publish history: %v", err4)
	}

	// Publish a dead letter for this run
	dlPayload, err5 := json.Marshal(map[string]any{
		"run_id":  "inspect-run-1",
		"step_id": "step-a",
	})
	if err5 != nil {
		t.Fatalf("Marshal dead letter: %v", err5)
	}
	if _, err6 := js.Publish(
		"dead.failing-task", dlPayload,
	); err6 != nil {
		t.Fatalf("Publish dead letter: %v", err6)
	}

	output := captureOutput(func() {
		runInspectCmd([]string{"inspect-run-1"})
	})

	// Positive: should contain run ID and step failure inline
	if !strings.Contains(output, "inspect-run-1") {
		t.Fatal("output should contain run ID")
	}
	if !strings.Contains(output, "step.failed") {
		t.Fatal("output should contain inline failure events")
	}
	if !strings.Contains(output, "connection timeout") {
		t.Fatal("output should contain step error")
	}

	// Negative: should NOT have separate Failures: section
	if strings.Contains(output, "\nFailures:") {
		t.Fatal("should not have separate Failures section")
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
		DeadLetters: []api.DeadLetterView{
			{
				DeadLetter: api.DeadLetter{
					Sequence: 1,
					RunID:    "json-inspect-1",
					StepID:   "step-a",
					Task:     "failing-task",
					Error:    "timeout",
				},
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

	t.Setenv("NATS_URL", srv.ClientURL())

	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	svc := api.NewService(nc)
	wb := dag.NewWorkflow("clean-wf")
	wb.Task("a", "task-a")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := svc.RegisterWorkflow(
		context.Background(), def,
	); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	store := engine.NewSnapshotStore(jsNew)
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
	if err := store.Save(
		context.Background(), run,
	); err != nil {
		t.Fatalf("Save: %v", err)
	}

	output := captureOutput(func() {
		runInspectCmd([]string{"clean-run-1"})
	})

	// Positive: should show completed status
	if !strings.Contains(output, "completed") {
		t.Fatal("output should contain completed status")
	}

	// Negative: should not contain failure or DLQ inline data
	if strings.Contains(output, "Failures:") {
		t.Fatal("clean run should not show Failures section")
	}
	if strings.Contains(output, "Dead Letters:") {
		t.Fatal("clean run should not show Dead Letters section")
	}
	if strings.Contains(output, "DLQ #") {
		t.Fatal("clean run should not show DLQ entries")
	}
	if strings.Contains(output, "replay:") {
		t.Fatal("clean run should not show replay hints")
	}
}

func TestHasFlagAndStripFlag(t *testing.T) {
	args := []string{"run-123", "--trace", "--json"}

	// Positive: detects --trace flag.
	if !hasFlag(args, "--trace") {
		t.Fatal("should detect --trace flag")
	}

	// Positive: strips --trace flag.
	stripped := stripFlag(args, "--trace")
	if len(stripped) != 2 {
		t.Fatalf("expected 2 args, got %d", len(stripped))
	}

	// Negative: --trace no longer present.
	if hasFlag(stripped, "--trace") {
		t.Fatal("should not contain --trace after strip")
	}

	// Negative: flag not present returns false.
	if hasFlag(args, "--missing") {
		t.Fatal("should not detect missing flag")
	}
}

func TestInspectNoTraceWithoutFlag(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	t.Setenv("NATS_URL", srv.ClientURL())

	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	store := engine.NewSnapshotStore(jsNew)
	run := dag.WorkflowRun{
		RunID:      "notrace-run-1",
		WorkflowID: "test-wf",
		Status:     dag.RunStatusCompleted,
		Steps: map[string]dag.StepState{
			"a": {
				Status:   dag.StepStatusCompleted,
				Attempts: 1,
			},
		},
		CreatedAt: time.Now().UTC(),
	}
	if err := store.Save(
		context.Background(), run,
	); err != nil {
		t.Fatalf("Save: %v", err)
	}

	output := captureOutput(func() {
		runInspectCmd([]string{"notrace-run-1"})
	})

	// Positive: shows run status.
	if !strings.Contains(output, "notrace-run-1") {
		t.Fatal("should contain run ID")
	}

	// Negative: no Trace section without --trace flag.
	if strings.Contains(output, "Trace:") {
		t.Fatal("should not show trace without --trace")
	}
}

func TestCollectStepContextsGroupsByStep(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	failures := []api.RunEvent{
		{
			Type:        "step.failed",
			StepID:      "deploy",
			Data:        "conn refused",
			TraceParent: "00-abc123-def456-01",
			Timestamp: time.Date(
				2026, 4, 6, 15, 4, 2, 0, time.UTC,
			),
		},
		{
			Type:   "step.failed",
			StepID: "deploy",
			Data:   "conn refused again",
			Timestamp: time.Date(
				2026, 4, 6, 15, 4, 11, 0, time.UTC,
			),
		},
	}
	deadLetters := []api.DeadLetterView{
		{
			DeadLetter: api.DeadLetter{
				Sequence: 42,
				StepID:   "deploy",
				Error:    "connection refused",
			},
		},
	}

	contexts := collectStepContexts(failures, deadLetters)

	// Positive: deploy step should have 2 failures and 1 DLQ
	ctx, ok := contexts["deploy"]
	if !ok {
		t.Fatal("should have context for deploy step")
	}
	if len(ctx.Failures) != 2 {
		t.Fatalf("expected 2 failures, got %d",
			len(ctx.Failures))
	}
	if len(ctx.DeadLetters) != 1 {
		t.Fatalf("expected 1 dead letter, got %d",
			len(ctx.DeadLetters))
	}
	if ctx.TraceID != "abc123" {
		t.Fatalf("expected trace abc123, got %s", ctx.TraceID)
	}

	// Negative: no context for unrelated step
	_, exists := contexts["fetch"]
	if exists {
		t.Fatal("should not have context for fetch step")
	}
}

func TestSortedStepIDsReturnsAlphabetical(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	steps := map[string]dag.StepState{
		"deploy": {Status: dag.StepStatusFailed},
		"fetch":  {Status: dag.StepStatusCompleted},
		"build":  {Status: dag.StepStatusCompleted},
		"notify": {Status: dag.StepStatusSkipped},
	}

	ids := sortedStepIDs(steps)

	// Positive: should be alphabetically sorted
	expected := []string{"build", "deploy", "fetch", "notify"}
	if len(ids) != len(expected) {
		t.Fatalf("expected %d ids, got %d",
			len(expected), len(ids))
	}
	for i, id := range ids {
		if id != expected[i] {
			t.Fatalf("position %d: expected %s, got %s",
				i, expected[i], id)
		}
	}

	// Negative: should not be in original map iteration order
	// (which is random, but at least verify length matches)
	if len(ids) != 4 {
		t.Fatal("should have exactly 4 step IDs")
	}
}

func TestPrintStepDebugLinesOutput(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	ctx := stepDebugContext{
		Failures: []api.RunEvent{
			{
				Type: "step.failed",
				Data: "connection refused",
				Timestamp: time.Date(
					2026, 4, 6, 15, 4, 2, 0, time.UTC,
				),
			},
		},
		DeadLetters: []api.DeadLetterView{
			{
				DeadLetter: api.DeadLetter{
					Sequence: 42,
					Error:    "connection refused",
				},
			},
		},
		TraceID: "abc123def456",
	}

	output := captureOutput(func() {
		printStepDebugLines(ctx)
	})

	// Positive: should contain failure timestamp and type
	if !strings.Contains(output, "15:04:02") {
		t.Fatal("should contain failure timestamp")
	}
	if !strings.Contains(output, "step.failed") {
		t.Fatal("should contain event type")
	}

	// Positive: should contain trace and view hints
	if !strings.Contains(output, "trace: abc123def456") {
		t.Fatal("should contain trace ID")
	}
	if !strings.Contains(output,
		"view:  dagnats trace abc123def456") {
		t.Fatal("should contain view hint")
	}

	// Positive: should contain DLQ and replay hints
	if !strings.Contains(output, "DLQ #42") {
		t.Fatal("should contain DLQ entry")
	}
	if !strings.Contains(output,
		"replay: dagnats dlq replay 42") {
		t.Fatal("should contain replay hint")
	}

	// Negative: should not contain separate section headers
	if strings.Contains(output, "Failures:") {
		t.Fatal("should not contain Failures section header")
	}
	if strings.Contains(output, "Dead Letters:") {
		t.Fatal("should not contain Dead Letters header")
	}
}
