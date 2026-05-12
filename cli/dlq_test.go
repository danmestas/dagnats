// cli/dlq_test.go
// Tests for dead-letter queue CLI commands.
// Methodology: integration tests with embedded NATS. Publish dead letters,
// verify CLI commands read/replay them correctly.
package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// publishModernDLQ publishes a post-#200 DLQ entry: body is the
// marshalled TaskPayload that would have been dispatched, metadata
// in structured headers including the original task subject.
func publishModernDLQ(
	t *testing.T, js nats.JetStreamContext,
	runID, stepID, task string,
	input []byte,
) {
	t.Helper()
	payload, err := json.Marshal(protocol.TaskPayload{
		TaskID: runID + "." + stepID,
		RunID:  runID,
		StepID: stepID,
		Input:  input,
	})
	if err != nil {
		t.Fatalf("publishModernDLQ: marshal: %v", err)
	}
	subject := "dead." + task + "." + runID + "." + stepID
	taskSubject := "task." + task + "." + runID
	// Include task in the dedup key so multi-task batch tests don't
	// collide on (runID, stepID); the production code key includes
	// attempts which serves the same disambiguation.
	dedupID := "dlq:" + runID + ":" + stepID + ":" + task
	msg := &nats.Msg{
		Subject: subject,
		Data:    payload,
		Header: nats.Header{
			"Nats-Msg-Id":                 {dedupID},
			engine.HeaderDLQRunID:         {runID},
			engine.HeaderDLQStepID:        {stepID},
			engine.HeaderDLQTask:          {task},
			engine.HeaderDLQError:         {"simulated failure"},
			engine.HeaderDLQAttempts:      {"3"},
			engine.HeaderDLQDeliveryCount: {"3"},
			engine.HeaderDLQConsumer:      {engine.DLQConsumerTaskQueues},
			engine.HeaderDLQTaskSubject:   {taskSubject},
		},
	}
	if _, err := js.PublishMsg(msg); err != nil {
		t.Fatalf("publishModernDLQ: publish: %v", err)
	}
}

func TestDLQListShowsMessages(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	// Set NATS_URL env var for the CLI to use
	t.Setenv("NATS_URL", srv.ClientURL())

	js, _ := nc.JetStream()

	// Publish a dead letter message
	payload, _ := json.Marshal(map[string]any{
		"run_id":   "run-123",
		"step_id":  "step-a",
		"task":     "failing-task",
		"error":    "simulated failure",
		"attempts": 3,
	})
	subject := "dead.failing-task.run-123.step-a"
	_, err := js.Publish(subject, payload)
	if err != nil {
		t.Fatalf("publish dead letter: %v", err)
	}

	// Positive: list should show the dead letter
	output := captureOutput(func() {
		runDLQListCmd([]string{})
	})

	if !strings.Contains(output, "run-123") {
		t.Fatal("output should contain run_id")
	}
	if !strings.Contains(output, "failing-task") {
		t.Fatal("output should contain task name")
	}

	// Negative: should not show unrelated data
	if strings.Contains(output, "phantom-run") {
		t.Fatal("output should not contain phantom data")
	}
}

func TestDLQReplayRepublishes(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	t.Setenv("NATS_URL", srv.ClientURL())

	js, _ := nc.JetStream()

	// Modern DLQ entry: body is the TaskPayload, metadata in headers.
	// Replay must re-publish that body verbatim onto the task subject.
	input := []byte(`{"timeout":"5s"}`)
	publishModernDLQ(t, js, "run-456", "step-b", "retry-task", input)

	sub, _ := js.SubscribeSync("task.retry-task.>",
		nats.AckExplicit(), nats.DeliverAll())

	runDLQReplayCmd([]string{"1"})

	// Positive: message should appear on task queue
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("replayed message not received: %v", err)
	}

	var replayed protocol.TaskPayload
	if err := json.Unmarshal(msg.Data, &replayed); err != nil {
		t.Fatalf("unmarshal replayed message: %v", err)
	}
	if replayed.RunID != "run-456" {
		t.Fatal("replayed message should have correct run_id")
	}
	if string(replayed.Input) != string(input) {
		t.Fatalf("replayed input must equal original; got %q want %q",
			replayed.Input, input)
	}

	// Negative: should not receive duplicate
	_, err = sub.NextMsg(500 * time.Millisecond)
	if err == nil {
		t.Fatal("should not receive duplicate replayed message")
	}
}

func TestDLQListRespectsLimit(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	t.Setenv("NATS_URL", srv.ClientURL())

	js, _ := nc.JetStream()

	// Publish 5 dead letters
	for i := 0; i < 5; i++ {
		payload, _ := json.Marshal(map[string]any{
			"run_id":  fmt.Sprintf("run-%d", i),
			"step_id": "step-a",
		})
		subject := fmt.Sprintf("dead.task-%d", i)
		_, err := js.Publish(subject, payload)
		if err != nil {
			t.Fatalf("publish dead letter %d: %v", i, err)
		}
	}

	// With --limit=2, should see exactly 2 data rows
	output := captureOutput(func() {
		runDLQListCmd([]string{"--limit=2"})
	})
	dataLines := countDataLines(output)
	if dataLines != 2 {
		t.Fatalf("expected 2 data lines with --limit=2, got %d",
			dataLines)
	}

	// Without limit (default 50), should see all 5
	outputAll := captureOutput(func() {
		runDLQListCmd([]string{})
	})
	dataLinesAll := countDataLines(outputAll)
	if dataLinesAll != 5 {
		t.Fatalf("expected 5 data lines without limit, got %d",
			dataLinesAll)
	}
}

func TestDLQReplayByRun(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	t.Setenv("NATS_URL", srv.ClientURL())

	js, _ := nc.JetStream()

	// Publish 2 modern DLQ entries for target run, 1 for other run.
	for i, item := range []struct {
		runID, task string
	}{
		{"target-run", "task-a"},
		{"target-run", "task-b"},
		{"other-run", "task-c"},
	} {
		input := []byte(fmt.Sprintf(`{"i":%d}`, i))
		publishModernDLQ(t, js,
			item.runID, "step-1", item.task, input)
	}

	// Subscribe to task queues to count replayed messages
	sub, _ := js.SubscribeSync("task.>",
		nats.AckExplicit(), nats.DeliverAll())

	output := captureOutput(func() {
		runDLQReplayCmd([]string{"--run=target-run"})
	})

	// Positive: should replay 2 messages for target run
	if !strings.Contains(output, "Replayed 2 dead letters") {
		t.Fatalf("expected 2 replayed, got: %s", output)
	}

	// Verify 2 messages on task queue
	for i := 0; i < 2; i++ {
		_, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatalf("expected message %d on task queue: %v",
				i+1, err)
		}
	}

	// Negative: third message should not exist (other-run)
	_, err := sub.NextMsg(500 * time.Millisecond)
	if err == nil {
		t.Fatal("should not replay other-run messages")
	}
}

func TestDLQListJSONOutput(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	t.Setenv("NATS_URL", srv.ClientURL())

	js, _ := nc.JetStream()

	// Legacy-shape entry — exercises the listDeadLettersInner
	// backward-compat path (body_preserved is false for these).
	payload, _ := json.Marshal(map[string]any{
		"run_id":  "run-json-1",
		"step_id": "step-j",
		"task":    "json-task",
		"error":   "json error",
	})
	_, err := js.Publish("dead.json-task.run-json-1.step-j", payload)
	if err != nil {
		t.Fatalf("publish dead letter: %v", err)
	}

	output := captureOutput(func() {
		runDLQListCmd([]string{"--json"})
	})

	// Positive: should be valid JSON array with correct fields
	var letters []map[string]any
	if err := json.Unmarshal([]byte(output), &letters); err != nil {
		t.Fatalf("output should be valid JSON: %v\n%s", err, output)
	}
	if len(letters) != 1 {
		t.Fatalf("expected 1 letter, got %d", len(letters))
	}
	if letters[0]["run_id"] != "run-json-1" {
		t.Fatal("JSON should contain correct run_id")
	}
	// Legacy entry: body_preserved must be false.
	if letters[0]["body_preserved"] != false {
		t.Fatalf("legacy entry must report body_preserved=false; got %v",
			letters[0]["body_preserved"])
	}

	// Negative: should not contain table header
	if strings.Contains(output, "SEQ") {
		t.Fatal("JSON output should not contain table header")
	}
}

// TestDLQListBodyPreservedField is the #200 acceptance test for the
// CLI surface: dagnats dlq list --json must emit body_preserved=true
// for post-fix DLQ entries so operators can tell at a glance which
// entries are replayable.
func TestDLQListBodyPreservedField(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	t.Setenv("NATS_URL", srv.ClientURL())

	js, _ := nc.JetStream()
	publishModernDLQ(t, js,
		"run-bp-1", "step-bp", "bp-task", []byte(`{"k":"v"}`))

	output := captureOutput(func() {
		runDLQListCmd([]string{"--json"})
	})

	// Output is pretty-printed JSON; parse instead of substring.
	var letters []map[string]any
	if err := json.Unmarshal([]byte(output), &letters); err != nil {
		t.Fatalf("output must be valid JSON: %v\n%s", err, output)
	}
	if len(letters) != 1 {
		t.Fatalf("expected 1 letter, got %d", len(letters))
	}
	if got, want := letters[0]["body_preserved"], true; got != want {
		t.Fatalf("body_preserved = %v, want %v", got, want)
	}
}

func TestDLQReplayJSONSingleOutput(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	t.Setenv("NATS_URL", srv.ClientURL())

	js, _ := nc.JetStream()

	publishModernDLQ(t, js, "run-rj-1", "step-r", "replay-json",
		[]byte(`{"k":"v"}`))

	output := captureOutput(func() {
		runDLQReplayCmd([]string{"1", "--json"})
	})

	// Positive: should be valid JSON with sequence and replayed
	var result map[string]any
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatalf("output should be valid JSON: %v\n%s", err, output)
	}
	if result["replayed"] != true {
		t.Fatal("JSON should have replayed=true")
	}

	// Negative: should not contain human-readable text
	if strings.Contains(output, "Replayed dead letter") {
		t.Fatal("JSON output should not contain text message")
	}
}

func TestDLQReplayJSONBatchOutput(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	t.Setenv("NATS_URL", srv.ClientURL())

	js, _ := nc.JetStream()

	for _, task := range []string{"batch-a", "batch-b"} {
		publishModernDLQ(t, js, "run-batch-json", "step-1", task,
			[]byte(`{"k":"v"}`))
	}

	output := captureOutput(func() {
		runDLQReplayCmd(
			[]string{"--run=run-batch-json", "--json"},
		)
	})

	// Positive: should be valid JSON with batch result
	var result map[string]any
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatalf("output should be valid JSON: %v\n%s", err, output)
	}
	if result["run_id"] != "run-batch-json" {
		t.Fatal("JSON should contain correct run_id")
	}
	if result["replayed"] != float64(2) {
		t.Fatalf("expected 2 replayed, got %v", result["replayed"])
	}

	// Negative: should not contain text output
	if strings.Contains(output, "Replayed") {
		t.Fatal("JSON output should not contain text message")
	}
}

// countDataLines counts non-header, non-empty lines in tabwriter output.
func countDataLines(output string) int {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) <= 1 {
		return 0
	}
	// First line is header
	return len(lines) - 1
}

// captureOutput runs a function and captures its stdout output.
//
// Reads the pipe concurrently with fn() so writes longer than the OS
// pipe buffer (~64 KiB) don't deadlock. The previous fixed-buffer
// single-Read version (1) silently truncated long output, making
// strings.Contains assertions test the wrong bytes, and (2) hung
// indefinitely if fn wrote past the pipe buffer before w.Close.
func captureOutput(fn func()) string {
	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		panic("captureOutput: os.Pipe: " + err.Error())
	}
	os.Stdout = w

	var (
		got     []byte
		readErr error
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		got, readErr = io.ReadAll(r)
	}()

	fn()

	if err := w.Close(); err != nil {
		panic("captureOutput: w.Close: " + err.Error())
	}
	os.Stdout = oldStdout
	<-done

	if readErr != nil {
		panic("captureOutput: read: " + readErr.Error())
	}
	return string(got)
}
