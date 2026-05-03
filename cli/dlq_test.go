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

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
)

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

	// Publish a dead letter message
	payload, _ := json.Marshal(map[string]any{
		"run_id":   "run-456",
		"step_id":  "step-b",
		"task":     "retry-task",
		"error":    "timeout",
		"attempts": 5,
	})
	subject := "dead.retry-task.run-456.step-b"
	_, err := js.Publish(subject, payload)
	if err != nil {
		t.Fatalf("publish dead letter: %v", err)
	}

	// Get sequence number (should be 1 for first message)
	// Subscribe to task queue to verify replay
	sub, _ := js.SubscribeSync("task.retry-task.>",
		nats.AckExplicit(), nats.DeliverAll())

	// Replay the dead letter by sequence number
	runDLQReplayCmd([]string{"1"})

	// Positive: message should appear on task queue
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("replayed message not received: %v", err)
	}

	var replayed map[string]any
	if err := json.Unmarshal(msg.Data, &replayed); err != nil {
		t.Fatalf("unmarshal replayed message: %v", err)
	}
	if replayed["run_id"] != "run-456" {
		t.Fatal("replayed message should have correct run_id")
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

	// Publish 2 dead letters for target run, 1 for other run
	for _, item := range []struct {
		runID string
		task  string
	}{
		{"target-run", "task-a"},
		{"target-run", "task-b"},
		{"other-run", "task-c"},
	} {
		payload, _ := json.Marshal(map[string]any{
			"run_id":  item.runID,
			"step_id": "step-1",
		})
		js.Publish("dead."+item.task, payload)
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

	// Negative: should not contain table header
	if strings.Contains(output, "SEQ") {
		t.Fatal("JSON output should not contain table header")
	}
}

func TestDLQReplayJSONSingleOutput(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	t.Setenv("NATS_URL", srv.ClientURL())

	js, _ := nc.JetStream()

	payload, _ := json.Marshal(map[string]any{
		"run_id":  "run-rj-1",
		"step_id": "step-r",
		"task":    "replay-json",
		"error":   "fail",
	})
	_, err := js.Publish("dead.replay-json.run-rj-1.step-r", payload)
	if err != nil {
		t.Fatalf("publish dead letter: %v", err)
	}

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
		payload, _ := json.Marshal(map[string]any{
			"run_id":  "run-batch-json",
			"step_id": "step-1",
			"task":    task,
			"error":   "fail",
		})
		js.Publish("dead."+task, payload)
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
