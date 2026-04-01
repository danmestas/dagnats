// cli/dlq_test.go
// Tests for dead-letter queue CLI commands.
// Methodology: integration tests with embedded NATS. Publish dead letters,
// verify CLI commands read/replay them correctly.
package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/natsutil"
	"github.com/nats-io/nats.go"
)

func TestDLQListShowsMessages(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	// Set NATS_URL env var for the CLI to use
	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	js, _ := nc.JetStream()

	// Publish a dead letter message
	payload, _ := json.Marshal(map[string]interface{}{
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

	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	js, _ := nc.JetStream()

	// Publish a dead letter message
	payload, _ := json.Marshal(map[string]interface{}{
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

	var replayed map[string]interface{}
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

	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	js, _ := nc.JetStream()

	// Publish 5 dead letters
	for i := 0; i < 5; i++ {
		payload, _ := json.Marshal(map[string]interface{}{
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
func captureOutput(fn func()) string {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	fn()

	w.Close()
	os.Stdout = oldStdout

	buf := make([]byte, 4096)
	n, _ := r.Read(buf)
	return string(buf[:n])
}
