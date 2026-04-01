// cli/logs_test.go
// Tests for the logs command that tails the NATS telemetry log stream.
// Methodology: unit tests for formatLogLine and buildLogSubject verify
// formatting and subject construction. Integration test with embedded NATS
// confirms end-to-end log consumption from the TELEMETRY stream.
package cli

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe/simple"
	"github.com/nats-io/nats.go"
)

func TestBuildLogSubject(t *testing.T) {
	tests := []struct {
		name    string
		service string
		level   string
		want    string
	}{
		{"no filters", "", "", "telemetry.logs.>"},
		{"service only", "engine", "", "telemetry.logs.engine.>"},
		{"level only", "", "error", "telemetry.logs.*.error"},
		{"both filters", "worker", "info", "telemetry.logs.worker.info"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildLogSubject(tt.service, tt.level)
			if got != tt.want {
				t.Fatalf("buildLogSubject(%q, %q) = %q, want %q",
					tt.service, tt.level, got, tt.want)
			}
			// Negative: must never produce empty subject
			if got == "" {
				t.Fatal("subject must not be empty")
			}
		})
	}
}

func TestFormatLogLine(t *testing.T) {
	// Disable color so ANSI codes don't interfere with assertions
	oldNoColor := os.Getenv("NO_COLOR")
	os.Setenv("NO_COLOR", "1")
	defer os.Setenv("NO_COLOR", oldNoColor)

	ts := time.Date(2025, 6, 15, 14, 30, 45, 0, time.UTC)

	t.Run("basic info log", func(t *testing.T) {
		rec := simple.LogRecord{
			Level:     "info",
			Message:   "server started",
			Service:   "engine",
			Timestamp: ts,
		}
		line := formatLogLine(rec)

		// Positive: contains time, level, service, message
		if !strings.Contains(line, "14:30:45") {
			t.Fatal("line should contain formatted timestamp")
		}
		if !strings.Contains(line, "INFO") {
			t.Fatal("line should contain uppercase level")
		}
		if !strings.Contains(line, "engine") {
			t.Fatal("line should contain service name")
		}
		if !strings.Contains(line, "server started") {
			t.Fatal("line should contain message")
		}

		// Negative: no field brackets when fields empty
		if strings.Contains(line, "[") {
			t.Fatal("line should not have brackets without fields")
		}
	})

	t.Run("error log with fields", func(t *testing.T) {
		rec := simple.LogRecord{
			Level:     "error",
			Message:   "task failed",
			Service:   "worker",
			Timestamp: ts,
			Error:     "connection refused",
			Fields:    map[string]any{"run_id": "abc", "attempt": 3},
		}
		line := formatLogLine(rec)

		// Positive: contains error field
		if !strings.Contains(line, "error=connection refused") {
			t.Fatalf("should contain error, got: %s", line)
		}

		// Positive: contains sorted fields
		if !strings.Contains(line, "attempt=3") {
			t.Fatalf("should contain attempt field, got: %s", line)
		}
		if !strings.Contains(line, "run_id=abc") {
			t.Fatalf("should contain run_id field, got: %s", line)
		}

		// Fields should be sorted: attempt before run_id
		attemptIdx := strings.Index(line, "attempt=3")
		runIdx := strings.Index(line, "run_id=abc")
		if attemptIdx > runIdx {
			t.Fatal("fields should be sorted alphabetically")
		}
	})

	t.Run("warn log level padding", func(t *testing.T) {
		rec := simple.LogRecord{
			Level:     "warn",
			Message:   "slow query",
			Service:   "api",
			Timestamp: ts,
		}
		line := formatLogLine(rec)

		// Positive: WARN should be padded to 7 chars
		if !strings.Contains(line, "WARN   ") {
			t.Fatalf("level should be padded to 7, got: %q", line)
		}

		// Negative: should not contain ERROR
		if strings.Contains(line, "ERROR") {
			t.Fatal("warn log should not contain ERROR")
		}
	})
}

func TestLogsStreamIntegration(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	rec := simple.LogRecord{
		Level:     "info",
		Message:   "integration test log",
		Service:   "testservice",
		Timestamp: time.Now().UTC(),
		Fields:    map[string]any{"key": "value"},
	}
	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal LogRecord: %v", err)
	}

	_, err = js.Publish("telemetry.logs.testservice.info", data)
	if err != nil {
		t.Fatalf("publish log: %v", err)
	}

	// Verify subscription can read the published log record
	syncSub, err := js.SubscribeSync(
		"telemetry.logs.testservice.info",
		nats.AckExplicit(), nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	msg, err := syncSub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("receive log message: %v", err)
	}

	// Positive: message data should unmarshal to LogRecord
	var received simple.LogRecord
	if err := json.Unmarshal(msg.Data, &received); err != nil {
		t.Fatalf("unmarshal received: %v", err)
	}
	if received.Message != "integration test log" {
		t.Fatal("received message should match published")
	}

	// Negative: service should not be empty
	if received.Service == "" {
		t.Fatal("received service should not be empty")
	}

	// Verify formatLogLine produces correct output for this record
	line := formatLogLine(received)
	if !strings.Contains(line, "integration test log") {
		t.Fatal("formatted line should contain the message")
	}
	if !strings.Contains(line, "key=value") {
		t.Fatal("formatted line should contain fields")
	}
}
