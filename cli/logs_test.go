// cli/logs_test.go
// Tests for the logs command that tails the NATS telemetry log stream.
// Methodology: unit tests for formatLogLine and buildLogSubject verify
// formatting and subject construction. Integration test with embedded NATS
// confirms end-to-end log consumption from the TELEMETRY stream.
package cli

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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

	tsStr := "2025-06-15T14:30:45.000000000Z"

	t.Run("basic info log", func(t *testing.T) {
		rec := LogRecord{
			Severity:    "info",
			Body:        "server started",
			ServiceName: "engine",
			Timestamp:   tsStr,
		}
		line := formatLogLine(rec)

		// Positive: contains time, severity, service, body
		if !strings.Contains(line, "14:30:45") {
			t.Fatal("line should contain formatted timestamp")
		}
		if !strings.Contains(line, "INFO") {
			t.Fatal("line should contain uppercase severity")
		}
		if !strings.Contains(line, "engine") {
			t.Fatal("line should contain service name")
		}
		if !strings.Contains(line, "server started") {
			t.Fatal("line should contain body")
		}

		// Negative: no brackets when attributes empty
		if strings.Contains(line, "[") {
			t.Fatal("line should not have brackets without attrs")
		}
	})

	t.Run("error log with attributes", func(t *testing.T) {
		rec := LogRecord{
			Severity:    "error",
			Body:        "task failed",
			ServiceName: "worker",
			Timestamp:   tsStr,
			Attributes: map[string]string{
				"run_id":  "abc",
				"attempt": "3",
				"error":   "connection refused",
			},
		}
		line := formatLogLine(rec)

		// Positive: contains error attribute
		if !strings.Contains(line, "error=connection refused") {
			t.Fatalf("should contain error attr, got: %s", line)
		}

		// Positive: contains sorted attributes
		if !strings.Contains(line, "attempt=3") {
			t.Fatalf("should contain attempt attr, got: %s", line)
		}
		if !strings.Contains(line, "run_id=abc") {
			t.Fatalf("should contain run_id attr, got: %s", line)
		}

		// Attributes should be sorted: attempt before run_id
		attemptIdx := strings.Index(line, "attempt=3")
		runIdx := strings.Index(line, "run_id=abc")
		if attemptIdx > runIdx {
			t.Fatal("attributes should be sorted alphabetically")
		}
	})

	t.Run("warn log severity padding", func(t *testing.T) {
		rec := LogRecord{
			Severity:    "warn",
			Body:        "slow query",
			ServiceName: "api",
			Timestamp:   tsStr,
		}
		line := formatLogLine(rec)

		// Positive: WARN should be padded to 7 chars
		if !strings.Contains(line, "WARN   ") {
			t.Fatalf("severity should be padded to 7, got: %q",
				line)
		}

		// Negative: should not contain ERROR
		if strings.Contains(line, "ERROR") {
			t.Fatal("warn log should not contain ERROR")
		}
	})
}

func TestParseTailFlag(t *testing.T) {
	t.Run("absent returns zero", func(t *testing.T) {
		got := parseTailFlag([]string{"--level=info"})
		if got != 0 {
			t.Fatalf("expected 0, got %d", got)
		}
		// Negative: should not return negative
		if got < 0 {
			t.Fatal("tail count must not be negative")
		}
	})

	t.Run("valid value", func(t *testing.T) {
		got := parseTailFlag([]string{"--tail=50"})
		if got != 50 {
			t.Fatalf("expected 50, got %d", got)
		}
		// Negative: must not exceed max
		if got > tailCountMax {
			t.Fatal("tail count must not exceed max")
		}
	})

	t.Run("with other flags", func(t *testing.T) {
		got := parseTailFlag(
			[]string{"--level=info", "--tail=10", "--service=api"},
		)
		if got != 10 {
			t.Fatalf("expected 10, got %d", got)
		}
		// Negative: must be positive
		if got <= 0 {
			t.Fatal("tail count must be positive")
		}
	})
}

func TestCollectTailMessages(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	_ = srv
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	jsLegacy, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	// Publish 5 log records in OTLP format (test setup only).
	const totalMessages = 5
	for i := 0; i < totalMessages; i++ {
		rec := LogRecord{
			Severity:    "info",
			Body:        "msg-" + strconv.Itoa(i),
			ServiceName: "testsvc",
			Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
		}
		data, marshalErr := json.Marshal(rec)
		if marshalErr != nil {
			t.Fatalf("marshal: %v", marshalErr)
		}
		_, pubErr := jsLegacy.Publish(
			"telemetry.logs.testsvc.info", data,
		)
		if pubErr != nil {
			t.Fatalf("publish: %v", pubErr)
		}
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	cons, err := js.OrderedConsumer(
		context.Background(), "TELEMETRY",
		jetstream.OrderedConsumerConfig{
			FilterSubjects: []string{
				"telemetry.logs.testsvc.info",
			},
			DeliverPolicy: jetstream.DeliverAllPolicy,
		},
	)
	if err != nil {
		t.Fatalf("ordered consumer: %v", err)
	}

	// Collect last 3 of 5 messages
	buf := collectTailMessages(cons, 3)

	// Positive: should return exactly 3 records
	if len(buf) != 3 {
		t.Fatalf("expected 3 records, got %d", len(buf))
	}

	// Positive: should be the LAST 3 messages (msg-2..msg-4)
	if buf[0].Body != "msg-2" {
		t.Fatalf(
			"expected first record 'msg-2', got %q",
			buf[0].Body,
		)
	}
	if buf[2].Body != "msg-4" {
		t.Fatalf(
			"expected last record 'msg-4', got %q",
			buf[2].Body,
		)
	}

	// Negative: should not contain msg-0 or msg-1
	for _, rec := range buf {
		if rec.Body == "msg-0" || rec.Body == "msg-1" {
			t.Fatalf("should not contain %q", rec.Body)
		}
	}
}

func TestCollectTailMessagesFewerThanCount(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	_ = srv
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	jsLegacy, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	// Publish only 2 messages but request 10
	for i := 0; i < 2; i++ {
		rec := LogRecord{
			Severity:    "warn",
			Body:        "few-" + strconv.Itoa(i),
			ServiceName: "fewsvc",
			Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
		}
		data, marshalErr := json.Marshal(rec)
		if marshalErr != nil {
			t.Fatalf("marshal: %v", marshalErr)
		}
		_, pubErr := jsLegacy.Publish(
			"telemetry.logs.fewsvc.warn", data,
		)
		if pubErr != nil {
			t.Fatalf("publish: %v", pubErr)
		}
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	cons, err := js.OrderedConsumer(
		context.Background(), "TELEMETRY",
		jetstream.OrderedConsumerConfig{
			FilterSubjects: []string{
				"telemetry.logs.fewsvc.warn",
			},
			DeliverPolicy: jetstream.DeliverAllPolicy,
		},
	)
	if err != nil {
		t.Fatalf("ordered consumer: %v", err)
	}

	buf := collectTailMessages(cons, 10)

	// Positive: returns all available (2), not 10
	if len(buf) != 2 {
		t.Fatalf("expected 2 records, got %d", len(buf))
	}

	// Negative: buffer should not be empty
	if len(buf) == 0 {
		t.Fatal("buffer must not be empty")
	}
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

	rec := LogRecord{
		Severity:    "info",
		Body:        "integration test log",
		ServiceName: "testservice",
		Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
		Attributes:  map[string]string{"key": "value"},
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
	var received LogRecord
	if err := json.Unmarshal(msg.Data, &received); err != nil {
		t.Fatalf("unmarshal received: %v", err)
	}
	if received.Body != "integration test log" {
		t.Fatal("received body should match published")
	}

	// Negative: service name should not be empty
	if received.ServiceName == "" {
		t.Fatal("received service name should not be empty")
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
