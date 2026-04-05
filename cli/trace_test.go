// cli/trace_test.go
// Tests for trace ID extraction and display in CLI output.
// Methodology: unit tests for extractTraceID, integration test
// for trace visibility in events output.
package cli

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
)

func TestExtractTraceID(t *testing.T) {
	tp := "00-abcdef1234567890abcdef1234567890-1234567890abcdef-01"
	got := extractTraceID(tp)

	// Positive: should return the trace ID segment
	if got != "abcdef1234567890abcdef1234567890" {
		t.Fatalf("expected trace ID, got %q", got)
	}

	// Negative: should not return the full traceparent
	if got == tp {
		t.Fatal("should not return the full traceparent string")
	}
}

func TestExtractTraceIDEmpty(t *testing.T) {
	got := extractTraceID("")

	// Positive: empty input returns empty string
	if got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}

	// Negative: should not panic on empty input
	// (test reaching this line proves no panic)
	if len(got) != 0 {
		t.Fatal("length should be zero")
	}
}

func TestExtractTraceIDInvalid(t *testing.T) {
	// Malformed: wrong version
	got := extractTraceID("01-abc-def-01")
	if got != "" {
		t.Fatalf("wrong version should return empty, got %q", got)
	}

	// Malformed: too few parts
	got = extractTraceID("00-abc-def")
	if got != "" {
		t.Fatalf("too few parts should return empty, got %q", got)
	}

	// Malformed: too many parts
	got = extractTraceID("00-a-b-c-d")

	// Negative: none of these should return a trace ID
	if got != "" {
		t.Fatalf("too many parts should return empty, got %q", got)
	}
}

func TestEventsTableShowsTrace(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	js, _ := nc.JetStream()

	// Publish event with TraceParent set
	evt := protocol.Event{
		Type:        protocol.EventStepCompleted,
		RunID:       "tt000000111111112222222233333333",
		StepID:      "step-a",
		Timestamp:   time.Now().UTC(),
		TraceParent: "00-abcdef1234567890abcdef1234567890-1234567890abcdef-01",
	}
	data, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	subj := "history.tt000000111111112222222233333333"
	_, err = js.Publish(subj, data)
	if err != nil {
		t.Fatalf("publish event: %v", err)
	}

	traceRunID := "tt000000111111112222222233333333"
	output := captureOutput(func() {
		runEventsCmd([]string{traceRunID})
	})

	// Positive: output should contain truncated trace ID
	if !strings.Contains(output, "abcdef1234567890") {
		t.Fatalf(
			"output should contain trace ID prefix, got:\n%s",
			output,
		)
	}

	// Positive: output should have TRACE column header
	if !strings.Contains(output, "TRACE") {
		t.Fatal("output should contain TRACE column header")
	}

	// Negative: output should not contain the full traceparent
	if strings.Contains(output, "1234567890abcdef-01") {
		t.Fatal("output should not contain span ID or flags")
	}
}
