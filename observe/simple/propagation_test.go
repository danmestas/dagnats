// observe/simple/propagation_test.go
// Tests for W3C traceparent propagation. Methodology: verify inject/extract
// round-trip for both NATS headers and protocol.Event fields. Assert both
// locations are written atomically.
package simple

import (
	"testing"

	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestGenerateTraceID(t *testing.T) {
	id := generateTraceID()
	if len(id) != 32 {
		t.Fatalf("trace ID len = %d, want 32 hex chars", len(id))
	}
	id2 := generateTraceID()
	if id == id2 {
		t.Fatal("two generated trace IDs should not be equal")
	}
}

func TestGenerateSpanID(t *testing.T) {
	id := generateSpanID()
	if len(id) != 16 {
		t.Fatalf("span ID len = %d, want 16 hex chars", len(id))
	}
	id2 := generateSpanID()
	if id == id2 {
		t.Fatal("two generated span IDs should not be equal")
	}
}

func TestFormatTraceparent(t *testing.T) {
	tp := formatTraceparent("aaa", "bbb")
	if tp != "00-aaa-bbb-01" {
		t.Fatalf("traceparent = %q, want 00-aaa-bbb-01", tp)
	}
}

func TestParseTraceparent(t *testing.T) {
	traceID, spanID, ok := parseTraceparent("00-abc123-def456-01")
	if !ok {
		t.Fatal("parseTraceparent returned false for valid input")
	}
	if traceID != "abc123" || spanID != "def456" {
		t.Fatalf("got trace=%q span=%q, want abc123/def456",
			traceID, spanID)
	}
	_, _, ok = parseTraceparent("invalid")
	if ok {
		t.Fatal("parseTraceparent should return false for invalid")
	}
}

func TestInjectExtractRoundTrip(t *testing.T) {
	msg := &nats.Msg{Header: nats.Header{}}
	evt := &protocol.Event{
		RunID: "run-1",
		Type:  protocol.EventStepStarted,
	}
	traceID := generateTraceID()
	spanID := generateSpanID()

	injectTraceparent(msg, evt, traceID, spanID)

	// Both locations should have the value
	if evt.TraceParent == "" {
		t.Fatal("TraceParent not set on event")
	}
	if msg.Header.Get("traceparent") == "" {
		t.Fatal("traceparent not set on NATS header")
	}

	// Extract should recover the IDs
	gotTrace, gotSpan, ok := extractTraceparent(msg, evt)
	if !ok {
		t.Fatal("extractTraceparent returned false")
	}
	if gotTrace != traceID || gotSpan != spanID {
		t.Fatalf("extract got %q/%q, want %q/%q",
			gotTrace, gotSpan, traceID, spanID)
	}
}

func TestExtractFallsBackToEvent(t *testing.T) {
	// No NATS headers — should fall back to event fields
	msg := &nats.Msg{} // no Header
	evt := &protocol.Event{
		TraceParent: "00-trace1-span1-01",
	}
	traceID, spanID, ok := extractTraceparent(msg, evt)
	if !ok {
		t.Fatal("should extract from event when no header")
	}
	if traceID != "trace1" || spanID != "span1" {
		t.Fatalf("got %q/%q, want trace1/span1", traceID, spanID)
	}
}
