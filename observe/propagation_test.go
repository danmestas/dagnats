// Methodology: round-trip tests for trace context propagation helpers.
// Each test verifies inject/extract with at least 2 assertions covering
// both positive and negative space. Tests register a W3C propagator
// to match production InitTelemetry behavior.

package observe

import (
	"context"
	"testing"

	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// installPropagator sets the global OTel propagator for testing
// and returns a restore function.
func installPropagator() func() {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)
	return func() { otel.SetTextMapPropagator(prev) }
}

// fakeSpanContext creates a context carrying a remote span
// context with the given traceparent for injection testing.
func fakeSpanContext(tp string) context.Context {
	hdr := nats.Header{}
	hdr.Set("traceparent", tp)
	prop := propagation.TraceContext{}
	return prop.Extract(
		context.Background(),
		NATSHeaderCarrier{Header: hdr},
	)
}

func TestInjectExtract_RoundTrip(t *testing.T) {
	restore := installPropagator()
	defer restore()

	tp := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	ctx := fakeSpanContext(tp)

	// Inject into msg + event.
	msg := &nats.Msg{}
	evt := &protocol.Event{}
	InjectTraceContext(ctx, msg, evt)

	// Positive: header and event field both populated.
	if got := msg.Header.Get("traceparent"); got != tp {
		t.Fatalf(
			"header traceparent: got %q, want %q", got, tp,
		)
	}
	if evt.TraceParent != tp {
		t.Fatalf(
			"event TraceParent: got %q, want %q",
			evt.TraceParent, tp,
		)
	}

	// Extract back from a jetstream-compatible wrapper.
	extracted := ExtractTraceContextRaw(msg, evt)
	sc := trace.SpanContextFromContext(extracted)

	// Positive: trace ID survives the round trip.
	wantTraceID := "4bf92f3577b34da6a3ce929d0e0e4736"
	if sc.TraceID().String() != wantTraceID {
		t.Fatalf(
			"trace ID: got %q, want %q",
			sc.TraceID().String(), wantTraceID,
		)
	}

	// Negative: context must be remote.
	if !sc.IsRemote() {
		t.Fatal("expected remote span context")
	}
}

func TestExtract_EventFallback(t *testing.T) {
	restore := installPropagator()
	defer restore()

	tp := "00-abcdef1234567890abcdef1234567890-1234567890abcdef-01"

	// Simulate a replayed event with no NATS headers but
	// TraceParent persisted in the Event payload.
	msg := &nats.Msg{}
	evt := &protocol.Event{TraceParent: tp}

	ctx := ExtractTraceContextRaw(msg, evt)
	sc := trace.SpanContextFromContext(ctx)

	// Positive: trace ID extracted from event field.
	wantTraceID := "abcdef1234567890abcdef1234567890"
	if sc.TraceID().String() != wantTraceID {
		t.Fatalf(
			"trace ID: got %q, want %q",
			sc.TraceID().String(), wantTraceID,
		)
	}

	// Negative: span ID must also be valid.
	if !sc.SpanID().IsValid() {
		t.Fatal("expected valid span ID from event fallback")
	}
}

func TestExtract_NoContext(t *testing.T) {
	restore := installPropagator()
	defer restore()

	// No headers, no event TraceParent — should return
	// background context with no span context.
	msg := &nats.Msg{}
	evt := &protocol.Event{}

	ctx := ExtractTraceContextRaw(msg, evt)
	sc := trace.SpanContextFromContext(ctx)

	// Positive: returns invalid (empty) span context.
	if sc.IsValid() {
		t.Fatal("expected invalid span context for empty input")
	}

	// Negative: trace ID must be zero.
	if sc.TraceID().IsValid() {
		t.Fatal("expected zero trace ID for empty input")
	}
}

func TestInject_NilEvent(t *testing.T) {
	restore := installPropagator()
	defer restore()

	tp := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	ctx := fakeSpanContext(tp)

	// Inject with nil event — should still populate headers.
	msg := &nats.Msg{}
	InjectTraceContext(ctx, msg, nil)

	// Positive: header is populated even without event.
	if got := msg.Header.Get("traceparent"); got != tp {
		t.Fatalf(
			"header traceparent: got %q, want %q", got, tp,
		)
	}

	// Negative: no panic occurred (implicit).
	if msg.Header == nil {
		t.Fatal("expected non-nil header after inject")
	}
}

func TestExtract_NilEvent(t *testing.T) {
	restore := installPropagator()
	defer restore()

	// Headers present, nil event — should extract from headers.
	tp := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	msg := &nats.Msg{Header: nats.Header{}}
	msg.Header.Set("traceparent", tp)

	ctx := ExtractTraceContextRaw(msg, nil)
	sc := trace.SpanContextFromContext(ctx)

	// Positive: trace ID extracted from header.
	wantTraceID := "4bf92f3577b34da6a3ce929d0e0e4736"
	if sc.TraceID().String() != wantTraceID {
		t.Fatalf(
			"trace ID: got %q, want %q",
			sc.TraceID().String(), wantTraceID,
		)
	}

	// Negative: IsRemote must be true.
	if !sc.IsRemote() {
		t.Fatal("expected remote span context")
	}
}

func TestExtractTraceContextHeader(t *testing.T) {
	restore := installPropagator()
	defer restore()

	tp := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	hdr := nats.Header{}
	hdr.Set("traceparent", tp)
	sc := trace.SpanContextFromContext(ExtractTraceContextHeader(hdr))

	// Positive: trace ID extracted, and marked remote.
	wantTraceID := "4bf92f3577b34da6a3ce929d0e0e4736"
	if sc.TraceID().String() != wantTraceID {
		t.Fatalf(
			"trace ID: got %q, want %q",
			sc.TraceID().String(), wantTraceID,
		)
	}
	if !sc.IsRemote() {
		t.Fatal("expected remote span context")
	}

	// Negative: a nil header yields a bare background context.
	nilSC := trace.SpanContextFromContext(
		ExtractTraceContextHeader(nil),
	)
	if nilSC.IsValid() {
		t.Fatalf("nil header: got valid span context %v", nilSC)
	}
	if nilSC.IsRemote() {
		t.Fatal("nil header: span context must not be remote")
	}
}
