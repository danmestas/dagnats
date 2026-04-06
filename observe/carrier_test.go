// Methodology: unit tests for NATSHeaderCarrier. Each test verifies the
// propagation.TextMapCarrier contract with at least 2 assertions covering
// both positive and negative space.

package observe

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/propagation"
)

func TestNATSHeaderCarrier_RoundTrip(t *testing.T) {
	propagator := propagation.TraceContext{}

	// Simulate an incoming traceparent header.
	original := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	src := nats.Header{}
	src.Set("traceparent", original)

	// Extract trace context from the source header.
	ctx := propagator.Extract(context.Background(), NATSHeaderCarrier{Header: src})

	// Re-inject into a fresh header.
	dst := nats.Header{}
	propagator.Inject(ctx, NATSHeaderCarrier{Header: dst})

	got := dst.Get("traceparent")
	if got != original {
		t.Fatalf("traceparent mismatch: got %q, want %q", got, original)
	}

	// Negative space: an unrelated key must not appear.
	if dst.Get("unrelated") != "" {
		t.Fatal("unexpected key 'unrelated' in destination header")
	}
}

func TestNATSHeaderCarrier_Keys(t *testing.T) {
	h := nats.Header{}
	h.Set("traceparent", "tp-value")
	h.Set("tracestate", "ts-value")

	carrier := NATSHeaderCarrier{Header: h}
	keys := carrier.Keys()

	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}

	// Verify both trace header keys are present.
	keySet := map[string]bool{}
	for _, k := range keys {
		keySet[k] = true
	}
	for _, want := range []string{"traceparent", "tracestate"} {
		if !keySet[want] {
			t.Fatalf("missing key %q in %v", want, keys)
		}
	}

	// Negative space: nil header returns nil keys.
	nilCarrier := NATSHeaderCarrier{}
	if nilCarrier.Keys() != nil {
		t.Fatal("expected nil keys for nil Header")
	}
}
