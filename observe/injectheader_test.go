// Methodology: unit tests for InjectTraceContextHeader, the header-level
// inject counterpart to ExtractTraceContextHeader. Each test covers both
// positive space (a valid span context writes the W3C fields) and
// negative space (a context with no span context writes nothing at all,
// so callers using omitempty never emit an empty or malformed field).
// Tests register a W3C propagator to match production InitTelemetry.

package observe

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestInjectTraceContextHeader_ValidContext(t *testing.T) {
	restore := installPropagator()
	defer restore()

	tp := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	hdr := nats.Header{}
	InjectTraceContextHeader(fakeSpanContext(tp), hdr)

	if got := hdr.Get("traceparent"); got != tp {
		t.Fatalf("traceparent: got %q, want %q", got, tp)
	}
	// Negative space: no tracestate on the source means none written,
	// not an empty-string entry.
	if got := hdr.Get("tracestate"); got != "" {
		t.Fatalf("tracestate: got %q, want empty", got)
	}
}

func TestInjectTraceContextHeader_BackgroundContext(t *testing.T) {
	restore := installPropagator()
	defer restore()

	hdr := nats.Header{}
	InjectTraceContextHeader(context.Background(), hdr)

	if got := hdr.Get("traceparent"); got != "" {
		t.Fatalf("traceparent: got %q, want empty", got)
	}
	if len(hdr) != 0 {
		t.Fatalf("header should stay empty, got %v", hdr)
	}
}
