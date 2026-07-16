// Methodology: unit tests for EnsureDefaultPropagator's install/no-clobber/
// idempotence contract. Each test saves and restores the global OTel
// propagator so tests do not leak state to each other. At least 2
// assertions per test covering both positive and negative space.

package observe

import (
	"context"
	"sync"

	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// fakePropagator is a minimal sentinel TextMapPropagator used to
// prove EnsureDefaultPropagator never clobbers a pre-installed
// custom propagator.
type fakePropagator struct{}

func (fakePropagator) Inject(context.Context, propagation.TextMapCarrier) {}

func (fakePropagator) Extract(
	ctx context.Context, _ propagation.TextMapCarrier,
) context.Context {
	return ctx
}

func (fakePropagator) Fields() []string { return []string{"x-sentinel"} }

func TestEnsureDefaultPropagator_InstallsWhenNoop(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	defer otel.SetTextMapPropagator(prev)

	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(),
	)

	EnsureDefaultPropagator()

	fields := otel.GetTextMapPropagator().Fields()
	// Positive: TraceContext field present.
	if !containsField(fields, "traceparent") {
		t.Fatalf("Fields() = %v, want traceparent present", fields)
	}
	// Positive: Baggage field also present (proves composite, not
	// just TraceContext alone).
	if !containsField(fields, "tracestate") {
		t.Fatalf("Fields() = %v, want tracestate present", fields)
	}
}

func TestEnsureDefaultPropagator_DoesNotClobberCustom(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	defer otel.SetTextMapPropagator(prev)

	otel.SetTextMapPropagator(fakePropagator{})

	EnsureDefaultPropagator()

	fields := otel.GetTextMapPropagator().Fields()
	// Positive: sentinel propagator is still installed.
	if len(fields) != 1 || fields[0] != "x-sentinel" {
		t.Fatalf(
			"Fields() = %v, want unchanged sentinel [x-sentinel]",
			fields,
		)
	}
	// Negative: default composite was not installed on top of it.
	if containsField(fields, "traceparent") {
		t.Fatalf(
			"Fields() = %v, want no traceparent (custom clobbered)",
			fields,
		)
	}
}

func TestEnsureDefaultPropagator_Idempotent(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	defer otel.SetTextMapPropagator(prev)

	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(),
	)

	EnsureDefaultPropagator()
	EnsureDefaultPropagator()

	fields := otel.GetTextMapPropagator().Fields()
	// Positive: both fields present after two calls.
	if !containsField(fields, "traceparent") ||
		!containsField(fields, "tracestate") {
		t.Fatalf(
			"Fields() = %v, want traceparent+tracestate", fields,
		)
	}
	// Negative: exactly 3 fields (traceparent, tracestate,
	// baggage) — not double-wrapped by the second call, which
	// would produce 6.
	if len(fields) != 3 {
		t.Fatalf(
			"Fields() len = %d, want 3 (not double-wrapped)",
			len(fields),
		)
	}
}

// TestEnsureDefaultPropagator_ConcurrentInstall proves the mutex
// serializes concurrent first-party callers (e.g. many NewWorker
// calls racing at startup) instead of interleaving the get-check-set
// sequence. Run with -race: a missing or too-narrow lock shows up as
// a detected data race, not just a wrong assertion.
func TestEnsureDefaultPropagator_ConcurrentInstall(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	defer otel.SetTextMapPropagator(prev)

	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(),
	)

	const goroutineCount = 16
	var wg sync.WaitGroup
	wg.Add(goroutineCount)
	for i := 0; i < goroutineCount; i++ {
		go func() {
			defer wg.Done()
			EnsureDefaultPropagator()
		}()
	}
	wg.Wait()

	fields := otel.GetTextMapPropagator().Fields()
	// Positive: exactly the composite's 3 fields survived concurrent
	// installs — no double-wrapping from an interleaved get-check-set.
	if len(fields) != 3 ||
		!containsField(fields, "traceparent") ||
		!containsField(fields, "tracestate") ||
		!containsField(fields, "baggage") {
		t.Fatalf(
			"Fields() = %v, want exactly [traceparent tracestate baggage]",
			fields,
		)
	}

	otel.SetTextMapPropagator(fakePropagator{})
	wg.Add(goroutineCount)
	for i := 0; i < goroutineCount; i++ {
		go func() {
			defer wg.Done()
			EnsureDefaultPropagator()
		}()
	}
	wg.Wait()

	fields = otel.GetTextMapPropagator().Fields()
	// Negative: a pre-installed custom propagator is not clobbered by
	// a later concurrent burst of calls.
	if len(fields) != 1 || fields[0] != "x-sentinel" {
		t.Fatalf(
			"Fields() = %v, want unchanged sentinel [x-sentinel]",
			fields,
		)
	}
}

// containsField reports whether name is present in fields.
func containsField(fields []string, name string) bool {
	for _, f := range fields {
		if f == name {
			return true
		}
	}
	return false
}
