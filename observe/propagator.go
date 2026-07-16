// propagator.go installs the default W3C trace-context propagator
// when nothing else has claimed the global slot. Kept separate from
// propagation.go so that file stays focused on NATS header carriers.
package observe

import (
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// ensureDefaultPropagatorMu serializes the get-check-set sequence in
// EnsureDefaultPropagator so concurrent first-party callers (many
// NewWorker calls at startup) cannot interleave it.
var ensureDefaultPropagatorMu sync.Mutex

// EnsureDefaultPropagator installs a TraceContext+Baggage composite
// as the global OTel TextMapPropagator if — and only if — the
// current global is the no-op default (Fields() empty). Never
// overwrites an already-installed propagator, custom or otherwise.
// Idempotent: safe to call from every component constructor. A
// package-level mutex serializes concurrent first-party callers, so
// this function is safe to race from many goroutines; it cannot
// defend against an out-of-band otel.SetTextMapPropagator call racing
// it from outside this package, since OTel's global setter has no
// compare-and-swap — first-party installs must route through here.
func EnsureDefaultPropagator() {
	ensureDefaultPropagatorMu.Lock()
	defer ensureDefaultPropagatorMu.Unlock()

	current := otel.GetTextMapPropagator()
	// Negative-space guard: a non-empty Fields() means something —
	// custom or previously installed default — already claimed the
	// global slot. Leave it untouched.
	if len(current.Fields()) != 0 {
		return
	}
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	// Positive-space post-condition: the install above must have
	// taken effect. A still-empty Fields() here means the OTel API
	// contract changed underneath us — a programmer error worth
	// panicking on rather than silently leaving tracing dark.
	installed := otel.GetTextMapPropagator()
	if len(installed.Fields()) == 0 {
		panic("EnsureDefaultPropagator: post-condition failed: Fields() still empty after install")
	}
}
