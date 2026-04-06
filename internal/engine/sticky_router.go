// engine/sticky_router.go
// StickyRouter manages worker affinity bindings for workflow runs.
// A binding maps a run ID to a specific worker ID so subsequent steps
// in a sticky workflow route to the same worker. The router may be nil
// when the sticky_bindings KV bucket does not exist — all methods are
// safe to call on a nil receiver.
package engine

import (
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// StickyRouter owns the lifecycle of run-to-worker bindings.
type StickyRouter struct {
	kv               jetstream.KeyValue
	js               jetstream.JetStream
	sleepTimer       *SleepTimer
	tracer           trace.Tracer
	stepEnqueueCount metric.Int64Counter
}

// NewStickyRouter returns a StickyRouter for the given KV bucket.
// Returns nil if kv is nil — callers can safely call methods on nil.
func NewStickyRouter(
	kv jetstream.KeyValue,
	js jetstream.JetStream,
	sleepTimer *SleepTimer,
	tracer trace.Tracer,
	stepEnqueueCount metric.Int64Counter,
) *StickyRouter {
	if kv == nil {
		return nil
	}
	if js == nil {
		panic("NewStickyRouter: js must not be nil when kv is set")
	}
	if tracer == nil {
		panic("NewStickyRouter: tracer must not be nil")
	}
	return &StickyRouter{
		kv:               kv,
		js:               js,
		sleepTimer:       sleepTimer,
		tracer:           tracer,
		stepEnqueueCount: stepEnqueueCount,
	}
}
