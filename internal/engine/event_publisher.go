// internal/engine/event_publisher.go
// Lifecycle event publisher for engine-emitted events. Mirrors the
// worker-side publishEvent pattern at worker/context.go but without a
// per-task context — engine has only a TracingPublisher handle and an Event.
//
// The single deeper helper hides marshal + Nats-Msg-Id header + publish
// from each call site so the orchestrator dispatch path stays a thin
// orchestration loop. Trace context is injected by the TracingPublisher
// wrapper before the message goes on the wire.
package engine

import (
	"context"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// publishLifecycleEvent publishes evt to the history stream with
// proper Nats-Msg-Id dedup. Caller has already populated the Event
// (Type, RunID, StepID, AttemptNumber, etc.); this function only
// handles marshal + msg-id + publish. Trace context is injected by
// the TracingPublisher into both the *nats.Msg header and the
// Event payload (dual-write) so replay still has the trace ID.
func publishLifecycleEvent(
	ctx context.Context,
	tp *natsutil.TracingPublisher,
	evt protocol.Event,
) error {
	if tp == nil {
		panic("publishLifecycleEvent: tp must not be nil")
	}
	if evt.RunID == "" {
		panic("publishLifecycleEvent: evt.RunID must not be empty")
	}
	if evt.Type == "" {
		panic("publishLifecycleEvent: evt.Type must not be empty")
	}
	msg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Header: nats.Header{
			"Nats-Msg-Id": {evt.NATSMsgID()},
		},
	}
	// JSPublishMsgEvent injects trace context, then marshals evt
	// internally so the persisted body carries TraceParent. Don't
	// pre-set msg.Data here.
	_, err := tp.JSPublishMsgEvent(ctx, msg, &evt)
	return err
}
