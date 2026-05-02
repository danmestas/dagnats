// internal/engine/event_publisher.go
// Lifecycle event publisher for engine-emitted events. Mirrors the
// worker-side publishEvent pattern at worker/context.go but without a
// per-task context — engine has only a JetStream handle and an Event.
//
// The single deeper helper hides marshal + Nats-Msg-Id header + publish
// from each call site so the orchestrator dispatch path stays a thin
// orchestration loop.
package engine

import (
	"context"

	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// publishLifecycleEvent publishes evt to the history stream with
// proper Nats-Msg-Id dedup. Caller has already populated the Event
// (Type, RunID, StepID, AttemptNumber, etc.); this function only
// handles marshal + msg-id + publish.
//
// No trace-context propagation: engine is not running inside an OTEL
// span at dispatch time the way the worker is inside a handler span.
// If telemetry surfaces a need later, add observe.InjectTraceContext
// at the publish site (one-line change).
func publishLifecycleEvent(
	ctx context.Context,
	js jetstream.JetStream,
	evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("publishLifecycleEvent: evt.RunID must not be empty")
	}
	if evt.Type == "" {
		panic("publishLifecycleEvent: evt.Type must not be empty")
	}
	data, err := evt.Marshal()
	if err != nil {
		return err
	}
	msg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Data:    data,
		Header: nats.Header{
			"Nats-Msg-Id": {evt.NATSMsgID()},
		},
	}
	_, err = js.PublishMsg(ctx, msg)
	return err
}
