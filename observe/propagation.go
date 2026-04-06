// propagation.go provides trace context propagation helpers for
// NATS message boundaries. InjectTraceContext writes W3C trace
// context to both NATS headers and the Event payload for
// persistence. ExtractTraceContext reads from headers first,
// falling back to Event.TraceParent for replay scenarios.
package observe

import (
	"context"

	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
)

// InjectTraceContext writes W3C trace context to both NATS
// headers and the Event's TraceParent/TraceState fields for
// persistence. Panics on nil msg (programmer error). evt may
// be nil when no event dual-write is needed.
func InjectTraceContext(
	ctx context.Context,
	msg *nats.Msg,
	evt *protocol.Event,
) {
	if ctx == nil {
		panic("InjectTraceContext: ctx must not be nil")
	}
	if msg == nil {
		panic("InjectTraceContext: msg must not be nil")
	}
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	prop := otel.GetTextMapPropagator()
	prop.Inject(ctx, NATSHeaderCarrier{Header: msg.Header})
	if evt != nil {
		evt.TraceParent = msg.Header.Get("traceparent")
		evt.TraceState = msg.Header.Get("tracestate")
	}
}

// ExtractTraceContext reads W3C trace context from NATS
// headers, falling back to Event.TraceParent for replay.
// Accepts jetstream.Msg for consumer message handling.
// evt may be nil when no event fallback is needed.
func ExtractTraceContext(
	msg jetstream.Msg,
	evt *protocol.Event,
) context.Context {
	if msg == nil {
		panic("ExtractTraceContext: msg must not be nil")
	}
	if hdrs := msg.Headers(); hdrs != nil {
		if hdrs.Get("traceparent") != "" {
			return otel.GetTextMapPropagator().Extract(
				context.Background(),
				NATSHeaderCarrier{Header: hdrs},
			)
		}
	}
	if evt != nil && evt.TraceParent != "" {
		hdr := nats.Header{}
		hdr.Set("traceparent", evt.TraceParent)
		if evt.TraceState != "" {
			hdr.Set("tracestate", evt.TraceState)
		}
		return otel.GetTextMapPropagator().Extract(
			context.Background(),
			NATSHeaderCarrier{Header: hdr},
		)
	}
	return context.Background()
}

// ExtractTraceContextRaw reads W3C trace context from a raw
// *nats.Msg header, falling back to Event.TraceParent for
// replay. Used in publish paths that work with *nats.Msg.
func ExtractTraceContextRaw(
	msg *nats.Msg,
	evt *protocol.Event,
) context.Context {
	if msg == nil {
		panic("ExtractTraceContextRaw: msg must not be nil")
	}
	if msg.Header != nil {
		if msg.Header.Get("traceparent") != "" {
			return otel.GetTextMapPropagator().Extract(
				context.Background(),
				NATSHeaderCarrier{Header: msg.Header},
			)
		}
	}
	if evt != nil && evt.TraceParent != "" {
		hdr := nats.Header{}
		hdr.Set("traceparent", evt.TraceParent)
		if evt.TraceState != "" {
			hdr.Set("tracestate", evt.TraceState)
		}
		return otel.GetTextMapPropagator().Extract(
			context.Background(),
			NATSHeaderCarrier{Header: hdr},
		)
	}
	return context.Background()
}
