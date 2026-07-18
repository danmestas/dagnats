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
	return extractWithFallback(msg.Headers(), evt)
}

// ExtractTraceContextHeader reads W3C trace context directly from a
// NATS header map. Returns context.Background() when hdr is nil or
// carries no traceparent. This is the header-level entry point for
// transports (e.g. nats-micro requests) that expose headers without a
// *nats.Msg or jetstream.Msg.
func ExtractTraceContextHeader(hdr nats.Header) context.Context {
	if hdr == nil {
		return context.Background()
	}
	if hdr.Get("traceparent") == "" {
		return context.Background()
	}
	return otel.GetTextMapPropagator().Extract(
		context.Background(),
		NATSHeaderCarrier{Header: hdr},
	)
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
	return extractWithFallback(msg.Header, evt)
}

// extractWithFallback prefers the wire header and falls back to the
// event's persisted traceparent, which is the only carrier available
// when history is replayed rather than consumed live.
func extractWithFallback(
	hdr nats.Header,
	evt *protocol.Event,
) context.Context {
	if hdr.Get("traceparent") != "" {
		return ExtractTraceContextHeader(hdr)
	}
	if evt != nil && evt.TraceParent != "" {
		replay := nats.Header{}
		replay.Set("traceparent", evt.TraceParent)
		if evt.TraceState != "" {
			replay.Set("tracestate", evt.TraceState)
		}
		return ExtractTraceContextHeader(replay)
	}
	return context.Background()
}
