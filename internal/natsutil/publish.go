// natsutil/publish.go
// TracingPublisher wraps NATS publish operations so W3C trace
// context is auto-injected into every outgoing message header.
// Wrap once at construction (engine, worker, trigger service,
// api Service) and pass *TracingPublisher to subsystems —
// individual publish sites never call observe.InjectTraceContext
// inline. A CI grep forbids raw js.Publish/PublishMsg or
// nc.Publish/PublishMsg outside this file, so a forgotten new
// publish site cannot ship without trace propagation.
//
// Subscribe side: HandlerExtractor wraps a jetstream consumer
// handler to extract W3C context from incoming headers and stash
// it on the message via ContextFromMsg lookups inside the
// handler. The handler does not need to call observe.ExtractTraceContext
// itself for the common case — direct extraction at message
// boundary keeps the propagator concern out of business logic.
package natsutil

import (
	"context"

	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TracingPublisher wraps an nc + js pair, injecting W3C trace
// context into outgoing message headers before delegating to the
// underlying NATS publish. Construct once via NewTracingPublisher
// at the top of each long-lived component; pass the same instance
// down to every subsystem that publishes.
//
// The wrapper holds *nats.Conn (for core publish) and
// jetstream.JetStream (for JS publish). Both are required so the
// same wrapper can be used uniformly across the codebase.
type TracingPublisher struct {
	nc *nats.Conn
	js jetstream.JetStream
}

// NewTracingPublisher constructs a TracingPublisher. Panics on
// nil nc or js — both are programmer errors at startup. The
// returned wrapper is safe for concurrent use by multiple
// goroutines (delegates to the thread-safe NATS client).
func NewTracingPublisher(
	nc *nats.Conn, js jetstream.JetStream,
) *TracingPublisher {
	if nc == nil {
		panic("NewTracingPublisher: nc must not be nil")
	}
	if js == nil {
		panic("NewTracingPublisher: js must not be nil")
	}
	return &TracingPublisher{nc: nc, js: js}
}

// NewTracingPublisherJSOnly constructs a TracingPublisher with
// no core *nats.Conn — only the JetStream surface (JSPublish,
// JSPublishMsg, JSPublishMsgEvent) is functional. Calls to
// Publish or PublishMsg (core NATS) on the returned wrapper will
// panic.
//
// Intended for unit tests that inject a fake jetstream.JetStream
// and never touch core NATS. Production code MUST use
// NewTracingPublisher with a real connection.
func NewTracingPublisherJSOnly(
	js jetstream.JetStream,
) *TracingPublisher {
	if js == nil {
		panic("NewTracingPublisherJSOnly: js must not be nil")
	}
	return &TracingPublisher{nc: nil, js: js}
}

// NewTracingPublisherCoreOnly constructs a TracingPublisher with
// no JetStream surface — only core NATS Publish / PublishMsg are
// functional. Calls to JSPublish / JSPublishMsg / JSPublishMsgEvent
// on the returned wrapper will panic.
//
// Intended for unit tests that exercise only the core-publish path
// (e.g. PutStream's stream.* subject). Production code MUST use
// NewTracingPublisher with both nc and js.
func NewTracingPublisherCoreOnly(
	nc *nats.Conn,
) *TracingPublisher {
	if nc == nil {
		panic("NewTracingPublisherCoreOnly: nc must not be nil")
	}
	return &TracingPublisher{nc: nc, js: nil}
}

// JS returns the wrapped JetStream instance for non-publish
// operations (CreateOrUpdateConsumer, Stream, KeyValue, etc.).
// Read-only access — callers must never invoke Publish or
// PublishMsg on the returned value; the CI lint will reject
// such calls outside this file.
func (tp *TracingPublisher) JS() jetstream.JetStream {
	if tp == nil {
		panic("TracingPublisher.JS: tp must not be nil")
	}
	return tp.js
}

// NC returns the wrapped *nats.Conn for non-publish operations
// (Subscribe, Request, Flush, RequestMsg). Same lint constraint
// as JS: do not call Publish or PublishMsg on the returned conn.
func (tp *TracingPublisher) NC() *nats.Conn {
	if tp == nil {
		panic("TracingPublisher.NC: tp must not be nil")
	}
	return tp.nc
}

// Publish sends data on the given core-NATS subject after
// injecting W3C trace context. Core NATS Publish takes no
// headers, so a *nats.Msg is synthesized to attach the
// traceparent before delegating to PublishMsg. ctx must carry
// the active span for injection to do useful work — call sites
// must thread context, not pass context.Background() blindly.
func (tp *TracingPublisher) Publish(
	ctx context.Context, subject string, data []byte,
) error {
	if tp == nil {
		panic("TracingPublisher.Publish: tp must not be nil")
	}
	if subject == "" {
		panic("TracingPublisher.Publish: subject must not be empty")
	}
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{},
	}
	observe.InjectTraceContext(ctx, msg, nil)
	return tp.nc.PublishMsg(msg)
}

// PublishMsg sends an already-built *nats.Msg over core NATS
// after injecting trace context into its header. If msg has an
// Event payload that should also carry traceparent for replay,
// callers use the four-arg variant PublishMsgEvent instead.
func (tp *TracingPublisher) PublishMsg(
	ctx context.Context, msg *nats.Msg,
) error {
	if tp == nil {
		panic("TracingPublisher.PublishMsg: tp must not be nil")
	}
	if msg == nil {
		panic("TracingPublisher.PublishMsg: msg must not be nil")
	}
	observe.InjectTraceContext(ctx, msg, nil)
	return tp.nc.PublishMsg(msg)
}

// JSPublish sends data on the given subject via JetStream after
// injecting W3C trace context. Returns the PubAck for callers
// that need the assigned sequence. opts are passed through to
// the underlying client (typical use: jetstream.WithMsgID for
// dedup). A *nats.Msg is synthesized to carry the traceparent
// header even though the caller passed only subject+data.
func (tp *TracingPublisher) JSPublish(
	ctx context.Context,
	subject string,
	data []byte,
	opts ...jetstream.PublishOpt,
) (*jetstream.PubAck, error) {
	if tp == nil {
		panic("TracingPublisher.JSPublish: tp must not be nil")
	}
	if subject == "" {
		panic("TracingPublisher.JSPublish: subject must not be empty")
	}
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{},
	}
	observe.InjectTraceContext(ctx, msg, nil)
	return tp.js.PublishMsg(ctx, msg, opts...)
}

// JSPublishMsg sends an already-built *nats.Msg via JetStream
// after injecting W3C trace context into its header. opts are
// passed through (jetstream.WithMsgID, jetstream.WithExpectStream,
// etc.). Caller retains ownership of the *nats.Msg; the wrapper
// only mutates its Header to add traceparent and tracestate.
func (tp *TracingPublisher) JSPublishMsg(
	ctx context.Context,
	msg *nats.Msg,
	opts ...jetstream.PublishOpt,
) (*jetstream.PubAck, error) {
	if tp == nil {
		panic("TracingPublisher.JSPublishMsg: tp must not be nil")
	}
	if msg == nil {
		panic("TracingPublisher.JSPublishMsg: msg must not be nil")
	}
	observe.InjectTraceContext(ctx, msg, nil)
	return tp.js.PublishMsg(ctx, msg, opts...)
}

// JSPublishMsgEvent is the dual-write variant: injects W3C trace
// context into the *nats.Msg header AND mirrors the traceparent
// onto the Event payload so a later replay (post-restart, archive
// import) still has the trace ID inside the persisted record.
//
// IMPORTANT: callers MUST leave msg.Data unset (or empty) — the
// wrapper marshals evt internally AFTER injecting trace context
// so the persisted bytes carry the traceparent. If the caller
// pre-marshals and stuffs msg.Data, the trace ID won't appear in
// the JetStream record's body (header only) and post-restart
// replay will lose it. This is the lesson learned from #334
// integration: the header carries trace context for live
// consumers, but Event.TraceParent is the durable side that the
// snapshot store and post-restart replay rely on.
//
// opts pass through to the underlying client (typical use:
// jetstream.WithMsgID for dedup — though the caller usually sets
// Nats-Msg-Id directly in msg.Header).
func (tp *TracingPublisher) JSPublishMsgEvent(
	ctx context.Context,
	msg *nats.Msg,
	evt *protocol.Event,
	opts ...jetstream.PublishOpt,
) (*jetstream.PubAck, error) {
	if tp == nil {
		panic("JSPublishMsgEvent: tp must not be nil")
	}
	if msg == nil {
		panic("JSPublishMsgEvent: msg must not be nil")
	}
	if evt == nil {
		panic("JSPublishMsgEvent: evt must not be nil")
	}
	if tp.js == nil {
		panic("JSPublishMsgEvent: js must not be nil")
	}
	observe.InjectTraceContext(ctx, msg, evt)
	// Re-marshal evt now that TraceParent / TraceState are set so
	// the body carries the trace ID for durable replay.
	data, err := evt.Marshal()
	if err != nil {
		return nil, err
	}
	msg.Data = data
	return tp.js.PublishMsg(ctx, msg, opts...)
}

// HandlerExtractor wraps a jetstream consumer handler so the
// W3C trace context carried in incoming message headers is
// extracted and made available to the inner handler via a
// fresh context. Subscribe-side counterpart to the inject-on-
// publish concern: the same trace_id flows from publisher to
// consumer without each handler calling observe.ExtractTraceContext
// inline.
//
// The inner handler receives ctx with the extracted span context
// attached. If no traceparent is present (legacy producer, test
// fixture), ctx is context.Background() and the consumer just
// starts a fresh root span. Handlers that need to look up the
// event payload for the TraceParent fallback should call
// observe.ExtractTraceContext directly with their parsed Event.
type HandlerExtractor = jetstream.MessageHandler

// WrapHandler returns a jetstream.MessageHandler that extracts
// W3C trace context from the incoming message before invoking
// the inner handler. The inner handler signature is
// func(ctx context.Context, msg jetstream.Msg) — this differs
// from the raw jetstream.MessageHandler so existing handlers
// must explicitly opt in to context extraction.
//
// Most existing handlers already call observe.ExtractTraceContext
// in their body (e.g. orchestrator.handleEventJS); WrapHandler
// is for new handlers that want the extraction lifted out.
// Panics if inner is nil — programmer error at registration time.
func WrapHandler(
	inner func(ctx context.Context, msg jetstream.Msg),
) HandlerExtractor {
	if inner == nil {
		panic("WrapHandler: inner must not be nil")
	}
	return func(msg jetstream.Msg) {
		ctx := observe.ExtractTraceContext(msg, nil)
		inner(ctx, msg)
	}
}
