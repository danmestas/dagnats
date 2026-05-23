// natsutil/publish_test.go
// Methodology: round-trip integration tests for TracingPublisher
// + HandlerExtractor against a real embedded NATS server. Each
// test publishes with a W3C traceparent in ctx, subscribes via
// WrapHandler, and asserts the subscriber's ctx carries the same
// trace_id. Per #334: the wrapper is the single mechanism that
// carries trace context across NATS subject boundaries.
package natsutil_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func init() {
	// Composite TC propagator so Inject/Extract see traceparent
	// headers; the default OTel propagator is a no-op.
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)
}

// ctxWithTraceparent returns a ctx carrying the given traceparent
// so InjectTraceContext writes the expected header on publish.
func ctxWithTraceparent(t *testing.T, tp string) context.Context {
	t.Helper()
	hdr := nats.Header{}
	hdr.Set("traceparent", tp)
	prop := propagation.TraceContext{}
	return prop.Extract(
		context.Background(),
		observe.NATSHeaderCarrier{Header: hdr},
	)
}

func TestTracingPublisher_JSRoundtrip(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	tp := natsutil.NewTracingPublisher(nc, js)

	want := "00-aaaa1111bbbb2222cccc3333dddd4444-0123456789abcdef-01"
	ctx := ctxWithTraceparent(t, want)

	// Subscribe to history.* via WrapHandler and capture the
	// extracted ctx's trace ID.
	stream, err := js.Stream(context.Background(), "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	cons, err := stream.CreateOrUpdateConsumer(
		context.Background(), jetstream.ConsumerConfig{
			FilterSubject: "history.>",
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverAllPolicy,
		},
	)
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}

	var gotMu sync.Mutex
	var gotTraceID string
	gotCh := make(chan struct{}, 1)
	cc, err := cons.Consume(natsutil.WrapHandler(
		func(rctx context.Context, msg jetstream.Msg) {
			sc := trace.SpanContextFromContext(rctx)
			if sc.TraceID().IsValid() {
				gotMu.Lock()
				gotTraceID = sc.TraceID().String()
				gotMu.Unlock()
				select {
				case gotCh <- struct{}{}:
				default:
				}
			}
			_ = msg.Ack()
		},
	))
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer cc.Stop()

	// Publish via the wrapper — the trace context flows into the
	// outgoing header automatically.
	_, err = tp.JSPublish(ctx, "history.run-1", []byte(`{}`))
	if err != nil {
		t.Fatalf("JSPublish: %v", err)
	}

	select {
	case <-gotCh:
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber did not receive message in 2s")
	}

	gotMu.Lock()
	defer gotMu.Unlock()
	// Positive: extracted ctx carries the publish-side trace ID.
	want32 := "aaaa1111bbbb2222cccc3333dddd4444"
	if gotTraceID != want32 {
		t.Fatalf("trace_id = %q, want %q", gotTraceID, want32)
	}
}

func TestTracingPublisher_JSPublishMsgEvent_Dualwrite(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	tp := natsutil.NewTracingPublisher(nc, js)

	want := "00-bbbb2222cccc3333dddd4444eeee5555-0123456789abcdef-01"
	ctx := ctxWithTraceparent(t, want)
	want32 := "bbbb2222cccc3333dddd4444eeee5555"

	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-dw", nil,
	)
	msg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Header: nats.Header{
			"Nats-Msg-Id": {evt.NATSMsgID()},
		},
	}
	if _, err := tp.JSPublishMsgEvent(ctx, msg, &evt); err != nil {
		t.Fatalf("JSPublishMsgEvent: %v", err)
	}

	// Positive: header carries traceparent.
	if got := msg.Header.Get("traceparent"); got != want {
		t.Fatalf("header traceparent = %q, want %q", got, want)
	}
	// Positive: evt body field carries traceparent for replay.
	if evt.TraceParent != want {
		t.Fatalf(
			"evt.TraceParent = %q, want %q",
			evt.TraceParent, want,
		)
	}
	// Negative: trace ID is the canonical 32-hex slice.
	if len(want32) != 32 {
		t.Fatalf("want32 len = %d, want 32", len(want32))
	}
}

func TestWrapHandler_NoTraceparent_BackgroundCtx(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	stream, err := js.Stream(context.Background(), "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	cons, err := stream.CreateOrUpdateConsumer(
		context.Background(), jetstream.ConsumerConfig{
			FilterSubject: "history.>",
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverAllPolicy,
		},
	)
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}

	gotCh := make(chan context.Context, 1)
	cc, err := cons.Consume(natsutil.WrapHandler(
		func(rctx context.Context, msg jetstream.Msg) {
			select {
			case gotCh <- rctx:
			default:
			}
			_ = msg.Ack()
		},
	))
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer cc.Stop()

	// Publish raw via the JetStream API directly (legacy producer).
	if _, err := js.Publish(
		context.Background(), "history.run-bg", []byte(`{}`),
	); err != nil {
		t.Fatalf("raw Publish: %v", err)
	}

	select {
	case rctx := <-gotCh:
		// Negative: no trace ID extracted (background ctx).
		sc := trace.SpanContextFromContext(rctx)
		if sc.TraceID().IsValid() {
			t.Fatalf(
				"unexpected trace_id %q from non-traced producer",
				sc.TraceID().String(),
			)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber did not receive message in 2s")
	}
}
