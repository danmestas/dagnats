// pollsdk_test.go
// Verifies that the Go SDK's poll decode path surfaces the per-task W3C
// trace context the bridge emits (issue #538), and that a worker can
// turn it into a parent context without hand-rolling W3C extraction.
//
// Methodology: real NATS server, real bridge, real HTTP roundtrip, and
// the REAL sdk/httpclient decode path — httpclient.Client.Poll, not
// encoding/json directly. The bug being guarded lives in that path: the
// bridge already emits the fields (#537) and plain json.Decode with no
// DisallowUnknownFields discards anything protocol.TaskPayload does not
// declare, so a struct-level round-trip test passes even when the JSON
// tag is wrong. protocol.Event uses the UNDERSCORED trace_parent; this
// seam must use the W3C literal traceparent. Driving the live bridge
// rather than a static fixture keeps the expected wire keys from
// drifting away from what the producer actually writes.
package bridge

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/sdk/httpclient"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// sdkPollTimeout bounds the long-poll so a broken dispatch path fails
// the test rather than hanging it.
const sdkPollTimeout = 3 * time.Second

// installRealPropagator swaps in a working W3C propagator for the test
// and restores the previous one. Without it the default noop propagator
// injects nothing and every trace assertion passes vacuously.
func installRealPropagator(t *testing.T) {
	t.Helper()
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })
}

// TestSDKPollDecodesDispatchTraceContext pins #538: a task decoded
// through sdk/httpclient must carry the bridge's traceparent, and
// observe must turn it into a usable remote parent context.
func TestSDKPollDecodesDispatchTraceContext(t *testing.T) {
	installRealPropagator(t)

	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc, natsutil.WithStoreBudget(storeBudgetBytes))
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	b := newTestBridge(t, nc)
	exporter := recordBridgeSpans(t, b)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	const inboundTP = "00-4bf92f3577b34da6a3ce929d0e0e4736-" +
		"00f067aa0ba902b7-01"
	const inboundSpanID = "00f067aa0ba902b7"
	wantTraceID := w3cTraceID(t, inboundTP)

	hdr := nats.Header{}
	hdr.Set("traceparent", inboundTP)
	publishTaskMsg(t, nc, "run-pollsdk", hdr)

	client := httpclient.New(ts.URL)
	tasks, err := client.Poll(
		context.Background(), []string{"echo"}, 1, sdkPollTimeout,
	)
	if err != nil {
		t.Fatalf("client.Poll: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("got %d tasks, want 1", len(tasks))
	}

	// The whole point of the issue: this field is silently dropped when
	// the tag reads trace_parent instead of the W3C traceparent.
	if tasks[0].TraceParent == "" {
		t.Fatalf(
			"TaskPayload.TraceParent is empty after decoding a live "+
				"bridge poll response; the JSON tag does not match the "+
				"wire key (task %+v)", tasks[0],
		)
	}
	matches := traceparentPattern.FindStringSubmatch(tasks[0].TraceParent)
	if matches == nil {
		t.Fatalf(
			"TraceParent %q does not match W3C grammar",
			tasks[0].TraceParent,
		)
	}
	if matches[1] != wantTraceID {
		t.Fatalf(
			"TraceParent trace_id=%s, want inbound trace_id=%s",
			matches[1], wantTraceID,
		)
	}

	span := dispatchSpanStub(t, exporter)
	wantSpanID := span.SpanContext.SpanID().String()
	if matches[2] != wantSpanID {
		t.Fatalf(
			"TraceParent span_id=%s, want bridge.dispatch span_id=%s",
			matches[2], wantSpanID,
		)
	}

	// The decoded fields must be usable as a parent without the worker
	// reimplementing W3C extraction.
	sc := trace.SpanContextFromContext(
		observe.TraceContextFromTask(tasks[0]),
	)
	if !sc.IsValid() {
		t.Fatalf(
			"TraceContextFromTask produced an invalid span context "+
				"from TraceParent %q", tasks[0].TraceParent,
		)
	}
	if sc.TraceID().String() != wantTraceID {
		t.Fatalf(
			"extracted trace_id=%s, want %s",
			sc.TraceID().String(), wantTraceID,
		)
	}
	// Negative space: parenting onto the inbound span instead of
	// bridge.dispatch would make worker spans siblings of dispatch.
	if sc.SpanID().String() == inboundSpanID {
		t.Fatalf(
			"extracted span_id equals inbound span_id %s", inboundSpanID,
		)
	}
}

// TestSDKPollLeavesTraceFieldsEmptyWhenAbsent pins the degraded path: a
// response with no trace context must decode to empty strings, and the
// extraction helper must yield no span context rather than garbage.
func TestSDKPollLeavesTraceFieldsEmptyWhenAbsent(t *testing.T) {
	// A real propagator is installed deliberately: the property under
	// test is "no inbound context yields no field even when injection
	// is fully wired", which the noop propagator would satisfy for the
	// wrong reason.
	installRealPropagator(t)

	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc, natsutil.WithStoreBudget(storeBudgetBytes))
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	// No recordBridgeSpans: b.tracer stays the global noop tracer, which
	// is what a deployment with no exporter configured actually runs.
	b := newTestBridge(t, nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	publishTaskMsg(t, nc, "run-pollsdk-degraded", nil)

	client := httpclient.New(ts.URL)
	tasks, err := client.Poll(
		context.Background(), []string{"echo"}, 1, sdkPollTimeout,
	)
	if err != nil {
		t.Fatalf("client.Poll: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("got %d tasks, want 1", len(tasks))
	}
	if tasks[0].TraceParent != "" {
		t.Fatalf(
			"TraceParent = %q, want empty", tasks[0].TraceParent,
		)
	}
	if tasks[0].TraceState != "" {
		t.Fatalf(
			"TraceState = %q, want empty", tasks[0].TraceState,
		)
	}
	sc := trace.SpanContextFromContext(
		observe.TraceContextFromTask(tasks[0]),
	)
	if sc.IsValid() {
		t.Fatalf(
			"TraceContextFromTask fabricated a span context from an "+
				"empty TraceParent: %s", sc.TraceID().String(),
		)
	}
}
