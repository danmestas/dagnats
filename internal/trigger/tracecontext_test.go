// trigger/tracecontext_test.go
// Methodology: integration tests with a real embedded NATS server plus a
// context-capturing jetstream.KeyValue stand-in swapped onto the service,
// so the exact context each KV call receives can be asserted directly —
// no production test seam required. The span recorder / composite W3C
// propagator come from fire_test.go's installSpanRecorder, since
// extraction is inert under the noop propagator.
//
// Assertions are on trace ID only, never span parentage: a non-recording
// span reuses its parent's span ID, so a parentage assertion can pass for
// the wrong reason. Every test carries >=2 assertions (the propagated
// trace ID plus the negative space that would survive a naive refactor:
// the 5s KV bound and its derivation from the caller's context).
package trigger

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/trace"
)

// inboundTraceparent is a fixed, valid W3C traceparent. The trace ID is
// the only field asserted on; the sampled flag is set so the extracted
// span context is valid.
const inboundTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

// inboundTraceID is the trace-id field of inboundTraceparent.
const inboundTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"

// ctxCapturingKV records the context handed to Get so tests can assert
// what the production code derived. Embeds jetstream.KeyValue so only
// the method under test is implemented; any other call is a test bug and
// panics on the nil embedded interface, which is the desired loud
// failure.
// When release is non-nil, Get blocks until it is closed. That keeps the
// call in flight so a test can observe the captured context BEFORE
// loadTriggerType's deferred cancel() runs — without it, the captured
// context is always Canceled by the time the test inspects it and a
// cancellation assertion passes for the wrong reason.
type ctxCapturingKV struct {
	jetstream.KeyValue
	got     chan context.Context
	release chan struct{}
}

func newCtxCapturingKV() *ctxCapturingKV {
	return &ctxCapturingKV{got: make(chan context.Context, 8)}
}

func (k *ctxCapturingKV) Get(
	ctx context.Context, _ string,
) (jetstream.KeyValueEntry, error) {
	k.got <- ctx
	if k.release != nil {
		<-k.release
	}
	// Short-circuit the handler: the context is what is under test, so
	// a canonical miss keeps the rest of the ack path out of the way.
	return nil, jetstream.ErrKeyNotFound
}

// awaitCapturedContext returns the first context Get received, failing
// the test on a bounded timeout rather than blocking CI forever.
func awaitCapturedContext(
	t *testing.T, kv *ctxCapturingKV,
) context.Context {
	t.Helper()
	select {
	case ctx := <-kv.got:
		return ctx
	case <-time.After(3 * time.Second):
		t.Fatal("KV Get was never called")
		return nil
	}
}

// startServiceWithCapturingKV boots a TriggerService against a real
// embedded NATS server with the trigger_types KV swapped for the
// capturing stand-in, after construction (which needs the real bucket)
// and before Start (which gates the ack micro service on non-nil KV).
func startServiceWithCapturingKV(
	t *testing.T,
) (*nats.Conn, *TriggerService, *ctxCapturingKV) {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	setupWithTriggerTypes(t, nc)
	svc, err := NewTriggerService(nc, "1.2.3")
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	kv := newCtxCapturingKV()
	svc.triggerTypesKV = kv
	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(svc.Stop)
	return nc, svc, kv
}

// requestAck sends a well-formed ack request carrying hdr and returns
// the reply payload.
func requestAckWithHeader(
	t *testing.T, nc *nats.Conn, hdr nats.Header,
) []byte {
	t.Helper()
	body, err := json.Marshal(RegisterTriggerTypeRequest{
		Name:          "demo-kind",
		OwnerWorkerID: "worker-1",
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	reply, err := nc.RequestMsg(&nats.Msg{
		Subject: ackSubject,
		Header:  hdr,
		Data:    body,
	}, 3*time.Second)
	if err != nil {
		t.Fatalf("RequestMsg %s: %v", ackSubject, err)
	}
	return reply.Data
}

// TestAckMicroThreadsInboundTraceContextToKV proves the traceparent on
// the inbound micro request reaches the context the trigger_types KV
// call runs under — the property #528 established for the API handlers.
func TestAckMicroThreadsInboundTraceContextToKV(t *testing.T) {
	installSpanRecorder(t)
	nc, _, kv := startServiceWithCapturingKV(t)

	hdr := nats.Header{}
	hdr.Set("traceparent", inboundTraceparent)
	reply := requestAckWithHeader(t, nc, hdr)

	ctx := awaitCapturedContext(t, kv)
	got := trace.SpanContextFromContext(ctx).TraceID().String()
	if got != inboundTraceID {
		t.Fatalf("KV context trace ID = %q, want %q", got, inboundTraceID)
	}
	// Negative space: the wire contract is unchanged — a missing key
	// still yields the canonical "not registered" error envelope, so
	// threading the context did not alter what workers parse.
	var resp ackResponse
	if err := json.Unmarshal(reply, &resp); err != nil {
		t.Fatalf("unmarshal reply %q: %v", reply, err)
	}
	if resp.Error == "" {
		t.Fatal("ack reply error is empty, want 'not registered'")
	}
}

// TestAckMicroWithoutTraceparentStillWorks is the negative-space
// counterpart: no inbound trace context must not break the ack path, and
// the KV context must carry an invalid (absent) span context rather than
// an inherited or fabricated one.
func TestAckMicroWithoutTraceparentStillWorks(t *testing.T) {
	installSpanRecorder(t)
	nc, _, kv := startServiceWithCapturingKV(t)

	reply := requestAckWithHeader(t, nc, nil)

	ctx := awaitCapturedContext(t, kv)
	if trace.SpanContextFromContext(ctx).IsValid() {
		t.Fatalf(
			"KV context has a valid span context %q, want none",
			trace.SpanContextFromContext(ctx).TraceID(),
		)
	}
	if len(reply) == 0 {
		t.Fatal("ack reply is empty, want an error envelope")
	}
}

// TestLoadTriggerTypeBoundsKVTimeout proves the 5s bound on the KV read
// survives the context-threading refactor. This is the property most
// likely to be silently lost: swapping context.Background() for the
// caller's context without re-deriving the timeout would leave the KV
// call unbounded.
func TestLoadTriggerTypeBoundsKVTimeout(t *testing.T) {
	_, svc, kv := startServiceWithCapturingKV(t)

	if _, err := svc.loadTriggerType(context.Background(), "demo-kind"); err == nil {
		t.Fatal("loadTriggerType error = nil, want 'not registered'")
	}

	ctx := awaitCapturedContext(t, kv)
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("KV context has no deadline, want a 5s bound")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > 5*time.Second {
		t.Fatalf(
			"KV context deadline in %v, want within (0s, 5s]", remaining,
		)
	}
}

// TestLoadTriggerTypeDerivesFromCallerContext proves the 5s bound is
// derived FROM the caller's context rather than rooted independently:
// cancelling the caller must cancel the in-flight KV call's context.
//
// The KV call is held open across the cancel so the assertion cannot be
// satisfied by loadTriggerType's own deferred cancel() on return — that
// fires for a Background-rooted timeout too, which would make a
// re-rooted implementation pass.
func TestLoadTriggerTypeDerivesFromCallerContext(t *testing.T) {
	_, svc, kv := startServiceWithCapturingKV(t)
	kv.release = make(chan struct{})

	callerCtx, cancelCaller := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := svc.loadTriggerType(callerCtx, "demo-kind"); err == nil {
			t.Error("loadTriggerType error = nil, want a failure")
		}
	}()

	ctx := awaitCapturedContext(t, kv)
	// Positive space: the bound is still applied on the derived context.
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("KV context has no deadline, want the 5s bound preserved")
	}
	cancelCaller()
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal(
			"cancelling the caller did not cancel the KV context: " +
				"the 5s timeout is not derived from the caller",
		)
	}
	close(kv.release)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("loadTriggerType did not return")
	}
}

// TestSubjectTriggerPropagatesInboundTraceContext covers the second
// instance of the same class bug: SubjectTrigger.handleMessage received
// a *nats.Msg carrying a traceparent and rooted context.Background(),
// so the workflow.started event it published was trace-detached despite
// the type's doc comment claiming auto-flow (#334).
func TestSubjectTriggerPropagatesInboundTraceContext(t *testing.T) {
	installSpanRecorder(t)
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	trig, sub := setupSubjectTrigger(t, nc, js)
	t.Cleanup(func() {
		if cerr := trig.Close(); cerr != nil {
			t.Errorf("Close: %v", cerr)
		}
	})

	hdr := nats.Header{}
	hdr.Set("traceparent", inboundTraceparent)
	if err := nc.PublishMsg(&nats.Msg{
		Subject: "events.user.created",
		Header:  hdr,
		Data:    []byte(`{"user_id":"12345"}`),
	}); err != nil {
		t.Fatalf("PublishMsg: %v", err)
	}

	msg, err := sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("expected workflow.started event: %v", err)
	}
	got := trace.SpanContextFromContext(
		observe.ExtractTraceContextRaw(msg, nil),
	).TraceID().String()
	if got != inboundTraceID {
		t.Fatalf(
			"workflow.started trace ID = %q, want %q", got, inboundTraceID,
		)
	}
	// Negative space: propagation must not have disturbed the payload.
	var evt protocol.Event
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	if evt.Type != protocol.EventWorkflowStarted {
		t.Fatalf("event type = %q, want %q",
			evt.Type, protocol.EventWorkflowStarted)
	}
}
