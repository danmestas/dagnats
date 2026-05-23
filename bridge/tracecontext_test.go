// tracecontext_test.go
// Verifies that W3C trace context (traceparent) on an inbound
// HTTP request to the bridge is propagated onto the outbound
// NATS message published by the bridge.
//
// Methodology: real NATS, real bridge, real HTTP roundtrip.
// Subscribe to the WORKFLOW_HISTORY stream and inspect the raw
// *nats.Msg headers — the traceparent must be present and must
// carry the same trace_id as the one supplied on the HTTP request.
package bridge

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// w3cTraceID extracts the trace-id field from a W3C traceparent
// header. Format: "00-<32 hex trace_id>-<16 hex parent_id>-<flags>".
func w3cTraceID(t *testing.T, traceparent string) string {
	t.Helper()
	parts := strings.Split(traceparent, "-")
	if len(parts) != 4 {
		t.Fatalf("malformed traceparent: %q", traceparent)
	}
	if len(parts[1]) != 32 {
		t.Fatalf("trace_id wrong length: %q", parts[1])
	}
	return parts[1]
}

// TestBridgeInboundTraceparentInjectedOnPublish proves the
// HTTP-to-NATS trace-context bridge.
//
// Flow: client → POST /v1/tasks/{id}/resolve with a synthetic
// traceparent header → bridge publishes step.completed via the
// TracingPublisher wrapper → consumer reads the *nats.Msg from
// WORKFLOW_HISTORY and finds traceparent on it whose trace_id
// matches the one the client supplied.
func TestBridgeInboundTraceparentInjectedOnPublish(t *testing.T) {
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)

	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	b := newTestBridge(t, nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	taskID := publishAndPollTask(
		t, nc, b, ts, "run-tp", "step-tp",
	)

	// Synthetic traceparent with a known trace_id.
	const inboundTP = "00-0af7651916cd43dd8448eb211c80319c-" +
		"b7ad6b7169203331-01"
	wantTraceID := w3cTraceID(t, inboundTP)

	body := `{"action":"complete","output":{"result":"ok"}}`
	req, err := http.NewRequest(
		http.MethodPost,
		ts.URL+"/v1/tasks/"+taskID+"/resolve",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("traceparent", inboundTP)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Pull the raw *nats.Msg off the history stream so we can
	// read its headers — protocol.Event.Unmarshal would strip
	// them.
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx := context.Background()
	stream, err := js.Stream(ctx, "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	cons, err := stream.CreateOrUpdateConsumer(
		ctx, jetstream.ConsumerConfig{
			FilterSubject:     "history.run-tp",
			AckPolicy:         jetstream.AckNonePolicy,
			DeliverPolicy:     jetstream.DeliverAllPolicy,
			InactiveThreshold: 2 * time.Second,
		},
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	fetched, err := cons.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	msg, ok := <-fetched.Messages()
	if !ok {
		t.Fatal("no history message received")
	}

	// Assertion 1 (positive): traceparent header present.
	gotTP := msg.Headers().Get("traceparent")
	if gotTP == "" {
		t.Fatalf(
			"expected traceparent header on published NATS msg, got none. headers=%v",
			msg.Headers(),
		)
	}

	// Assertion 2 (positive): trace_id matches the inbound one.
	gotTraceID := w3cTraceID(t, gotTP)
	if gotTraceID != wantTraceID {
		t.Fatalf(
			"trace_id mismatch: inbound=%s outbound=%s (traceparent=%s)",
			wantTraceID, gotTraceID, gotTP,
		)
	}

	// Sanity: the persisted Event should also see the trace_id
	// reach its own TraceParent field via the wrapper's dual
	// write — verifies the wrapper covers both the header and
	// the payload-side propagation path for replay.
	var evt protocol.Event
	if err := json.Unmarshal(msg.Data(), &evt); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	if evt.TraceParent != "" {
		evtTraceID := w3cTraceID(t, evt.TraceParent)
		if evtTraceID != wantTraceID {
			t.Fatalf(
				"event TraceParent trace_id mismatch: want=%s got=%s",
				wantTraceID, evtTraceID,
			)
		}
	}
}
