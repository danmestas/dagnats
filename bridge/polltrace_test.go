// polltrace_test.go
// Verifies that the poll response carries per-task W3C trace context so
// HTTP worker execution spans can parent onto bridge.dispatch instead of
// orphaning at the dispatch boundary (issue #534).
//
// Methodology: real NATS server, real bridge, real HTTP roundtrip. The
// response body is read RAW and decoded into
// []map[string]json.RawMessage — decoding into pollResponse itself would
// pass even if the JSON key were misspelled, which is exactly the silent
// key-mismatch class this seam is exposed to (protocol.Event uses the
// UNDERSCORED trace_parent; the wire format here must be the W3C
// lowercase traceparent). The Bridge tracer is swapped for a per-test
// synchronous in-memory exporter via recordBridgeSpans so no global
// provider is mutated; the global propagator IS set, matching the
// precedent in bridge/dispatchspan_test.go.
package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// traceparentPattern is the W3C version-00 traceparent grammar. The test
// parses against the spec rather than trusting the producer's own
// formatter, so a malformed-but-present value still fails.
var traceparentPattern = regexp.MustCompile(
	`^00-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}$`,
)

// pollRawTasks issues one poll and decodes the UNDECODED response body
// into per-task key maps, so the wire key names stay observable.
// Decoding into pollResponse would hide a misspelled key entirely.
func pollRawTasks(
	t *testing.T, ts *httptest.Server,
	taskTypes string, maxTasks int, timeoutMs int,
) ([]map[string]json.RawMessage, string) {
	t.Helper()
	if ts == nil {
		t.Fatalf("pollRawTasks: ts must not be nil")
	}
	status, raw := postPollRaw(t, ts, fmt.Sprintf(
		`{"task_types":["%s"],"max_tasks":%d,"timeout_ms":%d}`,
		taskTypes, maxTasks, timeoutMs,
	))
	if status != http.StatusOK {
		t.Fatalf("poll status: got %d, want 200 (body %s)", status, raw)
	}
	if raw == "" {
		t.Fatalf("poll body empty")
	}
	var tasks []map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &tasks); err != nil {
		t.Fatalf("decode raw poll body %q: %v", raw, err)
	}
	return tasks, raw
}

// jsonString unwraps a raw JSON string field.
func jsonString(
	t *testing.T, raw json.RawMessage, field string,
) string {
	t.Helper()
	var out string
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("field %s is not a JSON string (%s): %v", field, raw, err)
	}
	if out == "" {
		t.Fatalf("field %s is empty", field)
	}
	return out
}

// dispatchSpanStub returns the single recorded bridge.dispatch span.
func dispatchSpanStub(
	t *testing.T, exporter *tracetest.InMemoryExporter,
) tracetest.SpanStub {
	t.Helper()
	if exporter == nil {
		t.Fatalf("dispatchSpanStub: exporter must not be nil")
	}
	var found []tracetest.SpanStub
	for _, s := range exporter.GetSpans() {
		if s.Name == "bridge.dispatch" {
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		t.Fatalf("got %d bridge.dispatch spans, want 1", len(found))
	}
	return found[0]
}

// publishTaskMsg publishes one task onto the echo queue, optionally with
// trace headers, and returns the run ID it used.
func publishTaskMsg(
	t *testing.T, nc *nats.Conn, runID string, hdr nats.Header,
) {
	t.Helper()
	if nc == nil {
		t.Fatalf("publishTaskMsg: nc must not be nil")
	}
	if runID == "" {
		t.Fatalf("publishTaskMsg: runID must not be empty")
	}
	payload := protocol.TaskPayload{
		RunID:  runID,
		StepID: "step-trace",
		Input:  json.RawMessage(`{"x":1}`),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	_, err = js.PublishMsg(context.Background(), &nats.Msg{
		Subject: "task.echo." + runID,
		Data:    data,
		Header:  hdr,
	})
	if err != nil {
		t.Fatalf("publish task msg: %v", err)
	}
}

// TestPollResponseCarriesDispatchTraceContext pins #534: each task in the
// poll response must carry a literal "traceparent" field whose trace ID
// is the INBOUND task's trace ID (whole-chain proof) and whose span ID is
// the bridge.dispatch span the worker must parent onto.
func TestPollResponseCarriesDispatchTraceContext(t *testing.T) {
	// Restored on cleanup so this cannot decide a sibling test's result
	// by leaving a working propagator installed globally.
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

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
	publishTaskMsg(t, nc, "run-polltrace", hdr)

	tasks, raw := pollRawTasks(t, ts, "echo", 1, 3000)
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d (body %s)", len(tasks), raw)
	}

	// Literal wire key: protocol.Event uses "trace_parent", this seam
	// must use the W3C "traceparent". Struct decoding cannot tell them
	// apart, so assert on the key itself.
	field, ok := tasks[0]["traceparent"]
	if !ok {
		t.Fatalf("poll task has no \"traceparent\" key; body %s", raw)
	}
	got := jsonString(t, field, "traceparent")
	matches := traceparentPattern.FindStringSubmatch(got)
	if matches == nil {
		t.Fatalf("traceparent %q does not match W3C grammar", got)
	}
	if matches[1] != wantTraceID {
		t.Fatalf(
			"traceparent trace_id=%s, want inbound trace_id=%s",
			matches[1], wantTraceID,
		)
	}

	span := dispatchSpanStub(t, exporter)
	wantSpanID := span.SpanContext.SpanID().String()
	if matches[2] != wantSpanID {
		t.Fatalf(
			"traceparent span_id=%s, want bridge.dispatch span_id=%s",
			matches[2], wantSpanID,
		)
	}
	// Negative space: a response echoing the inbound span ID would leave
	// worker spans siblings of dispatch rather than children of it.
	if matches[2] == inboundSpanID {
		t.Fatalf(
			"traceparent span_id equals inbound span_id %s",
			inboundSpanID,
		)
	}
}

// TestPollResponseOmitsTraceContextWhenAbsent pins the degraded path: a
// noop tracer plus a task with no inbound trace headers must emit NO
// traceparent/tracestate keys at all — never an empty or malformed one.
func TestPollResponseOmitsTraceContextWhenAbsent(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc, natsutil.WithStoreBudget(storeBudgetBytes))
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	// Install a REAL propagator, restored after. Without this the test
	// passes vacuously against the default noop propagator — which
	// injects nothing regardless of the code under test — and its
	// outcome would depend on whether a sibling test had already set
	// the global. The property under test is "no inbound context yields
	// no key even when injection is fully wired", so the propagator has
	// to work for the assertion to mean anything.
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	// No recordBridgeSpans: b.tracer stays the global noop tracer, which
	// is what a deployment without an exporter configured actually runs.
	b := newTestBridge(t, nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	publishTaskMsg(t, nc, "run-polltrace-degraded", nil)

	tasks, raw := pollRawTasks(t, ts, "echo", 1, 3000)
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d (body %s)", len(tasks), raw)
	}
	if _, ok := tasks[0]["traceparent"]; ok {
		t.Fatalf("degraded poll emitted a traceparent key; body %s", raw)
	}
	if _, ok := tasks[0]["tracestate"]; ok {
		t.Fatalf("degraded poll emitted a tracestate key; body %s", raw)
	}
}
