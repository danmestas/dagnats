// trigger/fire_test.go
// Methodology: integration tests with embedded NATS
// (natsutil.StartTestServer) plus an in-memory OTel span recorder
// installed via otel.SetTracerProvider — the save/set/restore
// pattern from observe/propagation_test.go's installPropagator,
// extended to also swap the TracerProvider so recorded spans can be
// asserted directly instead of only inspecting headers. Every test:
// its own isolated NATS server, bounded timeouts on all waits,
// >=2 assertions covering positive and negative space.
package trigger

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// spanRecorderOnce/sharedSpanExporter back installSpanRecorder. The
// OTel Go SDK's global TracerProvider only ever "delegates" a tracer
// obtained before any SDK was installed exactly once, process-wide
// (go.opentelemetry.io/otel/internal/global's delegateTraceOnce).
// fireTracer (fire.go's package var) is obtained at package init,
// before any test runs, so it permanently binds to whichever
// TracerProvider the FIRST otel.SetTracerProvider call in this test
// binary installs — a second SetTracerProvider call would only
// change what brand-new otel.Tracer() calls resolve to, not
// fireTracer itself. So every test in this file must share one
// provider/exporter installed exactly once, and isolate itself by
// resetting the exporter's buffer instead of swapping providers.
var (
	spanRecorderOnce   sync.Once
	sharedSpanExporter *tracetest.InMemoryExporter
)

// installSpanRecorder returns the shared in-memory span exporter
// (synchronous, via WithSyncer, so spans are visible immediately
// after span.End() returns), installing it and the composite W3C
// propagator InjectTraceContext/ExtractTraceContext expect on first
// use. Resets the exporter's buffer before and after the test so
// tests never see each other's spans, mirroring
// observe/propagation_test.go's installPropagator save/set/restore
// intent within the constraint above.
func installSpanRecorder(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	spanRecorderOnce.Do(func() {
		sharedSpanExporter = tracetest.NewInMemoryExporter()
		otel.SetTracerProvider(sdktrace.NewTracerProvider(
			sdktrace.WithSyncer(sharedSpanExporter),
		))
		otel.SetTextMapPropagator(
			propagation.NewCompositeTextMapPropagator(
				propagation.TraceContext{}, propagation.Baggage{},
			),
		)
	})
	sharedSpanExporter.Reset()
	t.Cleanup(sharedSpanExporter.Reset)
	return sharedSpanExporter
}

// countFireSpans returns how many recorded spans are named
// "trigger.fire".
func countFireSpans(spans tracetest.SpanStubs) int {
	count := 0
	for _, s := range spans {
		if s.Name == "trigger.fire" {
			count++
		}
	}
	return count
}

// onlyFireSpan fails the test unless exactly one "trigger.fire" span
// was recorded, then returns it.
func onlyFireSpan(
	t *testing.T, spans tracetest.SpanStubs,
) tracetest.SpanStub {
	t.Helper()
	var found []tracetest.SpanStub
	for _, s := range spans {
		if s.Name == "trigger.fire" {
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		t.Fatalf(
			"trigger.fire span count = %d, want 1", len(found),
		)
	}
	return found[0]
}

// attrString returns the string value of the named attribute on a
// recorded span stub, failing the test if it is absent.
func attrString(
	t *testing.T, s tracetest.SpanStub, key string,
) string {
	t.Helper()
	for _, kv := range s.Attributes {
		if string(kv.Key) == key {
			return kv.Value.AsString()
		}
	}
	t.Fatalf("span %q: missing attribute %q", s.Name, key)
	return ""
}

// newTestPublisher provisions an isolated embedded NATS server with
// the streams/KV Fire needs (WORKFLOW_HISTORY, TRIGGER_HISTORY via
// SetupAll's defaults, plus trigger_state for parity with the
// scheduler's own setup) and returns a ready TracingPublisher.
func newTestPublisher(t *testing.T) *natsutil.TracingPublisher {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return natsutil.NewTracingPublisher(nc, js)
}

// TestFire_StartsTriggerFireSpanWithIdentity proves Fire roots a
// single "trigger.fire" Producer span carrying the trigger's
// identity, and that the scheduler's #173 minute-dedup guard still
// keeps a second Tick for the same minute from reaching Fire again
// (so no second span is ever recorded either).
func TestFire_StartsTriggerFireSpanWithIdentity(t *testing.T) {
	exporter := installSpanRecorder(t)
	tp := newTestPublisher(t)

	def := TriggerDef{ID: "t-identity", WorkflowID: "wf-identity"}
	now := time.Now()

	runID, err := Fire(context.Background(), tp, def, SourceCron, now)
	if err != nil {
		t.Fatalf("Fire: %v", err)
	}
	if runID == "" {
		t.Fatal("Fire: expected non-empty run ID")
	}

	fireSpan := onlyFireSpan(t, exporter.GetSpans())

	// Positive: SpanKind and every identity attribute match contract.
	if fireSpan.SpanKind != trace.SpanKindProducer {
		t.Fatalf(
			"trigger.fire SpanKind = %v, want Producer",
			fireSpan.SpanKind,
		)
	}
	if got := attrString(t, fireSpan, "trigger_id"); got != def.ID {
		t.Fatalf("trigger_id = %q, want %q", got, def.ID)
	}
	if got := attrString(t, fireSpan, "workflow_id"); got != def.WorkflowID {
		t.Fatalf("workflow_id = %q, want %q", got, def.WorkflowID)
	}
	if got := attrString(t, fireSpan, "trigger_source"); got != SourceCron {
		t.Fatalf("trigger_source = %q, want %q", got, SourceCron)
	}
	if got := attrString(t, fireSpan, "run_id"); got != runID {
		t.Fatalf("run_id = %q, want %q", got, runID)
	}

	// Negative: regression guard on #173 — Scheduler.Tick's
	// claimMinute dedup keeps a second tick for the same matching
	// minute from ever calling Fire again, so no second trigger.fire
	// span appears no matter how many times the same minute ticks.
	exporter.Reset()
	scheduler, err := NewScheduler(tp.NC())
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	dedupDef := TriggerDef{
		ID: "t-dedup", WorkflowID: "wf-dedup", Enabled: true,
		Cron: &CronConfig{Expression: "* * * * *", Timezone: "UTC"},
	}
	if err := scheduler.AddTrigger(dedupDef); err != nil {
		t.Fatalf("AddTrigger: %v", err)
	}
	tickTime := time.Date(2026, 3, 31, 12, 30, 0, 0, time.UTC)
	if err := scheduler.Tick(tickTime); err != nil {
		t.Fatalf("first Tick: %v", err)
	}
	if err := scheduler.Tick(tickTime); err != nil {
		t.Fatalf("second Tick: %v", err)
	}
	if got := countFireSpans(exporter.GetSpans()); got != 1 {
		t.Fatalf(
			"trigger.fire span count after dedup ticks = %d, want 1",
			got,
		)
	}
}

// TestFire_DualWritesTraceParent proves the workflow.started publish
// carries the trigger.fire trace ID both in the NATS header and in
// the persisted Event body (JSPublishMsgEvent's dual-write), and that
// a publish failure is recorded as an error on the span.
func TestFire_DualWritesTraceParent(t *testing.T) {
	exporter := installSpanRecorder(t)
	tp := newTestPublisher(t)

	oldJS, err := tp.NC().JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	sub, err := oldJS.SubscribeSync("history.>")
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}

	def := TriggerDef{ID: "t-dualwrite", WorkflowID: "wf-dualwrite"}
	now := time.Now()
	runID, err := Fire(context.Background(), tp, def, SourceCron, now)
	if err != nil {
		t.Fatalf("Fire: %v", err)
	}

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("expected workflow.started publish: %v", err)
	}

	fireSpan := onlyFireSpan(t, exporter.GetSpans())
	wantTraceID := fireSpan.SpanContext.TraceID().String()

	// Positive: header traceparent encodes the trigger.fire trace ID.
	headerTP := msg.Header.Get("traceparent")
	if headerTP == "" {
		t.Fatal("expected traceparent header on workflow.started")
	}
	parts := strings.Split(headerTP, "-")
	if len(parts) != 4 || parts[1] != wantTraceID {
		t.Fatalf(
			"header traceparent %q does not encode trace ID %q",
			headerTP, wantTraceID,
		)
	}

	// Positive: durable dual-write — Event.TraceParent is non-empty
	// and encodes the same trace ID, so a post-restart replay still
	// has it even without the header.
	evt, err := protocol.UnmarshalEvent(msg.Data)
	if err != nil {
		t.Fatalf("UnmarshalEvent: %v", err)
	}
	if evt.TraceParent == "" {
		t.Fatal("expected non-empty Event.TraceParent")
	}
	if !strings.Contains(evt.TraceParent, wantTraceID) {
		t.Fatalf(
			"Event.TraceParent %q does not encode trace ID %q",
			evt.TraceParent, wantTraceID,
		)
	}
	if evt.RunID != runID {
		t.Fatalf("Event.RunID = %q, want %q", evt.RunID, runID)
	}

	// Negative: a publish failure (closed connection) still returns
	// an error to the caller and marks the trigger.fire span Error.
	exporter.Reset()
	tp.NC().Close()
	failDef := TriggerDef{ID: "t-dualwrite-fail", WorkflowID: "wf-dualwrite"}
	if _, err := Fire(
		context.Background(), tp, failDef, SourceCron, now,
	); err == nil {
		t.Fatal("expected error from Fire over a closed connection")
	}
	failSpan := onlyFireSpan(t, exporter.GetSpans())
	if failSpan.Status.Code != codes.Error {
		t.Fatalf(
			"trigger.fire span status = %v, want codes.Error",
			failSpan.Status.Code,
		)
	}
}
