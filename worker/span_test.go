// worker/span_test.go
// Tests for startTaskSpan's span name and workflow_name attribute
// (#503). Methodology: an in-memory synchronous OTel span exporter
// installed directly on a bare *Worker (no NATS connection needed —
// startTaskSpan only touches w.tracer and the fields newTaskContext
// tolerates as nil), so spans are asserted without any process-wide
// global-provider swapping. Every test: bounded (no waits needed,
// span.End() is synchronous), >=2 assertions covering positive and
// negative space.
package worker

import (
	"context"
	"testing"

	"github.com/danmestas/dagnats/protocol"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// newSpanRecordingWorker builds a bare *Worker backed by a synchronous
// in-memory span exporter, for tests that only exercise startTaskSpan.
func newSpanRecordingWorker(
	t *testing.T,
) (*Worker, *tracetest.InMemoryExporter) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
	)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
	})
	return &Worker{tracer: provider.Tracer("test")}, exporter
}

// onlySpanNamed fails the test unless exactly one span with the given
// name was recorded, then returns it.
func onlySpanNamed(
	t *testing.T, spans tracetest.SpanStubs, name string,
) tracetest.SpanStub {
	t.Helper()
	var found []tracetest.SpanStub
	for _, s := range spans {
		if s.Name == name {
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		t.Fatalf("span %q count = %d, want 1 (spans: %v)", name, len(found), spans)
	}
	return found[0]
}

// findAttrString returns the string value of the named attribute on a
// span stub and whether it was present at all.
func findAttrString(s tracetest.SpanStub, key string) (string, bool) {
	for _, kv := range s.Attributes {
		if string(kv.Key) == key {
			return kv.Value.AsString(), true
		}
	}
	return "", false
}

func TestStartTaskSpanNameAndWorkflowAttribute(t *testing.T) {
	// RED: startTaskSpan must name its span "executeTask <type>" (bounded
	// cardinality — task type only) and attach workflow_name when the
	// payload carries one.
	w, exporter := newSpanRecordingWorker(t)
	payload := protocol.TaskPayload{
		RunID:        "run-1",
		StepID:       "step-a",
		WorkflowName: "deploy-pipeline",
	}
	msg := &testJetstreamMsg{data: []byte("{}")}

	_, span, _ := w.startTaskSpan(payload, "compile", msg)
	span.End()

	spans := exporter.GetSpans()
	// Positive: span named "executeTask compile" carries workflow_name.
	recorded := onlySpanNamed(t, spans, "executeTask compile")
	got, ok := findAttrString(recorded, "workflow_name")
	if !ok {
		t.Fatal("expected workflow_name attribute to be present")
	}
	if got != "deploy-pipeline" {
		t.Fatalf("workflow_name attr = %q, want %q", got, "deploy-pipeline")
	}
	// Negative: the old constant span name must never appear.
	for _, s := range spans {
		if s.Name == "worker.executeTask" {
			t.Fatalf("found legacy constant span name %q", s.Name)
		}
	}
}

func TestStartTaskSpanOmitsWorkflowAttributeForLegacyPayload(t *testing.T) {
	// Back-compat: a payload from an older engine build (empty
	// WorkflowName) must still produce a valid, correctly-named span
	// and must NOT emit an empty-string workflow_name attribute —
	// the attribute is simply absent.
	w, exporter := newSpanRecordingWorker(t)
	payload := protocol.TaskPayload{
		RunID:  "run-2",
		StepID: "step-b",
		// WorkflowName intentionally left empty (legacy payload).
	}
	msg := &testJetstreamMsg{data: []byte("{}")}

	_, span, _ := w.startTaskSpan(payload, "test", msg)
	span.End()

	spans := exporter.GetSpans()
	// Positive: span still gets the new bounded-cardinality name.
	recorded := onlySpanNamed(t, spans, "executeTask test")
	// Negative: no workflow_name attribute at all (not even "").
	if _, ok := findAttrString(recorded, "workflow_name"); ok {
		t.Fatal("expected no workflow_name attribute for legacy payload, got one")
	}
}
