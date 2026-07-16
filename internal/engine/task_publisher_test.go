// engine/task_publisher_test.go
// Tests for TaskPublisher metric correctness: stepEnqueueCount
// must only increment after a successful NATS publish.
// Methodology: inject a failing JetStream mock and verify the
// counter is NOT incremented when publish fails. Then verify it
// IS incremented when publish succeeds.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/embedded"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// errPublishFailed is a sentinel for the failing mock.
var errPublishFailed = errors.New("publish failed: connection lost")

// --- Fake JetStream that fails PublishMsg ---

// failingJS is a JetStream mock that always fails PublishMsg.
// All other methods panic because they must not be called.
type failingJS struct {
	jetstream.JetStream
}

func (f *failingJS) PublishMsg(
	_ context.Context, _ *nats.Msg, _ ...jetstream.PublishOpt,
) (*jetstream.PubAck, error) {
	return nil, errPublishFailed
}

// --- Fake JetStream that succeeds PublishMsg ---

type succeedingJS struct {
	jetstream.JetStream
}

func (s *succeedingJS) PublishMsg(
	_ context.Context, _ *nats.Msg, _ ...jetstream.PublishOpt,
) (*jetstream.PubAck, error) {
	return &jetstream.PubAck{Stream: "TASK_QUEUES"}, nil
}

// --- Recording Int64Counter ---

// recordingCounter records the total value passed to Add.
type recordingCounter struct {
	embedded.Int64Counter
	total atomic.Int64
}

func (r *recordingCounter) Add(
	_ context.Context, val int64,
	_ ...metric.AddOption,
) {
	r.total.Add(val)
}

func (r *recordingCounter) Enabled(
	_ context.Context,
) bool {
	return true
}

// --- noopCounter does nothing (for unused metric slots) ---

type noopCounter struct {
	embedded.Int64Counter
}

func (n *noopCounter) Add(
	_ context.Context, _ int64, _ ...metric.AddOption,
) {
}

func (n *noopCounter) Enabled(_ context.Context) bool {
	return true
}

// --- Tests ---

func TestDoPublishMetricNotIncrementedOnFailure(t *testing.T) {
	// RED: stepEnqueueCount must NOT increment when publish fails.
	counter := &recordingCounter{}
	tracer := tracenoop.NewTracerProvider().Tracer("test")

	failJS := &failingJS{}
	tp := &TaskPublisher{
		js:     failJS,
		pub:    natsutil.NewTracingPublisherJSOnly(failJS),
		tracer: tracer,
		metrics: pubMetrics{
			stepEnqueue:      counter,
			taskConcAcquired: &noopCounter{},
			taskConcRejected: &noopCounter{},
		},
		loadRunAndDef: func(
			_ context.Context, _ string,
		) (dag.WorkflowDef, dag.WorkflowRun, error) {
			return dag.WorkflowDef{}, dag.WorkflowRun{},
				errors.New("not needed")
		},
	}

	step := dag.StepDef{
		ID:   "step-1",
		Task: "my-task",
		Type: dag.StepTypeNormal,
	}

	err := tp.doPublish(
		context.Background(), "run-1", step, []byte(`{}`), 1, "", "",
	)

	// doPublish must return an error on failed publish.
	if err == nil {
		t.Fatal(
			"expected doPublish to return error on publish " +
				"failure, got nil",
		)
	}
	if !errors.Is(err, errPublishFailed) {
		t.Fatalf(
			"expected errPublishFailed, got: %v", err,
		)
	}

	// Counter must NOT have been incremented.
	got := counter.total.Load()
	if got != 0 {
		t.Fatalf(
			"stepEnqueueCount should be 0 on publish "+
				"failure, got %d", got,
		)
	}
}

func TestDoPublishMetricIncrementedOnSuccess(t *testing.T) {
	// GREEN counterpart: metric IS incremented on success.
	counter := &recordingCounter{}
	tracer := tracenoop.NewTracerProvider().Tracer("test")

	okJS := &succeedingJS{}
	tp := &TaskPublisher{
		js:     okJS,
		pub:    natsutil.NewTracingPublisherJSOnly(okJS),
		tracer: tracer,
		metrics: pubMetrics{
			stepEnqueue:      counter,
			taskConcAcquired: &noopCounter{},
			taskConcRejected: &noopCounter{},
		},
		loadRunAndDef: func(
			_ context.Context, _ string,
		) (dag.WorkflowDef, dag.WorkflowRun, error) {
			return dag.WorkflowDef{}, dag.WorkflowRun{},
				errors.New("not needed")
		},
	}

	step := dag.StepDef{
		ID:   "step-1",
		Task: "my-task",
		Type: dag.StepTypeNormal,
	}

	err := tp.doPublish(
		context.Background(), "run-1", step, []byte(`{}`), 1, "", "",
	)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	got := counter.total.Load()
	if got != 1 {
		t.Fatalf(
			"stepEnqueueCount should be 1 on success, "+
				"got %d", got,
		)
	}
}

func TestPublishIterationMetricNotIncrementedOnFailure(
	t *testing.T,
) {
	// Same bug in PublishIteration: metric before error check.
	counter := &recordingCounter{}
	tracer := tracenoop.NewTracerProvider().Tracer("test")

	failJS := &failingJS{}
	tp := &TaskPublisher{
		js:     failJS,
		pub:    natsutil.NewTracingPublisherJSOnly(failJS),
		tracer: tracer,
		metrics: pubMetrics{
			stepEnqueue:      counter,
			taskConcAcquired: &noopCounter{},
			taskConcRejected: &noopCounter{},
		},
		loadRunAndDef: func(
			_ context.Context, _ string,
		) (dag.WorkflowDef, dag.WorkflowRun, error) {
			return dag.WorkflowDef{}, dag.WorkflowRun{},
				errors.New("not needed")
		},
	}

	step := dag.StepDef{
		ID:   "step-1",
		Task: "my-task",
		Type: dag.StepTypeNormal,
	}

	err := tp.PublishIteration(
		context.Background(), "run-1", step, []byte(`{}`), 1, "", "",
	)
	if err == nil {
		t.Fatal(
			"expected PublishIteration to return error, " +
				"got nil",
		)
	}

	got := counter.total.Load()
	if got != 0 {
		t.Fatalf(
			"stepEnqueueCount should be 0 on publish "+
				"failure, got %d", got,
		)
	}
}

// --- Fake JetStream that records the last published msg ---

// recordingJS is a JetStream mock that always succeeds PublishMsg and
// captures the last message it was asked to publish, so tests can
// unmarshal the TaskPayload that hit the wire.
type recordingJS struct {
	jetstream.JetStream
	lastMsg *nats.Msg
}

func (r *recordingJS) PublishMsg(
	_ context.Context, msg *nats.Msg, _ ...jetstream.PublishOpt,
) (*jetstream.PubAck, error) {
	r.lastMsg = msg
	return &jetstream.PubAck{Stream: "TASK_QUEUES"}, nil
}

// newSpanRecordingPublisher builds a TaskPublisher backed by a
// synchronous in-memory span exporter (spans visible immediately after
// span.End() returns) so tests can assert on recorded span name and
// attributes without any global-provider swapping.
func newSpanRecordingPublisher(
	t *testing.T,
) (*TaskPublisher, *recordingJS, *tracetest.InMemoryExporter) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
	)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
	})
	js := &recordingJS{}
	tp := &TaskPublisher{
		js:     js,
		pub:    natsutil.NewTracingPublisherJSOnly(js),
		tracer: provider.Tracer("test"),
		metrics: pubMetrics{
			stepEnqueue:      &noopCounter{},
			taskConcAcquired: &noopCounter{},
			taskConcRejected: &noopCounter{},
		},
		loadRunAndDef: func(
			_ context.Context, _ string,
		) (dag.WorkflowDef, dag.WorkflowRun, error) {
			return dag.WorkflowDef{}, dag.WorkflowRun{},
				errors.New("not needed")
		},
	}
	return tp, js, exporter
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

// spanAttrString returns the string value of the named attribute on a
// recorded span stub, failing the test if it is absent.
func spanAttrString(
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

func TestDoPublishSpanNameAndWorkflowAttribute(t *testing.T) {
	// RED: doPublish must name its span "enqueueTask <task>" (bounded
	// cardinality — task type only, never run_id) and attach a
	// workflow_name attribute equal to the workflow's name.
	tp, js, exporter := newSpanRecordingPublisher(t)
	step := dag.StepDef{ID: "step-1", Task: "compile", Type: dag.StepTypeNormal}

	err := tp.doPublish(
		context.Background(), "run-1", step, []byte(`{}`), 1,
		"deploy-pipeline", "",
	)
	if err != nil {
		t.Fatalf("doPublish failed: %v", err)
	}

	spans := exporter.GetSpans()
	// Positive: span named "enqueueTask compile" carries workflow_name.
	span := onlySpanNamed(t, spans, "enqueueTask compile")
	if got := spanAttrString(t, span, "workflow_name"); got != "deploy-pipeline" {
		t.Fatalf("workflow_name attr = %q, want %q", got, "deploy-pipeline")
	}
	// Negative: the old constant span name must never appear.
	for _, s := range spans {
		if s.Name == "dagnats.engine enqueueTask" {
			t.Fatalf("found legacy constant span name %q", s.Name)
		}
	}

	// The published TaskPayload also carries WorkflowName on the wire
	// so the worker can attach it to its own execute span.
	if js.lastMsg == nil {
		t.Fatal("expected a published message, got none")
	}
	var payload protocol.TaskPayload
	if err := json.Unmarshal(js.lastMsg.Data, &payload); err != nil {
		t.Fatalf("unmarshal published payload: %v", err)
	}
	if payload.WorkflowName != "deploy-pipeline" {
		t.Fatalf(
			"payload.WorkflowName = %q, want %q",
			payload.WorkflowName, "deploy-pipeline",
		)
	}
}

func TestPublishIterationSpanNameAndWorkflowAttribute(t *testing.T) {
	// RED: PublishIteration must name its span "enqueueTask <task>" and
	// attach workflow_name, mirroring doPublish's behavior for the
	// agent-loop re-enqueue path.
	tp, js, exporter := newSpanRecordingPublisher(t)
	step := dag.StepDef{ID: "step-1", Task: "agent-loop", Type: dag.StepTypeAgent}

	err := tp.PublishIteration(
		context.Background(), "run-1", step, []byte(`{}`), 2,
		"deploy-pipeline", "",
	)
	if err != nil {
		t.Fatalf("PublishIteration failed: %v", err)
	}

	spans := exporter.GetSpans()
	// Positive: span named "enqueueTask agent-loop" carries workflow_name.
	span := onlySpanNamed(t, spans, "enqueueTask agent-loop")
	if got := spanAttrString(t, span, "workflow_name"); got != "deploy-pipeline" {
		t.Fatalf("workflow_name attr = %q, want %q", got, "deploy-pipeline")
	}
	// Negative: the old constant span name must never appear.
	for _, s := range spans {
		if s.Name == "dagnats.engine enqueueTask" {
			t.Fatalf("found legacy constant span name %q", s.Name)
		}
	}

	if js.lastMsg == nil {
		t.Fatal("expected a published message, got none")
	}
	var payload protocol.TaskPayload
	if err := json.Unmarshal(js.lastMsg.Data, &payload); err != nil {
		t.Fatalf("unmarshal published payload: %v", err)
	}
	if payload.WorkflowName != "deploy-pipeline" {
		t.Fatalf(
			"payload.WorkflowName = %q, want %q",
			payload.WorkflowName, "deploy-pipeline",
		)
	}
}

// spanAttrAbsent fails the test if the named attribute IS present on the
// span. Mirror image of spanAttrString, for asserting the omit-when-empty
// guard (#513) doesn't leak an empty-string workflow_name attribute.
func spanAttrAbsent(
	t *testing.T, s tracetest.SpanStub, key string,
) {
	t.Helper()
	for _, kv := range s.Attributes {
		if string(kv.Key) == key {
			t.Fatalf("span %q: attribute %q must be absent, got %q", s.Name, key, kv.Value.AsString())
		}
	}
}

func TestDoPublishOmitsWorkflowNameAttributeWhenEmpty(t *testing.T) {
	// RED: doPublish must NOT attach a workflow_name attribute when the
	// caller passes an empty workflowName (#513 -- mirrors worker/worker.go's
	// startTaskSpan, which already omits on empty).
	tp, _, exporter := newSpanRecordingPublisher(t)
	step := dag.StepDef{ID: "step-1", Task: "compile", Type: dag.StepTypeNormal}

	err := tp.doPublish(
		context.Background(), "run-1", step, []byte(`{}`), 1,
		"", "",
	)
	if err != nil {
		t.Fatalf("doPublish failed: %v", err)
	}

	spans := exporter.GetSpans()
	span := onlySpanNamed(t, spans, "enqueueTask compile")
	// Positive: no workflow_name attribute leaked.
	spanAttrAbsent(t, span, "workflow_name")
	// Negative space: other identifying attributes are still present.
	if got := spanAttrString(t, span, "run_id"); got != "run-1" {
		t.Fatalf("run_id attr = %q, want %q", got, "run-1")
	}
}

func TestPublishIterationOmitsWorkflowNameAttributeWhenEmpty(t *testing.T) {
	// RED: PublishIteration must NOT attach a workflow_name attribute when
	// the caller passes an empty workflowName. Companion to
	// TestPublishIterationSpanNameAndWorkflowAttribute (C7).
	tp, _, exporter := newSpanRecordingPublisher(t)
	step := dag.StepDef{ID: "step-1", Task: "agent-loop", Type: dag.StepTypeAgent}

	err := tp.PublishIteration(
		context.Background(), "run-1", step, []byte(`{}`), 2,
		"", "",
	)
	if err != nil {
		t.Fatalf("PublishIteration failed: %v", err)
	}

	spans := exporter.GetSpans()
	span := onlySpanNamed(t, spans, "enqueueTask agent-loop")
	// Positive: no workflow_name attribute leaked.
	spanAttrAbsent(t, span, "workflow_name")
	// Negative space: other identifying attributes are still present.
	if got := spanAttrString(t, span, "step_id"); got != "step-1" {
		t.Fatalf("step_id attr = %q, want %q", got, "step-1")
	}
}
