// engine/task_publisher_test.go
// Tests for TaskPublisher metric correctness: stepEnqueueCount
// must only increment after a successful NATS publish.
// Methodology: inject a failing JetStream mock and verify the
// counter is NOT incremented when publish fails. Then verify it
// IS incremented when publish succeeds.
package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/embedded"
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

	tp := &TaskPublisher{
		js:                      &failingJS{},
		tracer:                  tracer,
		stepEnqueueCount:        counter,
		taskConcurrencyAcquired: &noopCounter{},
		taskConcurrencyRejected: &noopCounter{},
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
		context.Background(), "run-1", step, []byte(`{}`), 1,
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

	tp := &TaskPublisher{
		js:                      &succeedingJS{},
		tracer:                  tracer,
		stepEnqueueCount:        counter,
		taskConcurrencyAcquired: &noopCounter{},
		taskConcurrencyRejected: &noopCounter{},
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
		context.Background(), "run-1", step, []byte(`{}`), 1,
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

	tp := &TaskPublisher{
		js:                      &failingJS{},
		tracer:                  tracer,
		stepEnqueueCount:        counter,
		taskConcurrencyAcquired: &noopCounter{},
		taskConcurrencyRejected: &noopCounter{},
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
		context.Background(), "run-1", step, []byte(`{}`), 1,
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
