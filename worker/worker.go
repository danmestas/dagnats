package worker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// TaskContext is the deep interface workers use to report step results.
// Workers call exactly one of Complete, Fail, or Continue per execution.
// They never deal with retries, timeouts, or DAG logic directly.
type TaskContext interface {
	Input() []byte
	RunID() string
	StepID() string
	RetryCount() int
	Complete(output []byte) error
	Fail(err error) error
	Continue(output []byte) error
	PutStream(data []byte) error
}

// HandlerFunc is the function signature for task handlers
// registered with a Worker.
type HandlerFunc func(ctx TaskContext) error

// Worker subscribes to task subjects and dispatches messages to
// registered handlers. Each task type gets its own JetStream
// subscription; messages are ack'd after the handler returns so
// failures are retried by JetStream's MaxDeliver policy.
type Worker struct {
	nc       *nats.Conn
	js       nats.JetStreamContext
	tel      *observe.Telemetry
	handlers map[string]HandlerFunc
	subs     []*nats.Subscription

	// Pre-allocated metric instruments — created once in constructor.
	stepDuration observe.Histogram
	stepRetries  observe.Counter
	tasksActive  observe.Gauge
}

// NewWorker creates a Worker using the given connection and
// telemetry bundle. Panics if nc or tel is nil, or if JetStream
// cannot be initialised — all are programmer errors at startup.
func NewWorker(
	nc *nats.Conn, tel *observe.Telemetry,
) *Worker {
	if nc == nil {
		panic("NewWorker: nc must not be nil")
	}
	if tel == nil {
		panic("NewWorker: tel must not be nil")
	}
	js, err := nc.JetStream()
	if err != nil {
		panic("NewWorker: JetStream init failed: " + err.Error())
	}
	return &Worker{
		nc:       nc,
		js:       js,
		tel:      tel,
		handlers: make(map[string]HandlerFunc),
		stepDuration: tel.Metrics.Histogram(
			"step.duration_ms", nil,
		),
		stepRetries: tel.Metrics.Counter(
			"step.retries", nil,
		),
		tasksActive: tel.Metrics.Gauge(
			"worker.tasks.active", nil,
		),
	}
}

// Handle registers a HandlerFunc for the given task type.
// Panics on empty taskType or nil handler — both are programmer
// errors.
func (w *Worker) Handle(
	taskType string, handler HandlerFunc,
) {
	if taskType == "" {
		panic("Worker.Handle: taskType must not be empty")
	}
	if handler == nil {
		panic("Worker.Handle: handler must not be nil")
	}
	w.handlers[taskType] = handler
}

// Start creates JetStream subscriptions for all registered task
// types. Panics if any subscription fails — stream
// misconfiguration is a startup error.
func (w *Worker) Start() {
	for taskType, handler := range w.handlers {
		subject := "task." + taskType + ".>"
		h := handler
		tt := taskType
		sub, err := w.js.Subscribe(subject, func(msg *nats.Msg) {
			w.handleMessage(tt, h, msg)
		}, nats.AckExplicit(), nats.DeliverAll())
		if err != nil {
			panic(
				"Worker.Start: Subscribe failed for " +
					taskType + ": " + err.Error(),
			)
		}
		w.subs = append(w.subs, sub)
	}
}

// Stop unsubscribes all active subscriptions. Safe to call after
// Start.
func (w *Worker) Stop() {
	for _, sub := range w.subs {
		sub.Unsubscribe()
	}
}

// handleMessage unmarshals the task payload, creates a traced
// context, executes the handler, and records metrics.
func (w *Worker) handleMessage(
	taskType string, handler HandlerFunc, msg *nats.Msg,
) {
	if msg == nil {
		panic("handleMessage: msg must not be nil")
	}
	if handler == nil {
		panic("handleMessage: handler must not be nil")
	}
	var payload protocol.TaskPayload
	err := json.Unmarshal(msg.Data, &payload)
	if err != nil {
		w.tel.Logger.Error(
			"failed to unmarshal task payload", err,
			observe.String("task_type", taskType),
		)
		msg.Ack()
		return
	}
	ctx := extractWorkerTraceCtx(msg)
	ctx, span := w.tel.Tracer.Start(ctx,
		"worker.executeTask",
		observe.WithSpanKind(observe.SpanKindServer),
		observe.WithAttributes(
			observe.StringAttr("run_id", payload.RunID),
			observe.StringAttr("step_id", payload.StepID),
			observe.StringAttr("task_name", taskType),
		),
	)
	defer span.End()
	w.tasksActive.Inc()
	start := time.Now()
	w.tel.Logger.Info("executing task",
		observe.String("task_type", taskType),
		observe.String("run_id", payload.RunID),
		observe.String("step_id", payload.StepID),
	)
	tc := newTaskContext(w.nc, w.tel, w.js, payload, ctx, span)
	err = handler(tc)
	elapsed := float64(time.Since(start).Milliseconds())
	w.stepDuration.Observe(elapsed)
	w.tasksActive.Dec()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(
			observe.StatusError, err.Error(),
		)
		var nre *NonRetryableError
		if errors.As(err, &nre) {
			w.tel.Logger.Error(
				"task failed permanently", nre.Err,
				observe.String("task_type", taskType),
				observe.String("run_id", payload.RunID),
			)
			tc.Fail(nre.Err)
			msg.Ack()
			return
		}
		w.tel.Logger.Error(
			"task handler returned error, will retry", err,
			observe.String("task_type", taskType),
			observe.String("run_id", payload.RunID),
		)
		w.stepRetries.Inc()
		msg.NakWithDelay(5 * time.Second)
		return
	}
	msg.Ack()
}

// extractWorkerTraceCtx reads W3C traceparent from the NATS
// message header and returns a context with parent span info.
func extractWorkerTraceCtx(msg *nats.Msg) context.Context {
	if msg == nil {
		panic("extractWorkerTraceCtx: msg must not be nil")
	}
	if msg.Header == nil {
		return context.Background()
	}
	tp := msg.Header.Get("traceparent")
	if tp == "" {
		return context.Background()
	}
	traceID, spanID, ok := splitWorkerTraceparent(tp)
	if !ok {
		return context.Background()
	}
	return observe.ContextWithParentInfo(
		context.Background(), traceID, spanID,
	)
}

// splitWorkerTraceparent parses "00-{traceID}-{spanID}-{flags}".
func splitWorkerTraceparent(
	tp string,
) (traceID, spanID string, ok bool) {
	parts := strings.Split(tp, "-")
	if len(parts) != 4 || parts[0] != "00" {
		return "", "", false
	}
	return parts[1], parts[2], true
}

// injectWorkerTraceCtx writes traceparent to a NATS message
// header and event TraceParent field. No-op when the span does
// not implement SpanContext or has empty IDs.
func injectWorkerTraceCtx(
	span observe.Span, evt *protocol.Event, msg *nats.Msg,
) {
	if msg == nil {
		panic("injectWorkerTraceCtx: msg must not be nil")
	}
	if evt == nil {
		panic("injectWorkerTraceCtx: evt must not be nil")
	}
	sc, ok := span.(observe.SpanContext)
	if !ok {
		return
	}
	traceID := sc.TraceID()
	spanID := sc.SpanID()
	if traceID == "" || spanID == "" {
		return
	}
	tp := "00-" + traceID + "-" + spanID + "-01"
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	msg.Header.Set("traceparent", tp)
	evt.TraceParent = tp
}
