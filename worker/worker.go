package worker

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
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
	Heartbeat() error
	Checkpoint(state []byte) error
	LoadCheckpoint() ([]byte, error)
	WaitForSignal(name string, timeout time.Duration) ([]byte, error)
	SendSignal(runID, name string, data []byte) error
}

// HandlerFunc is the function signature for task handlers
// registered with a Worker.
type HandlerFunc func(ctx TaskContext) error

// Worker subscribes to task subjects and dispatches messages to
// registered handlers. Each task type gets its own JetStream
// subscription; messages are ack'd after the handler returns so
// failures are retried by JetStream's MaxDeliver policy.
type Worker struct {
	nc           *nats.Conn
	js           nats.JetStreamContext
	tel          *observe.Telemetry
	handlers     map[string]HandlerFunc
	subs         []*nats.Subscription
	checkpointKV nats.KeyValue
	signalKV     nats.KeyValue
	groups       []string

	// Directory registration (observability only)
	dir           *Directory
	workerID      string
	stopHeartbeat chan struct{}

	// Pre-allocated metric instruments — created once in constructor.
	stepDuration observe.Histogram
	stepRetries  observe.Counter
	tasksActive  observe.Gauge
}

// WorkerOption configures optional Worker behavior.
type WorkerOption func(*Worker)

// WithGroups configures the worker to subscribe only to specific
// worker groups. When provided, the worker subscribes to
// task.{taskType}.{group}.> instead of task.{taskType}.>.
func WithGroups(groups ...string) WorkerOption {
	if len(groups) == 0 {
		panic("WithGroups: groups must not be empty")
	}
	for _, g := range groups {
		if g == "" {
			panic("WithGroups: group name must not be empty")
		}
	}
	return func(w *Worker) { w.groups = groups }
}

// generateWorkerID creates a unique worker ID using crypto/rand.
// Panics if crypto/rand fails — that is a system-level error.
func generateWorkerID() string {
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		panic("generateWorkerID: crypto/rand failed: " + err.Error())
	}
	return fmt.Sprintf("worker-%x", b)
}

// NewWorker creates a Worker using the given connection and
// optional telemetry bundle. Panics if nc is nil or if JetStream
// cannot be initialised — both are programmer errors at startup.
// When tel is nil, a noop telemetry is used so callers are not
// forced to import observe for simple use cases.
func NewWorker(
	nc *nats.Conn, tel *observe.Telemetry, opts ...WorkerOption,
) *Worker {
	if nc == nil {
		panic("NewWorker: nc must not be nil")
	}
	if tel == nil {
		tel = observe.NewNoopTelemetry()
	}
	js, err := nc.JetStream()
	if err != nil {
		panic("NewWorker: JetStream init failed: " + err.Error())
	}
	w := &Worker{
		nc:       nc,
		js:       js,
		tel:      tel,
		handlers: make(map[string]HandlerFunc),
		workerID: generateWorkerID(),
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
	for _, opt := range opts {
		opt(w)
	}
	return w
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

// newDirectoryOptional creates a Directory if the workers KV
// bucket exists. Returns error (not panic) if the bucket is
// missing — directory is observability only, not critical path.
func newDirectoryOptional(js nats.JetStreamContext) (*Directory, error) {
	if js == nil {
		panic("newDirectoryOptional: js must not be nil")
	}
	kv, err := js.KeyValue("workers")
	if err != nil {
		return nil, err
	}
	if kv == nil {
		panic("newDirectoryOptional: kv must not be nil when err is nil")
	}
	return &Directory{kv: kv}, nil
}

// Start creates JetStream subscriptions for all registered task
// types. Panics if any subscription fails — stream
// misconfiguration is a startup error. Binds optional KV buckets
// for checkpoints and signals (nil if not present). When groups
// are configured, subscribes to group-specific subjects.
func (w *Worker) Start() {
	if len(w.handlers) == 0 {
		panic("Worker.Start: no handlers registered")
	}
	if w.js == nil {
		panic("Worker.Start: js must not be nil")
	}
	// Bind optional KV buckets — no error if missing
	w.checkpointKV, _ = w.js.KeyValue("checkpoints")
	w.signalKV, _ = w.js.KeyValue("signals")

	// Worker directory registration (observability only — not critical path)
	d, err := newDirectoryOptional(w.js)
	if err == nil {
		w.dir = d
		w.stopHeartbeat = make(chan struct{})
		taskTypes := make([]string, 0, len(w.handlers))
		for t := range w.handlers {
			taskTypes = append(taskTypes, t)
		}
		reg := WorkerRegistration{
			WorkerID:  w.workerID,
			TaskTypes: taskTypes,
			Language:  "go",
			Transport: "nats",
			MaxTasks:  len(taskTypes),
		}
		_ = w.dir.Register(reg)
		go w.heartbeatLoop(reg)
	}

	for taskType, handler := range w.handlers {
		h := handler
		tt := taskType
		if len(w.groups) == 0 {
			// No groups — subscribe to all tasks of this type
			subject := "task." + taskType + ".>"
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
		} else {
			// Groups configured — subscribe to each group
			for _, group := range w.groups {
				subject := "task." + taskType + "." + group + ".>"
				sub, err := w.js.Subscribe(subject, func(msg *nats.Msg) {
					w.handleMessage(tt, h, msg)
				}, nats.AckExplicit(), nats.DeliverAll())
				if err != nil {
					panic(
						"Worker.Start: Subscribe failed for " +
							taskType + "." + group + ": " + err.Error(),
					)
				}
				w.subs = append(w.subs, sub)
			}
		}
	}
}

// heartbeatLoop re-registers the worker every 30s to refresh the
// KV TTL (bucket TTL is 60s). Stops when stopHeartbeat is closed.
func (w *Worker) heartbeatLoop(reg WorkerRegistration) {
	if w.dir == nil {
		panic("heartbeatLoop: dir must not be nil")
	}
	if w.stopHeartbeat == nil {
		panic("heartbeatLoop: stopHeartbeat must not be nil")
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = w.dir.Register(reg)
		case <-w.stopHeartbeat:
			return
		}
	}
}

// Stop unsubscribes all active subscriptions. Safe to call after
// Start.
func (w *Worker) Stop() {
	if w.handlers == nil {
		panic("Worker.Stop: worker not initialized")
	}
	if w.nc == nil {
		panic("Worker.Stop: nc must not be nil")
	}
	if w.stopHeartbeat != nil {
		close(w.stopHeartbeat)
	}
	if w.dir != nil {
		_ = w.dir.Deregister(w.workerID)
	}
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
	tc := newTaskContext(
		w.nc, w.tel, w.js, payload, ctx, span, msg,
		w.checkpointKV, w.signalKV,
	)
	err = handler(tc)
	elapsed := float64(time.Since(start).Milliseconds())
	w.stepDuration.Observe(elapsed)
	w.tasksActive.Dec()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(
			observe.StatusError, err.Error(),
		)
		w.handleTaskError(
			err, tc, msg, taskType, payload.RunID,
		)
		return
	}
	msg.Ack()
}

// handleTaskError processes a handler error by either failing
// permanently (NonRetryableError) or scheduling a retry via NAK.
func (w *Worker) handleTaskError(
	err error,
	tc *taskContext,
	msg *nats.Msg,
	taskType string,
	runID string,
) {
	if err == nil {
		panic("handleTaskError: err must not be nil")
	}
	if msg == nil {
		panic("handleTaskError: msg must not be nil")
	}
	var nre *NonRetryableError
	if errors.As(err, &nre) {
		w.tel.Logger.Error(
			"task failed permanently", nre.Err,
			observe.String("task_type", taskType),
			observe.String("run_id", runID),
		)
		tc.Fail(nre.Err)
		msg.Ack()
		return
	}
	w.tel.Logger.Error(
		"task handler returned error, will retry", err,
		observe.String("task_type", taskType),
		observe.String("run_id", runID),
	)
	w.stepRetries.Inc()
	msg.NakWithDelay(5 * time.Second)
}

// extractWorkerTraceCtx reads W3C traceparent from the NATS
// message header and returns a context with parent span info.
func extractWorkerTraceCtx(msg *nats.Msg) context.Context {
	if msg == nil {
		panic("extractWorkerTraceCtx: msg must not be nil")
	}
	if msg.Data == nil {
		panic("extractWorkerTraceCtx: msg.Data must not be nil")
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
	if tp == "" {
		panic("splitWorkerTraceparent: tp must not be empty")
	}
	if len(tp) > 256 {
		panic("splitWorkerTraceparent: tp exceeds max length")
	}
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
