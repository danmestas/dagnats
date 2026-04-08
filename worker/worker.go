package worker

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/pcgroups"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// TaskContext is the interface workers use to interact with the
// DagNats engine. Includes step completion, checkpointing, signals,
// and streaming. Workers call exactly one of Complete, Fail, or
// Continue per execution.
//
// Checkpoint and signal methods depend on optional KV buckets
// ("checkpoints" and "signals"). They return an error if the bucket
// was not provisioned at startup — check your natsutil.SetupAll call.
type TaskContext interface {
	// Step identity and input
	Input() []byte
	RunID() string
	StepID() string
	RetryCount() int
	Context() context.Context

	// Step completion — call exactly one per execution
	Complete(output []byte) error
	Fail(err error) error
	FailPermanent(err error) error
	FailRetryAfter(err error, after time.Duration) error
	Continue(output []byte) error

	// Streaming and heartbeat
	PutStream(data []byte) error
	Heartbeat() error

	// Checkpointing — save/restore handler state across retries
	Checkpoint(state []byte) error
	LoadCheckpoint() ([]byte, error)
	Pause(name string, duration time.Duration) error

	// Signals — coordinate between steps
	WaitForSignal(
		name string, timeout time.Duration,
	) ([]byte, error)
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
	js           jetstream.JetStream
	tracer       trace.Tracer
	handlers     map[string]HandlerFunc
	stoppers     []interface{ Stop() } // all consumer lifecycles
	checkpointKV jetstream.KeyValue
	signalKV     jetstream.KeyValue
	groups       []string
	partitions   int
	singletons   map[string]bool

	// Directory registration (observability only)
	dir           *Directory
	workerID      string
	stopHeartbeat chan struct{}

	// Pre-allocated metric instruments — created once in constructor.
	stepDuration metric.Float64Histogram
	stepRetries  metric.Int64Counter
	tasksActive  metric.Int64UpDownCounter
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

// WithPartitions configures pcgroups elastic consumer groups
// with the given partition count. 0 = legacy consumer (default).
func WithPartitions(n int) WorkerOption {
	if n < 0 {
		panic("WithPartitions: n must be >= 0")
	}
	if n > 256 {
		panic("WithPartitions: n must be <= 256")
	}
	return func(w *Worker) { w.partitions = n }
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

// NewWorker creates a Worker using the given connection.
// Panics if nc is nil or if JetStream cannot be initialised
// — both are programmer errors at startup. Tracing and
// metrics use the global OTel providers (noop by default).
func NewWorker(
	nc *nats.Conn, opts ...WorkerOption,
) *Worker {
	if nc == nil {
		panic("NewWorker: nc must not be nil")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		panic("NewWorker: jetstream.New failed: " + err.Error())
	}
	m := otel.Meter("dagnats/worker")
	stepDur, _ := m.Float64Histogram(
		"step.duration_ms",
	)
	stepRet, _ := m.Int64Counter("step.retries")
	active, _ := m.Int64UpDownCounter(
		"worker.tasks.active",
	)
	w := &Worker{
		nc:           nc,
		js:           js,
		tracer:       otel.Tracer("dagnats/worker"),
		handlers:     make(map[string]HandlerFunc),
		workerID:     generateWorkerID(),
		stepDuration: stepDur,
		stepRetries:  stepRet,
		tasksActive:  active,
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

// HandleSingleton registers a handler that runs as a single-
// partition elastic consumer group. Only one consumer processes
// messages at a time across all worker instances. Implicitly
// enables partitioned mode if not already configured.
func (w *Worker) HandleSingleton(
	taskType string, handler HandlerFunc,
) {
	if taskType == "" {
		panic("HandleSingleton: taskType must not be empty")
	}
	if handler == nil {
		panic("HandleSingleton: handler must not be nil")
	}
	w.handlers[taskType] = handler
	if w.singletons == nil {
		w.singletons = make(map[string]bool)
	}
	w.singletons[taskType] = true
	// Singleton requires pcgroups — enable partitioned mode
	// if the caller didn't explicitly set WithPartitions.
	if w.partitions == 0 {
		w.partitions = 1
	}
}

// newDirectoryOptional creates a Directory if the workers KV
// bucket exists. Returns error (not panic) if the bucket is
// missing — directory is observability only, not critical path.
func newDirectoryOptional(
	js jetstream.JetStream,
) (*Directory, error) {
	if js == nil {
		panic("newDirectoryOptional: js must not be nil")
	}
	kv, err := js.KeyValue(
		context.Background(), "workers",
	)
	if err != nil {
		return nil, err
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

	w.bindOptionalKV()
	w.registerDirectory()

	for taskType, handler := range w.handlers {
		w.subscribeTask(taskType, handler)
	}
}

// bindOptionalKV binds checkpoint and signal KV buckets if they
// exist. Missing buckets are fine — features degrade gracefully.
// Logs warnings so operators know which features are unavailable.
func (w *Worker) bindOptionalKV() {
	w.checkpointKV, _ = w.js.KeyValue(
		context.Background(), "checkpoints",
	)
	if w.checkpointKV == nil {
		slog.Warn(
			"checkpoint KV bucket not found" +
				" — Checkpoint/LoadCheckpoint/Pause" +
				" will return errors",
		)
	}
	w.signalKV, _ = w.js.KeyValue(
		context.Background(), "signals",
	)
	if w.signalKV == nil {
		slog.Warn(
			"signal KV bucket not found" +
				" — WaitForSignal/SendSignal" +
				" will return errors",
		)
	}
}

// registerDirectory registers this worker in the observability
// directory and starts a heartbeat goroutine. No-op if the
// workers KV bucket doesn't exist — directory is not critical.
func (w *Worker) registerDirectory() {
	d, err := newDirectoryOptional(w.js)
	if err != nil {
		return
	}
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

// subscribeTask creates JetStream subscriptions for a single task
// type. When groups are configured, subscribes per-group. Also
// subscribes to the sticky subject for worker-affinity routing.
func (w *Worker) subscribeTask(
	taskType string, handler HandlerFunc,
) {
	if taskType == "" {
		panic("subscribeTask: taskType must not be empty")
	}
	if handler == nil {
		panic("subscribeTask: handler must not be nil")
	}

	h := handler
	tt := taskType

	if w.partitions > 0 {
		partCount := w.partitions
		if w.singletons[tt] {
			partCount = 1
		}
		filter := "task." + tt + ".>"
		groupName := "workers-" + tt
		if len(w.groups) > 0 {
			for _, group := range w.groups {
				gFilter := "task." + tt + "." +
					group + ".>"
				gName := "workers-" + tt +
					"-" + group
				cc := w.createElasticConsumer(
					tt, gName, gFilter,
					partCount, h,
				)
				w.stoppers = append(
					w.stoppers, cc,
				)
			}
		} else {
			cc := w.createElasticConsumer(
				tt, groupName, filter,
				partCount, h,
			)
			w.stoppers = append(
				w.stoppers, cc,
			)
		}
	} else {
		if len(w.groups) == 0 {
			subject := "task." + taskType + ".>"
			cc := w.createConsumer(tt, subject, h)
			w.stoppers = append(
				w.stoppers, cc,
			)
			// Sticky subscription on STICKY_TASKS stream
			// (separate from TASK_QUEUES to avoid work queue
			// filter conflict). Missing stream is fine.
			stickyCC := w.createStickyConsumer(
				tt, h,
			)
			if stickyCC != nil {
				w.stoppers = append(
					w.stoppers, stickyCC,
				)
			}
		} else {
			for _, group := range w.groups {
				subject := "task." + tt + "." +
					group + ".>"
				cc := w.createConsumer(
					tt+"."+group, subject, h,
				)
				w.stoppers = append(
					w.stoppers, cc,
				)
			}
		}
	}
}

// createConsumer sets up a JetStream pull consumer for the given
// subject on the TASK_QUEUES stream and starts consuming. Panics
// on any setup failure — stream misconfiguration is a startup error.
func (w *Worker) createConsumer(
	name string, subject string, handler HandlerFunc,
) jetstream.ConsumeContext {
	if subject == "" {
		panic("createConsumer: subject must not be empty")
	}
	if handler == nil {
		panic("createConsumer: handler must not be nil")
	}
	cons, err := w.js.CreateOrUpdateConsumer(
		context.Background(), "TASK_QUEUES",
		jetstream.ConsumerConfig{
			FilterSubject: subject,
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverAllPolicy,
		},
	)
	if err != nil {
		panic(
			"Worker.Start: consumer failed for " +
				name + ": " + err.Error(),
		)
	}
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		w.handleMessage(name, handler, msg)
	})
	if err != nil {
		panic(
			"Worker.Start: Consume failed for " +
				name + ": " + err.Error(),
		)
	}
	return cc
}

// createStickyConsumer sets up a pull consumer on the STICKY_TASKS
// stream for worker-affinity routing. Returns nil if the stream
// does not exist — sticky routing is optional.
func (w *Worker) createStickyConsumer(
	taskType string, handler HandlerFunc,
) jetstream.ConsumeContext {
	if taskType == "" {
		panic(
			"createStickyConsumer: taskType must not be empty",
		)
	}
	if handler == nil {
		panic(
			"createStickyConsumer: handler must not be nil",
		)
	}
	subject := "sticky." + taskType + "." +
		w.workerID + ".>"
	ctx := context.Background()
	stream, err := w.js.Stream(ctx, "STICKY_TASKS")
	if err != nil {
		return nil // Stream not provisioned — skip
	}
	cons, err := stream.CreateOrUpdateConsumer(
		ctx, jetstream.ConsumerConfig{
			FilterSubject: subject,
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverAllPolicy,
		},
	)
	if err != nil {
		return nil
	}
	tt := taskType
	h := handler
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		w.handleMessage(tt, h, msg)
	})
	if err != nil {
		return nil
	}
	return cc
}

// createElasticConsumer sets up a pcgroups elastic consumer
// group for the given task type. Creates the group (idempotent)
// then joins as a member.
func (w *Worker) createElasticConsumer(
	taskType string,
	groupName string,
	filter string,
	partitions int,
	handler HandlerFunc,
) pcgroups.ConsumerGroupConsumeContext {
	if taskType == "" {
		panic(
			"createElasticConsumer: taskType must not be empty",
		)
	}
	if partitions <= 0 {
		panic(
			"createElasticConsumer: partitions must be positive",
		)
	}

	ctx := context.Background()

	_, err := pcgroups.CreateElastic(
		ctx, w.js, "TASK_QUEUES", groupName,
		uint(partitions),
		[]pcgroups.PartitioningFilter{{
			Filter: filter,
		}},
		1024, // maxBufferedMessages
		0,    // maxBufferedBytes (unlimited)
	)
	if err != nil {
		panic("createElasticConsumer: CreateElastic: " +
			groupName + ": " + err.Error())
	}

	// Register this worker as a member so the group
	// assigns partitions and the consumer starts receiving.
	_, err = pcgroups.AddMembers(
		ctx, w.js, "TASK_QUEUES", groupName,
		[]string{w.workerID},
	)
	if err != nil {
		panic("createElasticConsumer: AddMembers: " +
			groupName + ": " + err.Error())
	}

	h := handler
	tt := taskType
	cc, err := pcgroups.ElasticConsume(
		ctx, w.js, "TASK_QUEUES", groupName,
		w.workerID,
		func(msg jetstream.Msg) {
			w.handleMessage(tt, h, msg)
		},
		jetstream.ConsumerConfig{
			AckPolicy: jetstream.AckExplicitPolicy,
		},
	)
	if err != nil {
		panic("createElasticConsumer: ElasticConsume: " +
			groupName + ": " + err.Error())
	}

	slog.Info("elastic consumer group joined",
		"task_type", taskType,
		"group", groupName,
		"partitions", partitions,
	)

	return cc
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
	for _, s := range w.stoppers {
		s.Stop()
	}
}

// handleMessage unmarshals the task payload, creates a traced
// context, executes the handler, and records metrics.
func (w *Worker) handleMessage(
	taskType string, handler HandlerFunc, msg jetstream.Msg,
) {
	if msg == nil {
		panic("handleMessage: msg must not be nil")
	}
	if handler == nil {
		panic("handleMessage: handler must not be nil")
	}
	var payload protocol.TaskPayload
	err := json.Unmarshal(msg.Data(), &payload)
	if err != nil {
		slog.Error(
			"failed to unmarshal task payload",
			"error", err,
			"task_type", taskType,
		)
		msg.Ack()
		return
	}
	ctx := observe.ExtractTraceContext(msg, nil)
	ctx, span := w.tracer.Start(ctx,
		"worker.executeTask",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("run_id", payload.RunID),
			attribute.String("step_id", payload.StepID),
			attribute.String("task_name", taskType),
		),
	)
	defer span.End()
	w.tasksActive.Add(ctx, 1)
	start := time.Now()
	slog.InfoContext(ctx, "executing task",
		"task_type", taskType,
		"run_id", payload.RunID,
		"step_id", payload.StepID,
	)
	tc := newTaskContext(
		w.nc, w.tracer, w.js, payload, ctx, span, msg,
		w.checkpointKV, w.signalKV,
	)
	tc.workerID = w.workerID
	err = handler(tc)
	elapsed := float64(time.Since(start).Milliseconds())
	w.stepDuration.Record(ctx, elapsed)
	w.tasksActive.Add(ctx, -1)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(
			codes.Error, err.Error(),
		)
		w.handleTaskError(
			err, tc, msg, taskType, payload.RunID,
		)
		return
	}
	// Pause() already NAK'd the message — don't double-ack.
	if !tc.paused {
		msg.Ack()
	}
}

// handleTaskError processes a handler error by either failing
// permanently (NonRetryableError) or scheduling a retry via NAK.
func (w *Worker) handleTaskError(
	err error,
	tc *taskContext,
	msg jetstream.Msg,
	taskType string,
	runID string,
) {
	if err == nil {
		panic("handleTaskError: err must not be nil")
	}
	if msg == nil {
		panic("handleTaskError: msg must not be nil")
	}
	var rle *RateLimitError
	if errors.As(err, &rle) {
		slog.Error(
			"task hit rate limit, will retry after delay",
			"error", rle.Err,
			"retry_after", rle.RetryAfter,
			"task_type", taskType,
			"run_id", runID,
		)
		tc.FailRetryAfter(rle.Err, rle.RetryAfter)
		msg.Ack()
		return
	}
	var nre *NonRetryableError
	if errors.As(err, &nre) {
		slog.Error(
			"task failed permanently",
			"error", nre.Err,
			"task_type", taskType,
			"run_id", runID,
		)
		tc.FailPermanent(nre.Err)
		msg.Ack()
		return
	}
	slog.Error(
		"task handler returned error, will retry",
		"error", err,
		"task_type", taskType,
		"run_id", runID,
	)
	w.stepRetries.Add(context.Background(), 1)
	msg.NakWithDelay(5 * time.Second)
}
