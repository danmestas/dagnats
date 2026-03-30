package worker

import (
	"encoding/json"
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
	Complete(output []byte) error
	Fail(err error) error
	Continue(output []byte) error
}

// HandlerFunc is the function signature for task handlers registered with a Worker.
type HandlerFunc func(ctx TaskContext) error

// Worker subscribes to task subjects and dispatches messages to registered handlers.
// Each task type gets its own JetStream subscription; messages are ack'd after the
// handler returns so failures are retried by JetStream's MaxDeliver policy.
type Worker struct {
	nc       *nats.Conn
	js       nats.JetStreamContext
	tel      *observe.Telemetry
	handlers map[string]HandlerFunc
	subs     []*nats.Subscription
}

// NewWorker creates a Worker using the given connection and telemetry bundle.
// Panics if nc or tel is nil, or if JetStream cannot be initialised — all are
// programmer errors at startup, not recoverable runtime conditions.
func NewWorker(nc *nats.Conn, tel *observe.Telemetry) *Worker {
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
	}
}

// Handle registers a HandlerFunc for the given task type.
// Panics on empty taskType or nil handler — both are programmer errors.
func (w *Worker) Handle(taskType string, handler HandlerFunc) {
	if taskType == "" {
		panic("Worker.Handle: taskType must not be empty")
	}
	if handler == nil {
		panic("Worker.Handle: handler must not be nil")
	}
	w.handlers[taskType] = handler
}

// Start creates JetStream subscriptions for all registered task types.
// Panics if any subscription fails — stream misconfiguration is a startup error.
func (w *Worker) Start() {
	for taskType, handler := range w.handlers {
		subject := "task." + taskType + ".>"
		h := handler
		tt := taskType
		sub, err := w.js.Subscribe(subject, func(msg *nats.Msg) {
			w.handleMessage(tt, h, msg)
		}, nats.AckExplicit(), nats.DeliverAll())
		if err != nil {
			panic("Worker.Start: Subscribe failed for " + taskType + ": " + err.Error())
		}
		w.subs = append(w.subs, sub)
	}
}

// Stop unsubscribes all active subscriptions. Safe to call after Start.
func (w *Worker) Stop() {
	for _, sub := range w.subs {
		sub.Unsubscribe()
	}
}

func (w *Worker) handleMessage(taskType string, handler HandlerFunc, msg *nats.Msg) {
	var payload protocol.TaskPayload
	err := json.Unmarshal(msg.Data, &payload)
	if err != nil {
		w.tel.Logger.Error("failed to unmarshal task payload", err,
			observe.String("task_type", taskType))
		msg.Ack()
		return
	}
	ctx := newTaskContext(w.js, payload)
	w.tel.Logger.Info("executing task",
		observe.String("task_type", taskType),
		observe.String("run_id", payload.RunID),
		observe.String("step_id", payload.StepID),
	)
	err = handler(ctx)
	if err != nil {
		w.tel.Logger.Error("task handler returned error, will retry", err,
			observe.String("task_type", taskType),
			observe.String("run_id", payload.RunID),
		)
		msg.NakWithDelay(5 * time.Second)
		return
	}
	msg.Ack()
}
