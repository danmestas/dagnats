package worker

import (
	"encoding/json"
	"errors"

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

// HandlerFunc is the function signature for task handlers registered
// with a Worker.
type HandlerFunc func(ctx TaskContext) error

// Worker subscribes to task subjects and dispatches messages to
// registered handlers. Every handler error publishes step.failed and
// acks — the orchestrator owns retry decisions via StepDef.Retries.
type Worker struct {
	nc       *nats.Conn
	js       nats.JetStreamContext
	tel      *observe.Telemetry
	handlers map[string]HandlerFunc
	subs     []*nats.Subscription
}

// NewWorker creates a Worker using the given connection and telemetry
// bundle. Panics if nc or tel is nil, or if JetStream cannot be
// initialised — all are programmer errors at startup.
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
// Panics on empty taskType or nil handler — programmer errors.
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
// Panics if any subscription fails — startup error.
func (w *Worker) Start() {
	for taskType, handler := range w.handlers {
		subject := "task." + taskType + ".>"
		h := handler
		tt := taskType
		sub, err := w.js.Subscribe(subject, func(msg *nats.Msg) {
			w.handleMessage(tt, h, msg)
		}, nats.AckExplicit(), nats.DeliverAll())
		if err != nil {
			panic("Worker.Start: Subscribe failed for " +
				taskType + ": " + err.Error())
		}
		w.subs = append(w.subs, sub)
	}
}

// Stop unsubscribes all active subscriptions.
func (w *Worker) Stop() {
	for _, sub := range w.subs {
		sub.Unsubscribe()
	}
}

// handleMessage dispatches a task message to the handler. On any error
// the worker publishes step.failed and acks — the orchestrator decides
// whether to retry based on StepDef.Retries. No NakWithDelay: retry
// ownership lives entirely in the orchestrator.
func (w *Worker) handleMessage(
	taskType string, handler HandlerFunc, msg *nats.Msg,
) {
	var payload protocol.TaskPayload
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		w.tel.Logger.Error("unmarshal task payload failed", err,
			observe.String("task_type", taskType))
		msg.Ack()
		return
	}
	ctx := newTaskContext(w.nc, w.js, payload)
	w.tel.Logger.Info("executing task",
		observe.String("task_type", taskType),
		observe.String("run_id", payload.RunID),
		observe.String("step_id", payload.StepID),
	)
	if err := handler(ctx); err != nil {
		w.logHandlerError(taskType, payload, err)
		ctx.Fail(err)
		msg.Ack()
		return
	}
	msg.Ack()
}

func (w *Worker) logHandlerError(
	taskType string, p protocol.TaskPayload, err error,
) {
	var nre *NonRetryableError
	if errors.As(err, &nre) {
		w.tel.Logger.Error("task failed permanently", nre.Err,
			observe.String("task_type", taskType),
			observe.String("run_id", p.RunID),
		)
		return
	}
	w.tel.Logger.Error("task failed", err,
		observe.String("task_type", taskType),
		observe.String("run_id", p.RunID),
		observe.Int("attempt", p.Attempt),
	)
}
