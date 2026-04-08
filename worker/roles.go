package worker

import (
	"context"
	"time"
)

// Role-based interfaces narrow the full TaskContext surface so
// handlers declare only the capabilities they need. The underlying
// taskContext struct satisfies all roles — no adapter needed. This
// follows Ousterhout's principle of deep modules: same rich
// implementation, but callers see a small interface matched to
// their actual needs.

// SimpleTask is the minimal interface for basic task handlers.
// Most handlers only need Input, Complete, and Fail.
type SimpleTask interface {
	Input() []byte
	RunID() string
	StepID() string
	Context() context.Context
	Complete(output []byte) error
	Fail(err error) error
	FailPermanent(err error) error
}

// CheckpointTask adds checkpoint/resume capability for handlers
// that need to persist state across retries.
type CheckpointTask interface {
	SimpleTask
	Checkpoint(state []byte) error
	LoadCheckpoint() ([]byte, error)
	RetryCount() int
}

// LoopTask adds agent-loop iteration capability for handlers
// that call Continue to request another execution cycle.
// Includes streaming and heartbeat for long-running iterations.
type LoopTask interface {
	CheckpointTask
	Continue(output []byte) error
	FailRetryAfter(err error, after time.Duration) error
	PutStream(data []byte) error
	Heartbeat() error
}

// StreamTask adds streaming output and heartbeat for handlers
// that produce incremental results or need keep-alive signals.
type StreamTask interface {
	SimpleTask
	PutStream(data []byte) error
	Heartbeat() error
}

// SignalTask adds inter-step coordination for handlers that
// wait on or send signals to other steps in the workflow.
type SignalTask interface {
	SimpleTask
	WaitForSignal(
		name string, timeout time.Duration,
	) ([]byte, error)
	SendSignal(runID, name string, data []byte) error
}

// HandleSimple registers a handler that receives only SimpleTask.
// The handler sees a narrow interface — it cannot call Continue,
// Checkpoint, or other advanced methods.
func HandleSimple(
	w *Worker, taskType string, fn func(SimpleTask) error,
) {
	if fn == nil {
		panic("HandleSimple: fn must not be nil")
	}
	w.Handle(taskType, func(ctx TaskContext) error {
		return fn(ctx)
	})
}

// HandleCheckpoint registers a handler that receives
// CheckpointTask for save/restore across retries.
func HandleCheckpoint(
	w *Worker,
	taskType string,
	fn func(CheckpointTask) error,
) {
	if fn == nil {
		panic("HandleCheckpoint: fn must not be nil")
	}
	w.Handle(taskType, func(ctx TaskContext) error {
		return fn(ctx)
	})
}

// HandleLoop registers a handler that receives LoopTask for
// agent-loop iteration with Continue and FailRetryAfter.
func HandleLoop(
	w *Worker, taskType string, fn func(LoopTask) error,
) {
	if fn == nil {
		panic("HandleLoop: fn must not be nil")
	}
	w.Handle(taskType, func(ctx TaskContext) error {
		return fn(ctx)
	})
}

// HandleStream registers a handler that receives StreamTask
// for streaming output and heartbeat keep-alive.
func HandleStream(
	w *Worker,
	taskType string,
	fn func(StreamTask) error,
) {
	if fn == nil {
		panic("HandleStream: fn must not be nil")
	}
	w.Handle(taskType, func(ctx TaskContext) error {
		return fn(ctx)
	})
}

// HandleSignal registers a handler that receives SignalTask
// for inter-step coordination via WaitForSignal/SendSignal.
func HandleSignal(
	w *Worker,
	taskType string,
	fn func(SignalTask) error,
) {
	if fn == nil {
		panic("HandleSignal: fn must not be nil")
	}
	w.Handle(taskType, func(ctx TaskContext) error {
		return fn(ctx)
	})
}
