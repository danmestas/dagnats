// worker/cancel.go
// Cancellation reaching a running handler (#802). The engine publishes
// dagnats.cancel.{runID} on core NATS when a run is cancelled; the
// worker ends the context of every in-flight handler of that run. The
// signal is at-most-once, so Heartbeat also reads the run state.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"

	"github.com/danmestas/dagnats/internal/runstate"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// ErrRunCancelled is returned by Heartbeat when the task's run has been
// cancelled. The handler's Context() is already done at that point; a
// handler should stop and return.
var ErrRunCancelled = errors.New("worker: run cancelled")

// cancelRegistry maps run ids to the cancel funcs of their in-flight
// handlers. Bounded by the number of concurrently running handlers.
type cancelRegistry struct {
	mu    sync.Mutex
	next  uint64
	byRun map[string]map[uint64]context.CancelFunc
}

// add registers cancel under runID and returns a func that removes the
// entry and calls cancel (so the context is released on every exit).
func (r *cancelRegistry) add(runID string, cancel context.CancelFunc) func() {
	if runID == "" {
		panic("cancelRegistry.add: runID must not be empty")
	}
	if cancel == nil {
		panic("cancelRegistry.add: cancel must not be nil")
	}
	r.mu.Lock()
	if r.byRun == nil {
		r.byRun = make(map[string]map[uint64]context.CancelFunc)
	}
	r.next++
	id := r.next
	if r.byRun[runID] == nil {
		r.byRun[runID] = make(map[uint64]context.CancelFunc)
	}
	r.byRun[runID][id] = cancel
	r.mu.Unlock()
	return func() {
		r.mu.Lock()
		delete(r.byRun[runID], id)
		if len(r.byRun[runID]) == 0 {
			delete(r.byRun, runID)
		}
		r.mu.Unlock()
		cancel()
	}
}

// cancelRun ends the context of every registered handler of runID.
func (r *cancelRegistry) cancelRun(runID string) {
	if runID == "" {
		panic("cancelRegistry.cancelRun: runID must not be empty")
	}
	r.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(r.byRun[runID]))
	for _, cancel := range r.byRun[runID] {
		cancels = append(cancels, cancel)
	}
	r.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

// subscribeCancel listens for run cancel signals. A worker built
// without a connection has nothing to listen on.
func (w *Worker) subscribeCancel() {
	if w.nc == nil {
		return
	}
	sub, err := w.nc.Subscribe(protocol.CancelSubjectPrefix+"*", func(m *nats.Msg) {
		runID := strings.TrimPrefix(m.Subject, protocol.CancelSubjectPrefix)
		if runID != "" {
			w.cancels.cancelRun(runID)
		}
	})
	if err != nil {
		// Heartbeat still catches the cancel, only later.
		slog.Warn("subscribe to run cancel signals failed", "error", err)
		return
	}
	w.cancelSub = sub
}

// bindCancel gives tc a context that ends on cancel and a run-state
// check for Heartbeat. It returns the release func handleMessage defers.
func (w *Worker) bindCancel(
	ctx context.Context, tc *taskContext,
) (context.Context, func()) {
	if tc == nil {
		panic("bindCancel: tc must not be nil")
	}
	if ctx == nil {
		panic("bindCancel: ctx must not be nil")
	}
	ctx, cancel := context.WithCancel(ctx)
	release := w.cancels.add(tc.runID, cancel)
	tc.ctx = ctx
	tc.runCancelled = func() bool {
		if !runstate.Cancelled(ctx, w.workflowRunsKV, tc.runID) {
			return false
		}
		cancel()
		return true
	}
	return ctx, release
}
