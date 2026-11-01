package program

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/nats-io/nats.go"
)

// Running is a program Run attached. Stop ends it; Done reports when it
// ended, including when its connection closed for good on its own.
type Running struct {
	name       string
	nc         *nats.Conn
	stopAttach func(context.Context) error

	stopOnce sync.Once
	stopErr  error
	// stopping marks a close Stop (or a failed Run) caused, which Done
	// reports as nil rather than as a failure.
	stopping atomic.Bool

	doneOnce sync.Once
	done     chan error
}

func newRunning(name string) *Running {
	if name == "" {
		panic("program.newRunning: name must not be empty")
	}
	return &Running{name: name, done: make(chan error, 1)}
}

// Stop drains the worker, calls prog.Stop and closes the connection. It
// makes no control-plane request, so it works while dagnats is down.
// Later calls return the first result.
func (r *Running) Stop(ctx context.Context) error {
	if ctx == nil {
		panic("program.Running.Stop: ctx must not be nil")
	}
	if r.nc == nil || r.stopAttach == nil {
		panic("program.Running.Stop: not running")
	}
	r.stopOnce.Do(func() {
		r.stopping.Store(true)
		r.stopErr = r.stopAttach(ctx)
		r.nc.Close()
	})
	return r.stopErr
}

// Done receives exactly one value and is then closed: nil after Stop, or
// the reason the connection closed for good without Stop. A host should
// exit on a non-nil value so its supervisor restarts it; the program is
// no longer reachable.
func (r *Running) Done() <-chan error {
	if r.done == nil {
		panic("program.Running.Done: not constructed by Run")
	}
	return r.done
}

func (r *Running) finish(err error) {
	r.doneOnce.Do(func() {
		r.done <- err
		close(r.done)
	})
}

// closedHandler fires Done when the connection closes, then calls the
// caller's own ClosedHandler, if any.
func (r *Running) closedHandler(callerClosed nats.ConnHandler) nats.ConnHandler {
	if r.done == nil {
		panic("program.Running.closedHandler: not constructed by Run")
	}
	return func(nc *nats.Conn) {
		if r.stopping.Load() {
			r.finish(nil)
		} else {
			reason := nc.LastError()
			if reason == nil {
				reason = nats.ErrConnectionClosed
			}
			r.finish(fmt.Errorf("program %s: connection closed for good: %w", r.name, reason))
		}
		if callerClosed != nil {
			callerClosed(nc)
		}
	}
}
