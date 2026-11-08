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

// ConnState is the state of a program's NATS connection.
type ConnState string

// The states a program's connection reports. Reconnecting covers a
// connection that is down and retrying, which it does forever.
const (
	ConnConnected    ConnState = "connected"
	ConnReconnecting ConnState = "reconnecting"
	ConnClosed       ConnState = "closed"
)

// ConnStatus is a point-in-time view of the program's own connection,
// for a host's health endpoint. It is a value, not the connection, so a
// host cannot publish under the program's identity or bypass Run's inbox
// and auth guarantees.
type ConnStatus struct {
	// Program is the manifest name.
	Program string
	State   ConnState
	// ServerURL, ServerName, ServerVersion and ClusterName describe the
	// server the connection is on; they are empty unless connected.
	ServerURL     string
	ServerName    string
	ServerVersion string
	ClusterName   string
	// Reconnects counts successful reconnections since Run.
	Reconnects uint64
	// LastError is the connection's most recent error, "" if none. It can
	// outlive the failure it describes, so read State first.
	LastError string
}

// Connected reports whether the connection is up.
func (s ConnStatus) Connected() bool { return s.State == ConnConnected }

// Status describes the program's connection (not Run's short-lived
// registration one). It is safe to call concurrently and after Stop or
// after the connection closed for good, when it reports ConnClosed.
func (r *Running) Status() ConnStatus {
	if r.nc == nil {
		panic("program.Running.Status: not running")
	}
	status := ConnStatus{
		Program:    r.name,
		State:      connState(r.nc.Status()),
		Reconnects: r.nc.Stats().Reconnects,
	}
	if err := r.nc.LastError(); err != nil {
		status.LastError = err.Error()
	}
	if status.State == ConnConnected {
		status.ServerURL = r.nc.ConnectedUrl()
		status.ServerName = r.nc.ConnectedServerName()
		status.ServerVersion = r.nc.ConnectedServerVersion()
		status.ClusterName = r.nc.ConnectedClusterName()
	}
	return status
}

// connState maps nats.go's states: anything that is neither up nor
// closed (connecting, reconnecting, draining) is not serving.
func connState(status nats.Status) ConnState {
	switch status {
	case nats.CONNECTED:
		return ConnConnected
	case nats.CLOSED:
		return ConnClosed
	default:
		return ConnReconnecting
	}
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
