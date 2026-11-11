package server

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

// natsLogAdapter forwards the embedded nats-server's log into slog with
// component=nats-server. Without it the server's own account of a
// failure (for example "JetStream out of resources, will be DISABLED",
// #999) never reaches an operator.
//
// Debug and Trace are dropped: they are per-message protocol chatter.
// Notices logged before the server is ready are its startup banner
// (version, ids, a multi-line JetStream ASCII logo), so they go out at
// DEBUG to keep startup quiet next to dagnats's own banner; after ready,
// notices are route, leaf and leader changes and go out at INFO.
// Fatal is logged at ERROR with fatal=true and never exits: the server
// package is embedded by other binaries, and with logging off before
// this adapter nats-server's Fatalf was already a no-op here.
type natsLogAdapter struct {
	// logger is read per call because the process's default slog
	// handler is installed after the embedded server starts.
	logger func() *slog.Logger
	ready  atomic.Bool
}

var _ natsserver.Logger = (*natsLogAdapter)(nil)

func newNATSLogAdapter(logger func() *slog.Logger) *natsLogAdapter {
	if logger == nil {
		panic("newNATSLogAdapter: logger must not be nil")
	}
	adapter := &natsLogAdapter{logger: logger}
	if adapter.ready.Load() {
		panic("newNATSLogAdapter: adapter must start not ready")
	}
	return adapter
}

// markReady switches notices from startup DEBUG to runtime INFO.
func (a *natsLogAdapter) markReady() {
	if a == nil {
		panic("natsLogAdapter.markReady: nil adapter")
	}
	a.ready.Store(true)
}

func (a *natsLogAdapter) emit(level slog.Level, format string, args []any, attrs ...any) {
	if a.logger == nil {
		panic("natsLogAdapter.emit: logger must not be nil")
	}
	if level < slog.LevelDebug || level > slog.LevelError {
		panic(fmt.Sprintf("natsLogAdapter.emit: unmapped level %v", level))
	}
	// The attribute rides on the record, not on a logger.With: the
	// console's log ring forks its buffer and lock per derived handler,
	// and nats-server logs from many goroutines at once.
	attrs = append([]any{"component", "nats-server"}, attrs...)
	a.logger().Log(context.Background(), level, fmt.Sprintf(format, args...), attrs...)
}

func (a *natsLogAdapter) Noticef(format string, v ...any) {
	level := slog.LevelDebug
	if a.ready.Load() {
		level = slog.LevelInfo
	}
	a.emit(level, format, v)
}

func (a *natsLogAdapter) Warnf(format string, v ...any) {
	a.emit(slog.LevelWarn, format, v)
}

func (a *natsLogAdapter) Errorf(format string, v ...any) {
	a.emit(slog.LevelError, format, v)
}

func (a *natsLogAdapter) Fatalf(format string, v ...any) {
	a.emit(slog.LevelError, format, v, "fatal", true)
}

func (a *natsLogAdapter) Debugf(string, ...any) {}

func (a *natsLogAdapter) Tracef(string, ...any) {}
