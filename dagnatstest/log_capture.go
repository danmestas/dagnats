// dagnatstest/log_capture.go
// LogCapture installs a process-wide slog handler that records every
// log record emitted while the test runs. The capture is restored on
// t.Cleanup, so tests cannot leak captured state into siblings.
//
// Motivation: engine/worker code paths emit warnings via slog package
// functions (slog.WarnContext, slog.InfoContext) which always route
// through slog.Default(). To assert "log fires exactly once" we swap
// the default handler with a capturing one for the duration of the
// test. See PR 5 of the AFK plan (issue #195) for the consuming test.
package dagnatstest

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

// LogCapture is a thread-safe, in-memory slog handler that records
// every record emitted via slog package functions while it is the
// default. Hits(substr) counts records whose message contains substr.
type LogCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

// NewLogCapture installs a fresh LogCapture as the slog default and
// arranges for the prior default to be restored on t.Cleanup.
// Panics on nil t — programmer error.
func NewLogCapture(t *testing.T) *LogCapture {
	if t == nil {
		panic("NewLogCapture: t must not be nil")
	}
	t.Helper()
	c := &LogCapture{}
	prior := slog.Default()
	slog.SetDefault(slog.New(c))
	t.Cleanup(func() { slog.SetDefault(prior) })
	return c
}

// Hits returns the number of captured records whose message contains
// substr. Empty substr panics — empty match would count every record
// and almost certainly indicates a caller bug.
func (c *LogCapture) Hits(substr string) int {
	if c == nil {
		panic("LogCapture.Hits: receiver must not be nil")
	}
	if substr == "" {
		panic("LogCapture.Hits: substr must not be empty")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, r := range c.records {
		if strings.Contains(r.Message, substr) {
			count++
		}
	}
	return count
}

// Enabled accepts every record so callers' WarnContext / DebugContext
// hits land in the capture regardless of the prior log level.
func (c *LogCapture) Enabled(context.Context, slog.Level) bool { return true }

// Handle records the slog.Record under the mutex. The Record's
// Clone() is unnecessary here: slog.Record is a struct value, and we
// only read its Message field.
func (c *LogCapture) Handle(_ context.Context, r slog.Record) error {
	if c == nil {
		panic("LogCapture.Handle: receiver must not be nil")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, r)
	return nil
}

// WithAttrs returns the receiver unchanged. Captured records keep
// only their Message, so per-handler attrs are discarded by design.
func (c *LogCapture) WithAttrs([]slog.Attr) slog.Handler { return c }

// WithGroup returns the receiver unchanged for the same reason as
// WithAttrs.
func (c *LogCapture) WithGroup(string) slog.Handler { return c }
