// ring_test.go covers the bounded-by-count, bounded-by-age, drop-
// oldest, and live-subscribe contracts of logring.Handler.
//
// Methodology:
//   - Each test constructs its own Handler with a fixed Options.Now
//     so age-pruning is deterministic without sleeping.
//   - Inner handler is a slog.NewTextHandler over io.Discard — the
//     pass-through is verified separately (the inner-handler error
//     path is exercised in TestRing_PassThrough).
//   - Bounded waits (≤ 100ms) on every channel read so a regression
//     never hangs CI.
//   - Minimum 2 assertions per test (positive + negative space).
package logring

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// discardHandler is a tiny slog.Handler that discards records but
// records calls; lets the pass-through tests verify that the inner
// handler ran without needing to scan a buffer.
type discardHandler struct {
	mu     sync.Mutex
	calls  int
	retErr error
}

func (d *discardHandler) Enabled(context.Context, slog.Level) bool { return true }
func (d *discardHandler) Handle(_ context.Context, _ slog.Record) error {
	d.mu.Lock()
	d.calls++
	d.mu.Unlock()
	return d.retErr
}
func (d *discardHandler) WithAttrs(_ []slog.Attr) slog.Handler { return d }
func (d *discardHandler) WithGroup(_ string) slog.Handler      { return d }

// makeRecord builds a slog.Record at the given offset from t0 with
// the supplied message. Levels rotate INFO/WARN/ERROR so trace-id
// and severity filters have something to discriminate on.
func makeRecord(t0 time.Time, offset time.Duration, msg string, lvl slog.Level) slog.Record {
	rec := slog.NewRecord(t0.Add(offset), lvl, msg, 0)
	return rec
}

// silentInner returns a slog.Handler that drops every record without
// allocating — used when the test does not care about inner behaviour.
func silentInner() slog.Handler {
	return slog.NewTextHandler(io.Discard, nil)
}

func TestLogRing_BoundedCap(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	h := NewWithOptions(silentInner(), Options{
		CapEntries: 10_000,
		MaxAge:     time.Hour, // disable age path for this case.
		Now:        func() time.Time { return now },
	})
	for i := 0; i < 10_001; i++ {
		_ = h.Handle(context.Background(), makeRecord(
			now, time.Duration(i)*time.Millisecond,
			"msg", slog.LevelInfo,
		))
	}
	snap := h.Snapshot()
	if got, want := len(snap), 10_000; got != want {
		t.Fatalf("Snapshot len = %d, want %d", got, want)
	}
	// Negative space: the oldest record must have been evicted. The
	// retained slice starts at offset 1ms (record 1), not 0ms.
	if !snap[0].Time.Equal(now.Add(1 * time.Millisecond)) {
		t.Fatalf("oldest retained record Time = %v, want %v",
			snap[0].Time, now.Add(1*time.Millisecond))
	}
}

func TestLogRing_TimeWindow(t *testing.T) {
	t.Parallel()
	// "Wall clock" advances under test control. Records at -45m and
	// -10m are fed; the older one must be pruned at the 30m boundary.
	tick := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := tick
	h := NewWithOptions(silentInner(), Options{
		CapEntries: 100,
		MaxAge:     30 * time.Minute,
		Now:        func() time.Time { return clock },
	})
	_ = h.Handle(context.Background(), makeRecord(
		tick, -45*time.Minute, "stale", slog.LevelInfo,
	))
	_ = h.Handle(context.Background(), makeRecord(
		tick, -10*time.Minute, "fresh", slog.LevelInfo,
	))
	// Advance the clock past the 30m horizon for the -45m entry.
	// The first append above already had clock == tick so the stale
	// entry was retained at insertion time (it was 45m old at the
	// moment we appended; but our maxAge is 30m so it should have
	// been pruned right there). Verify directly via Snapshot.
	snap := h.Snapshot()
	if got, want := len(snap), 1; got != want {
		t.Fatalf("Snapshot len = %d, want %d (stale should be pruned)",
			got, want)
	}
	if snap[0].Message != "fresh" {
		t.Fatalf("retained record Message = %q, want %q",
			snap[0].Message, "fresh")
	}
}

func TestLogRing_OverflowDropsOldest(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	h := NewWithOptions(silentInner(), Options{
		CapEntries: 3,
		MaxAge:     time.Hour,
		Now:        func() time.Time { return now },
	})
	for i := 0; i < 5; i++ {
		_ = h.Handle(context.Background(), makeRecord(
			now, time.Duration(i)*time.Millisecond,
			"r", slog.LevelInfo,
		))
	}
	snap := h.Snapshot()
	if got, want := len(snap), 3; got != want {
		t.Fatalf("Snapshot len = %d, want %d", got, want)
	}
	// The three retained records must be the last three: offsets 2, 3, 4.
	for i, want := range []int{2, 3, 4} {
		got := snap[i].Time.Sub(now).Milliseconds()
		if got != int64(want) {
			t.Fatalf("snap[%d] offset = %dms, want %dms", i, got, want)
		}
	}
}

func TestLogRing_SubscribeLiveTail(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	h := NewWithOptions(silentInner(), Options{
		CapEntries: 100,
		MaxAge:     time.Hour,
		Now:        func() time.Time { return now },
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, cleanup := h.Subscribe(ctx)
	defer cleanup()
	go func() {
		// Append after a tiny delay so the test reads BEFORE the
		// record lands — verifying real fanout, not pre-buffered
		// state.
		time.Sleep(2 * time.Millisecond)
		_ = h.Handle(context.Background(), makeRecord(
			now, 0, "live", slog.LevelInfo,
		))
	}()
	select {
	case rec, ok := <-ch:
		if !ok {
			t.Fatalf("subscriber channel closed before delivery")
		}
		if rec.Message != "live" {
			t.Fatalf("received Message = %q, want %q",
				rec.Message, "live")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("did not receive live record within 100ms")
	}
	// Negative space: cleanup closes the channel and Subscribe is
	// safe to call multiple times.
	cleanup()
	cleanup() // second call must not panic.
	if _, stillOpen := <-ch; stillOpen {
		t.Fatalf("channel still open after cleanup")
	}
}

func TestLogRing_PassThrough(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	inner := &discardHandler{}
	h := NewWithOptions(inner, Options{
		CapEntries: 4,
		MaxAge:     time.Hour,
		Now:        func() time.Time { return now },
	})
	for i := 0; i < 3; i++ {
		_ = h.Handle(context.Background(), makeRecord(
			now, time.Duration(i)*time.Millisecond,
			"r", slog.LevelInfo,
		))
	}
	if got := inner.calls; got != 3 {
		t.Fatalf("inner.calls = %d, want 3", got)
	}
	// Negative space: inner-handler errors propagate.
	inner.retErr = errors.New("sink down")
	gotErr := h.Handle(context.Background(), makeRecord(
		now, 4*time.Millisecond, "r", slog.LevelInfo,
	))
	if gotErr == nil || gotErr.Error() != "sink down" {
		t.Fatalf("Handle err = %v, want %q", gotErr, "sink down")
	}
}

func TestLogRing_SubscribeContextCancelCleansUp(t *testing.T) {
	t.Parallel()
	h := NewWithOptions(silentInner(), Options{
		CapEntries: 8,
		MaxAge:     time.Hour,
	})
	ctx, cancel := context.WithCancel(context.Background())
	ch, _ := h.Subscribe(ctx)
	cancel()
	// Bounded wait: the cleanup goroutine must close the channel
	// within a reasonable window. We poll for closure.
	deadline := time.After(200 * time.Millisecond)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return // channel closed → cleanup ran. Done.
			}
		case <-deadline:
			t.Fatalf("channel not closed after ctx cancel")
		}
	}
}
