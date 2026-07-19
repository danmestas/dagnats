// ackmap_test.go
// Unit tests for AckMap — pure Go, no NATS dependency.
// Methodology: test store/load/delete lifecycle and panic assertions.
// Reaper tests drive an injected fake clock rather than sleeping on the
// wall clock: the reap window is tied to DefaultAckWait (5m), so a
// sleeping test would either take minutes or have to shrink the window
// and thereby test a different system.
package bridge

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/consumername"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// fakeClock is a deterministic time source for reaper tests. AckMap
// reads it while holding its own lock, and -race runs exercise that,
// so guard the field rather than relying on test sequencing.
type fakeClock struct {
	mu      sync.Mutex
	current time.Time
}

func newFakeClock() *fakeClock {
	// Non-zero: newAckMapWithClock rejects a zero-valued clock.
	return &fakeClock{
		current: time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC),
	}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current = c.current.Add(d)
}

// stubMsg implements jetstream.Msg for unit testing the AckMap
// without a real NATS connection. Only the interface is needed;
// the AckMap never calls any methods on the stored message.
type stubMsg struct {
	subject string
}

func (s *stubMsg) Data() []byte                    { return nil }
func (s *stubMsg) Headers() nats.Header            { return nil }
func (s *stubMsg) Subject() string                 { return s.subject }
func (s *stubMsg) Reply() string                   { return "" }
func (s *stubMsg) Ack() error                      { return nil }
func (s *stubMsg) DoubleAck(context.Context) error { return nil }
func (s *stubMsg) Nak() error                      { return nil }
func (s *stubMsg) NakWithDelay(time.Duration) error {
	return nil
}
func (s *stubMsg) InProgress() error { return nil }
func (s *stubMsg) Term() error       { return nil }
func (s *stubMsg) TermWithReason(string) error {
	return nil
}
func (s *stubMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return nil, nil
}

func TestAckMapStoreAndLoad(t *testing.T) {
	am := NewAckMap()
	msg := &stubMsg{subject: "task.echo.run1"}

	am.Store("run1.step1", msg)

	got, ok := am.Load("run1.step1")
	if !ok {
		t.Fatal("expected Load to return true for stored key")
	}
	if got != msg {
		t.Fatal("expected Load to return the same message")
	}

	// Negative: key not present
	_, ok = am.Load("run1.missing")
	if ok {
		t.Fatal("expected Load to return false for missing key")
	}
}

func TestAckMapDelete(t *testing.T) {
	am := NewAckMap()
	msg := &stubMsg{subject: "task.echo.run1"}

	am.Store("run1.step1", msg)
	am.Delete("run1.step1")

	_, ok := am.Load("run1.step1")
	if ok {
		t.Fatal("expected Load to return false after Delete")
	}

	// Count should be zero
	if am.Count() != 0 {
		t.Fatalf("expected count 0 after delete, got %d", am.Count())
	}
}

func TestAckMapCount(t *testing.T) {
	am := NewAckMap()
	msg1 := &stubMsg{subject: "task.echo.run1"}
	msg2 := &stubMsg{subject: "task.echo.run2"}

	if am.Count() != 0 {
		t.Fatalf("expected count 0, got %d", am.Count())
	}

	am.Store("run1.step1", msg1)
	am.Store("run2.step1", msg2)

	if am.Count() != 2 {
		t.Fatalf("expected count 2, got %d", am.Count())
	}
}

func TestAckMapDeleteNonExistent(t *testing.T) {
	am := NewAckMap()

	// Should not panic or go negative
	am.Delete("nonexistent")

	if am.Count() != 0 {
		t.Fatalf("expected count 0, got %d", am.Count())
	}
}

func TestAckMapStorePanicsEmptyID(t *testing.T) {
	am := NewAckMap()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on empty taskID")
		}
	}()
	am.Store("", &stubMsg{})
}

func TestAckMapStorePanicsNilMsg(t *testing.T) {
	am := NewAckMap()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on nil msg")
		}
	}()
	am.Store("run1.step1", nil)
}

// TestAckMapReapsAbandonedEntry is the leak-closed proof: a worker
// that polls a task and never resolves must not hold its entry past
// the reap window. Count() returning to the baseline of one live
// entry — rather than two — is what distinguishes a closed leak from
// a merely slowed one.
func TestAckMapReapsAbandonedEntry(t *testing.T) {
	clock := newFakeClock()
	am := newAckMapWithClock(clock.Now)

	am.Store("run1.abandoned", &stubMsg{subject: "task.echo.run1"})
	if am.Count() != 1 {
		t.Fatalf("expected count 1 after store, got %d", am.Count())
	}

	clock.Advance(ackMapReapAfter + time.Second)
	// Sweep is throttled and runs on insert, so a later Store is what
	// drives it. This mirrors the real bridge: entries only accumulate
	// while tasks are being dispatched.
	am.Store("run2.live", &stubMsg{subject: "task.echo.run2"})

	if _, ok := am.Load("run1.abandoned"); ok {
		t.Fatal("expected abandoned entry to be reaped")
	}
	if _, ok := am.Load("run2.live"); !ok {
		t.Fatal("expected freshly stored entry to survive the sweep")
	}
	if am.Count() != 1 {
		t.Fatalf("expected count to return to 1, got %d", am.Count())
	}
}

// TestAckMapDoesNotReapBeforeWindow guards the opposite failure: an
// over-aggressive reaper silently shortens the worker's real budget,
// turning a legitimate in-window resolve into "task not found". Time
// advances past the sweep interval so the sweep genuinely runs — the
// entry survives because it is young, not because nothing swept.
func TestAckMapDoesNotReapBeforeWindow(t *testing.T) {
	clock := newFakeClock()
	am := newAckMapWithClock(clock.Now)

	msg := &stubMsg{subject: "task.echo.run1"}
	am.Store("run1.slow", msg)

	clock.Advance(ackMapReapAfter / 2)
	am.Store("run2.other", &stubMsg{subject: "task.echo.run2"})

	got, ok := am.Load("run1.slow")
	if !ok {
		t.Fatal("expected in-window entry to survive the sweep")
	}
	if got != msg {
		t.Fatal("expected Load to return the originally stored message")
	}
	if am.Count() != 2 {
		t.Fatalf("expected count 2 before reap window, got %d", am.Count())
	}
}

// TestAckMapLoadDoesNotReap pins the "do NOT reap on Load" constraint:
// a resolve racing the reaper must not observe a "task not found" the
// worker cannot distinguish from a genuine unknown-task error. Load is
// a pure read even for an entry well past the reap window.
func TestAckMapLoadDoesNotReap(t *testing.T) {
	clock := newFakeClock()
	am := newAckMapWithClock(clock.Now)

	am.Store("run1.stale", &stubMsg{subject: "task.echo.run1"})
	clock.Advance(ackMapReapAfter * 2)

	if _, ok := am.Load("run1.stale"); !ok {
		t.Fatal("expected Load to return the entry without reaping it")
	}
	if am.Count() != 1 {
		t.Fatalf("expected Load to leave count at 1, got %d", am.Count())
	}
}

// TestAckMapCapEvictsOldestAndLogs covers the burst backstop. The
// clock is frozen so the sweep cannot fire — the cap must hold on its
// own. Silent truncation would be worse than the leak, so the log line
// is asserted, not just the eviction.
func TestAckMapCapEvictsOldestAndLogs(t *testing.T) {
	clock := newFakeClock()
	am := newAckMapWithClock(clock.Now)

	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	// Distinct timestamps so "oldest" is well defined.
	for i := 0; i < ackMapMaxEntries; i++ {
		am.Store(fmt.Sprintf("run%d.step1", i), &stubMsg{})
		clock.Advance(time.Millisecond)
	}
	if am.Count() != ackMapMaxEntries {
		t.Fatalf("expected count %d at cap, got %d",
			ackMapMaxEntries, am.Count())
	}

	am.Store("run-overflow.step1", &stubMsg{})

	if am.Count() != ackMapMaxEntries {
		t.Fatalf("expected count to stay at cap %d, got %d",
			ackMapMaxEntries, am.Count())
	}
	if _, ok := am.Load("run0.step1"); ok {
		t.Fatal("expected oldest entry to be evicted at cap")
	}
	if _, ok := am.Load("run-overflow.step1"); !ok {
		t.Fatal("expected newest entry to be retained at cap")
	}
	if !strings.Contains(logged.String(), "ackmap at capacity") {
		t.Fatalf("expected capacity eviction to be logged, got %q",
			logged.String())
	}
}

// TestAckMapReapWindowExceedsAckWait pins the bound to the delivery it
// describes: reaping at or before AckWait would race NATS redelivery
// and could cut a worker's budget short.
func TestAckMapReapWindowExceedsAckWait(t *testing.T) {
	if ackMapReapAfter <= consumername.DefaultAckWait {
		t.Fatalf("reap window %v must exceed AckWait %v",
			ackMapReapAfter, consumername.DefaultAckWait)
	}
	if ackMapSweepInterval >= ackMapReapAfter {
		t.Fatalf("sweep interval %v must be shorter than reap window %v",
			ackMapSweepInterval, ackMapReapAfter)
	}
}

func TestAckMapLoadPanicsEmptyID(t *testing.T) {
	am := NewAckMap()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on empty taskID")
		}
	}()
	am.Load("")
}
