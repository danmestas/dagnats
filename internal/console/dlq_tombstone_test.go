// dlq_tombstone_test.go exercises the in-memory soft-discard
// tombstone tracker.
//
// Methodology:
//   - Unit tests with a controlled clock so window math is
//     deterministic.
//   - Each test creates its own store; nothing is shared.
//   - Minimum 2 assertions per test: state-machine transition AND
//     boundary/error.
package console

import (
	"sync"
	"testing"
	"time"
)

// fakeClock is a deterministic clock for tombstone tests. now() returns
// the internal time; advance() ticks it forward.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newStoreWithClock(
	t *testing.T, c *fakeClock, onExpire func(uint64),
) *dlqTombstoneStore {
	t.Helper()
	s := newDLQTombstoneStore(time.Second, onExpire)
	s.now = c.now
	return s
}

func TestDLQTombstone_undoWithinWindowSucceeds(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	expired := 0
	s := newStoreWithClock(t, clock, func(uint64) { expired++ })
	tok, exp := s.Tombstone(42)
	if tok == "" || exp.IsZero() {
		t.Fatalf("Tombstone returned empty token or expiry")
	}
	if !s.HasTombstone(42) {
		t.Fatalf("HasTombstone(42) false; want true")
	}
	if !s.Undo(42, tok) {
		t.Fatalf("Undo within window returned false")
	}
	if s.HasTombstone(42) {
		t.Fatalf("HasTombstone(42) still true after undo")
	}
	if expired != 0 {
		t.Fatalf("expired calls = %d; want 0 (undo cancelled sweep)",
			expired)
	}
}

func TestDLQTombstone_undoOutsideWindowFails(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	expired := 0
	s := newStoreWithClock(t, clock, func(uint64) { expired++ })
	tok, _ := s.Tombstone(7)
	clock.advance(2 * time.Second)
	if s.Undo(7, tok) {
		t.Fatalf("Undo past window returned true; want false")
	}
	if !s.HasTombstone(7) {
		t.Fatalf("entry should still be in registry until sweep runs")
	}
}

func TestDLQTombstone_undoWrongTokenFails(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	s := newStoreWithClock(t, clock, func(uint64) {})
	_, _ = s.Tombstone(11)
	if s.Undo(11, "definitely-not-the-token") {
		t.Fatalf("Undo with wrong token returned true; want false")
	}
	if s.Undo(11, "") {
		t.Fatalf("Undo with empty token returned true; want false")
	}
}

func TestDLQTombstone_undoUnknownSeqFails(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	s := newStoreWithClock(t, clock, func(uint64) {})
	if s.Undo(99, "anything") {
		t.Fatalf("Undo with unknown seq returned true; want false")
	}
	if s.Undo(0, "x") {
		t.Fatalf("Undo with zero seq returned true; want false")
	}
}

func TestDLQTombstone_sweepRunsOnExpire(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	expired := map[uint64]int{}
	var mu sync.Mutex
	s := newStoreWithClock(t, clock, func(seq uint64) {
		mu.Lock()
		expired[seq]++
		mu.Unlock()
	})
	_, _ = s.Tombstone(100)
	_, _ = s.Tombstone(200)
	clock.advance(2 * time.Second)
	swept := s.SweepOnce()
	if swept != 2 {
		t.Fatalf("SweepOnce returned %d; want 2", swept)
	}
	mu.Lock()
	defer mu.Unlock()
	if expired[100] != 1 || expired[200] != 1 {
		t.Fatalf("expired counts = %v; want 1 each", expired)
	}
}

func TestDLQTombstone_overflowEvictsOldest(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	s := newStoreWithClock(t, clock, func(uint64) {})
	// Insert max+5 entries; first 5 must be evicted.
	for i := uint64(1); i <= uint64(dlqTombstoneMax+5); i++ {
		_, _ = s.Tombstone(i)
		clock.advance(time.Millisecond)
	}
	missing := 0
	for i := uint64(1); i <= 5; i++ {
		if !s.HasTombstone(i) {
			missing++
		}
	}
	if missing != 5 {
		t.Fatalf("evicted = %d; want 5", missing)
	}
	if !s.HasTombstone(uint64(dlqTombstoneMax + 5)) {
		t.Fatalf("newest tombstone evicted; want preserved")
	}
}
