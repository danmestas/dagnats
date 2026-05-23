package console

// fire_ratelimit_test.go exercises the per-trigger rolling-counter
// limiter that gates POST /console/triggers/{id}/fire (#352).
//
// Methodology:
//   - Each test builds its own limiter with a deterministic fakeClock
//     (defined in dlq_tombstone_test.go) so window arithmetic is
//     reproducible across CI.
//   - Each test asserts both the state-machine transition AND the
//     boundary / retry-after path — minimum 2 assertions per test.

import (
	"strconv"
	"testing"
	"time"
)

func newRateLimiterWithClock(
	t *testing.T, c *fakeClock, limit int, window time.Duration,
) *fireRateLimiter {
	t.Helper()
	l := newFireRateLimiter(limit, window)
	l.now = c.now
	return l
}

// TestFireRateLimit_underBucketAllows admits calls up to the bucket
// limit and rejects the (limit+1)th with a positive retry-after.
func TestFireRateLimit_underBucketAllows(t *testing.T) {
	c := &fakeClock{t: time.Now()}
	l := newRateLimiterWithClock(t, c, 3, 60*time.Second)
	for i := 0; i < 3; i++ {
		ok, retry := l.Allow("trig-A")
		if !ok {
			t.Fatalf("Allow #%d returned false; want true", i+1)
		}
		if retry != 0 {
			t.Fatalf("retry on success = %v; want 0", retry)
		}
	}
	ok, retry := l.Allow("trig-A")
	if ok {
		t.Fatalf("Allow past limit returned true; want false")
	}
	if retry <= 0 {
		t.Fatalf("retry-after on deny = %v; want positive", retry)
	}
}

// TestFireRateLimit_windowSlide allows additional calls once the
// oldest timestamp has aged out of the window.
func TestFireRateLimit_windowSlide(t *testing.T) {
	c := &fakeClock{t: time.Now()}
	l := newRateLimiterWithClock(t, c, 2, 60*time.Second)
	if ok, _ := l.Allow("trig-B"); !ok {
		t.Fatalf("first Allow returned false")
	}
	if ok, _ := l.Allow("trig-B"); !ok {
		t.Fatalf("second Allow returned false")
	}
	if ok, _ := l.Allow("trig-B"); ok {
		t.Fatalf("third Allow returned true; want false")
	}
	c.advance(61 * time.Second)
	if ok, _ := l.Allow("trig-B"); !ok {
		t.Fatalf("Allow after window slide returned false")
	}
}

// TestFireRateLimit_independentKeys confirms per-ID accounting — one
// noisy trigger can't starve another.
func TestFireRateLimit_independentKeys(t *testing.T) {
	c := &fakeClock{t: time.Now()}
	l := newRateLimiterWithClock(t, c, 1, 60*time.Second)
	if ok, _ := l.Allow("trig-X"); !ok {
		t.Fatalf("trig-X first Allow returned false")
	}
	if ok, _ := l.Allow("trig-X"); ok {
		t.Fatalf("trig-X second Allow returned true")
	}
	if ok, _ := l.Allow("trig-Y"); !ok {
		t.Fatalf("trig-Y first Allow returned false")
	}
}

// TestFireRateLimit_sweeperResetsWindow asserts the sweeper drops
// fully-aged entries so the map shrinks back below population peak.
func TestFireRateLimit_sweeperResetsWindow(t *testing.T) {
	c := &fakeClock{t: time.Now()}
	l := newRateLimiterWithClock(t, c, 2, 60*time.Second)
	_, _ = l.Allow("trig-K")
	_, _ = l.Allow("trig-L")
	if l.Size() != 2 {
		t.Fatalf("size after seed = %d; want 2", l.Size())
	}
	c.advance(61 * time.Second)
	if swept := l.SweepOnce(); swept != 2 {
		t.Fatalf("SweepOnce returned %d; want 2", swept)
	}
	if l.Size() != 0 {
		t.Fatalf("size after sweep = %d; want 0", l.Size())
	}
}

// TestFireRateLimit_mapBound populates 10001 distinct IDs and asserts
// the map stays at fireLimiterMax with the oldest entry evicted.
func TestFireRateLimit_mapBound(t *testing.T) {
	c := &fakeClock{t: time.Now()}
	l := newRateLimiterWithClock(t, c, 10, 60*time.Second)
	const overflow = fireLimiterMax + 1
	for i := 0; i < overflow; i++ {
		c.advance(time.Millisecond)
		id := "t-" + strconv.Itoa(i)
		if ok, _ := l.Allow(id); !ok {
			t.Fatalf("Allow %q returned false during populate", id)
		}
	}
	if l.Size() > fireLimiterMax {
		t.Fatalf("Size after overflow = %d; want <= %d",
			l.Size(), fireLimiterMax)
	}
	// Oldest entry ("t-0") must have been evicted to make room.
	l.mu.Lock()
	_, stillThere := l.entries["t-0"]
	l.mu.Unlock()
	if stillThere {
		t.Fatalf("oldest entry t-0 still present; eviction failed")
	}
}
