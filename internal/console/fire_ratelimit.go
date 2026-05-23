package console

import (
	"sync"
	"time"
)

// fire_ratelimit.go implements the per-trigger fire-now rate limiter
// for #352. Shape is modelled on dlq_tombstone.go (mu + per-ID map +
// sweeper) so two operator-state structures in this package read in
// parallel.
//
// Pattern: rolling counter — each entry is a bounded slice of recent
// fire timestamps. Allow prunes timestamps older than `window`, then
// admits the call iff the remaining count is below `limit`. The
// limiter holds no per-window timer goroutine; expiry is driven on
// the `Allow` path plus a periodic sweeper that evicts whole entries
// once their newest timestamp falls outside the window.
//
// Bounded:
//   - fireLimiterMax = 10000 trigger IDs in flight; oldest evicted on
//     overflow. Stops a buggy client / attacker pumping fire-now at
//     thousands of synthetic IDs from growing the map unbounded.
//   - Per-entry timestamp slice is capped at `limit` — Allow never
//     stores more than that many timestamps for one ID.
//   - Sweeper tick: 100ms (matches dlq_tombstone). Bounded loop pass
//     over the map; no recursion.

// fireRateLimitDefault is the bucket size + window applied by the
// console wiring when the operator hasn't overridden the env vars.
// 10 fires per 60s per trigger ID matches the issue spec.
const (
	fireRateLimitDefault  = 10
	fireRateWindowDefault = 60 * time.Second
	fireLimiterMax        = 10000
)

// fireRateEntry is the bounded ring of recent fire timestamps for one
// trigger ID. Times slice is append-prune; LastSeen is the newest
// timestamp, used for whole-entry sweep eviction.
type fireRateEntry struct {
	Times    []time.Time
	LastSeen time.Time
}

// fireRateLimiter is the in-memory rolling-counter limiter keyed by
// trigger ID. Allow is the only state-mutating entry the handler
// calls; SweepOnce is a maintenance path the sweeper goroutine drives
// on a ticker (tests call it directly for determinism).
type fireRateLimiter struct {
	mu      sync.Mutex
	entries map[string]*fireRateEntry
	limit   int
	window  time.Duration
	now     func() time.Time
}

// newFireRateLimiter returns a limiter with the given bucket / window.
// Panics on programmer error (non-positive limit or window) so
// misconfiguration trips at startup, not first request.
func newFireRateLimiter(
	limit int, window time.Duration,
) *fireRateLimiter {
	if limit <= 0 {
		panic("newFireRateLimiter: limit must be positive")
	}
	if window <= 0 {
		panic("newFireRateLimiter: window must be positive")
	}
	return &fireRateLimiter{
		entries: make(map[string]*fireRateEntry),
		limit:   limit,
		window:  window,
		now:     time.Now,
	}
}

// Allow reports whether one more fire for triggerID is permitted
// under the rolling window. When the call is denied, the second
// return is the duration the caller should set on Retry-After (time
// until the oldest in-window timestamp ages out).
//
// Allow prunes the per-ID timestamp slice in place so the bound on
// stored timestamps stays at `limit` per ID.
func (l *fireRateLimiter) Allow(
	triggerID string,
) (bool, time.Duration) {
	if triggerID == "" {
		panic("fireRateLimiter.Allow: triggerID is empty")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	e, ok := l.entries[triggerID]
	if !ok {
		if len(l.entries) >= fireLimiterMax {
			l.evictOldestLocked()
		}
		e = &fireRateEntry{Times: make([]time.Time, 0, l.limit)}
		l.entries[triggerID] = e
	}
	cutoff := now.Add(-l.window)
	pruned := e.Times[:0]
	for _, t := range e.Times {
		if t.After(cutoff) {
			pruned = append(pruned, t)
		}
	}
	e.Times = pruned
	if len(e.Times) >= l.limit {
		oldest := e.Times[0]
		retry := l.window - now.Sub(oldest)
		if retry < time.Second {
			retry = time.Second
		}
		return false, retry
	}
	e.Times = append(e.Times, now)
	e.LastSeen = now
	return true, 0
}

// SweepOnce removes entries whose newest timestamp aged out of the
// window. Returns the count for tests + observability. Bounded loop:
// one pass over the map, no recursion.
func (l *fireRateLimiter) SweepOnce() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	cutoff := now.Add(-l.window)
	swept := 0
	for id, e := range l.entries {
		if e.LastSeen.Before(cutoff) {
			delete(l.entries, id)
			swept++
		}
	}
	return swept
}

// Size returns the count of trigger IDs the limiter is currently
// tracking. Used by tests; not part of the production fire path.
func (l *fireRateLimiter) Size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

// evictOldestLocked drops the entry with the earliest LastSeen so
// inserts at the map cap don't grow it past fireLimiterMax. Caller
// must hold l.mu.
func (l *fireRateLimiter) evictOldestLocked() {
	var oldestID string
	oldestT := time.Time{}
	first := true
	for id, e := range l.entries {
		if first || e.LastSeen.Before(oldestT) {
			oldestID = id
			oldestT = e.LastSeen
			first = false
		}
	}
	if oldestID != "" {
		delete(l.entries, oldestID)
	}
}
