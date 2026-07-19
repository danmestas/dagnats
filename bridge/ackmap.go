package bridge

import (
	"log/slog"
	"sync"
	"time"

	"github.com/danmestas/dagnats/internal/consumername"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// ackMapReapMargin keeps the reap window strictly wider than
	// AckWait so a worker resolving right at its deadline still finds
	// its entry — the reaper must never shorten the real budget.
	ackMapReapMargin = 30 * time.Second

	// ackMapReapAfter bounds an entry's life to the delivery it
	// describes. Past AckWait, NATS has redelivered the message, so the
	// held jetstream.Msg refers to a superseded delivery and acking it
	// is silently discarded. Keeping the entry beyond this point is not
	// merely useless, it is misleading.
	ackMapReapAfter = consumername.DefaultAckWait + ackMapReapMargin

	// ackMapSweepInterval throttles the sweep so a burst of inserts
	// does not make every Store an O(n) scan. It sets the reap latency
	// overshoot: an abandoned entry is gone within
	// ackMapReapAfter + ackMapSweepInterval.
	ackMapSweepInterval = 30 * time.Second

	// ackMapMaxEntries backstops a burst that outruns the sweep
	// cadence. Reaching it means genuinely that many concurrent
	// in-flight HTTP tasks, which is already pathological.
	ackMapMaxEntries = 10000
)

// ackEntry pairs a polled message with its insertion time so the
// reaper can tell a live delivery from a superseded one.
type ackEntry struct {
	msg      jetstream.Msg
	storedAt time.Time
}

// AckMap tracks in-flight tasks for HTTP workers. Maps task_id
// ({runID}.{stepID}) to the NATS message so the bridge can ack/nak
// on behalf of the HTTP client when it resolves the task.
//
// Thread-safe: multiple poll/resolve handlers run concurrently.
//
// Bounded two ways, because an HTTP worker that dies mid-task never
// resolves and would otherwise leak its entry for the process
// lifetime: entries older than ackMapReapAfter are swept on insert,
// and ackMapMaxEntries caps the map between sweeps.
//
// The sweep runs on insert rather than on a ticker because Bridge has
// no shutdown path to stop a goroutine against, and because entries
// are only ever created by traffic — an idle bridge cannot grow.
type AckMap struct {
	mu        sync.Mutex
	entries   map[string]ackEntry
	lastSweep time.Time
	now       func() time.Time
}

// NewAckMap creates an empty AckMap ready for use.
func NewAckMap() *AckMap {
	return newAckMapWithClock(time.Now)
}

// newAckMapWithClock injects the time source so reaper tests can
// advance time deterministically instead of sleeping out a 5m window.
func newAckMapWithClock(now func() time.Time) *AckMap {
	if now == nil {
		panic("newAckMapWithClock: now must not be nil")
	}
	start := now()
	if start.IsZero() {
		panic("newAckMapWithClock: clock must not return zero time")
	}
	return &AckMap{
		entries:   make(map[string]ackEntry),
		lastSweep: start,
		now:       now,
	}
}

// Store saves a NATS message keyed by task ID, stamped with the
// insertion time. Sweeps expired entries and enforces the size cap
// before inserting.
// Panics on empty taskID or nil msg — both are programmer errors.
func (am *AckMap) Store(taskID string, msg jetstream.Msg) {
	if taskID == "" {
		panic("AckMap.Store: taskID must not be empty")
	}
	if msg == nil {
		panic("AckMap.Store: msg must not be nil")
	}
	am.mu.Lock()
	defer am.mu.Unlock()
	now := am.now()
	am.sweepLocked(now)
	if len(am.entries) >= ackMapMaxEntries {
		am.evictOldestLocked()
	}
	am.entries[taskID] = ackEntry{msg: msg, storedAt: now}
}

// Load retrieves the NATS message for the given task ID.
// Returns (nil, false) if not found.
//
// Deliberately does not reap: a resolve arriving concurrently with the
// reaper must not race into a "task not found" that the worker cannot
// distinguish from a genuine unknown-task error.
func (am *AckMap) Load(taskID string) (jetstream.Msg, bool) {
	if am == nil {
		panic("AckMap.Load: nil receiver")
	}
	if taskID == "" {
		panic("AckMap.Load: taskID must not be empty")
	}
	am.mu.Lock()
	defer am.mu.Unlock()
	entry, ok := am.entries[taskID]
	if !ok {
		return nil, false
	}
	return entry.msg, true
}

// Delete removes a task from the map after resolution.
func (am *AckMap) Delete(taskID string) {
	if am == nil {
		panic("AckMap.Delete: nil receiver")
	}
	if taskID == "" {
		panic("AckMap.Delete: taskID must not be empty")
	}
	am.mu.Lock()
	defer am.mu.Unlock()
	delete(am.entries, taskID)
}

// Count returns the number of in-flight tasks.
func (am *AckMap) Count() int64 {
	am.mu.Lock()
	defer am.mu.Unlock()
	if am.entries == nil {
		panic("AckMap.Count: entries must not be nil")
	}
	return int64(len(am.entries))
}

// sweepLocked drops entries whose delivery NATS has already
// superseded. Throttled to one pass per ackMapSweepInterval; the loop
// is bounded by ackMapMaxEntries. Caller must hold am.mu.
func (am *AckMap) sweepLocked(now time.Time) {
	if now.IsZero() {
		panic("AckMap.sweepLocked: now must not be zero")
	}
	if am.entries == nil {
		panic("AckMap.sweepLocked: entries must not be nil")
	}
	if now.Sub(am.lastSweep) < ackMapSweepInterval {
		return
	}
	am.lastSweep = now
	for taskID, entry := range am.entries {
		if now.Sub(entry.storedAt) >= ackMapReapAfter {
			delete(am.entries, taskID)
		}
	}
}

// evictOldestLocked drops the single oldest entry to make room at the
// cap. The oldest is the closest to being reaped anyway, whereas the
// incoming entry has a worker actively waiting on it. Logged because
// silent truncation would be worse than the leak it guards against.
// Caller must hold am.mu.
func (am *AckMap) evictOldestLocked() {
	if am.entries == nil {
		panic("AckMap.evictOldestLocked: entries must not be nil")
	}
	if len(am.entries) == 0 {
		panic("AckMap.evictOldestLocked: nothing to evict")
	}
	var oldestID string
	var oldestAt time.Time
	for taskID, entry := range am.entries {
		if oldestID == "" || entry.storedAt.Before(oldestAt) {
			oldestID, oldestAt = taskID, entry.storedAt
		}
	}
	delete(am.entries, oldestID)
	slog.Warn("ackmap at capacity, evicted oldest in-flight task",
		"task_id", oldestID,
		"stored_at", oldestAt,
		"max_entries", ackMapMaxEntries,
	)
}
