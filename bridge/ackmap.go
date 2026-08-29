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

	// resolvedTaskTTL bounds how long a resolved-task marker survives
	// in the separate resolvedTasks map (#624 review round 2). It
	// exists purely so POST /v1/tasks/{id}/logs can tell a caller
	// "already resolved" (409) instead of "never existed" (404) for a
	// window after resolve — no correctness depends on how long the
	// distinction survives past that, so this is generous but bounded.
	resolvedTaskTTL = 5 * time.Minute

	// resolvedTaskMax backstops resolvedTasks the same way
	// ackMapMaxEntries backstops entries.
	resolvedTaskMax = 10000
)

// ackEntry pairs a polled message with its insertion time so the
// reaper can tell a live delivery from a superseded one, plus the
// TokenID of the caller that claimed it (#627) so resolve can enforce
// that only the claiming caller -- or an admin -- may act on it.
// tokenID is empty for the env admin bearer or dev mode.
type ackEntry struct {
	msg      jetstream.Msg
	storedAt time.Time
	tokenID  string

	// Log-ingest counters for POST /v1/tasks/{id}/logs (#624). Piggybacks
	// on AckMap's existing lifecycle rather than a second expiring map:
	// log ingest is only valid while the task is in-flight, which is
	// exactly the window an ackEntry exists for, so these counters age
	// out (and get reset for a re-claimed task ID) for free.
	logSeq        uint64
	logTotalBytes int64
	logTruncated  bool
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
	// resolvedTasks records taskIDs recently removed from entries via
	// resolution (complete/fail/pause/continue), separately from
	// entries itself, so handleLogs can distinguish "already resolved"
	// (409) from "never existed" (404) after Delete has already
	// dropped the live claim entry (#624 review round 2). Bounded and
	// TTL'd the same way entries is; see resolvedTaskMax/resolvedTaskTTL.
	resolvedTasks map[string]time.Time
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
		entries:       make(map[string]ackEntry),
		lastSweep:     start,
		now:           now,
		resolvedTasks: make(map[string]time.Time),
	}
}

// Store saves a NATS message keyed by task ID, stamped with the
// insertion time and the TokenID of the caller that claimed it (#627;
// empty for the admin bearer or dev mode). Sweeps expired entries and
// enforces the size cap before inserting.
// Panics on empty taskID or nil msg — both are programmer errors.
func (am *AckMap) Store(taskID string, msg jetstream.Msg, tokenID string) {
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
	am.entries[taskID] = ackEntry{msg: msg, storedAt: now, tokenID: tokenID}
}

// Load retrieves the NATS message for the given task ID.
// Returns (nil, false) if not found. A thin wrapper over
// LoadWithTokenID for callers that don't need the claiming TokenID.
//
// Deliberately does not reap: a resolve arriving concurrently with the
// reaper must not race into a "task not found" that the worker cannot
// distinguish from a genuine unknown-task error.
func (am *AckMap) Load(taskID string) (jetstream.Msg, bool) {
	msg, _, ok := am.LoadWithTokenID(taskID)
	return msg, ok
}

// LoadWithTokenID retrieves the NATS message and the TokenID recorded
// at Store time for the given task ID (#627 per-task authorization:
// a resolve must present the same TokenID that claimed the task, or
// be Admin). Returns (nil, "", false) if not found. Same
// does-not-reap contract as Load.
func (am *AckMap) LoadWithTokenID(taskID string) (jetstream.Msg, string, bool) {
	if am == nil {
		panic("AckMap.LoadWithTokenID: nil receiver")
	}
	if taskID == "" {
		panic("AckMap.LoadWithTokenID: taskID must not be empty")
	}
	am.mu.Lock()
	defer am.mu.Unlock()
	entry, ok := am.entries[taskID]
	if !ok {
		return nil, "", false
	}
	return entry.msg, entry.tokenID, true
}

// WithLogState atomically reads and updates the log-ingest counters
// for taskID under AckMap's own mutex (#624). fn receives the current
// (seq, totalBytes, truncated) and returns the updated values, which
// are written back before the lock releases — so concurrent
// POST /v1/tasks/{id}/logs calls for the same task never race each
// other's seq assignment or truncation decision. Returns false (fn not
// called) if taskID has no entry — same does-not-reap contract as
// LoadWithTokenID: a resolve racing the reaper must not surface as an
// ambiguous "not found" to the caller.
func (am *AckMap) WithLogState(
	taskID string,
	fn func(seq uint64, totalBytes int64, truncated bool) (
		newSeq uint64, newTotalBytes int64, newTruncated bool,
	),
) bool {
	if am == nil {
		panic("AckMap.WithLogState: nil receiver")
	}
	if taskID == "" {
		panic("AckMap.WithLogState: taskID must not be empty")
	}
	if fn == nil {
		panic("AckMap.WithLogState: fn must not be nil")
	}
	am.mu.Lock()
	defer am.mu.Unlock()
	entry, ok := am.entries[taskID]
	if !ok {
		return false
	}
	entry.logSeq, entry.logTotalBytes, entry.logTruncated = fn(
		entry.logSeq, entry.logTotalBytes, entry.logTruncated,
	)
	am.entries[taskID] = entry
	return true
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

// MarkResolved records that taskID was just resolved (complete/fail/
// pause/continue) — called alongside Delete, from the same resolve
// call, so a POST /v1/tasks/{id}/logs that arrives afterwards can be
// told 409 (already resolved) instead of 404 (never existed). Bounded
// and TTL'd the same way Store bounds entries: evicts the oldest
// resolvedTasks entry at resolvedTaskMax, and WasResolved expires an
// entry past resolvedTaskTTL.
func (am *AckMap) MarkResolved(taskID string) {
	if am == nil {
		panic("AckMap.MarkResolved: nil receiver")
	}
	if taskID == "" {
		panic("AckMap.MarkResolved: taskID must not be empty")
	}
	am.mu.Lock()
	defer am.mu.Unlock()
	if len(am.resolvedTasks) >= resolvedTaskMax {
		var oldestID string
		var oldestAt time.Time
		for id, at := range am.resolvedTasks {
			if oldestID == "" || at.Before(oldestAt) {
				oldestID, oldestAt = id, at
			}
		}
		delete(am.resolvedTasks, oldestID)
	}
	am.resolvedTasks[taskID] = am.now()
}

// WasResolved reports whether taskID was resolved within the last
// resolvedTaskTTL. Expires (and removes) a stale entry on read rather
// than waiting for a separate sweep — resolvedTasks is only ever
// queried from handleLogs's rejection path, so a lazy expiry costs
// nothing extra there.
func (am *AckMap) WasResolved(taskID string) bool {
	if am == nil {
		panic("AckMap.WasResolved: nil receiver")
	}
	if taskID == "" {
		panic("AckMap.WasResolved: taskID must not be empty")
	}
	am.mu.Lock()
	defer am.mu.Unlock()
	at, ok := am.resolvedTasks[taskID]
	if !ok {
		return false
	}
	if am.now().Sub(at) >= resolvedTaskTTL {
		delete(am.resolvedTasks, taskID)
		return false
	}
	return true
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
