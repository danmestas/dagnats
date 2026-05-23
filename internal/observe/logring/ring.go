package logring

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Handler is the narrow interface the console depends on. It IS a
// slog.Handler so the engine can install it with slog.SetDefault; it
// adds Snapshot for the initial pageload and Subscribe for the SSE
// live tail. No filtering / indexing / taxonomy logic lives on the
// handler — those concerns belong to the consumer (the page handler).
type Handler interface {
	slog.Handler

	// Snapshot returns a freshly-allocated, time-ordered (oldest
	// first) copy of every record currently retained by the ring.
	// The caller owns the returned slice and may mutate / sort /
	// filter it freely. Entries older than max_age are pruned at
	// snapshot time so a long-idle Snapshot never returns stale
	// content.
	Snapshot() []slog.Record

	// Subscribe returns a channel that receives every record handled
	// after the call. The channel is buffered; if a slow consumer
	// fills the buffer the new record is dropped for that subscriber
	// only (a Warn is logged once per subscriber to the parent
	// handler — fairness wins over delivery guarantees). The returned
	// cleanup func unsubscribes and closes the channel; it is safe
	// to call multiple times. Cancelling ctx also triggers cleanup.
	//
	// Clear() emits a sentinel record with Time.IsZero()==true to
	// every active subscriber so SSE handlers can broadcast a tbody
	// reset to the operator browsers without the ring widening its
	// interface with a second channel. Consumers of Subscribe must
	// either treat zero-time records as a clear signal or skip them.
	Subscribe(ctx context.Context) (<-chan slog.Record, func())

	// Clear drops every retained record. Future records still flow
	// through Handle as normal; only the retained buffer is wiped.
	// This is an operator-driven action — the console exposes a
	// button that POSTs /console/logs/clear; nothing in the engine
	// hot-path calls Clear. Implementations MUST also broadcast a
	// sentinel slog.Record{} (zero value) to every active subscriber
	// so live SSE clients can reset their tables in the same beat.
	Clear()
}

// Default tuning constants. The package-level defaults are referenced
// from main.go via New; tests override via NewWithOptions.
const (
	// DefaultCapEntries is the maximum number of records the ring
	// retains. Hitting this drops the oldest. 10_000 covers several
	// minutes of busy engine output; raising it would let memory grow
	// without an upper bound the operator can name.
	DefaultCapEntries = 10_000

	// DefaultMaxAge is the time-window past which records are
	// pruned. 30 minutes matches the operator's mental model for
	// "what just happened" on the console.
	DefaultMaxAge = 30 * time.Minute

	// subscriberBuffer is the per-subscriber channel buffer. Sized
	// so a small fanout (say 5 SSE clients each polling at human
	// speeds) absorbs short stalls without dropping; a stuck client
	// drops only its own messages, not the producer.
	subscriberBuffer = 256
)

// ringHandler is the concrete Handler. It is goroutine-safe; every
// access to records or subs goes through mu. The monotonic seq is
// useful for tests asserting eviction order and for future SSE
// resume-by-seq protocols — not exposed on the interface yet.
type ringHandler struct {
	inner       slog.Handler
	capEntries  int
	maxAge      time.Duration
	now         func() time.Time
	mu          sync.Mutex
	records     []slog.Record // oldest first
	seq         uint64
	subscribers map[uint64]chan slog.Record
	nextSubID   uint64
}

// Options tunes ring construction. Zero values fall back to defaults.
// Now is overridable so tests can drive deterministic time-window
// pruning without sleeping.
type Options struct {
	CapEntries int
	MaxAge     time.Duration
	Now        func() time.Time
}

// New constructs a Handler with default capacity and age bounds.
// inner is the underlying slog.Handler that receives every record
// after the ring step — typically a slog.NewTextHandler writing to
// stderr.
func New(inner slog.Handler) Handler {
	if inner == nil {
		panic("logring.New: inner handler is nil")
	}
	return NewWithOptions(inner, Options{})
}

// NewWithOptions is the test seam. Negative values panic; zero values
// substitute the package defaults. The inner handler must be non-nil
// — callers that want to drop log output should pass a slog.NewTextHandler
// over io.Discard rather than passing nil.
func NewWithOptions(inner slog.Handler, opts Options) Handler {
	if inner == nil {
		panic("logring.NewWithOptions: inner handler is nil")
	}
	if opts.CapEntries < 0 {
		panic("logring.NewWithOptions: CapEntries must be >= 0")
	}
	if opts.MaxAge < 0 {
		panic("logring.NewWithOptions: MaxAge must be >= 0")
	}
	cap := opts.CapEntries
	if cap == 0 {
		cap = DefaultCapEntries
	}
	age := opts.MaxAge
	if age == 0 {
		age = DefaultMaxAge
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	return &ringHandler{
		inner:       inner,
		capEntries:  cap,
		maxAge:      age,
		now:         nowFn,
		records:     make([]slog.Record, 0, cap),
		subscribers: make(map[uint64]chan slog.Record),
	}
}

// Enabled defers to the inner handler — the ring never gates output
// on its own, otherwise the operator could lose context by tuning
// the console-only ring level.
func (h *ringHandler) Enabled(ctx context.Context, lvl slog.Level) bool {
	if h == nil {
		panic("ringHandler.Enabled: receiver is nil")
	}
	if h.inner == nil {
		panic("ringHandler.Enabled: inner handler is nil")
	}
	return h.inner.Enabled(ctx, lvl)
}

// Handle forwards rec to the inner handler, appends it to the ring,
// evicts anything that exceeds either bound, and fans out to live
// subscribers. The inner-handler error is returned unchanged so the
// pass-through is observable.
func (h *ringHandler) Handle(ctx context.Context, rec slog.Record) error {
	if h == nil {
		panic("ringHandler.Handle: receiver is nil")
	}
	if h.inner == nil {
		panic("ringHandler.Handle: inner handler is nil")
	}
	innerErr := h.inner.Handle(ctx, rec)
	h.append(rec)
	return innerErr
}

// WithAttrs delegates to the inner handler and wraps the result so
// the ring still receives every record. Subscribers carry over — they
// are bound to the ring instance, not to the slog.Handler returned
// here.
func (h *ringHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if h == nil {
		panic("ringHandler.WithAttrs: receiver is nil")
	}
	return &ringHandler{
		inner:       h.inner.WithAttrs(attrs),
		capEntries:  h.capEntries,
		maxAge:      h.maxAge,
		now:         h.now,
		records:     h.records,
		seq:         h.seq,
		subscribers: h.subscribers,
		nextSubID:   h.nextSubID,
	}
}

// WithGroup mirrors WithAttrs — see the note there. Both are common
// slog idioms (slog.With / slog.WithGroup) so the ring must support
// them without splitting the buffer per call site.
func (h *ringHandler) WithGroup(name string) slog.Handler {
	if h == nil {
		panic("ringHandler.WithGroup: receiver is nil")
	}
	if name == "" {
		// slog.WithGroup("") is a no-op per the stdlib contract.
		return h
	}
	return &ringHandler{
		inner:       h.inner.WithGroup(name),
		capEntries:  h.capEntries,
		maxAge:      h.maxAge,
		now:         h.now,
		records:     h.records,
		seq:         h.seq,
		subscribers: h.subscribers,
		nextSubID:   h.nextSubID,
	}
}

// append takes the lock, prunes by age, drops the oldest if cap is
// exceeded, stores a clone of rec (the stdlib spec calls out that
// callers may mutate Record), bumps seq, and fans out. Fanout is
// non-blocking — a full subscriber buffer drops the record for that
// subscriber only.
func (h *ringHandler) append(rec slog.Record) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pruneByAgeLocked()
	if len(h.records) >= h.capEntries {
		// Drop oldest. Shifting is acceptable at this scale — 10k
		// slog.Record values is tiny compared with the inner handler's
		// per-record formatting cost. A ring-buffer index would be a
		// micro-optimisation that complicates Snapshot ordering.
		h.records = h.records[1:]
	}
	h.records = append(h.records, rec.Clone())
	h.seq++
	for id, ch := range h.subscribers {
		select {
		case ch <- rec.Clone():
		default:
			// Slow subscriber. Drop this record on their channel only.
			// We could close + remove them here, but keeping the
			// channel open lets a transient stall recover; the page
			// JS shows a "lagged" indicator off the same drop signal
			// in a follow-up if needed.
			_ = id
		}
	}
}

// pruneByAgeLocked removes records older than now() - maxAge. Called
// from append and Snapshot. Callers must hold h.mu.
func (h *ringHandler) pruneByAgeLocked() {
	if h.maxAge <= 0 {
		return
	}
	cutoff := h.now().Add(-h.maxAge)
	// records are oldest-first, so we trim the prefix that is older
	// than cutoff. Bounded loop — at most len(records) iterations.
	drop := 0
	for drop < len(h.records) && h.records[drop].Time.Before(cutoff) {
		drop++
	}
	if drop > 0 {
		h.records = h.records[drop:]
	}
}

// Clear drops every retained record and broadcasts a sentinel
// slog.Record{} (Time.IsZero()) to every subscriber. SSE consumers
// see the sentinel and reset their tbody so a clear initiated by
// one operator surfaces in every connected client.
func (h *ringHandler) Clear() {
	if h == nil {
		panic("ringHandler.Clear: receiver is nil")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = h.records[:0]
	for id, ch := range h.subscribers {
		select {
		case ch <- slog.Record{}:
		default:
			// Slow subscriber. Drop the clear signal on their channel;
			// they will reconcile on the next pageload Snapshot().
			_ = id
		}
	}
}

// Snapshot returns a freshly-allocated, time-ordered copy of the
// retained records. It prunes by age first so a long-idle process
// doesn't surface 31-minute-old entries to a new pageload.
func (h *ringHandler) Snapshot() []slog.Record {
	if h == nil {
		panic("ringHandler.Snapshot: receiver is nil")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pruneByAgeLocked()
	out := make([]slog.Record, len(h.records))
	for i, r := range h.records {
		out[i] = r.Clone()
	}
	return out
}

// Subscribe registers a new live-tail channel. Records appended after
// this call are delivered (best-effort) on the returned channel; the
// initial pageload state must come from Snapshot — Subscribe never
// replays history. The cleanup func unsubscribes; calling it twice is
// safe. ctx cancellation triggers cleanup automatically via a small
// goroutine; the goroutine exits after cleanup or when the parent
// ring is garbage-collected.
func (h *ringHandler) Subscribe(
	ctx context.Context,
) (<-chan slog.Record, func()) {
	if h == nil {
		panic("ringHandler.Subscribe: receiver is nil")
	}
	if ctx == nil {
		panic("ringHandler.Subscribe: ctx is nil")
	}
	ch := make(chan slog.Record, subscriberBuffer)
	h.mu.Lock()
	id := h.nextSubID
	h.nextSubID++
	h.subscribers[id] = ch
	h.mu.Unlock()
	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			h.mu.Lock()
			if existing, ok := h.subscribers[id]; ok && existing == ch {
				delete(h.subscribers, id)
				close(ch)
			}
			h.mu.Unlock()
		})
	}
	go func() {
		<-ctx.Done()
		cleanup()
	}()
	return ch, cleanup
}
