package console

import (
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/danmestas/dagnats/internal/console/events"
)

// event_bus_binding.go owns the integration between the events sub-
// package's pure pub/sub and the console mutation handlers / SSE
// writers. Keeping the binding shim here means the events package
// stays dependency-free (no console types) and the console package
// has one place to look when wiring new event sources.

// eventBusBinding wraps an events.Bus with a Logger so the console
// can publish without re-resolving slog at every call site.
type eventBusBinding struct {
	bus    *events.Bus
	logger *slog.Logger
}

// newEventBusBinding constructs a binding around a fresh Bus. logger
// is nil-safe — the bus falls back to slog.Default in that case.
func newEventBusBinding(logger *slog.Logger) *eventBusBinding {
	return &eventBusBinding{
		bus:    events.NewBus(logger),
		logger: logger,
	}
}

// publish emits one event to the bus. Returns the subscriber count
// the event reached.
func (b *eventBusBinding) publish(evt events.Event) int {
	if b == nil || b.bus == nil {
		return 0
	}
	return b.bus.Publish(evt)
}

// subscribe is the read-side. Returns a receive-only channel and a
// cancel function the caller must invoke on disconnect.
func (b *eventBusBinding) subscribe(
	topic events.Topic,
) (<-chan events.Event, func()) {
	if b == nil || b.bus == nil {
		ch := make(chan events.Event)
		close(ch)
		return ch, func() {}
	}
	return b.bus.Subscribe(topic)
}

// EnableSoftDiscard turns on the DLQ soft-discard tombstone store for
// the given Config. window is the undo grace period (5s recommended);
// discard is the function called when a tombstone's window closes
// without an undo — typically cfg.Data.DiscardDeadLetter wrapped to
// add a timeout. Idempotent — calling twice replaces the prior store.
//
// Also lazy-binds the event bus when missing; the soft-discard flow
// publishes "row.remove" events that SSE handlers patch out, and the
// bus is the right scaffolding for that pipeline.
func EnableSoftDiscard(
	cfg *Config, window time.Duration,
	discard func(seq uint64),
) {
	if cfg == nil {
		panic("EnableSoftDiscard: cfg is nil")
	}
	if window <= 0 {
		panic("EnableSoftDiscard: window must be positive")
	}
	if discard == nil {
		panic("EnableSoftDiscard: discard fn is nil")
	}
	if cfg.bus == nil {
		cfg.bus = newEventBusBinding(cfg.Logger)
	}
	// Wrap the caller's permanent-removal fn so an expired tombstone
	// also emits the row.remove event. The SSE pipeline is a console
	// concern, so the caller (server.go) only has to supply "delete the
	// entry for real" — not know to patch every open list. Without this
	// the swept entry is gone server-side but lingers in the operator's
	// DLQ list until a manual refresh ("after discard nothing deletes").
	bus := cfg.bus
	onExpire := func(seq uint64) {
		discard(seq)
		bus.publish(busEventDLQRemove(strconv.FormatUint(seq, 10)))
	}
	cfg.tomb = newDLQTombstoneStore(window, onExpire)
	cfg.DLQSoftDiscard = true
}

// AttachBus is the explicit binding entry-point for tests + production
// wiring that want to share an event bus across handlers. Production
// callers usually let EnableSoftDiscard lazy-create the bus; tests
// that exercise SSE without soft-discard call this directly.
func AttachBus(cfg *Config) {
	if cfg == nil {
		panic("AttachBus: cfg is nil")
	}
	if cfg.bus == nil {
		cfg.bus = newEventBusBinding(cfg.Logger)
	}
}

// RunTombstoneSweeper loops calling tomb.SweepOnce on a small ticker
// until stop closes. Production wiring (server.go) starts this in a
// goroutine so soft-discard entries past their window are permanently
// removed.
func RunTombstoneSweeper(
	cfg *Config, interval time.Duration, stop <-chan struct{},
) {
	if cfg == nil {
		panic("RunTombstoneSweeper: cfg is nil")
	}
	if interval <= 0 {
		panic("RunTombstoneSweeper: interval must be positive")
	}
	if stop == nil {
		panic("RunTombstoneSweeper: stop is nil")
	}
	tomb := cfg.tombstones()
	if tomb == nil {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	const maxTicks = 1_000_000_000
	for i := 0; i < maxTicks; i++ {
		select {
		case <-stop:
			return
		case <-ticker.C:
			tomb.SweepOnce()
		}
	}
}

// busEventDLQRemove constructs the canonical row.remove event for one
// DLQ sequence. Pulled out so call sites stay one-liner.
func busEventDLQRemove(seqStr string) events.Event {
	return events.Event{
		Topic: events.TopicDLQ,
		Op:    events.OpRowRemove,
		Key:   seqStr,
	}
}

// busEventRunCompleted constructs the canonical event for a workflow
// run completion. The dashboard SSE handler reacts by patching the
// failed-1h / in-flight / success-rate / p99 tiles + recent-failures
// panel.
func busEventRunCompleted(runID string) events.Event {
	if runID == "" {
		panic("busEventRunCompleted: runID is empty")
	}
	return events.Event{
		Topic: events.TopicRun,
		Op:    events.OpRowReplace,
		Key:   runID,
	}
}

// busEventAuditRecorded constructs the canonical event for a new audit
// entry. The dashboard SSE handler reacts by patching the recent-
// actions panel.
func busEventAuditRecorded(key string) events.Event {
	if key == "" {
		panic("busEventAuditRecorded: key is empty")
	}
	return events.Event{
		Topic: events.TopicAudit,
		Op:    events.OpRowAdd,
		Key:   key,
	}
}

// _ ensures the sync import isn't dropped if the file shrinks later;
// we leave the import for future synchronization scaffolding (test
// helpers below the binding may need it).
var _ sync.Once
