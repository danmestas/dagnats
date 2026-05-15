// Package events provides an in-process publish-subscribe bus for
// ephemeral UI signals that travel between console mutation handlers
// and live SSE streams. The KV-watch pattern PR 3+ established works
// for *durable* state changes (workflow runs, triggers, DLQ inserts).
// This bus carries the *non-durable* signals — "I just retried DLQ #42,
// patch that row out with the fade-out animation" — that don't justify
// a JetStream stream.
//
// Design:
//
//   - One Bus per console.Mount instance. Subscribers register a
//     channel and a topic filter; publishers fan out to every matching
//     subscriber with non-blocking sends.
//   - Bounded back-pressure: each subscriber buffer holds up to 256
//     events. If the buffer is full, the publisher drops the event
//     (slog.Warn) rather than block. The operator UI never wants to
//     wedge mutation handlers waiting on a slow SSE consumer.
//   - Topics are short string tags ("dlq", "trigger") — coarse-grained
//     so subscribers filter, not the bus. Adding a new topic is one
//     constant.
//
// This is intentionally not a generic event-streaming library. It's a
// thin coordination primitive that lives alongside the KV-watch SSE
// pattern and complements it.
package events

import (
	"log/slog"
	"sync"
)

// Topic is the coarse channel tag every event carries. Subscribers
// register with one topic; publishers stamp the event with one topic.
// The set is finite and lives here so callers can't typo a string.
type Topic string

const (
	// TopicDLQ — DLQ rows: retry, discard, undo-discard.
	TopicDLQ Topic = "dlq"
	// TopicTrigger — trigger rows: enable/disable.
	TopicTrigger Topic = "trigger"
)

// Op identifies the operation that produced the event. Combined with
// the topic, subscribers know whether to remove, replace, or insert.
type Op string

const (
	// OpRowRemove — the entity at Key is gone; SSE writers patch the
	// matching row out with the fade-out motion.
	OpRowRemove Op = "row.remove"
	// OpRowReplace — the entity at Key has a new state; SSE writers
	// re-render the row from the up-to-date snapshot.
	OpRowReplace Op = "row.replace"
	// OpRowAdd — the entity at Key is new; SSE writers prepend with
	// highlight.
	OpRowAdd Op = "row.add"
)

// Event is one signal travelling through the bus. Key identifies the
// affected entity (e.g. "42" for a DLQ sequence, "cron-nightly" for a
// trigger id). Data is optional structured context for the consumer.
type Event struct {
	Topic Topic
	Op    Op
	Key   string
	Data  map[string]any
}

// subscriber holds one registered channel + its topic filter. Capacity
// is fixed at construction so the publisher can drop-without-blocking.
type subscriber struct {
	topic Topic
	ch    chan Event
}

// Bus is the in-process pub/sub. Safe for concurrent use; the publisher
// path is lock-free wrt subscribers (we take a read lock then non-block
// send to each).
type Bus struct {
	mu      sync.RWMutex
	subs    []*subscriber
	closed  bool
	logger  *slog.Logger
	bufSize int
}

// DefaultBufferSize is the per-subscriber queue depth. 256 is large
// enough for a burst of operator actions; small enough that backlog
// detection (drop-oldest count) flags slow consumers within seconds.
const DefaultBufferSize = 256

// NewBus returns a Bus with the default per-subscriber buffer size.
// logger may be nil; callers that want drop diagnostics pass one.
func NewBus(logger *slog.Logger) *Bus {
	return &Bus{
		logger:  loggerOr(logger),
		bufSize: DefaultBufferSize,
	}
}

// loggerOr collapses nil into slog.Default so the bus never panics on
// drop diagnostics regardless of caller hygiene.
func loggerOr(l *slog.Logger) *slog.Logger {
	if l != nil {
		return l
	}
	return slog.Default()
}

// Subscribe returns a receive-only channel that delivers every event
// published with the given topic, along with a cancel function that
// unregisters the subscription and closes the channel. Cancel is safe
// to call more than once; subsequent calls are no-ops.
func (b *Bus) Subscribe(topic Topic) (<-chan Event, func()) {
	if topic == "" {
		panic("events.Subscribe: topic is empty")
	}
	if b == nil {
		panic("events.Subscribe: bus is nil")
	}
	sub := &subscriber{
		topic: topic,
		ch:    make(chan Event, b.bufSize),
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		close(sub.ch)
		return sub.ch, func() {}
	}
	b.subs = append(b.subs, sub)
	b.mu.Unlock()
	cancel := b.makeCancel(sub)
	return sub.ch, cancel
}

// makeCancel returns an idempotent unsubscribe function. Pulled out so
// the closure captures only what it needs.
func (b *Bus) makeCancel(target *subscriber) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			for i, s := range b.subs {
				if s == target {
					b.subs = append(b.subs[:i], b.subs[i+1:]...)
					close(target.ch)
					return
				}
			}
		})
	}
}

// Publish fans out evt to every subscriber whose topic matches. Sends
// are non-blocking; a full subscriber buffer drops the event with a
// slog.Warn so the operator can detect a wedged consumer. Publish
// returns the number of subscribers that received the event.
func (b *Bus) Publish(evt Event) int {
	if b == nil {
		panic("events.Publish: bus is nil")
	}
	if evt.Topic == "" {
		panic("events.Publish: topic is empty")
	}
	if evt.Op == "" {
		panic("events.Publish: op is empty")
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return 0
	}
	delivered := 0
	for _, sub := range b.subs {
		if sub.topic != evt.Topic {
			continue
		}
		if b.send(sub, evt) {
			delivered++
		}
	}
	return delivered
}

// send pushes evt onto sub.ch without blocking. Returns true when the
// send landed; false when the buffer was full and the event got
// dropped. Drop emits a slog.Warn so operators see saturation.
func (b *Bus) send(sub *subscriber, evt Event) bool {
	select {
	case sub.ch <- evt:
		return true
	default:
		b.logger.Warn("console events: subscriber buffer full, dropping",
			"topic", string(evt.Topic),
			"op", string(evt.Op),
			"key", evt.Key)
		return false
	}
}

// Close stops the bus, closes every subscriber channel, and rejects
// future Subscribe / Publish calls. Idempotent. Useful in tests and
// shutdown paths; production callers tied to the process lifetime can
// skip it.
func (b *Bus) Close() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for _, sub := range b.subs {
		close(sub.ch)
	}
	b.subs = nil
}

// SubscriberCount returns the live subscriber count. Useful for tests
// and for an in-process health probe.
func (b *Bus) SubscriberCount() int {
	if b == nil {
		return 0
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs)
}
