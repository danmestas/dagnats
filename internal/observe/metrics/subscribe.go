package metrics

import (
	"sync"
)

// SubscriberBufferSize is the per-channel queue depth. 64 absorbs a
// burst of metric updates between SSE flushes without backpressuring
// the ingest loop. Dropping is fine — the dashboard re-reads the
// snapshot on every patch, so a missed event still lands the latest
// value at the next tick.
const SubscriberBufferSize = 64

// Subscribe returns a receive-only channel that emits one Update per
// accepted ingest matching the filter. Empty filter == match all
// metrics. Returns the channel + a cancel function the caller must
// invoke on disconnect.
//
// When the aggregator is full (MaxSubscribers in use) or closed,
// Subscribe returns an already-closed channel and a no-op cancel so
// callers can range/select without special-casing.
func (a *Aggregator) Subscribe(
	filter string,
) (<-chan Update, func()) {
	if a == nil {
		panic("Aggregator.Subscribe: a is nil")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closedFlag {
		ch := make(chan Update)
		close(ch)
		return ch, func() {}
	}
	if len(a.subs) >= MaxSubscribers {
		a.logger.Warn("metrics: subscriber cap reached, refusing",
			"max", MaxSubscribers)
		ch := make(chan Update)
		close(ch)
		return ch, func() {}
	}
	sub := &subscriber{
		ch:     make(chan Update, SubscriberBufferSize),
		filter: filter,
	}
	a.subs = append(a.subs, sub)
	cancel := a.makeCancel(sub)
	return sub.ch, cancel
}

// SubscriberCount returns how many subscribers are currently live.
// Used by tests; production callers don't need it but exposing it
// here keeps the internal state out of test packages.
func (a *Aggregator) SubscriberCount() int {
	if a == nil {
		panic("Aggregator.SubscriberCount: a is nil")
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.subs)
}

// makeCancel returns an idempotent unsubscribe closure. The sync.Once
// inside the subscriber guards repeated cancels from double-closing.
func (a *Aggregator) makeCancel(target *subscriber) func() {
	var local sync.Once
	return func() {
		local.Do(func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			for i, s := range a.subs {
				if s == target {
					a.subs = append(a.subs[:i], a.subs[i+1:]...)
					target.once.Do(func() { close(target.ch) })
					return
				}
			}
		})
	}
}
