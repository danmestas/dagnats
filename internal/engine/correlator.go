// engine/correlator.go
// Event correlator watches the EVENTS stream and matches incoming events
// against waiters stored in the event_waiters KV bucket. Uses an in-memory
// index populated by KV watch for O(1) lookup by event type. On match,
// publishes step.wait.matched to the history stream and deletes the KV entry.
package engine

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// maxWaitersPerEventType caps the in-memory index to prevent unbounded growth.
const maxWaitersPerEventType = 10000

// EventWaiter represents a registered wait-for-event entry.
type EventWaiter struct {
	RunID     string            `json:"run_id"`
	StepID    string            `json:"step_id"`
	EventType string            `json:"event_type"`
	Match     dag.ResolvedMatch `json:"match"`
}

// Correlator watches the EVENTS stream and matches incoming events
// against waiters stored in the event_waiters KV bucket.
type Correlator struct {
	nc       *nats.Conn
	js       nats.JetStreamContext
	waiterKV nats.KeyValue

	mu      sync.RWMutex
	waiters map[string][]EventWaiter // eventType -> []EventWaiter

	kvWatch  nats.KeyWatcher
	eventSub *nats.Subscription
}

// NewCorrelator creates a Correlator bound to the given connection.
// Panics on nil nc or js — these are programmer errors.
func NewCorrelator(
	nc *nats.Conn, jsLegacy nats.JetStreamContext,
) *Correlator {
	if nc == nil {
		panic("NewCorrelator: nc must not be nil")
	}
	if jsLegacy == nil {
		panic("NewCorrelator: jsLegacy must not be nil")
	}
	kv, err := jsLegacy.KeyValue("event_waiters")
	if err != nil {
		panic(
			"NewCorrelator: event_waiters bucket not found: " +
				err.Error(),
		)
	}
	return &Correlator{
		nc:       nc,
		js:       jsLegacy,
		waiterKV: kv,
		waiters:  make(map[string][]EventWaiter),
	}
}

// Start begins KV watch on event_waiters and subscribes to the
// EVENTS stream to match incoming events against waiters.
// Panics if already started.
func (c *Correlator) Start() error {
	if c.kvWatch != nil {
		panic("Correlator.Start: already started")
	}
	if c.waiterKV == nil {
		panic("Correlator.Start: waiterKV must not be nil")
	}
	watcher, err := c.waiterKV.WatchAll()
	if err != nil {
		return fmt.Errorf("watch event_waiters: %w", err)
	}
	c.kvWatch = watcher
	go c.processKVUpdates()

	sub, err := c.js.Subscribe(
		"event.>",
		c.handleEvent,
		nats.Durable("event-correlator"),
		nats.ManualAck(),
	)
	if err != nil {
		watcher.Stop()
		c.kvWatch = nil
		return fmt.Errorf("subscribe event.>: %w", err)
	}
	c.eventSub = sub
	return nil
}

// Stop stops the KV watch and event subscription.
// Safe to call multiple times.
func (c *Correlator) Stop() {
	if c.eventSub != nil {
		c.eventSub.Unsubscribe()
		c.eventSub = nil
	}
	if c.kvWatch != nil {
		c.kvWatch.Stop()
		c.kvWatch = nil
	}
}

// AddWaiter writes a waiter entry to the event_waiters KV bucket.
// Key format: {eventType}.{runID}.{stepID}. Bounded at
// maxWaitersPerEventType per event type.
func (c *Correlator) AddWaiter(w EventWaiter) error {
	if w.RunID == "" {
		panic("Correlator.AddWaiter: RunID must not be empty")
	}
	if w.StepID == "" {
		panic("Correlator.AddWaiter: StepID must not be empty")
	}
	if w.EventType == "" {
		panic("Correlator.AddWaiter: EventType must not be empty")
	}

	c.mu.RLock()
	count := len(c.waiters[w.EventType])
	c.mu.RUnlock()
	if count >= maxWaitersPerEventType {
		return fmt.Errorf(
			"event type %q has %d waiters (max %d)",
			w.EventType, count, maxWaitersPerEventType,
		)
	}

	data, err := json.Marshal(w)
	if err != nil {
		return fmt.Errorf("marshal EventWaiter: %w", err)
	}
	key := fmt.Sprintf(
		"%s.%s.%s", w.EventType, w.RunID, w.StepID,
	)
	_, err = c.waiterKV.Put(key, data)
	return err
}

// RemoveWaitersForRun deletes all KV entries for a given run and
// immediately removes them from the in-memory index. Used during
// cancellation cleanup. The in-memory removal is synchronous to
// avoid races between KV watch propagation and event matching.
func (c *Correlator) RemoveWaitersForRun(runID string) {
	if runID == "" {
		panic(
			"Correlator.RemoveWaitersForRun: runID must not be empty",
		)
	}
	if c.kvWatch == nil {
		return // correlator not started — nothing to clean up
	}

	c.mu.Lock()
	var keysToDelete []string
	for eventType, waiters := range c.waiters {
		filtered := make([]EventWaiter, 0, len(waiters))
		for _, w := range waiters {
			if w.RunID == runID {
				key := fmt.Sprintf(
					"%s.%s.%s", eventType, w.RunID, w.StepID,
				)
				keysToDelete = append(keysToDelete, key)
			} else {
				filtered = append(filtered, w)
			}
		}
		c.waiters[eventType] = filtered
	}
	c.mu.Unlock()

	for _, key := range keysToDelete {
		c.waiterKV.Delete(key)
	}
}

// processKVUpdates reads KV watch updates and maintains the
// in-memory waiter index. Runs in a goroutine started by Start.
func (c *Correlator) processKVUpdates() {
	if c.kvWatch == nil {
		panic("processKVUpdates: kvWatch must not be nil")
	}
	updates := c.kvWatch.Updates()
	if updates == nil {
		panic("processKVUpdates: updates channel must not be nil")
	}
	for entry := range updates {
		if entry == nil {
			continue // End of initial values marker
		}
		switch entry.Operation() {
		case nats.KeyValuePut:
			c.handleKVPut(entry)
		case nats.KeyValueDelete, nats.KeyValuePurge:
			c.handleKVDelete(entry)
		}
	}
}

// handleKVPut adds a waiter to the in-memory index from a KV put.
func (c *Correlator) handleKVPut(entry nats.KeyValueEntry) {
	if entry == nil {
		panic("handleKVPut: entry must not be nil")
	}
	if entry.Key() == "" {
		panic("handleKVPut: entry.Key() must not be empty")
	}
	var w EventWaiter
	if err := json.Unmarshal(entry.Value(), &w); err != nil {
		return
	}
	if w.EventType == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.waiters[w.EventType] = append(
		c.waiters[w.EventType], w,
	)
}

// handleKVDelete removes a waiter from the in-memory index.
// Key format: {eventType}.{runID}.{stepID}.
func (c *Correlator) handleKVDelete(entry nats.KeyValueEntry) {
	if entry == nil {
		panic("handleKVDelete: entry must not be nil")
	}
	key := entry.Key()
	if key == "" {
		panic("handleKVDelete: entry.Key() must not be empty")
	}
	parts := strings.SplitN(key, ".", 3)
	if len(parts) < 3 {
		return
	}
	eventType := parts[0]
	runID := parts[1]
	stepID := parts[2]

	c.mu.Lock()
	defer c.mu.Unlock()
	waiters := c.waiters[eventType]
	for i, w := range waiters {
		if w.RunID == runID && w.StepID == stepID {
			c.waiters[eventType] = append(
				waiters[:i], waiters[i+1:]...,
			)
			break
		}
	}
}

// handleEvent processes an event from the EVENTS stream.
// Extracts event type from the subject, looks up waiters, and
// evaluates matches.
func (c *Correlator) handleEvent(msg *nats.Msg) {
	if msg == nil {
		panic("Correlator.handleEvent: msg must not be nil")
	}
	if len(msg.Data) == 0 {
		panic("Correlator.handleEvent: msg.Data must not be empty")
	}
	eventType := extractEventType(msg.Subject)
	if eventType == "" {
		msg.Ack()
		return
	}
	c.mu.RLock()
	waiters := make([]EventWaiter, len(c.waiters[eventType]))
	copy(waiters, c.waiters[eventType])
	c.mu.RUnlock()

	for _, w := range waiters {
		c.evaluateWaiter(w, msg.Data)
	}
	msg.Ack()
}

// evaluateWaiter checks a single waiter against event data.
// On match, publishes step.wait.matched and deletes the KV entry.
func (c *Correlator) evaluateWaiter(
	w EventWaiter, eventData []byte,
) {
	if w.RunID == "" {
		panic("evaluateWaiter: RunID must not be empty")
	}
	if w.StepID == "" {
		panic("evaluateWaiter: StepID must not be empty")
	}
	matched, err := w.Match.Evaluate(eventData)
	if err != nil || !matched {
		return
	}
	c.publishMatchEvent(w, eventData)
	key := fmt.Sprintf(
		"%s.%s.%s", w.EventType, w.RunID, w.StepID,
	)
	c.waiterKV.Delete(key)
}

// publishMatchEvent publishes EventStepWaitMatched to the history
// stream for the given run. The event payload carries the matched
// event data so downstream steps can use it.
func (c *Correlator) publishMatchEvent(
	w EventWaiter, eventData []byte,
) {
	if w.RunID == "" {
		panic("publishMatchEvent: RunID must not be empty")
	}
	if w.StepID == "" {
		panic("publishMatchEvent: StepID must not be empty")
	}
	evt := protocol.NewStepEvent(
		protocol.EventStepWaitMatched,
		w.RunID, w.StepID, eventData,
	)
	data, err := evt.Marshal()
	if err != nil {
		return
	}
	c.js.Publish(
		evt.NATSSubject(), data,
		nats.MsgId(evt.NATSMsgID()),
	)
}

// extractEventType parses the event type from a NATS subject.
// Subject format: event.{type} (e.g., event.payment.completed).
// Returns the type portion after the "event." prefix.
func extractEventType(subject string) string {
	if subject == "" {
		panic("extractEventType: subject must not be empty")
	}
	if !strings.HasPrefix(subject, "event.") {
		panic("extractEventType: subject must start with 'event.'")
	}
	return strings.TrimPrefix(subject, "event.")
}
