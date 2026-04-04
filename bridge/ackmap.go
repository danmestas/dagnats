package bridge

import (
	"sync"

	"github.com/nats-io/nats.go"
)

// AckMap tracks in-flight tasks for HTTP workers. Maps task_id
// ({runID}.{stepID}) to the NATS message so the bridge can ack/nak
// on behalf of the HTTP client when it resolves the task.
//
// Thread-safe: multiple poll/resolve handlers run concurrently.
// Bounded by the number of in-flight tasks across all HTTP workers.
type AckMap struct {
	m     sync.Map
	count int64
	mu    sync.Mutex
}

// NewAckMap creates an empty AckMap ready for use.
func NewAckMap() *AckMap {
	return &AckMap{}
}

// Store saves a NATS message keyed by task ID.
// Panics on empty taskID or nil msg — both are programmer errors.
func (am *AckMap) Store(taskID string, msg *nats.Msg) {
	if taskID == "" {
		panic("AckMap.Store: taskID must not be empty")
	}
	if msg == nil {
		panic("AckMap.Store: msg must not be nil")
	}
	am.m.Store(taskID, msg)
	am.mu.Lock()
	am.count++
	am.mu.Unlock()
}

// Load retrieves the NATS message for the given task ID.
// Returns (nil, false) if not found.
func (am *AckMap) Load(taskID string) (*nats.Msg, bool) {
	if am == nil {
		panic("AckMap.Load: nil receiver")
	}
	if taskID == "" {
		panic("AckMap.Load: taskID must not be empty")
	}
	v, ok := am.m.Load(taskID)
	if !ok {
		return nil, false
	}
	return v.(*nats.Msg), true
}

// Delete removes a task from the map after resolution.
func (am *AckMap) Delete(taskID string) {
	if am == nil {
		panic("AckMap.Delete: nil receiver")
	}
	if taskID == "" {
		panic("AckMap.Delete: taskID must not be empty")
	}
	if _, loaded := am.m.LoadAndDelete(taskID); loaded {
		am.mu.Lock()
		am.count--
		am.mu.Unlock()
	}
}

// Count returns the number of in-flight tasks.
func (am *AckMap) Count() int64 {
	am.mu.Lock()
	defer am.mu.Unlock()
	return am.count
}
