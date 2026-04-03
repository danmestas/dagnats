package trigger

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const maxActiveTriggers = 500

// TriggerService coordinates all trigger types. It loads definitions
// from the triggers KV bucket on startup and watches for live changes.
type TriggerService struct {
	nc        *nats.Conn
	js        nats.JetStreamContext
	triggerKV nats.KeyValue
	scheduler *Scheduler
	subjects  map[string]*SubjectTrigger
	webhooks  map[string]*WebhookHandler
	stopChan  chan struct{}
	watcher   nats.KeyWatcher
	mu        sync.RWMutex
}

// NewTriggerService creates the service. KV buckets must exist.
// Panics if nc is nil or already closed (programmer error).
func NewTriggerService(nc *nats.Conn) (*TriggerService, error) {
	if nc == nil {
		panic("NewTriggerService: nc must not be nil")
	}
	if !nc.IsConnected() {
		panic("NewTriggerService: nc must be connected")
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("JetStream: %w", err)
	}

	triggerKV, err := js.KeyValue("triggers")
	if err != nil {
		return nil, fmt.Errorf("triggers KV bucket: %w", err)
	}

	scheduler, err := NewScheduler(nc)
	if err != nil {
		return nil, fmt.Errorf("NewScheduler: %w", err)
	}

	return &TriggerService{
		nc:        nc,
		js:        js,
		triggerKV: triggerKV,
		scheduler: scheduler,
		subjects:  make(map[string]*SubjectTrigger),
		webhooks:  make(map[string]*WebhookHandler),
		stopChan:  make(chan struct{}),
	}, nil
}

// Start loads triggers from KV, starts all handlers, and begins
// watching for changes. Panics if prerequisites are not initialized.
func (ts *TriggerService) Start() error {
	if ts.scheduler == nil {
		panic("Start: scheduler must not be nil")
	}
	if ts.stopChan == nil {
		panic("Start: stopChan must not be nil")
	}

	if err := ts.loadAllTriggers(); err != nil {
		return fmt.Errorf("loadAllTriggers: %w", err)
	}

	// Start scheduler in background
	go ts.scheduler.Start(30*time.Second, ts.stopChan)

	if err := ts.startKVWatcher(); err != nil {
		return fmt.Errorf("startKVWatcher: %w", err)
	}

	return nil
}

// Stop terminates all triggers and the KV watcher.
// Panics if called before initialization completes.
func (ts *TriggerService) Stop() {
	if ts.stopChan == nil {
		panic("Stop: stopChan must not be nil")
	}
	if ts.subjects == nil {
		panic("Stop: subjects map must not be nil")
	}

	close(ts.stopChan)

	ts.mu.Lock()
	for _, st := range ts.subjects {
		_ = st.Close()
	}
	ts.mu.Unlock()

	if ts.watcher != nil {
		_ = ts.watcher.Stop()
	}
}

// TickNow forces an immediate scheduler tick (for testing).
func (ts *TriggerService) TickNow() {
	if ts.scheduler == nil {
		panic("TickNow: scheduler is nil")
	}
	if ts.scheduler.js == nil {
		panic("TickNow: scheduler JetStream context is nil")
	}
	ts.scheduler.Tick(timeNow())
}

// WebhookHandler returns a unified HTTP handler for all webhook triggers.
// Panics if service webhooks map is not initialized.
func (ts *TriggerService) WebhookHandler() http.Handler {
	if ts.webhooks == nil {
		panic("WebhookHandler: webhooks map must not be nil")
	}
	if ts.subjects == nil {
		panic("WebhookHandler: service not fully initialized")
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ts.mu.RLock()
		handler, ok := ts.webhooks[r.URL.Path]
		ts.mu.RUnlock()

		if !ok {
			http.NotFound(w, r)
			return
		}

		handler.ServeHTTP(w, r)
	})
}

// TriggerCount returns the current number of active triggers.
// Panics if service or scheduler are not initialized.
func (ts *TriggerService) TriggerCount() int {
	if ts.scheduler == nil {
		panic("TriggerCount: scheduler must not be nil")
	}
	if ts.subjects == nil {
		panic("TriggerCount: subjects map must not be nil")
	}

	ts.mu.RLock()
	defer ts.mu.RUnlock()

	// Count scheduler triggers separately
	cronCount := 0
	ts.scheduler.mu.RLock()
	cronCount = len(ts.scheduler.triggers)
	ts.scheduler.mu.RUnlock()

	return len(ts.subjects) + len(ts.webhooks) + cronCount
}

func (ts *TriggerService) loadAllTriggers() error {
	if ts.triggerKV == nil {
		panic("loadAllTriggers: triggerKV must not be nil")
	}
	if ts.scheduler == nil {
		panic("loadAllTriggers: scheduler must not be nil")
	}

	keys, err := ts.triggerKV.Keys()
	if err != nil && err != nats.ErrNoKeysFound {
		return fmt.Errorf("keys: %w", err)
	}

	loaded := 0
	for _, key := range keys {
		if loaded >= maxActiveTriggers {
			break
		}

		entry, err := ts.triggerKV.Get(key)
		if err != nil {
			continue
		}

		var def TriggerDef
		if err := json.Unmarshal(entry.Value(), &def); err != nil {
			continue
		}

		if err := ts.addTrigger(def); err != nil {
			continue
		}
		loaded++
	}

	return nil
}

func (ts *TriggerService) addTrigger(def TriggerDef) error {
	if ts.scheduler == nil {
		panic("addTrigger: scheduler must not be nil")
	}
	if ts.nc == nil {
		panic("addTrigger: nc must not be nil")
	}

	if err := Validate(def); err != nil {
		return fmt.Errorf("validate: %w", err)
	}

	if !def.Enabled {
		return nil
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()

	switch {
	case def.Cron != nil:
		return ts.scheduler.AddTrigger(def)
	case def.Subject != nil:
		trigger, err := NewSubjectTrigger(ts.nc, def)
		if err != nil {
			return fmt.Errorf("NewSubjectTrigger: %w", err)
		}
		ts.subjects[def.ID] = trigger
		return nil
	case def.Webhook != nil:
		handler := NewWebhookHandler(ts.nc, def)
		if def.Webhook.Path != "" {
			ts.webhooks[def.Webhook.Path] = handler
		}
		return nil
	}

	return fmt.Errorf("no trigger type specified")
}

func (ts *TriggerService) removeTrigger(id string) error {
	if id == "" {
		panic("removeTrigger: id must not be empty")
	}
	if ts.scheduler == nil {
		panic("removeTrigger: scheduler must not be nil")
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()

	_ = ts.scheduler.RemoveTrigger(id)

	if st, ok := ts.subjects[id]; ok {
		_ = st.Close()
		delete(ts.subjects, id)
	}

	// For webhooks, we need to find by ID since map is keyed by path
	for path, handler := range ts.webhooks {
		if handler.def.ID == id {
			delete(ts.webhooks, path)
			break
		}
	}

	return nil
}

func (ts *TriggerService) startKVWatcher() error {
	if ts.triggerKV == nil {
		panic("startKVWatcher: triggerKV must not be nil")
	}
	if ts.stopChan == nil {
		panic("startKVWatcher: stopChan must not be nil")
	}

	watcher, err := ts.triggerKV.WatchAll()
	if err != nil {
		return fmt.Errorf("WatchAll: %w", err)
	}

	ts.watcher = watcher

	go func() {
		for entry := range watcher.Updates() {
			if entry == nil {
				continue
			}

			if entry.Operation() == nats.KeyValueDelete {
				_ = ts.removeTrigger(entry.Key())
				continue
			}

			var def TriggerDef
			if err := json.Unmarshal(entry.Value(), &def); err != nil {
				continue
			}

			// Remove old version and add new
			_ = ts.removeTrigger(def.ID)

			// Respect max triggers limit
			if ts.TriggerCount() < maxActiveTriggers {
				_ = ts.addTrigger(def)
			}
		}
	}()

	return nil
}

// timeNow is a testing seam for the current time.
var timeNow = time.Now
