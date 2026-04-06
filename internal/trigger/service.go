package trigger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"log/slog"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const maxActiveTriggers = 500

// TriggerService coordinates all trigger types. It loads definitions
// from the triggers KV bucket on startup and watches for live changes.
type TriggerService struct {
	nc        *nats.Conn
	js        jetstream.JetStream
	triggerKV jetstream.KeyValue
	scheduler *Scheduler
	subjects  map[string]*SubjectTrigger
	webhooks  map[string]*WebhookHandler
	ctx       context.Context
	cancel    context.CancelFunc
	watcher   jetstream.KeyWatcher
	mu        sync.RWMutex
}

// NewTriggerService creates the service. KV buckets must exist.
// Panics if nc is nil or nc is not connected (programmer error).
func NewTriggerService(
	nc *nats.Conn,
) (*TriggerService, error) {
	if nc == nil {
		panic("NewTriggerService: nc must not be nil")
	}
	if !nc.IsConnected() {
		panic("NewTriggerService: nc must be connected")
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream.New: %w", err)
	}

	kvCtx, kvCancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer kvCancel()
	triggerKV, err := js.KeyValue(kvCtx, "triggers")
	if err != nil {
		return nil, fmt.Errorf("triggers KV bucket: %w", err)
	}

	scheduler, err := NewScheduler(nc)
	if err != nil {
		return nil, fmt.Errorf("NewScheduler: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &TriggerService{
		nc:        nc,
		js:        js,
		triggerKV: triggerKV,
		scheduler: scheduler,
		subjects:  make(map[string]*SubjectTrigger),
		webhooks:  make(map[string]*WebhookHandler),
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

// Start loads triggers from KV, starts all handlers, and begins
// watching for changes. Panics if prerequisites are not initialized.
func (ts *TriggerService) Start() error {
	if ts.scheduler == nil {
		panic("Start: scheduler must not be nil")
	}
	if ts.ctx == nil {
		panic("Start: ctx must not be nil")
	}

	if err := ts.loadAllTriggers(); err != nil {
		return fmt.Errorf("loadAllTriggers: %w", err)
	}

	// Start scheduler in background
	go ts.scheduler.Start(ts.ctx, 30*time.Second)

	if err := ts.startKVWatcher(); err != nil {
		return fmt.Errorf("startKVWatcher: %w", err)
	}

	return nil
}

// Stop terminates all triggers and the KV watcher.
// Panics if called before initialization completes.
func (ts *TriggerService) Stop() {
	if ts.cancel == nil {
		panic("Stop: cancel must not be nil")
	}
	if ts.subjects == nil {
		panic("Stop: subjects map must not be nil")
	}

	ts.cancel()

	ts.mu.Lock()
	for _, st := range ts.subjects {
		_ = st.Close()
	}
	ts.mu.Unlock()

	if ts.watcher != nil {
		ts.watcher.Stop()
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

	return len(ts.subjects) + len(ts.webhooks) + ts.scheduler.Count()
}

func (ts *TriggerService) loadAllTriggers() error {
	if ts.triggerKV == nil {
		panic("loadAllTriggers: triggerKV must not be nil")
	}
	if ts.scheduler == nil {
		panic("loadAllTriggers: scheduler must not be nil")
	}

	keysCtx, keysCancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer keysCancel()
	keys, err := ts.triggerKV.Keys(keysCtx)
	if err != nil && !errors.Is(err, jetstream.ErrNoKeysFound) {
		return fmt.Errorf("keys: %w", err)
	}
	if len(keys) == 0 {
		return nil
	}

	// Cap to maxActiveTriggers
	if len(keys) > maxActiveTriggers {
		keys = keys[:maxActiveTriggers]
	}

	entries, err := natsutil.ParallelGetJS(
		ts.triggerKV, keys, natsutil.DefaultParallelism,
	)
	if err != nil {
		return fmt.Errorf("ParallelGetJS: %w", err)
	}

	for _, entry := range entries {
		var def TriggerDef
		if err := json.Unmarshal(entry.Value(), &def); err != nil {
			slog.Error("unmarshal trigger def on load",
				"error", err,
				"key", entry.Key(),
			)
			continue
		}
		if err := ts.addTrigger(def); err != nil {
			slog.Error("add trigger on load",
				"error", err,
				"trigger_id", def.ID,
			)
			continue
		}
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
	if ts.ctx == nil {
		panic("startKVWatcher: ctx must not be nil")
	}

	watcher, err := ts.triggerKV.WatchAll(ts.ctx)
	if err != nil {
		return fmt.Errorf("WatchAll: %w", err)
	}

	ts.watcher = watcher

	go func() {
		updates := watcher.Updates()
		for {
			select {
			case <-ts.ctx.Done():
				return
			case entry, ok := <-updates:
				if !ok {
					return
				}
				if entry != nil {
					ts.handleKVUpdate(entry)
				}
			}
		}
	}()

	return nil
}

// handleKVUpdate dispatches a single KV watcher entry: removes
// deleted triggers, replaces updated ones within the active limit.
func (ts *TriggerService) handleKVUpdate(
	entry jetstream.KeyValueEntry,
) {
	if entry == nil {
		panic("handleKVUpdate: entry must not be nil")
	}

	if entry.Operation() == jetstream.KeyValueDelete ||
		entry.Operation() == jetstream.KeyValuePurge {
		_ = ts.removeTrigger(entry.Key())
		return
	}

	var def TriggerDef
	if err := json.Unmarshal(entry.Value(), &def); err != nil {
		slog.Error("unmarshal trigger def from KV watch",
			"error", err,
		)
		return
	}

	// Remove old version and add new
	_ = ts.removeTrigger(def.ID)

	// Respect max triggers limit
	if ts.TriggerCount() < maxActiveTriggers {
		_ = ts.addTrigger(def)
	}
}

// timeNow is a testing seam for the current time.
var timeNow = time.Now
