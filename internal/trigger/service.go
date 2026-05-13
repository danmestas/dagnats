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
//
// httpRoutes maps method:path → HTTPHandler. The composite key matches
// the routing key the HTTPRouter dispatches on; collision detection is
// the ADR-013 "refuse second registration that conflicts on
// (method, path)" rule. PR 3 may surface conflicts as registration-
// time errors; PR 2 logs and skips so an unrelated KV update cannot
// brick the trigger service.
type TriggerService struct {
	nc         *nats.Conn
	js         jetstream.JetStream
	triggerKV  jetstream.KeyValue
	scheduler  *Scheduler
	subjects   map[string]*SubjectTrigger
	webhooks   map[string]*WebhookHandler
	httpRoutes map[string]*HTTPHandler
	// revisions tracks the highest KV revision applied for each
	// trigger ID. The KV watcher (jetstream.WatchAll) opens with
	// DeliverLastPerSubject and replays existing keys on startup —
	// so the same revision that loadAllTriggers just applied is
	// re-delivered shortly after. Without dedup, handleKVUpdate
	// removes the active SubjectTrigger and re-creates it, briefly
	// unsubscribing on the server. Any inbound NATS message that
	// hits the gap is lost (#217 / #221 / #223).
	revisions map[string]uint64
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
		nc:         nc,
		js:         js,
		triggerKV:  triggerKV,
		scheduler:  scheduler,
		subjects:   make(map[string]*SubjectTrigger),
		webhooks:   make(map[string]*WebhookHandler),
		httpRoutes: make(map[string]*HTTPHandler),
		revisions:  make(map[string]uint64),
		ctx:        ctx,
		cancel:     cancel,
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

	return len(ts.subjects) +
		len(ts.webhooks) +
		len(ts.httpRoutes) +
		ts.scheduler.Count()
}

// HTTPRouter returns an http.Handler that dispatches requests to the
// HTTPHandler registered for the request's (method, path). Returns
// 404 when no handler matches the path and 405 when the path is
// known but the method does not match. Safe to call before Start —
// the returned handler always reads the current httpRoutes map
// under the service's RWMutex.
//
// Why a single http.Handler vs. one registration per route: the API
// server already owns a mux; layering a second one would create two
// places to look when a route 404s. Returning a flat handler keeps
// route ownership in the trigger service and the API mux's only job
// is to forward "/api/*" (or whatever prefix) to this handler.
func (ts *TriggerService) HTTPRouter() http.Handler {
	if ts.httpRoutes == nil {
		panic("HTTPRouter: httpRoutes map must not be nil")
	}
	return http.HandlerFunc(func(
		w http.ResponseWriter, r *http.Request,
	) {
		ts.serveHTTPRoute(w, r)
	})
}

// serveHTTPRoute is the inner dispatch — split out so the closure
// in HTTPRouter stays small and exempt from the 70-line rule.
func (ts *TriggerService) serveHTTPRoute(
	w http.ResponseWriter, r *http.Request,
) {
	ts.mu.RLock()
	pathMethods := make(map[string]*HTTPHandler, 4)
	for key, handler := range ts.httpRoutes {
		if handler.def.HTTP == nil {
			continue
		}
		if handler.def.HTTP.Path != r.URL.Path {
			continue
		}
		pathMethods[handler.def.HTTP.Method] = handler
		_ = key
	}
	ts.mu.RUnlock()

	if len(pathMethods) == 0 {
		http.NotFound(w, r)
		return
	}
	handler, methodOK := pathMethods[r.Method]
	if !methodOK {
		http.Error(w, "method not allowed",
			http.StatusMethodNotAllowed)
		return
	}
	handler.ServeHTTP(w, r)
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
		ts.recordRevision(def.ID, entry.Revision())
	}

	return nil
}

// recordRevision stores the last-applied KV revision for a trigger
// ID. Subsequent watcher entries with the same or older revision are
// idempotent replays (DeliverLastPerSubject on watcher startup) and
// must be skipped to avoid unsubscribing an active SubjectTrigger.
func (ts *TriggerService) recordRevision(id string, rev uint64) {
	if id == "" {
		panic("recordRevision: id must not be empty")
	}
	if rev == 0 {
		panic("recordRevision: rev must be > 0")
	}
	ts.mu.Lock()
	if cur := ts.revisions[id]; rev > cur {
		ts.revisions[id] = rev
	}
	ts.mu.Unlock()
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
	case def.HTTP != nil:
		key := httpRouteKey(def.HTTP.Method, def.HTTP.Path)
		if _, exists := ts.httpRoutes[key]; exists {
			return fmt.Errorf(
				"http trigger %q: route %s already registered",
				def.ID, key,
			)
		}
		ts.httpRoutes[key] = NewHTTPHandler(ts.nc, def)
		return nil
	}

	return fmt.Errorf("no trigger type specified")
}

// httpRouteKey is the composite map key for the (method, path) →
// handler table. Defined in one place so the router and the
// registration code cannot drift.
func httpRouteKey(method string, path string) string {
	if method == "" {
		panic("httpRouteKey: method must not be empty")
	}
	if path == "" {
		panic("httpRouteKey: path must not be empty")
	}
	return method + " " + path
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

	// HTTP routes are keyed by "METHOD PATH"; locate by trigger ID.
	for key, handler := range ts.httpRoutes {
		if handler.def.ID == id {
			delete(ts.httpRoutes, key)
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
// Revisions seen by loadAllTriggers are skipped so the watcher's
// initial DeliverLastPerSubject replay does not unsubscribe-and-
// re-subscribe an already-active SubjectTrigger.
func (ts *TriggerService) handleKVUpdate(
	entry jetstream.KeyValueEntry,
) {
	if entry == nil {
		panic("handleKVUpdate: entry must not be nil")
	}

	if entry.Operation() == jetstream.KeyValueDelete ||
		entry.Operation() == jetstream.KeyValuePurge {
		ts.mu.Lock()
		delete(ts.revisions, entry.Key())
		ts.mu.Unlock()
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

	rev := entry.Revision()
	ts.mu.RLock()
	last := ts.revisions[def.ID]
	ts.mu.RUnlock()
	if rev <= last {
		// Idempotent replay (or out-of-order delivery from the
		// ordered consumer): trigger is already installed at this
		// revision or newer. Skipping avoids the unsubscribe gap.
		return
	}

	// Remove old version and add new
	_ = ts.removeTrigger(def.ID)

	// Respect max triggers limit
	if ts.TriggerCount() < maxActiveTriggers {
		if err := ts.addTrigger(def); err == nil {
			ts.recordRevision(def.ID, rev)
		}
	}
}

// timeNow is a testing seam for the current time.
var timeNow = time.Now
