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
	"github.com/nats-io/nats.go/micro"
)

const maxActiveTriggers = 500

// TriggerService coordinates all trigger types. It loads definitions
// from the triggers KV bucket on startup and watches for live changes.
//
// ADR-016 made this a thin orchestrator: per-kind state lives on the
// registrars, and addTrigger / removeTrigger are table dispatches
// keyed by kind. The kind-specific maps (subjects, webhooks,
// httpRoutes) remain as struct fields because their canonical owner
// is the matching registrar — TriggerService holds the same map
// reference so the in-package regression tests can observe pointer
// identity (#217 / #221 / #223 watcher-replay guard) without
// reaching through the registrar abstraction.
//
// httpRoutes maps method:path → HTTPHandler. The composite key matches
// the routing key the HTTPRouter dispatches on; collision detection is
// the ADR-013 "refuse second registration that conflicts on
// (method, path)" rule.
type TriggerService struct {
	nc             *nats.Conn
	js             jetstream.JetStream
	triggerKV      jetstream.KeyValue
	triggerTypesKV jetstream.KeyValue
	scheduler      *Scheduler
	subjects       map[string]*SubjectTrigger
	webhooks       map[string]*WebhookHandler
	httpRoutes     map[string]*HTTPHandler
	registrars     map[string]TriggerRegistrar
	subjectReg     *subjectRegistrar
	webhookReg     *webhookRegistrar
	httpReg        *httpRegistrar
	ackMicro       micro.Service
	// build is the binary's build string, threaded into the
	// dagnats-trigger micro service version (#449 Phase 2b).
	build string
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

// kind constants for the registrar dispatch table. Stringly typed
// because the keys appear in JSON payloads and registry lookups; an
// iota would not survive the wire crossing.
const (
	kindCron    = "cron"
	kindSubject = "subject"
	kindWebhook = "webhook"
	kindHTTP    = "http"
)

// triggerKind returns the kind name for def, or "" if no kind field
// is set. Used by addTrigger / removeTrigger to look up the registrar.
//
// External triggers fan out into a per-kind registrar keyed by
// externalKindPrefix+External.Kind so an arbitrary number of worker-
// contributed types can coexist without colliding with the built-in
// constants above.
func triggerKind(def TriggerDef) string {
	switch {
	case def.Cron != nil:
		return kindCron
	case def.Subject != nil:
		return kindSubject
	case def.Webhook != nil:
		return kindWebhook
	case def.HTTP != nil:
		return kindHTTP
	case def.External != nil:
		return externalKindPrefix + def.External.Kind
	}
	return ""
}

// NewTriggerService creates the service. KV buckets must exist. build is
// the binary's build string, threaded into the dagnats-trigger micro
// service version (#449 Phase 2b); it is sanitized to a valid SemVer.
// Panics if nc is nil or nc is not connected (programmer error).
func NewTriggerService(
	nc *nats.Conn, build string,
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

	// trigger_types is optional at NewTriggerService time — older
	// integration tests don't provision it. The ack endpoint refuses
	// requests when triggerTypesKV is nil, but everything else
	// continues to work for the cron/subject/webhook/http kinds.
	triggerTypesKV, err := js.KeyValue(kvCtx, "trigger_types")
	if err != nil {
		// Treat "not found" as "no External support in this engine"
		// rather than a fatal startup error. Other KV failures (auth,
		// transport) still surface.
		if !errors.Is(err, jetstream.ErrBucketNotFound) {
			return nil, fmt.Errorf("trigger_types KV bucket: %w", err)
		}
		triggerTypesKV = nil
	}

	scheduler, err := NewScheduler(nc)
	if err != nil {
		return nil, fmt.Errorf("NewScheduler: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ts := &TriggerService{
		nc:             nc,
		js:             js,
		triggerKV:      triggerKV,
		triggerTypesKV: triggerTypesKV,
		scheduler:      scheduler,
		subjects:       make(map[string]*SubjectTrigger),
		webhooks:       make(map[string]*WebhookHandler),
		httpRoutes:     make(map[string]*HTTPHandler),
		revisions:      make(map[string]uint64),
		ctx:            ctx,
		cancel:         cancel,
		build:          build,
	}
	ts.subjectReg = newSubjectRegistrar(nc, ts.subjects, &ts.mu)
	ts.webhookReg = newWebhookRegistrar(nc, ts.webhooks, &ts.mu)
	ts.httpReg = newHTTPRegistrar(nc, ts.httpRoutes, &ts.mu)
	ts.registrars = map[string]TriggerRegistrar{
		kindCron:    newCronRegistrar(scheduler),
		kindSubject: ts.subjectReg,
		kindWebhook: ts.webhookReg,
		kindHTTP:    ts.httpReg,
	}
	return ts, nil
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

	// _REGISTRY.trigger_types.ack micro endpoint (#327). Only wired
	// when the trigger_types KV bucket exists; absence is treated as
	// "no External trigger support in this engine" rather than a
	// startup failure.
	if ts.triggerTypesKV != nil {
		if err := ts.startAckMicro(); err != nil {
			return fmt.Errorf("startAckMicro: %w", err)
		}
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
	if ts.ackMicro != nil {
		if err := ts.ackMicro.Stop(); err != nil {
			slog.Warn("trigger micro stop: drain failed", "error", err)
		}
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
// Proxies to the webhook registrar (ADR-016 deep ownership).
func (ts *TriggerService) WebhookHandler() http.Handler {
	if ts.webhookReg == nil {
		panic("WebhookHandler: webhook registrar must not be nil")
	}
	return ts.webhookReg.Handler()
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
// Proxies to the HTTP registrar (ADR-016 deep ownership). The API
// server already owns a mux; layering a second one would create two
// places to look when a route 404s. Returning a flat handler keeps
// route ownership in the trigger service and the API mux's only job
// is to forward "/api/*" (or whatever prefix) to this handler.
func (ts *TriggerService) HTTPRouter() http.Handler {
	if ts.httpReg == nil {
		panic("HTTPRouter: http registrar must not be nil")
	}
	return ts.httpReg.Router()
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

	// External triggers need the trigger_types KV handle for schema
	// validation; use ValidateWithKV when available. Non-External
	// defs go through the cheaper KV-less Validate.
	if def.External != nil && ts.triggerTypesKV != nil {
		vctx, cancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		if err := ValidateWithKV(vctx, ts.triggerTypesKV, def); err != nil {
			cancel()
			return fmt.Errorf("validate: %w", err)
		}
		cancel()
	} else if err := Validate(def); err != nil {
		return fmt.Errorf("validate: %w", err)
	}

	if !def.Enabled {
		return nil
	}

	kind := triggerKind(def)
	if kind == "" {
		return fmt.Errorf("no trigger type specified")
	}
	reg, ok := ts.registrars[kind]
	if !ok {
		return fmt.Errorf("unknown trigger kind: %s", kind)
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()
	return reg.Activate(ts.ctx, def)
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

	// We don't know which kind owns this id without keeping a side
	// table — instead, ask each registrar to Deactivate. Each is
	// idempotent for unknown ids (ADR-016 contract), so the wrong
	// ones are no-ops. Cost is bounded: O(kinds * map walk per
	// kind) and the registrar set is fixed at boot.
	stub := TriggerDef{ID: id}
	for _, reg := range ts.registrars {
		_ = reg.Deactivate(ts.ctx, stub)
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
