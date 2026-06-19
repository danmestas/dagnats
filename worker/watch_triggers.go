// worker/watch_triggers.go
// Worker SDK for receiving External trigger activate/deactivate events
// (parent #273 Phase 2.4, #333). Bridges the engine's
// `_TRIGGER.<kind>.{activate,deactivate}` request subjects (driven by
// the externalRegistrar in internal/trigger/registrar_external.go)
// into user-supplied callbacks.
//
// Subscription lifecycle is internal — the audit (#333 comment) chose
// internal tracking over returning a Subscription handle because
// callers already own a Worker.Stop() lifecycle. A separate
// Unsubscribe() would add a third lifecycle they have to remember.
// Stop() drains every triggerSubs entry; see worker.go.
//
// Catch-up contract: on subscribe, the worker scans the `triggers` KV
// bucket and fires onActivate for every Enabled entry whose
// External.Kind matches. Bounded at maxCatchupKeys = 10000 — over-cap
// the loop stops and logs a warning rather than silently truncating
// half-way through (TigerStyle "all loops must have fixed upper
// bounds"; CLAUDE.md).
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/danmestas/dagnats/dagnatsext"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// maxCatchupKeys bounds the startup scan of the `triggers` KV bucket.
// Chosen to be an order of magnitude above the engine-side
// maxActiveTriggers=500 cap so a worker hosting many kinds doesn't
// silently truncate, but small enough that an unbounded KV
// (misconfiguration, runaway test) doesn't OOM a sidecar. Over-cap is
// logged + stopped — not truncated mid-scan, which would surface as
// "some triggers fired, some didn't" mystery state.
const maxCatchupKeys = 10000

// catchupListTimeout bounds the KV Keys() call. Long enough for a
// loaded production bucket (NATS Keys() is paginated under the hood)
// without letting a hung server block worker boot indefinitely.
const catchupListTimeout = 10 * time.Second

// catchupGetTimeout bounds each per-key Get() inside the catch-up
// loop. Much tighter than the list timeout — a single KV read should
// be sub-second; if it isn't, the worker is in trouble anyway.
const catchupGetTimeout = 2 * time.Second

// triggerHandler is a worker callback for activate/deactivate events.
// Empty-body reply on success; error becomes a JSON {"error":"..."}
// reply per the engine's requestOwner contract.
type triggerHandler func(ctx context.Context, def dagnatsext.TriggerDef) error

// WatchTriggers subscribes to `_TRIGGER.<kind>.activate` and
// `_TRIGGER.<kind>.deactivate`, decodes each request into a
// trigger.TriggerDef, and invokes the corresponding callback. The
// callback's returned error is reported back to the engine via a
// `{"error":"..."}` reply; a nil error replies with an empty body.
//
// Catch-up: before returning, the worker scans the `triggers` KV
// bucket and fires onActivate for every Enabled entry whose
// External.Kind == kind. Bounded at maxCatchupKeys (10000). Over-cap
// → warning logged and scan stopped (no fires beyond the cap).
//
// Lifecycle: both NATS subscriptions are appended to w.triggerSubs
// and drained by Worker.Stop(). Callers do not unsubscribe directly.
func (w *Worker) WatchTriggers(
	ctx context.Context,
	kind string,
	onActivate triggerHandler,
	onDeactivate triggerHandler,
) error {
	if w.nc == nil {
		panic("WatchTriggers: worker.nc must not be nil")
	}
	if w.js == nil {
		panic("WatchTriggers: worker.js must not be nil")
	}
	if ctx == nil {
		return fmt.Errorf("WatchTriggers: ctx must not be nil")
	}
	if kind == "" {
		return fmt.Errorf("WatchTriggers: kind must not be empty")
	}
	if onActivate == nil {
		return fmt.Errorf("WatchTriggers: onActivate must not be nil")
	}
	if onDeactivate == nil {
		return fmt.Errorf("WatchTriggers: onDeactivate must not be nil")
	}

	activateSubj := "_TRIGGER." + kind + ".activate"
	deactivateSubj := "_TRIGGER." + kind + ".deactivate"

	actSub, err := w.nc.Subscribe(activateSubj, func(msg *nats.Msg) {
		w.handleTriggerEvent(ctx, kind, msg, onActivate)
	})
	if err != nil {
		return fmt.Errorf("subscribe %s: %w", activateSubj, err)
	}
	deactSub, err := w.nc.Subscribe(deactivateSubj, func(msg *nats.Msg) {
		w.handleTriggerEvent(ctx, kind, msg, onDeactivate)
	})
	if err != nil {
		// Clean up the half-bound state — leaving actSub leaked would
		// drop fires onto the floor with no callback path.
		if uerr := actSub.Unsubscribe(); uerr != nil {
			slog.Warn("WatchTriggers: rollback unsubscribe failed",
				"subject", activateSubj, "error", uerr)
		}
		return fmt.Errorf("subscribe %s: %w", deactivateSubj, err)
	}

	w.triggerSubsMu.Lock()
	w.triggerSubs = append(w.triggerSubs, actSub, deactSub)
	w.triggerSubsMu.Unlock()

	// Catch-up happens AFTER the subscribers are armed so a
	// late-arriving live event during the scan is not lost. The KV
	// bucket may not exist (test harnesses that skip the trigger
	// surface); treat absence as "nothing to catch up" rather than a
	// hard error so a worker can WatchTriggers without coupling to
	// the engine's bucket lifecycle.
	w.runCatchUp(ctx, kind, onActivate)
	return nil
}

// handleTriggerEvent decodes one activate/deactivate request, calls
// the handler, and replies. Mirrors externalRegistrar.requestOwner's
// payload shape (TriggerID/WorkflowID/Config). The engine fires the
// kind into the External branch of TriggerDef — workers see a
// reconstructed def with Enabled=true (deactivate replies don't care
// about Enabled but we set it consistently).
func (w *Worker) handleTriggerEvent(
	ctx context.Context,
	kind string,
	msg *nats.Msg,
	handler triggerHandler,
) {
	if msg == nil {
		panic("handleTriggerEvent: msg must not be nil")
	}
	if handler == nil {
		panic("handleTriggerEvent: handler must not be nil")
	}
	var payload struct {
		TriggerID  string          `json:"trigger_id"`
		WorkflowID string          `json:"workflow_id"`
		Config     json.RawMessage `json:"config"`
	}
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		replyTriggerEvent(msg, fmt.Errorf("decode payload: %w", err))
		return
	}
	if payload.TriggerID == "" {
		replyTriggerEvent(msg, fmt.Errorf("trigger_id must not be empty"))
		return
	}
	def := dagnatsext.TriggerDef{
		ID:         payload.TriggerID,
		WorkflowID: payload.WorkflowID,
		Enabled:    true,
		External: dagnatsext.ExternalTriggerConfig{
			Kind:   kind,
			Config: payload.Config,
		},
	}
	if err := handler(ctx, def); err != nil {
		replyTriggerEvent(msg, err)
		return
	}
	replyTriggerEvent(msg, nil)
}

// replyTriggerEvent emits the engine-expected reply: empty body on
// success, JSON {"error":"..."} on failure. No-op when the msg has
// no Reply subject (the engine always sets one, but a hand-rolled
// test publisher might not — better to log than panic).
func replyTriggerEvent(msg *nats.Msg, err error) {
	if msg == nil {
		panic("replyTriggerEvent: msg must not be nil")
	}
	if msg.Reply == "" {
		if err != nil {
			slog.Warn("trigger reply with no Reply subject",
				"subject", msg.Subject, "error", err)
		}
		return
	}
	if err == nil {
		if rerr := msg.Respond(nil); rerr != nil {
			slog.Warn("trigger reply respond failed",
				"subject", msg.Subject, "error", rerr)
		}
		return
	}
	body, _ := json.Marshal(struct {
		Error string `json:"error,omitempty"`
	}{Error: err.Error()})
	if rerr := msg.Respond(body); rerr != nil {
		slog.Warn("trigger reply respond (error) failed",
			"subject", msg.Subject, "error", rerr)
	}
}

// runCatchUp scans the `triggers` KV bucket and fires onActivate for
// every Enabled entry whose External.Kind matches. Bounded at
// maxCatchupKeys — over-cap stops the scan with a warning rather
// than silently truncating, so operators see the cap was hit.
//
// Errors are logged and per-entry skipped (mirrors the engine's
// fireExistingEntries policy). The whole catch-up is best-effort
// from the caller's perspective: WatchTriggers does not return a
// catch-up error because a missing bucket / read failure should not
// prevent live events from flowing.
func (w *Worker) runCatchUp(
	ctx context.Context, kind string, onActivate triggerHandler,
) {
	if w.js == nil {
		panic("runCatchUp: w.js must not be nil")
	}
	listCtx, cancelList := context.WithTimeout(ctx, catchupListTimeout)
	defer cancelList()
	kv, err := w.js.KeyValue(listCtx, "triggers")
	if err != nil {
		slog.Info("WatchTriggers: triggers bucket unavailable, "+
			"skipping catch-up",
			"kind", kind, "error", err)
		return
	}
	keys, err := kv.Keys(listCtx)
	if err != nil {
		if isNoKeysFound(err) {
			return
		}
		slog.Warn("WatchTriggers: catch-up Keys() failed",
			"kind", kind, "error", err)
		return
	}
	if len(keys) > maxCatchupKeys {
		slog.Warn(
			"WatchTriggers: catch-up over cap, stopping scan",
			"kind", kind,
			"keys", len(keys),
			"cap", maxCatchupKeys,
		)
		keys = keys[:maxCatchupKeys]
	}
	for _, key := range keys {
		w.catchUpOne(ctx, kv, kind, key, onActivate)
	}
}

// catchUpOne handles a single catch-up KV key. Split out of runCatchUp
// to keep the loop body short and to give each Get() its own bounded
// timeout — a stuck single key shouldn't be able to consume the
// list-level deadline.
func (w *Worker) catchUpOne(
	ctx context.Context,
	kv jetstream.KeyValue,
	kind string,
	key string,
	onActivate triggerHandler,
) {
	getCtx, cancel := context.WithTimeout(ctx, catchupGetTimeout)
	defer cancel()
	entry, err := kv.Get(getCtx, key)
	if err != nil {
		slog.Warn("WatchTriggers: catch-up Get failed",
			"kind", kind, "key", key, "error", err)
		return
	}
	var richDef trigger.TriggerDef
	if err := json.Unmarshal(entry.Value(), &richDef); err != nil {
		slog.Warn("WatchTriggers: catch-up decode failed",
			"kind", kind, "key", key, "error", err)
		return
	}
	if richDef.External == nil || richDef.External.Kind != kind {
		return
	}
	if !richDef.Enabled {
		return
	}
	// Convert the rich internal TriggerDef to the slim public view
	// delivered to the handler. trigger.ExternalTriggerConfig is a type
	// alias for dagnatsext.ExternalTriggerConfig, so the deref copies the
	// value directly with no JSON re-encoding. Safe to deref: the
	// richDef.External == nil case returned above.
	extDef := dagnatsext.TriggerDef{
		ID:         richDef.ID,
		WorkflowID: richDef.WorkflowID,
		Enabled:    richDef.Enabled,
		External:   *richDef.External,
	}
	if err := onActivate(ctx, extDef); err != nil {
		slog.Warn("WatchTriggers: catch-up onActivate returned error",
			"kind", kind, "trigger_id", richDef.ID, "error", err)
	}
}

// isNoKeysFound centralises the "empty bucket is not an error"
// detection so the catch-up doesn't log a confusing warning on a
// fresh deployment.
func isNoKeysFound(err error) bool {
	if err == nil {
		return false
	}
	return err == jetstream.ErrNoKeysFound
}
