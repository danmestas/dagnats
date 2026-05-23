package console

// actions_trigger_fire.go owns the POST /console/triggers/{id}/fire
// handler (#352). Pulled into its own file so the fire-now surface
// stays grep-able and actions.go doesn't grow further. Behaviour
// follows the standard mutation-handler scaffold: read-only check →
// rate-limit check → execute → audit → respond.
//
// Two response bodies live here as untyped constants so the hot path
// stays allocation-light:
//   - rate-limit denial: 429 + Retry-After header (RFC 6585). This is
//     the first 429 in the console; documented here so future
//     rate-limited endpoints copy the wire shape.
//   - fire success: matches the DLQ retry / workflow run shape so
//     toast.js can lift the `toast` field without per-action branching.

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"

	dnapi "github.com/danmestas/dagnats/internal/api"
)

// rateLimitJSONBody is the body shape returned with 429. Documented:
//
//	{"error":"rate_limited","trigger_id":"<id>","retry_after_seconds":N}
//
// `retry_after_seconds` mirrors the Retry-After header so JSON clients
// don't need to read headers separately. RFC 6585 specifies status
// code 429; RFC 7231 §7.1.3 defines Retry-After in seconds.
const rateLimitJSONShape = `{"error":"rate_limited","trigger_id":%q,` +
	`"retry_after_seconds":%d}`

// handleTriggerFire dispatches the manual fire-now endpoint. Each
// branch emits one audit row — denied / failed / success — so the
// audit log captures every operator attempt regardless of outcome.
func handleTriggerFire(
	w http.ResponseWriter, r *http.Request,
	cfg Config, id string,
) {
	if w == nil {
		panic("handleTriggerFire: w is nil")
	}
	if r == nil {
		panic("handleTriggerFire: r is nil")
	}
	if id == "" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if cfg.ReadOnly {
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionTriggerFireManual, id, OutcomeDenied,
				map[string]any{"reason": "read_only"}))
		writeReadOnly(w)
		return
	}
	if ok, retry := cfg.fireLimiter().Allow(id); !ok {
		writeFireRateLimited(w, r, cfg, id, retry)
		return
	}
	executeTriggerFire(w, r, cfg, id)
}

// executeTriggerFire owns the DataSource call + audit emission. Pulled
// out so handleTriggerFire stays under the 70-line cap.
func executeTriggerFire(
	w http.ResponseWriter, r *http.Request, cfg Config, id string,
) {
	ds, ok := requireData(w, cfg, "trigger-fire")
	if !ok {
		return
	}
	runID, err := ds.FireTrigger(r.Context(), id)
	if err != nil {
		writeFireError(w, r, cfg, id, err)
		return
	}
	emitAuditBestEffort(r.Context(), cfg,
		attemptOf(r, ActionTriggerFireManual, id, OutcomeSuccess,
			map[string]any{"run_id": runID}))
	writeActionOK(w, fireSuccessBody(id, runID))
}

// writeFireRateLimited returns 429 + the RFC-6585 body shape and
// Retry-After header. The retry duration is rounded up to whole
// seconds so the operator's browser sees a sane countdown.
func writeFireRateLimited(
	w http.ResponseWriter, r *http.Request,
	cfg Config, id string, retry interface{ Seconds() float64 },
) {
	seconds := int(math.Ceil(retry.Seconds()))
	if seconds < 1 {
		seconds = 1
	}
	emitAuditBestEffort(r.Context(), cfg,
		attemptOf(r, ActionTriggerFireManual, id, OutcomeDenied,
			map[string]any{
				"reason":              "rate_limited",
				"retry_after_seconds": seconds,
			}))
	w.Header().Set("Content-Type",
		"application/json; charset=utf-8")
	w.Header().Set("Retry-After", strconv.Itoa(seconds))
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = fmt.Fprintf(w, rateLimitJSONShape, id, seconds)
}

// writeFireError maps the (typed) error return from FireTrigger to
// the right HTTP status + audit-denied / audit-failed row. Kept in
// one place so the wire contract stays consistent across UI + CLI.
func writeFireError(
	w http.ResponseWriter, r *http.Request,
	cfg Config, id string, err error,
) {
	switch {
	case errors.Is(err, dnapi.ErrTriggerKindNotFireable):
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionTriggerFireManual, id, OutcomeDenied,
				map[string]any{"reason": "not_fireable"}))
		http.Error(w,
			"trigger kind not fireable from manual fire-now",
			http.StatusBadRequest)
	case errors.Is(err, dnapi.ErrTriggerDisabled):
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionTriggerFireManual, id, OutcomeDenied,
				map[string]any{"reason": "disabled"}))
		http.Error(w,
			"trigger is disabled; enable it before firing",
			http.StatusConflict)
	default:
		cfg.Logger.Error("console: trigger fire",
			"trigger_id", id, "err", err)
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionTriggerFireManual, id, OutcomeFailed,
				map[string]any{"error": err.Error()}))
		http.Error(w, "fire failed: "+err.Error(),
			http.StatusInternalServerError)
	}
}

// fireSuccessBody returns the JSON response for a successful fire.
// Shape matches workflowRunBody: `ok` / `action` / `run_id` plus an
// embedded `toast` for the client-side notifier.
func fireSuccessBody(id, runID string) []byte {
	const tmpl = `{"ok":true,"action":"fire","trigger_id":%q,` +
		`"run_id":%q,"toast":{"level":"info",` +
		`"message":"Fired %s","href":"/console/runs/%s"}}`
	return []byte(fmt.Sprintf(tmpl, id, runID, id, runID))
}

// fireKindAllows reports whether the trigger kind supports manual
// fire-now. Used by the template to render the button only on cron /
// webhook rows. Centralised so the UI hide-rule and the server
// reject-rule (in api.Service.FireTrigger) can never drift.
func fireKindAllows(kind string) bool {
	switch kind {
	case "cron", "webhook":
		return true
	}
	return false
}
