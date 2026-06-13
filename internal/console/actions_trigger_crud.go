package console

// actions_trigger_crud.go owns the trigger Add / Edit / Delete mutation
// handlers, wiring the console to the existing api.Service Create /
// Update / DeleteTrigger methods. No engine code is added here — these
// are thin handlers following the same scaffold as the Fire / Toggle
// surface: parse → ReadOnly gate (emits the denied audit row) → resolve
// → execute → audit → respond.
//
// The whole mux is also wrapped by readOnlyMiddleware (handler.go), so a
// forged POST would 405 even without these in-handler checks. The
// in-handler ReadOnly branch exists to emit the `outcome=denied
// reason=read_only` audit row the middleware alone cannot produce — the
// two layers are intentional, not duplication.
//
// HONESTY: api.TriggerUpdates patches config fields only — it cannot
// change a trigger's kind, retarget its workflow, or flip Enabled. The
// Edit handler therefore rejects an http-kind config edit (no backing
// TriggerUpdates field) and never folds Enabled into Update; the toggle
// endpoint owns that bit.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/trigger"
)

// triggerDefFromForm is the single validation gate between operator form
// input and the panic-on-invariant CreateTrigger API. It returns a
// non-nil error whenever the id, the target workflow, the type, or the
// type-specific config field is empty / unknown — so no under-validated
// TriggerDef can reach the API and trip a programmer-error panic.
func triggerDefFromForm(form url.Values) (trigger.TriggerDef, error) {
	if form == nil {
		panic("triggerDefFromForm: form is nil")
	}
	id := strings.TrimSpace(form.Get("id"))
	workflowID := strings.TrimSpace(form.Get("workflow_id"))
	if id == "" {
		return trigger.TriggerDef{}, fmt.Errorf("trigger id is required")
	}
	if workflowID == "" {
		return trigger.TriggerDef{}, fmt.Errorf("target workflow is required")
	}
	def := trigger.TriggerDef{
		ID:         id,
		WorkflowID: workflowID,
		Enabled:    form.Get("enabled") != "",
		Source:     "console",
	}
	if err := applyFormKind(&def, form); err != nil {
		return trigger.TriggerDef{}, err
	}
	return def, nil
}

// applyFormKind sets the per-kind sub-config on def from the form's
// `type` + `config` (+ `http_method` / `secret`) fields. Returns an
// error for an unknown type or a missing required config value.
func applyFormKind(def *trigger.TriggerDef, form url.Values) error {
	if def == nil {
		panic("applyFormKind: def is nil")
	}
	kind := strings.TrimSpace(form.Get("type"))
	config := strings.TrimSpace(form.Get("config"))
	switch kind {
	case "cron":
		if config == "" {
			return fmt.Errorf("cron expression is required")
		}
		def.Cron = &trigger.CronConfig{Expression: config}
	case "subject":
		if config == "" {
			return fmt.Errorf("NATS subject is required")
		}
		def.Subject = &trigger.SubjectConfig{Subject: config}
	case "webhook":
		if config == "" {
			return fmt.Errorf("webhook path is required")
		}
		def.Webhook = &trigger.WebhookConfig{
			Path: config, Secret: strings.TrimSpace(form.Get("secret")),
		}
	case "http":
		return applyFormHTTP(def, form, config)
	default:
		return fmt.Errorf("unknown trigger type %q", kind)
	}
	return nil
}

// applyFormHTTP sets the HTTP sub-config. Method defaults to POST when
// the form omits it; path is required.
func applyFormHTTP(def *trigger.TriggerDef, form url.Values, path string) error {
	if def == nil {
		panic("applyFormHTTP: def is nil")
	}
	if path == "" {
		return fmt.Errorf("HTTP path is required")
	}
	method := strings.TrimSpace(form.Get("http_method"))
	if method == "" {
		method = "POST"
	}
	def.HTTP = &trigger.HTTPConfig{Method: method, Path: path}
	return nil
}

// handleTriggerCreate executes POST /console/triggers (add). Scaffold
// matches the Fire / Toggle handlers. The collection route already
// guarantees POST, but we keep the method guard as defence-in-depth.
func handleTriggerCreate(w http.ResponseWriter, r *http.Request, cfg Config) {
	if w == nil {
		panic("handleTriggerCreate: w is nil")
	}
	if r == nil {
		panic("handleTriggerCreate: r is nil")
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if cfg.ReadOnly {
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionTriggerCreate, "", OutcomeDenied,
				map[string]any{"reason": "read_only"}))
		writeReadOnly(w)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	executeTriggerCreate(w, r, cfg)
}

// executeTriggerCreate validates the form into a TriggerDef, calls
// CreateTrigger, and emits the audit row. Pulled out so the outer
// handler stays under the 70-line cap.
func executeTriggerCreate(w http.ResponseWriter, r *http.Request, cfg Config) {
	def, err := triggerDefFromForm(r.Form)
	if err != nil {
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionTriggerCreate, r.Form.Get("id"), OutcomeFailed,
				map[string]any{"reason": "invalid", "error": err.Error()}))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ds, ok := requireData(w, cfg, "trigger-create")
	if !ok {
		return
	}
	defs, err := ds.ListTriggers(r.Context())
	if err != nil {
		cfg.Logger.Error("console: trigger create list", "err", err)
		http.Error(w, "lookup failed: "+err.Error(),
			http.StatusInternalServerError)
		return
	}
	if _, found := triggerByID(defs, def.ID); found {
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionTriggerCreate, def.ID, OutcomeFailed,
				map[string]any{"reason": "conflict"}))
		http.Error(w, "trigger id already exists", http.StatusConflict)
		return
	}
	kind, _ := triggerKindAndTarget(def)
	if err := ds.CreateTrigger(r.Context(), def); err != nil {
		cfg.Logger.Error("console: trigger create", "id", def.ID, "err", err)
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionTriggerCreate, def.ID, OutcomeFailed,
				map[string]any{"error": err.Error()}))
		http.Error(w, "create failed: "+err.Error(),
			http.StatusInternalServerError)
		return
	}
	emitAuditBestEffort(r.Context(), cfg,
		attemptOf(r, ActionTriggerCreate, def.ID, OutcomeSuccess,
			map[string]any{"kind": kind}))
	writeActionOK(w, triggerCreateBody(def.ID))
}

// handleTriggerEdit executes POST /console/triggers/{id}/edit. Only the
// config field for the trigger's current kind is patched; http kind is
// rejected (TriggerUpdates has no HTTP field).
func handleTriggerEdit(w http.ResponseWriter, r *http.Request, cfg Config, id string) {
	if w == nil {
		panic("handleTriggerEdit: w is nil")
	}
	if r == nil {
		panic("handleTriggerEdit: r is nil")
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
			attemptOf(r, ActionTriggerUpdate, id, OutcomeDenied,
				map[string]any{"reason": "read_only"}))
		writeReadOnly(w)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	executeTriggerEdit(w, r, cfg, id)
}

// executeTriggerEdit resolves the trigger, builds config-only updates
// from its current kind, and calls UpdateTrigger.
func executeTriggerEdit(w http.ResponseWriter, r *http.Request, cfg Config, id string) {
	ds, ok := requireData(w, cfg, "trigger-edit")
	if !ok {
		return
	}
	defs, err := ds.ListTriggers(r.Context())
	if err != nil {
		cfg.Logger.Error("console: trigger edit list", "err", err)
		http.Error(w, "lookup failed: "+err.Error(),
			http.StatusInternalServerError)
		return
	}
	current, found := triggerByID(defs, id)
	if !found {
		http.NotFound(w, r)
		return
	}
	updates, err := triggerUpdatesFromForm(current, r.Form)
	if err != nil {
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionTriggerUpdate, id, OutcomeFailed,
				map[string]any{"reason": "invalid", "error": err.Error()}))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := ds.UpdateTrigger(r.Context(), id, updates); err != nil {
		cfg.Logger.Error("console: trigger edit", "id", id, "err", err)
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionTriggerUpdate, id, OutcomeFailed,
				map[string]any{"error": err.Error()}))
		http.Error(w, "edit failed: "+err.Error(),
			http.StatusInternalServerError)
		return
	}
	emitAuditBestEffort(r.Context(), cfg,
		attemptOf(r, ActionTriggerUpdate, id, OutcomeSuccess, nil))
	writeActionOK(w, triggerEditBody(id))
}

// triggerUpdatesFromForm maps the editable config field onto an
// api.TriggerUpdates for the trigger's CURRENT kind (kind is read-only
// in Edit). Returns an error for an http-kind trigger — TriggerUpdates
// has no HTTP field, so there is no backing mutation.
func triggerUpdatesFromForm(
	current trigger.TriggerDef, form url.Values,
) (api.TriggerUpdates, error) {
	if form == nil {
		panic("triggerUpdatesFromForm: form is nil")
	}
	config := strings.TrimSpace(form.Get("config"))
	var updates api.TriggerUpdates
	switch {
	case current.Cron != nil:
		if config == "" {
			return updates, fmt.Errorf("cron expression is required")
		}
		updates.CronExpr = &config
	case current.Subject != nil:
		if config == "" {
			return updates, fmt.Errorf("NATS subject is required")
		}
		updates.Subject = &config
	case current.Webhook != nil:
		if config == "" {
			return updates, fmt.Errorf("webhook path is required")
		}
		updates.Webhook = &config
		if secret := strings.TrimSpace(form.Get("secret")); secret != "" {
			updates.Secret = &secret
		}
	default:
		return updates, fmt.Errorf("this trigger kind cannot be edited here")
	}
	return updates, nil
}

// handleTriggerDelete executes POST /console/triggers/{id}/delete.
func handleTriggerDelete(w http.ResponseWriter, r *http.Request, cfg Config, id string) {
	if w == nil {
		panic("handleTriggerDelete: w is nil")
	}
	if r == nil {
		panic("handleTriggerDelete: r is nil")
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
			attemptOf(r, ActionTriggerDelete, id, OutcomeDenied,
				map[string]any{"reason": "read_only"}))
		writeReadOnly(w)
		return
	}
	executeTriggerDelete(w, r, cfg, id)
}

// executeTriggerDelete resolves the trigger (so an unknown id 404s
// before the mutation) and calls DeleteTrigger.
func executeTriggerDelete(w http.ResponseWriter, r *http.Request, cfg Config, id string) {
	ds, ok := requireData(w, cfg, "trigger-delete")
	if !ok {
		return
	}
	defs, err := ds.ListTriggers(r.Context())
	if err != nil {
		cfg.Logger.Error("console: trigger delete list", "err", err)
		http.Error(w, "lookup failed: "+err.Error(),
			http.StatusInternalServerError)
		return
	}
	if _, found := triggerByID(defs, id); !found {
		http.NotFound(w, r)
		return
	}
	if err := ds.DeleteTrigger(r.Context(), id); err != nil {
		cfg.Logger.Error("console: trigger delete", "id", id, "err", err)
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionTriggerDelete, id, OutcomeFailed,
				map[string]any{"error": err.Error()}))
		http.Error(w, "delete failed: "+err.Error(),
			http.StatusInternalServerError)
		return
	}
	emitAuditBestEffort(r.Context(), cfg,
		attemptOf(r, ActionTriggerDelete, id, OutcomeSuccess, nil))
	writeActionOK(w, triggerDeleteBody(id))
}

// actionToast is one toast payload the client lifts from a mutation's
// success body. Marshalled (not fmt-interpolated) so an operator id
// carrying a double-quote or backslash cannot corrupt the JSON and make
// the browser falsely report a succeeded mutation as failed.
type actionToast struct {
	Level   string `json:"level"`
	Message string `json:"message"`
	Href    string `json:"href,omitempty"`
}

// actionBody is the success envelope every trigger mutation returns.
type actionBody struct {
	OK       bool        `json:"ok"`
	Action   string      `json:"action"`
	ID       string      `json:"id"`
	Toast    actionToast `json:"toast"`
	Redirect string      `json:"redirect,omitempty"`
}

// marshalActionBody renders b to JSON. json.Marshal of these flat,
// string-only fields cannot fail, so a marshal error is a programmer
// error and panics rather than returning a corrupt body.
func marshalActionBody(b actionBody) []byte {
	out, err := json.Marshal(b)
	if err != nil {
		panic("marshalActionBody: " + err.Error())
	}
	return out
}

// triggerCreateBody / triggerEditBody / triggerDeleteBody return the
// success JSON the client toast lifts. Shape matches triggerToggleBody
// so toast.js handles every trigger mutation uniformly.
func triggerCreateBody(id string) []byte {
	return marshalActionBody(actionBody{
		OK: true, Action: "create", ID: id,
		Toast: actionToast{
			Level:   "info",
			Message: "Created trigger " + id,
			Href:    "/console/triggers/" + url.PathEscape(id),
		},
	})
}

func triggerEditBody(id string) []byte {
	return marshalActionBody(actionBody{
		OK: true, Action: "edit", ID: id,
		Toast: actionToast{Level: "info", Message: "Updated trigger " + id},
	})
}

func triggerDeleteBody(id string) []byte {
	return marshalActionBody(actionBody{
		OK: true, Action: "delete", ID: id,
		Toast:    actionToast{Level: "info", Message: "Deleted trigger " + id},
		Redirect: "/console/triggers",
	})
}
