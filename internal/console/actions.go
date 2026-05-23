package console

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/danmestas/dagnats/internal/trigger"
)

// actions.go owns the mutation endpoints PR 4 introduces: DLQ retry,
// DLQ discard, and trigger enable/disable. Every action goes through
// the same scaffold: parse → check ReadOnly → check CSRF → execute →
// audit emit → respond. The scaffold lives here so behaviour stays
// consistent across action types.

// readOnlyJSONBody is the verbatim JSON the console returns when
// CONSOLE_READ_ONLY=true and the operator hits a mutation endpoint.
// Pre-marshalled so the response path stays allocation-free.
const readOnlyJSONBody = `{"error":"console_read_only","message":` +
	`"The console is in read-only mode. Set CONSOLE_READ_ONLY=false to enable mutations."}`

// writeReadOnly responds to a denied mutation with 405 + the standard
// JSON body. The operator's audit-log row records the denied attempt
// so abuse / misconfig is visible.
func writeReadOnly(w http.ResponseWriter) {
	if w == nil {
		panic("writeReadOnly: w is nil")
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusMethodNotAllowed)
	_, _ = w.Write([]byte(readOnlyJSONBody))
}

// triggerByID searches defs for a trigger with the given id. Lives
// here next to the toggle handler that needs it; if other call sites
// pick it up later the helper can move to triggers.go.
func triggerByID(
	defs []trigger.TriggerDef, id string,
) (trigger.TriggerDef, bool) {
	if id == "" {
		panic("triggerByID: id is empty")
	}
	for _, t := range defs {
		if t.ID == id {
			return t, true
		}
	}
	return trigger.TriggerDef{}, false
}

// dispatchTriggers routes /console/triggers/<id> and /<id>/<action>.
// Action paths are POST; everything else falls through to the
// trigger-detail page renderer.
func dispatchTriggers(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("dispatchTriggers: w is nil")
	}
	if r == nil {
		panic("dispatchTriggers: r is nil")
	}
	rest := strings.TrimPrefix(r.URL.Path, "/console/triggers/")
	if rest == "" {
		serveNotFound(w, r, ts, cfg)
		return
	}
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) == 1 {
		servePageTriggerDetail(w, r, ts, cfg)
		return
	}
	id, action := parts[0], parts[1]
	switch action {
	case "toggle":
		handleTriggerToggle(w, r, cfg, id)
	case "fire":
		handleTriggerFire(w, r, cfg, id)
	default:
		serveNotFound(w, r, ts, cfg)
	}
}

// handleTriggerToggle inverts the trigger's enabled bit and emits the
// matching audit row. Same scaffold as DLQ actions: parse → check
// ReadOnly → execute → audit → respond. The flip is "read current,
// invert, write" — race-prone but the DLQ surface already accepts the
// same risk and the operator action is idempotent under the audit log.
func handleTriggerToggle(
	w http.ResponseWriter, r *http.Request,
	cfg Config, id string,
) {
	if w == nil {
		panic("handleTriggerToggle: w is nil")
	}
	if r == nil {
		panic("handleTriggerToggle: r is nil")
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
			attemptOf(r, ActionTriggerDisable, id, OutcomeDenied,
				map[string]any{"reason": "read_only"}))
		writeReadOnly(w)
		return
	}
	executeTriggerToggle(w, r, cfg, id)
}

// executeTriggerToggle runs the read-then-flip-then-write path,
// emits the audit, and writes the response. Pulled out so the
// outer handler stays at ≤70 lines under the project rule.
func executeTriggerToggle(
	w http.ResponseWriter, r *http.Request,
	cfg Config, id string,
) {
	ds, ok := requireData(w, cfg, "trigger-toggle")
	if !ok {
		return
	}
	defs, err := ds.ListTriggers(r.Context())
	if err != nil {
		cfg.Logger.Error("console: trigger toggle list", "err", err)
		http.Error(w, "lookup failed: "+err.Error(),
			http.StatusInternalServerError)
		return
	}
	current, found := triggerByID(defs, id)
	if !found {
		http.NotFound(w, r)
		return
	}
	desired := !current.Enabled
	action := actionFromToggle(desired)
	if err := ds.SetTriggerEnabled(r.Context(), id, desired); err != nil {
		cfg.Logger.Error("console: trigger toggle", "id", id, "err", err)
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, action, id, OutcomeFailed,
				map[string]any{"error": err.Error()}))
		http.Error(w, "toggle failed: "+err.Error(),
			http.StatusInternalServerError)
		return
	}
	emitAuditBestEffort(r.Context(), cfg,
		attemptOf(r, action, id, OutcomeSuccess,
			map[string]any{"enabled": desired}))
	writeActionOK(w, triggerToggleBody(id, desired))
}

// dispatchWorkflows routes /console/workflows/<name> and
// /console/workflows/<name>/<action>. Action paths are POST; everything
// else falls through to the workflow-detail page renderer.
func dispatchWorkflows(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("dispatchWorkflows: w is nil")
	}
	if r == nil {
		panic("dispatchWorkflows: r is nil")
	}
	rest := strings.TrimPrefix(r.URL.Path, "/console/workflows/")
	if rest == "" {
		serveNotFound(w, r, ts, cfg)
		return
	}
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) == 1 {
		servePageWorkflowDetail(w, r, ts, cfg)
		return
	}
	name, action := parts[0], parts[1]
	switch action {
	case "run":
		handleWorkflowRun(w, r, cfg, name)
	default:
		serveNotFound(w, r, ts, cfg)
	}
}

// handleWorkflowRun executes a POST /console/workflows/<name>/run.
// Scaffold matches the DLQ + trigger handlers: parse → check ReadOnly
// → resolve the workflow → check runnability → call StartRun → emit
// audit → respond. The handler is the sole owner of the runnability
// re-check; the template only hides the affordance, defence-in-depth
// lives here so a forged POST against an input-required workflow is
// still rejected.
func handleWorkflowRun(
	w http.ResponseWriter, r *http.Request,
	cfg Config, name string,
) {
	if w == nil {
		panic("handleWorkflowRun: w is nil")
	}
	if r == nil {
		panic("handleWorkflowRun: r is nil")
	}
	if name == "" {
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
			attemptOf(r, ActionWorkflowRun, name, OutcomeDenied,
				map[string]any{"reason": "read_only"}))
		writeReadOnly(w)
		return
	}
	executeWorkflowRun(w, r, cfg, name)
}

// executeWorkflowRun owns the work past the ReadOnly gate. Pulled out
// so the outer handler stays within the 70-line ceiling.
func executeWorkflowRun(
	w http.ResponseWriter, r *http.Request,
	cfg Config, name string,
) {
	ds, ok := requireData(w, cfg, "workflow-run")
	if !ok {
		return
	}
	def, err := ds.GetWorkflow(name)
	if err != nil {
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionWorkflowRun, name, OutcomeFailed,
				map[string]any{"reason": "not_found"}))
		http.NotFound(w, r)
		return
	}
	if !workflowRunnable(def) {
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionWorkflowRun, name, OutcomeDenied,
				map[string]any{"reason": "input_required"}))
		http.Error(w, "workflow requires a typed input payload",
			http.StatusBadRequest)
		return
	}
	runID, err := ds.StartRun(r.Context(), name, nil)
	if err != nil {
		cfg.Logger.Error("console: workflow run start",
			"workflow", name, "err", err)
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionWorkflowRun, name, OutcomeFailed,
				map[string]any{"error": err.Error()}))
		http.Error(w, "start run failed: "+err.Error(),
			http.StatusInternalServerError)
		return
	}
	emitAuditBestEffort(r.Context(), cfg,
		attemptOf(r, ActionWorkflowRun, name, OutcomeSuccess,
			map[string]any{"run_id": runID}))
	writeActionOK(w, workflowRunBody(name, runID))
}

// workflowRunBody returns the JSON response for a successful Run. The
// toast carries the new run id + a deep link so the operator can click
// straight to the run detail page; the level is `info` (no error) and
// the message phrasing matches the DLQ-retry success copy for parity.
func workflowRunBody(name, runID string) []byte {
	const tmpl = `{"ok":true,"action":"run","workflow":%q,` +
		`"run_id":%q,"toast":{"level":"info",` +
		`"message":"Started %s","href":"/console/runs/%s"}}`
	return []byte(fmt.Sprintf(tmpl, name, runID, name, runID))
}

// actionFromToggle picks the audit action constant for one toggle
// direction. desired=true → enable; desired=false → disable.
func actionFromToggle(desired bool) AuditAction {
	if desired {
		return ActionTriggerEnable
	}
	return ActionTriggerDisable
}

// triggerToggleBody returns the JSON response for a successful toggle.
// Includes the new state so the client can update the pill in-place
// without a refetch.
func triggerToggleBody(id string, enabled bool) []byte {
	state := "disabled"
	verb := "Disabled"
	if enabled {
		state = "enabled"
		verb = "Enabled"
	}
	const tmpl = `{"ok":true,"action":"toggle","id":%q,"enabled":%t,` +
		`"state":%q,"toast":{"level":"info","message":"%s trigger %s"}}`
	return []byte(fmt.Sprintf(tmpl, id, enabled, state, verb, id))
}

// dispatchDLQ routes /console/dlq/<seq> and /console/dlq/<seq>/<action>.
// Action paths must be POST; everything else is GET (the detail page).
func dispatchDLQ(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("dispatchDLQ: w is nil")
	}
	if r == nil {
		panic("dispatchDLQ: r is nil")
	}
	rest := strings.TrimPrefix(r.URL.Path, "/console/dlq/")
	if rest == "" {
		serveNotFound(w, r, ts, cfg)
		return
	}
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) == 1 {
		servePageDLQDetail(w, r, ts, cfg)
		return
	}
	seqStr, action := parts[0], parts[1]
	switch action {
	case "retry":
		handleDLQRetry(w, r, cfg, seqStr)
	case "discard":
		handleDLQDiscard(w, r, cfg, seqStr)
	case "undo-discard":
		handleDLQUndoDiscard(w, r, cfg, seqStr)
	default:
		serveNotFound(w, r, ts, cfg)
	}
}

// handleDLQUndoDiscard reverses a recent soft-discard. The undo token
// comes in via the X-Undo-Token header (the toast.js wire format).
// Inside the grace window the entry is restored (nothing actually
// happened to it yet — discard was soft); past the window the call is
// rejected with 410 Gone and an audit row records the late attempt.
func handleDLQUndoDiscard(
	w http.ResponseWriter, r *http.Request,
	cfg Config, seqStr string,
) {
	if w == nil {
		panic("handleDLQUndoDiscard: w is nil")
	}
	if r == nil {
		panic("handleDLQUndoDiscard: r is nil")
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if cfg.ReadOnly {
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionDLQUndoDiscard, seqStr, OutcomeDenied,
				map[string]any{"reason": "read_only"}))
		writeReadOnly(w)
		return
	}
	seq, err := parseDLQSequence(seqStr)
	if err != nil {
		http.Error(w, "invalid sequence", http.StatusBadRequest)
		return
	}
	token := r.Header.Get("X-Undo-Token")
	if token == "" {
		http.Error(w, "missing undo token", http.StatusBadRequest)
		return
	}
	tomb := cfg.tombstones()
	if tomb == nil || !tomb.Undo(seq, token) {
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionDLQUndoDiscard, seqStr, OutcomeFailed,
				map[string]any{"reason": "expired_or_invalid"}))
		http.Error(w, "undo window closed", http.StatusGone)
		return
	}
	emitAuditBestEffort(r.Context(), cfg,
		attemptOf(r, ActionDLQUndoDiscard, seqStr, OutcomeSuccess, nil))
	writeActionOK(w, undoSuccessBody(seq))
}

// undoSuccessBody returns the JSON body for a successful undo.
func undoSuccessBody(seq uint64) []byte {
	const tmpl = `{"ok":true,"action":"undo-discard","seq":%d,` +
		`"toast":{"level":"info","message":"Discard cancelled for #%d"}}`
	return []byte(fmt.Sprintf(tmpl, seq, seq))
}

// handleDLQRetry executes a POST /console/dlq/<seq>/retry. ReadOnly
// short-circuits to 405; otherwise we call ReplayDeadLetter and emit
// an audit event with the outcome.
func handleDLQRetry(
	w http.ResponseWriter, r *http.Request,
	cfg Config, seqStr string,
) {
	if w == nil {
		panic("handleDLQRetry: w is nil")
	}
	if r == nil {
		panic("handleDLQRetry: r is nil")
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if cfg.ReadOnly {
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionDLQRetry, seqStr, OutcomeDenied,
				map[string]any{"reason": "read_only"}))
		writeReadOnly(w)
		return
	}
	seq, err := parseDLQSequence(seqStr)
	if err != nil {
		http.Error(w, "invalid sequence", http.StatusBadRequest)
		return
	}
	ds, ok := requireData(w, cfg, "dlq-retry")
	if !ok {
		return
	}
	if err := ds.ReplayDeadLetter(r.Context(), seq); err != nil {
		cfg.Logger.Error("console: dlq replay", "seq", seq, "err", err)
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionDLQRetry, seqStr, OutcomeFailed,
				map[string]any{"error": err.Error()}))
		http.Error(w, "retry failed: "+err.Error(),
			http.StatusInternalServerError)
		return
	}
	// Best-effort discard of the original entry — the replay puts a
	// new task message in flight so leaving the DLQ row around would
	// confuse the operator. Failure to discard is non-fatal; we surface
	// it in the audit row.
	discardErr := ds.DiscardDeadLetter(r.Context(), seq)
	data := map[string]any{}
	if discardErr != nil &&
		!errors.Is(discardErr, errAlreadyGone) {
		data["discard_warning"] = discardErr.Error()
	}
	emitAuditBestEffort(r.Context(), cfg,
		attemptOf(r, ActionDLQRetry, seqStr, OutcomeSuccess, data))
	publishDLQRemoval(cfg, seqStr)
	writeActionOK(w, retrySuccessBody(seq))
}

// retrySuccessBody returns the JSON body for a successful retry.
// Includes the discarded sequence so the client toast can announce
// what disappeared from the list.
func retrySuccessBody(seq uint64) []byte {
	const tmpl = `{"ok":true,"action":"retry","seq":%d,` +
		`"toast":{"level":"info","message":"Retry queued — original ` +
		`DLQ entry #%d cleared"}}`
	return []byte(fmt.Sprintf(tmpl, seq, seq))
}

// errAlreadyGone is a marker for the discard-after-retry path. The
// replay-then-delete sequence sometimes races with the engine's own
// cleanup, and the resulting "no message" error is benign.
var errAlreadyGone = errors.New("dlq entry already gone")

// publishDLQRemoval emits a row.remove event on the event bus so any
// open SSE stream on /console/sse/dlq patches the row out without
// the operator having to refresh. Best-effort — no bus means no
// event, mutation succeeded anyway.
func publishDLQRemoval(cfg Config, seqStr string) {
	if cfg.bus == nil {
		return
	}
	cfg.bus.publish(busEventDLQRemove(seqStr))
}

// handleDLQDiscard executes a POST /console/dlq/<seq>/discard. With
// the soft-discard tombstone store configured (default), the entry is
// flagged as pending-removal and an undo token is issued. The toast
// shows an Undo button; a sweeper goroutine permanently removes the
// entry once the window elapses without an undo POST.
//
// Without the tombstone store (legacy path), discard removes the
// entry immediately — matches PR 4 behaviour for back-compat.
func handleDLQDiscard(
	w http.ResponseWriter, r *http.Request,
	cfg Config, seqStr string,
) {
	if w == nil {
		panic("handleDLQDiscard: w is nil")
	}
	if r == nil {
		panic("handleDLQDiscard: r is nil")
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if cfg.ReadOnly {
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionDLQDiscard, seqStr, OutcomeDenied,
				map[string]any{"reason": "read_only"}))
		writeReadOnly(w)
		return
	}
	seq, err := parseDLQSequence(seqStr)
	if err != nil {
		http.Error(w, "invalid sequence", http.StatusBadRequest)
		return
	}
	ds, ok := requireData(w, cfg, "dlq-discard")
	if !ok {
		return
	}
	if tomb := cfg.tombstones(); tomb != nil {
		executeDLQSoftDiscard(w, r, cfg, ds, seq, seqStr, tomb)
		return
	}
	executeDLQHardDiscard(w, r, cfg, ds, seq, seqStr)
}

// executeDLQSoftDiscard tombstones the entry and returns the undo
// token. The audit row records "outcome=success, soft=true" so the
// log makes the deferred removal visible.
func executeDLQSoftDiscard(
	w http.ResponseWriter, r *http.Request, cfg Config,
	ds DataSource, seq uint64, seqStr string,
	tomb *dlqTombstoneStore,
) {
	if ds == nil {
		panic("executeDLQSoftDiscard: ds is nil")
	}
	if tomb == nil {
		panic("executeDLQSoftDiscard: tomb is nil")
	}
	token, expires := tomb.Tombstone(seq)
	emitAuditBestEffort(r.Context(), cfg,
		attemptOf(r, ActionDLQDiscard, seqStr, OutcomeSuccess,
			map[string]any{"soft": true, "expires": expires.UTC()}))
	writeActionOK(w, softDiscardBody(seq, token))
}

// executeDLQHardDiscard runs the legacy non-undoable discard for
// installations that haven't enabled the tombstone store.
func executeDLQHardDiscard(
	w http.ResponseWriter, r *http.Request, cfg Config,
	ds DataSource, seq uint64, seqStr string,
) {
	if ds == nil {
		panic("executeDLQHardDiscard: ds is nil")
	}
	if err := ds.DiscardDeadLetter(r.Context(), seq); err != nil {
		cfg.Logger.Error("console: dlq discard", "seq", seq, "err", err)
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionDLQDiscard, seqStr, OutcomeFailed,
				map[string]any{"error": err.Error()}))
		http.Error(w, "discard failed: "+err.Error(),
			http.StatusInternalServerError)
		return
	}
	emitAuditBestEffort(r.Context(), cfg,
		attemptOf(r, ActionDLQDiscard, seqStr, OutcomeSuccess, nil))
	writeActionOK(w, discardSuccessBody(seq))
}

// discardSuccessBody returns the JSON body for a hard discard.
func discardSuccessBody(seq uint64) []byte {
	const tmpl = `{"ok":true,"action":"discard","seq":%d,` +
		`"toast":{"level":"info","message":"Entry #%d discarded"}}`
	return []byte(fmt.Sprintf(tmpl, seq, seq))
}

// softDiscardBody returns the JSON body for a soft discard. The toast
// fields carry the undo affordance: undoToken pinned to /undo-discard,
// undoHref the target POST path. The front-end renders an Undo button
// that holds the message visible for the 5-second window.
func softDiscardBody(seq uint64, token string) []byte {
	const tmpl = `{"ok":true,"action":"discard","seq":%d,` +
		`"toast":{"level":"info",` +
		`"message":"Discarded #%d — undo within 5s",` +
		`"undoToken":%q,"undoHref":"/console/dlq/%d/undo-discard"}}`
	return []byte(fmt.Sprintf(tmpl, seq, seq, token, seq))
}

// writeActionOK is the common 200-OK + JSON write path for mutation
// handlers. Centralising the response shape keeps every action's
// success body shaped the same way, which the client-side toast wire
// also depends on.
func writeActionOK(w http.ResponseWriter, body []byte) {
	if w == nil {
		panic("writeActionOK: w is nil")
	}
	if len(body) == 0 {
		panic("writeActionOK: body is empty")
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// actionAttempt returns a populated AuditEvent for one operator
// action. Pulled out so the action handlers stay readable.
//
// The string-action overload exists for legacy paths that haven't
// migrated to the typed constants yet. Prefer attemptOf for new code.
func actionAttempt(
	r *http.Request, action, target, outcome string,
	data map[string]any,
) AuditEvent {
	if r == nil {
		panic("actionAttempt: r is nil")
	}
	if action == "" {
		panic("actionAttempt: action is empty")
	}
	if outcome == "" {
		panic("actionAttempt: outcome is empty")
	}
	actor, _ := ActorFrom(r.Context())
	return AuditEvent{
		Time:    time.Now().UTC(),
		Actor:   actor.Display(),
		Action:  action,
		Target:  target,
		Outcome: outcome,
		Data:    data,
	}
}

// attemptOf is the typed-constants entry point. Every new call site
// builds AuditEvents through this function rather than the legacy
// string-arg actionAttempt. Behaviour is identical; the typed
// signature makes the constants table the single source of truth.
func attemptOf(
	r *http.Request, action AuditAction, target string,
	outcome AuditOutcome, data map[string]any,
) AuditEvent {
	if r == nil {
		panic("attemptOf: r is nil")
	}
	if action == "" {
		panic("attemptOf: action is empty")
	}
	if outcome == "" {
		panic("attemptOf: outcome is empty")
	}
	return actionAttempt(r, action.String(), target, outcome.String(), data)
}

// emitAuditBestEffort writes one audit event via the configured
// DataSource. Failures are slog.Warn'd and dropped — audit gaps must
// never block the response the operator is waiting on.
//
// Phase 2 T07: when cfg.bus is attached, also publishes a TopicAudit
// event so the dashboard SSE handler can patch the recent-actions
// panel without a refresh. The publish is best-effort + non-blocking
// — bus saturation drops the event with a slog.Warn upstream.
func emitAuditBestEffort(
	ctx context.Context, cfg Config, evt AuditEvent,
) {
	if cfg.Data == nil {
		return
	}
	if err := cfg.Data.EmitAuditEvent(ctx, evt); err != nil {
		cfg.Logger.Warn("console: audit emit failed",
			"action", evt.Action, "target", evt.Target,
			"outcome", evt.Outcome, "err", err)
	}
	if cfg.bus != nil {
		cfg.bus.publish(busEventAuditRecorded(evt.Action + "/" + evt.Target))
	}
}

// readOnlyMiddleware wraps next so every POST/PUT/PATCH/DELETE under
// /console/* returns 405 + the standard read-only body when active.
// Asset GETs, page GETs, and SSE GETs pass through unchanged. The
// middleware lets POST/etc. fall through to the per-action handlers
// FIRST so they get a chance to emit an audit-denied row that
// captures the action name. If a handler doesn't intercept the
// mutation (i.e. unknown route), this wrapper covers the case by
// returning 405 at the end. Each per-action handler is responsible
// for re-checking cfg.ReadOnly and short-circuiting itself; the
// middleware here is a defence-in-depth net.
func readOnlyMiddleware(active bool, next http.Handler) http.Handler {
	if next == nil {
		panic("readOnlyMiddleware: next is nil")
	}
	if !active {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if w == nil {
			panic("readOnlyMiddleware: w is nil")
		}
		if r == nil {
			panic("readOnlyMiddleware: r is nil")
		}
		switch r.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
			next.ServeHTTP(w, r)
			return
		}
		// Mutation methods: dispatch to the per-action handler so it
		// can emit a "denied" audit, then short-circuit ourselves if
		// the handler hasn't already responded. The recorder captures
		// what the handler wrote; if it wrote a 405 already, we pass
		// it through; otherwise we override with the canonical 405.
		rec := newReadOnlyRecorder(w)
		next.ServeHTTP(rec, r)
		if !rec.wrote {
			writeReadOnly(w)
		}
	})
}

// readOnlyRecorder records whether the inner handler wrote anything.
// Used by readOnlyMiddleware so the wrapper doesn't double-write to
// w when the per-action handler already emitted a 405.
type readOnlyRecorder struct {
	http.ResponseWriter
	wrote bool
}

func newReadOnlyRecorder(w http.ResponseWriter) *readOnlyRecorder {
	if w == nil {
		panic("newReadOnlyRecorder: w is nil")
	}
	return &readOnlyRecorder{ResponseWriter: w}
}

func (r *readOnlyRecorder) WriteHeader(code int) {
	r.wrote = true
	r.ResponseWriter.WriteHeader(code)
}

func (r *readOnlyRecorder) Write(b []byte) (int, error) {
	r.wrote = true
	return r.ResponseWriter.Write(b)
}

// ReadOnlyFromEnv reads the CONSOLE_READ_ONLY env var and returns true
// when set to a truthy value ("1", "true", "yes"). Centralising the
// parse keeps the env-var contract single-source-of-truth.
func ReadOnlyFromEnv(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}
