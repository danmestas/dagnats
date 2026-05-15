package console

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"
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
	default:
		serveNotFound(w, r, ts, cfg)
	}
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
			actionAttempt(r, "dlq.retry", seqStr, "denied",
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
			actionAttempt(r, "dlq.retry", seqStr, "failed",
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
		actionAttempt(r, "dlq.retry", seqStr, "success", data))
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok":true}`))
}

// errAlreadyGone is a marker for the discard-after-retry path. The
// replay-then-delete sequence sometimes races with the engine's own
// cleanup, and the resulting "no message" error is benign.
var errAlreadyGone = errors.New("dlq entry already gone")

// handleDLQDiscard executes a POST /console/dlq/<seq>/discard. Removes
// the DLQ entry permanently without re-injecting.
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
			actionAttempt(r, "dlq.discard", seqStr, "denied",
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
	if err := ds.DiscardDeadLetter(r.Context(), seq); err != nil {
		cfg.Logger.Error("console: dlq discard", "seq", seq, "err", err)
		emitAuditBestEffort(r.Context(), cfg,
			actionAttempt(r, "dlq.discard", seqStr, "failed",
				map[string]any{"error": err.Error()}))
		http.Error(w, "discard failed: "+err.Error(),
			http.StatusInternalServerError)
		return
	}
	emitAuditBestEffort(r.Context(), cfg,
		actionAttempt(r, "dlq.discard", seqStr, "success", nil))
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok":true}`))
}

// actionAttempt returns a populated AuditEvent for one operator
// action. Pulled out so the action handlers stay readable.
func actionAttempt(
	r *http.Request, action, target, outcome string,
	data map[string]any,
) AuditEvent {
	if r == nil {
		panic("actionAttempt: r is nil")
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

// emitAuditBestEffort writes one audit event via the configured
// DataSource. Failures are slog.Warn'd and dropped — audit gaps must
// never block the response the operator is waiting on.
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
