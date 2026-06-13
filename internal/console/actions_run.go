package console

// actions_run.go owns the POST /console/runs/{id}/cancel and
// /console/runs/{id}/signal handlers — the run-detail Signal + Cancel
// action bar wired to the existing engine api.Service.CancelRun /
// SendSignal. No new engine code: both methods already exist.
//
// Both follow the standard mutation-handler scaffold the trigger
// surface established: parse -> read-only check -> resolve -> validate
// -> execute -> audit -> respond. Each branch emits exactly one audit
// row (denied / failed / success) so the audit log captures every
// operator attempt.
//
// Honesty rules enforced here:
//   - Cancel and Signal are both gated on run terminality. A terminal
//     run cannot be cancelled and a signal to it would write to a KV
//     nothing will read again; the template hides both buttons and the
//     handler re-checks (409) so a forged POST can't drive a dead
//     affordance. IsTerminal() is the single source for both rules.
//   - The run-existence check (404) is a console UX gate, not an engine
//     guarantee: SendSignal writes to {runID}.{name} unconditionally and
//     has no notion of run existence. We GetRun first so an operator
//     signalling a typo'd run gets an honest 404 instead of a write that
//     vanishes into KV.
//   - Cancellation is asynchronous (the engine only publishes an event),
//     so the success toast says "requested", never "cancelled".
//
// Response bodies use the marshalled actionBody envelope (not
// fmt.Sprintf) so a run id / signal name carrying a quote or backslash
// can't corrupt the JSON — the same hardening the Trigger CRUD path
// adopted after a JSON-injection finding.

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
)

// handleRunCancel dispatches POST /console/runs/{id}/cancel.
func handleRunCancel(
	w http.ResponseWriter, r *http.Request, cfg Config, runID string,
) {
	if w == nil {
		panic("handleRunCancel: w is nil")
	}
	if r == nil {
		panic("handleRunCancel: r is nil")
	}
	if runID == "" {
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
			attemptOf(r, ActionRunCancel, runID, OutcomeDenied,
				map[string]any{"reason": "read_only"}))
		writeReadOnly(w)
		return
	}
	executeRunCancel(w, r, cfg, runID)
}

// executeRunCancel resolves the run, enforces non-terminality, calls
// CancelRun, and writes the response + audit. Pulled out so the outer
// handler stays under the 70-line cap.
func executeRunCancel(
	w http.ResponseWriter, r *http.Request, cfg Config, runID string,
) {
	ds, ok := requireData(w, cfg, "run-cancel")
	if !ok {
		return
	}
	run, err := ds.GetRun(r.Context(), runID)
	if err != nil {
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionRunCancel, runID, OutcomeFailed,
				map[string]any{"reason": "not_found"}))
		http.NotFound(w, r)
		return
	}
	if run.Status.IsTerminal() {
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionRunCancel, runID, OutcomeDenied,
				map[string]any{"reason": "terminal"}))
		http.Error(w, "run is already terminal", http.StatusConflict)
		return
	}
	if err := ds.CancelRun(r.Context(), runID); err != nil {
		cfg.Logger.Error("console: run cancel", "run", runID, "err", err)
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionRunCancel, runID, OutcomeFailed,
				map[string]any{"error": err.Error()}))
		http.Error(w, "cancel failed: "+err.Error(),
			http.StatusInternalServerError)
		return
	}
	emitAuditBestEffort(r.Context(), cfg,
		attemptOf(r, ActionRunCancel, runID, OutcomeSuccess, nil))
	writeActionOK(w, runCancelBody(runID))
}

// handleRunSignal dispatches POST /console/runs/{id}/signal.
func handleRunSignal(
	w http.ResponseWriter, r *http.Request, cfg Config, runID string,
) {
	if w == nil {
		panic("handleRunSignal: w is nil")
	}
	if r == nil {
		panic("handleRunSignal: r is nil")
	}
	if runID == "" {
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
			attemptOf(r, ActionRunSignal, runID, OutcomeDenied,
				map[string]any{"reason": "read_only"}))
		writeReadOnly(w)
		return
	}
	if err := r.ParseForm(); err != nil {
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionRunSignal, runID, OutcomeFailed,
				map[string]any{"reason": "invalid"}))
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	executeRunSignal(w, r, cfg, runID)
}

// executeRunSignal validates the (name, payload) pair, resolves the run,
// enforces non-terminality, calls SendSignal, and writes the response +
// audit. Pulled out so handleRunSignal stays under the 70-line cap.
func executeRunSignal(
	w http.ResponseWriter, r *http.Request, cfg Config, runID string,
) {
	name := strings.TrimSpace(r.Form.Get("name"))
	if name == "" {
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionRunSignal, runID, OutcomeFailed,
				map[string]any{"reason": "invalid"}))
		http.Error(w, "signal name is required", http.StatusBadRequest)
		return
	}
	payload := r.Form.Get("data")
	if payload != "" && !json.Valid([]byte(payload)) {
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionRunSignal, runID, OutcomeFailed,
				map[string]any{"reason": "invalid_json"}))
		http.Error(w, "signal payload is not valid JSON",
			http.StatusBadRequest)
		return
	}
	ds, ok := requireData(w, cfg, "run-signal")
	if !ok {
		return
	}
	run, err := ds.GetRun(r.Context(), runID)
	if err != nil {
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionRunSignal, runID, OutcomeFailed,
				map[string]any{"reason": "not_found"}))
		http.NotFound(w, r)
		return
	}
	if run.Status.IsTerminal() {
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionRunSignal, runID, OutcomeDenied,
				map[string]any{"reason": "terminal"}))
		http.Error(w, "run is already terminal", http.StatusConflict)
		return
	}
	finishRunSignal(w, r, cfg, ds, runID, name, payload)
}

// finishRunSignal performs the SendSignal call + audit + response once
// validation and resolution have passed. Split from executeRunSignal to
// keep both under the function-length cap.
func finishRunSignal(
	w http.ResponseWriter, r *http.Request, cfg Config,
	ds DataSource, runID, name, payload string,
) {
	if err := ds.SendSignal(r.Context(), runID, name, []byte(payload)); err != nil {
		cfg.Logger.Error("console: run signal",
			"run", runID, "signal", name, "err", err)
		emitAuditBestEffort(r.Context(), cfg,
			attemptOf(r, ActionRunSignal, runID, OutcomeFailed,
				map[string]any{"error": err.Error()}))
		http.Error(w, "signal failed: "+err.Error(),
			http.StatusInternalServerError)
		return
	}
	emitAuditBestEffort(r.Context(), cfg,
		attemptOf(r, ActionRunSignal, runID, OutcomeSuccess,
			map[string]any{"signal_name": name}))
	writeActionOK(w, runSignalBody(runID, name))
}

// runCancelBody returns the success JSON for a cancel. The toast copy
// says "requested" because cancelRunInner only publishes an event — the
// run is not yet stopped when this returns.
func runCancelBody(runID string) []byte {
	return marshalActionBody(actionBody{
		OK: true, Action: "cancel", ID: runID,
		Toast: actionToast{
			Level:   "info",
			Message: "Cancellation requested for " + shortRunID(runID),
			Href:    "/console/runs/" + url.PathEscape(runID),
		},
	})
}

// runSignalBody returns the success JSON for a sent signal.
func runSignalBody(runID, name string) []byte {
	return marshalActionBody(actionBody{
		OK: true, Action: "signal", ID: runID,
		Toast: actionToast{
			Level:   "info",
			Message: "Sent signal " + name + " to " + shortRunID(runID),
			Href:    "/console/runs/" + url.PathEscape(runID),
		},
	})
}
