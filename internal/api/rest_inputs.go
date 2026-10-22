// internal/api/rest_inputs.go
// REST transport of the input endpoints (ADR-027):
//
//	POST   /runs/{run}/steps/{step}/inputs                → submit
//	GET    /runs/{run}/steps/{step}/inputs/{request_id}   → status (long-poll: ?wait=30s&after=queued)
//	DELETE /runs/{run}/steps/{step}/inputs/{request_id}   → withdraw while queued
//
// Reached from routeRunByID; the body is bounded like a signal's.
package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/danmestas/dagnats/inputs"
	"github.com/danmestas/dagnats/internal/engine"
)

// inputBodyMax bounds a submission body: the bucket's own value cap.
const inputBodyMax = 1 << 20

// routeStepInputs dispatches the /runs/{run}/steps/{step}/inputs
// paths; parts is the path split after "/runs/".
func (s *Service) routeStepInputs(w http.ResponseWriter, r *http.Request, parts []string) {
	if len(parts) < 4 || parts[1] != "steps" || parts[3] != "inputs" {
		panic("routeStepInputs: caller must match /runs/{run}/steps/{step}/inputs")
	}
	if r == nil {
		panic("routeStepInputs: r must not be nil")
	}
	runID, stepID := parts[0], parts[2]
	if runID == "" || stepID == "" {
		http.Error(w, "missing run or step ID", http.StatusBadRequest)
		return
	}
	switch {
	case len(parts) == 4 && r.Method == http.MethodPost:
		handleSubmitInput(s, w, r, runID, stepID)
	case len(parts) == 5 && parts[4] != "" && r.Method == http.MethodGet:
		handleGetInput(s, w, r, runID, stepID, parts[4])
	case len(parts) == 5 && parts[4] != "" && r.Method == http.MethodDelete:
		handleWithdrawInput(s, w, r, runID, stepID, parts[4])
	case len(parts) == 4 || len(parts) == 5:
		w.WriteHeader(http.StatusMethodNotAllowed)
	default:
		http.NotFound(w, r)
	}
}

// handleSubmitInput decodes the body and creates the submission: 201
// when created, 200 for a repeat of the same request_id.
func handleSubmitInput(
	svc *Service, w http.ResponseWriter, r *http.Request, runID, stepID string,
) {
	if svc == nil {
		panic("handleSubmitInput: svc must not be nil")
	}
	if runID == "" || stepID == "" {
		panic("handleSubmitInput: run and step must not be empty")
	}
	var req InputRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, inputBodyMax)).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	sub, created, err := svc.SubmitInput(r.Context(), runID, stepID, req)
	if err != nil {
		writeInputError(w, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, sub)
}

// handleGetInput answers the status, after an optional long-poll
// (?wait=30s, ?after=queued: return once the status is not after).
func handleGetInput(
	svc *Service, w http.ResponseWriter, r *http.Request, runID, stepID, requestID string,
) {
	if svc == nil {
		panic("handleGetInput: svc must not be nil")
	}
	if requestID == "" {
		panic("handleGetInput: requestID must not be empty")
	}
	var wait time.Duration
	if raw := r.URL.Query().Get("wait"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 {
			http.Error(w, "wait must be a positive duration", http.StatusBadRequest)
			return
		}
		wait = parsed
	}
	after := inputs.Status(r.URL.Query().Get("after"))
	if after == "" {
		after = inputs.StatusQueued
	}
	sub, err := svc.GetInput(r.Context(), runID, stepID, requestID, after, wait)
	if err != nil {
		writeInputError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sub)
}

// handleWithdrawInput deletes a queued submission: 204.
func handleWithdrawInput(
	svc *Service, w http.ResponseWriter, r *http.Request, runID, stepID, requestID string,
) {
	if svc == nil {
		panic("handleWithdrawInput: svc must not be nil")
	}
	if requestID == "" {
		panic("handleWithdrawInput: requestID must not be empty")
	}
	if err := svc.WithdrawInput(r.Context(), runID, stepID, requestID); err != nil {
		writeInputError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// writeInputError maps the service's errors to status codes.
func writeInputError(w http.ResponseWriter, err error) {
	if err == nil {
		panic("writeInputError: err must not be nil")
	}
	if w == nil {
		panic("writeInputError: w must not be nil")
	}
	http.Error(w, err.Error(), inputErrorStatus(err))
}

// inputErrorStatus is the status code for a service error, shared by
// the REST and micro transports (the latter as inputErrorKind).
func inputErrorStatus(err error) int {
	switch {
	case errors.Is(err, inputs.ErrInvalidRequestID), errors.Is(err, inputs.ErrInvalidMode),
		errors.Is(err, inputs.ErrInvalidContent), errors.Is(err, inputs.ErrContentTooLarge):
		return http.StatusBadRequest
	case errors.Is(err, engine.ErrRunNotFound), errors.Is(err, ErrStepNotFound),
		errors.Is(err, inputs.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrStepFinished), errors.Is(err, inputs.ErrNotQueued),
		errors.Is(err, inputs.ErrConflict):
		return http.StatusConflict
	case errors.Is(err, inputs.ErrNotProvisioned):
		return http.StatusServiceUnavailable
	}
	return http.StatusInternalServerError
}

// inputErrorKind is the micro reply's "kind" for a service error.
func inputErrorKind(err error) string {
	switch inputErrorStatus(err) {
	case http.StatusBadRequest:
		return "invalid"
	case http.StatusNotFound:
		return "not_found"
	case http.StatusConflict:
		return "conflict"
	case http.StatusServiceUnavailable:
		return "unavailable"
	}
	return "internal"
}

// isStepInputsPath reports whether parts (the path after "/runs/")
// addresses a step's inputs.
func isStepInputsPath(parts []string) bool {
	if parts == nil {
		panic("isStepInputsPath: parts must not be nil")
	}
	if len(parts) > 0 && strings.Contains(parts[0], "/") {
		panic("isStepInputsPath: parts must be split on '/'")
	}
	return len(parts) >= 4 && parts[1] == "steps" && parts[3] == "inputs"
}
