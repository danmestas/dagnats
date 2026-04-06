// api/rest.go
// REST HTTP handlers for the DagNats control plane.
// Routes delegate to Service methods; transport concerns (status codes, JSON
// encoding) live here so Service stays transport-agnostic.
package api

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/nats-io/nats.go/jetstream"
)

// startRunRequest is the JSON body expected by POST /runs.
type startRunRequest struct {
	Workflow string          `json:"workflow"`
	Input    json.RawMessage `json:"input,omitempty"`
	RunAt    *time.Time      `json:"run_at,omitempty"`
}

// NewRESTHandler returns an http.Handler that routes the DagNats
// control-plane REST API. Panics if svc is nil.
func NewRESTHandler(svc *Service) http.Handler {
	if svc == nil {
		panic("NewRESTHandler: svc must not be nil")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/workflows", svc.routeWorkflows)
	mux.HandleFunc("/runs", svc.routeRuns)
	mux.HandleFunc("/runs/cancel", svc.routeBulkCancel)
	mux.HandleFunc("/runs/bulk", svc.routeBulkRun)
	mux.HandleFunc("/runs/retry", svc.routeBulkRetry)
	mux.HandleFunc("/runs/", svc.routeRunByID)
	mux.HandleFunc("/health/telemetry", svc.routeHealth)
	return mux
}

// routeWorkflows dispatches GET and POST /workflows.
func (s *Service) routeWorkflows(
	w http.ResponseWriter, r *http.Request,
) {
	switch r.Method {
	case http.MethodGet:
		handleListWorkflows(s, w, r)
	case http.MethodPost:
		handleRegisterWorkflow(s, w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// routeRuns dispatches GET and POST /runs.
func (s *Service) routeRuns(
	w http.ResponseWriter, r *http.Request,
) {
	switch r.Method {
	case http.MethodGet:
		handleListRuns(s, w, r)
	case http.MethodPost:
		handleStartRun(s, w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// routeRunByID dispatches /runs/{id} sub-paths:
//
//	GET  /runs/{id}              → handleGetRun
//	POST /runs/{id}/cancel       → handleCancelRun
//	POST /runs/{id}/signal/{name} → handleSendSignal
func (s *Service) routeRunByID(
	w http.ResponseWriter, r *http.Request,
) {
	parts := strings.Split(
		strings.TrimPrefix(r.URL.Path, "/runs/"), "/",
	)
	// parts[0] = runID, parts[1] = sub-action (optional)
	if len(parts) >= 2 && parts[1] == "cancel" {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handleCancelRun(s, w, r)
		return
	}
	if len(parts) >= 3 && parts[1] == "signal" {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handleSendSignal(s, w, r)
		return
	}
	if len(parts) >= 3 && parts[1] == "approval" {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handleApproval(s, w, r)
		return
	}
	if len(parts) >= 2 && parts[1] == "scheduled" {
		switch r.Method {
		case http.MethodGet:
			handleGetScheduledRun(s, w, r)
		case http.MethodDelete:
			handleCancelScheduledRun(s, w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
		return
	}
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	handleGetRun(s, w, r)
}

// routeHealth dispatches GET /health to handleHealth.
func (s *Service) routeHealth(
	w http.ResponseWriter, r *http.Request,
) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	handleHealth(s, w, r)
}

// handleRegisterWorkflow decodes a WorkflowDef from the request body
// and persists it via Service.RegisterWorkflow. Returns 201 on success.
func handleRegisterWorkflow(
	svc *Service, w http.ResponseWriter, r *http.Request,
) {
	if svc == nil {
		panic("handleRegisterWorkflow: svc must not be nil")
	}
	if r == nil {
		panic("handleRegisterWorkflow: r must not be nil")
	}
	var def dag.WorkflowDef
	if err := json.NewDecoder(r.Body).Decode(&def); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(),
			http.StatusBadRequest)
		return
	}
	if err := svc.RegisterWorkflow(r.Context(), def); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	encErr := json.NewEncoder(w).Encode(
		map[string]string{"status": "registered", "name": def.Name},
	)
	if encErr != nil {
		slog.Error("encode response", "error", encErr)
	}
}

// handleListWorkflows returns all registered workflow definitions as
// a JSON array. Returns 200 on success.
func handleListWorkflows(
	svc *Service, w http.ResponseWriter, r *http.Request,
) {
	if svc == nil {
		panic("handleListWorkflows: svc must not be nil")
	}
	if r == nil {
		panic("handleListWorkflows: r must not be nil")
	}
	defs, err := svc.ListWorkflows(r.Context())
	if err != nil {
		http.Error(w, err.Error(),
			http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	encErr := json.NewEncoder(w).Encode(defs)
	if encErr != nil {
		slog.Error("encode response", "error", encErr)
	}
}

// handleListRuns returns all workflow runs as a JSON array.
// Supports optional ?workflow= query parameter for filtering.
func handleListRuns(
	svc *Service, w http.ResponseWriter, r *http.Request,
) {
	if svc == nil {
		panic("handleListRuns: svc must not be nil")
	}
	if r == nil {
		panic("handleListRuns: r must not be nil")
	}
	workflowFilter := r.URL.Query().Get("workflow")
	runs, err := svc.ListRuns(r.Context(), workflowFilter)
	if err != nil {
		http.Error(w, err.Error(),
			http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	encErr := json.NewEncoder(w).Encode(runs)
	if encErr != nil {
		slog.Error("encode response", "error", encErr)
	}
}

// handleStartRun decodes a startRunRequest and calls StartRun or
// ScheduleRun based on the run_at field. Returns 201 on success.
func handleStartRun(
	svc *Service, w http.ResponseWriter, r *http.Request,
) {
	if svc == nil {
		panic("handleStartRun: svc must not be nil")
	}
	if r == nil {
		panic("handleStartRun: r must not be nil")
	}
	var req startRunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(),
			http.StatusBadRequest)
		return
	}

	// Scheduled run path: run_at within 1 second of now is treated
	// as immediate (spec: "in the past or within 1 second").
	const immediateThreshold = time.Second
	if req.RunAt != nil &&
		time.Until(*req.RunAt) > immediateThreshold {
		runID, err := svc.ScheduleRun(
			r.Context(), req.Workflow, req.Input, *req.RunAt,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{
			"run_id": runID,
			"status": "scheduled",
		})
		return
	}

	// Immediate run path (existing).
	runID, err := svc.StartRun(
		r.Context(), req.Workflow, req.Input,
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	encErr := json.NewEncoder(w).Encode(
		map[string]string{"run_id": runID},
	)
	if encErr != nil {
		slog.Error("encode response", "error", encErr)
	}
}

// handleGetRun extracts the run ID from the URL path and returns the
// current run snapshot as JSON. Returns 404 when run does not exist.
func handleGetRun(
	svc *Service, w http.ResponseWriter, r *http.Request,
) {
	if svc == nil {
		panic("handleGetRun: svc must not be nil")
	}
	if r == nil {
		panic("handleGetRun: r must not be nil")
	}
	parts := strings.Split(
		strings.TrimPrefix(r.URL.Path, "/runs/"), "/",
	)
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "missing run ID", http.StatusBadRequest)
		return
	}
	runID := parts[0]
	run, err := svc.GetRun(r.Context(), runID)
	if err != nil {
		if errors.Is(err, engine.ErrRunNotFound) {
			http.Error(w, "run not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	encErr := json.NewEncoder(w).Encode(run)
	if encErr != nil {
		slog.Error("encode response", "error", encErr)
	}
}

// handleCancelRun extracts the run ID and publishes a cancel event.
func handleCancelRun(
	svc *Service, w http.ResponseWriter, r *http.Request,
) {
	if svc == nil {
		panic("handleCancelRun: svc must not be nil")
	}
	if r == nil {
		panic("handleCancelRun: r must not be nil")
	}
	parts := strings.Split(
		strings.TrimPrefix(r.URL.Path, "/runs/"), "/",
	)
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "missing run ID", http.StatusBadRequest)
		return
	}
	runID := parts[0]
	if err := svc.CancelRun(r.Context(), runID); err != nil {
		http.Error(w, err.Error(),
			http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	encErr := json.NewEncoder(w).Encode(
		map[string]string{"status": "cancelled", "run_id": runID},
	)
	if encErr != nil {
		slog.Error("encode response", "error", encErr)
	}
}

// handleSendSignal extracts run ID and signal name from the path,
// reads the request body as signal data, and writes to KV.
func handleSendSignal(
	svc *Service, w http.ResponseWriter, r *http.Request,
) {
	if svc == nil {
		panic("handleSendSignal: svc must not be nil")
	}
	if r == nil {
		panic("handleSendSignal: r must not be nil")
	}
	parts := strings.Split(
		strings.TrimPrefix(r.URL.Path, "/runs/"), "/",
	)
	if len(parts) < 3 || parts[2] == "" {
		http.Error(w, "missing signal name",
			http.StatusBadRequest)
		return
	}
	runID := parts[0]
	signalName := parts[2]

	var data []byte
	if r.Body != nil {
		const maxSignalBytes = 1 << 20 // 1 MiB
		limited := io.LimitReader(r.Body, maxSignalBytes)
		var readErr error
		data, readErr = io.ReadAll(limited)
		if readErr != nil {
			http.Error(w, "read body: "+readErr.Error(),
				http.StatusBadRequest)
			return
		}
	}

	err := svc.SendSignal(r.Context(), runID, signalName, data)
	if err != nil {
		http.Error(w, err.Error(),
			http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	encErr := json.NewEncoder(w).Encode(
		map[string]string{
			"status": "sent", "run_id": runID,
			"signal": signalName,
		},
	)
	if encErr != nil {
		slog.Error("encode response", "error", encErr)
	}
}

// handleApproval processes POST /runs/{id}/approval/{step_id}.
// Query params: action (approve/reject), token.
// Optional JSON body with comment, approved_by.
func handleApproval(
	svc *Service, w http.ResponseWriter, r *http.Request,
) {
	if svc == nil {
		panic("handleApproval: svc must not be nil")
	}
	if r == nil {
		panic("handleApproval: r must not be nil")
	}
	parts := strings.Split(
		strings.TrimPrefix(r.URL.Path, "/runs/"), "/",
	)
	if len(parts) < 3 || parts[2] == "" {
		http.Error(w, "missing step ID",
			http.StatusBadRequest)
		return
	}
	runID := parts[0]
	stepID := parts[2]
	token := r.URL.Query().Get("token")
	action := r.URL.Query().Get("action")

	var body json.RawMessage
	if r.Body != nil {
		const maxBody = 1 << 20 // 1 MiB
		limited := io.LimitReader(r.Body, maxBody)
		data, readErr := io.ReadAll(limited)
		if readErr != nil {
			http.Error(w, "read body: "+readErr.Error(),
				http.StatusBadRequest)
			return
		}
		if len(data) > 0 {
			body = data
		}
	}

	err := svc.HandleApproval(
		r.Context(), runID, stepID, token, action, body,
	)
	if err != nil {
		code := approvalErrorCode(err)
		http.Error(w, err.Error(), code)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(
		map[string]string{
			"status": action + "d",
			"run_id": runID,
			"step":   stepID,
		},
	)
}

// approvalErrorCode maps approval errors to HTTP status codes.
func approvalErrorCode(err error) int {
	if err == nil {
		panic("approvalErrorCode: err must not be nil")
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "invalid token"),
		strings.Contains(msg, "not found or expired"):
		return http.StatusUnauthorized
	case strings.Contains(msg, "already consumed"):
		return http.StatusConflict
	default:
		return http.StatusBadRequest
	}
}

// routeBulkCancel dispatches POST /runs/cancel.
func (s *Service) routeBulkCancel(
	w http.ResponseWriter, r *http.Request,
) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	handleBulkCancel(s, w, r)
}

// handleBulkCancel parses a BulkCancelRequest and cancels
// matching runs.
func handleBulkCancel(
	svc *Service, w http.ResponseWriter, r *http.Request,
) {
	if svc == nil {
		panic("handleBulkCancel: svc must not be nil")
	}
	if r == nil {
		panic("handleBulkCancel: r must not be nil")
	}
	var req BulkCancelRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(),
			http.StatusBadRequest)
		return
	}
	resp, err := svc.BulkCancelRuns(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	encErr := json.NewEncoder(w).Encode(resp)
	if encErr != nil {
		slog.Error("encode response", "error", encErr)
	}
}

// routeBulkRun dispatches POST /runs/bulk.
func (s *Service) routeBulkRun(
	w http.ResponseWriter, r *http.Request,
) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	handleBulkRun(s, w, r)
}

// handleBulkRun starts multiple workflow runs.
func handleBulkRun(
	svc *Service, w http.ResponseWriter, r *http.Request,
) {
	if svc == nil {
		panic("handleBulkRun: svc must not be nil")
	}
	if r == nil {
		panic("handleBulkRun: r must not be nil")
	}
	var req BulkRunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(),
			http.StatusBadRequest)
		return
	}
	resp, err := svc.BulkStartRuns(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	encErr := json.NewEncoder(w).Encode(resp)
	if encErr != nil {
		slog.Error("encode response", "error", encErr)
	}
}

// routeBulkRetry dispatches POST /runs/retry.
func (s *Service) routeBulkRetry(
	w http.ResponseWriter, r *http.Request,
) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	handleBulkRetry(s, w, r)
}

// handleBulkRetry retries failed workflow runs.
func handleBulkRetry(
	svc *Service, w http.ResponseWriter, r *http.Request,
) {
	if svc == nil {
		panic("handleBulkRetry: svc must not be nil")
	}
	if r == nil {
		panic("handleBulkRetry: r must not be nil")
	}
	var req BulkRetryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(),
			http.StatusBadRequest)
		return
	}
	resp, err := svc.BulkRetryRuns(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	encErr := json.NewEncoder(w).Encode(resp)
	if encErr != nil {
		slog.Error("encode response", "error", encErr)
	}
}

// healthResponse is the JSON body returned by GET /health.
type healthResponse struct {
	Status    string         `json:"status"`
	Telemetry *telemetryInfo `json:"telemetry,omitempty"`
}

// telemetryInfo carries stream and backend status for health.
type telemetryInfo struct {
	Stream *streamInfo `json:"stream,omitempty"`
}

// streamInfo carries TELEMETRY stream usage stats.
type streamInfo struct {
	Messages uint64  `json:"messages"`
	Bytes    uint64  `json:"bytes"`
	Percent  float64 `json:"percent"`
}

// handleGetScheduledRun returns a scheduled run by ID.
func handleGetScheduledRun(
	svc *Service, w http.ResponseWriter, r *http.Request,
) {
	if svc == nil {
		panic("handleGetScheduledRun: svc must not be nil")
	}
	parts := strings.Split(
		strings.TrimPrefix(r.URL.Path, "/runs/"), "/",
	)
	runID := parts[0]
	sr, err := svc.GetScheduledRun(runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(sr)
}

// handleCancelScheduledRun cancels a pending scheduled run.
func handleCancelScheduledRun(
	svc *Service, w http.ResponseWriter, r *http.Request,
) {
	if svc == nil {
		panic(
			"handleCancelScheduledRun: svc must not be nil",
		)
	}
	parts := strings.Split(
		strings.TrimPrefix(r.URL.Path, "/runs/"), "/",
	)
	runID := parts[0]
	err := svc.CancelScheduledRun(runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(
		map[string]string{"status": "cancelled"},
	)
}

// handleHealth returns service health and optional telemetry stream
// status. Never fails the health check -- telemetry is informational.
func handleHealth(
	svc *Service, w http.ResponseWriter, r *http.Request,
) {
	if svc == nil {
		panic("handleHealth: svc must not be nil")
	}
	if w == nil {
		panic("handleHealth: w must not be nil")
	}
	ctx := r.Context()
	resp := healthResponse{Status: "healthy"}
	stream, err := svc.js.Stream(
		ctx, "TELEMETRY",
	)
	if err == nil {
		info, infoErr := stream.Info(ctx)
		if infoErr == nil && info != nil {
			resp.Telemetry = buildTelemetryInfo(info)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	encErr := json.NewEncoder(w).Encode(resp)
	if encErr != nil {
		slog.Error("encode health response", "error", encErr)
	}
}

// buildTelemetryInfo constructs telemetry info from JetStream stream
// metadata. Calculates usage percent from Bytes/MaxBytes.
func buildTelemetryInfo(info *jetstream.StreamInfo) *telemetryInfo {
	if info == nil {
		panic("buildTelemetryInfo: info must not be nil")
	}
	if info.Config.Name == "" {
		panic("buildTelemetryInfo: stream name must not be empty")
	}
	var pct float64
	maxBytes := info.Config.MaxBytes
	if maxBytes > 0 {
		pct = float64(info.State.Bytes) /
			float64(maxBytes) * 100.0
	}
	return &telemetryInfo{
		Stream: &streamInfo{
			Messages: info.State.Msgs,
			Bytes:    info.State.Bytes,
			Percent:  pct,
		},
	}
}
