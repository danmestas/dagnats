// api/rest.go
// REST HTTP handlers for the DagNats control plane.
// Routes delegate to Service methods; transport concerns (status codes, JSON
// encoding) live here so Service stays transport-agnostic.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/engine"
	"github.com/nats-io/nats.go"
)

// startRunRequest is the JSON body expected by POST /runs.
type startRunRequest struct {
	Workflow string          `json:"workflow"`
	Input    json.RawMessage `json:"input,omitempty"`
}

// NewRESTHandler returns an http.Handler that routes the DagNats
// control-plane REST API. Panics if svc is nil.
func NewRESTHandler(svc *Service) http.Handler {
	if svc == nil {
		panic("NewRESTHandler: svc must not be nil")
	}
	mux := http.NewServeMux()
	RegisterAPIRoutes(mux, svc)
	return mux
}

// RegisterAPIRoutes adds REST API routes to the given ServeMux.
// Use this when mounting API routes on a shared mux alongside UI.
func RegisterAPIRoutes(mux *http.ServeMux, svc *Service) {
	if mux == nil {
		panic("RegisterAPIRoutes: mux must not be nil")
	}
	if svc == nil {
		panic("RegisterAPIRoutes: svc must not be nil")
	}
	mux.HandleFunc("/api/workflows", svc.routeWorkflows)
	mux.HandleFunc("/api/workflows/", svc.routeWorkflowByName)
	mux.HandleFunc("/api/runs", svc.routeRuns)
	mux.HandleFunc("/api/runs/", svc.routeRunByID)
	mux.HandleFunc("/api/health", svc.routeHealth)
}

// routeWorkflows dispatches GET and POST /api/workflows.
func (s *Service) routeWorkflows(
	w http.ResponseWriter, r *http.Request,
) {
	switch r.Method {
	case http.MethodGet:
		handleListWorkflows(s, w)
	case http.MethodPost:
		handleRegisterWorkflow(s, w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// routeWorkflowByName dispatches GET /api/workflows/{name}.
func (s *Service) routeWorkflowByName(
	w http.ResponseWriter, r *http.Request,
) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	handleGetWorkflow(s, w, r)
}

// routeRuns dispatches GET and POST /api/runs.
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

// routeRunByID dispatches GET /api/runs/{id} and sub-paths.
func (s *Service) routeRunByID(
	w http.ResponseWriter, r *http.Request,
) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/runs/")
	if strings.HasSuffix(path, "/events") {
		handleGetRunEvents(s, w, r)
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
	handleHealth(s, w)
}

// handleRegisterWorkflow decodes a WorkflowDef from the request body
// and persists it via Service.RegisterWorkflow. Returns 201 on success.
func handleRegisterWorkflow(
	svc *Service, w http.ResponseWriter, r *http.Request,
) {
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
		svc.tel.Logger.Error("encode response", encErr)
	}
}

// handleStartRun decodes a startRunRequest and calls StartRun.
// Returns 201 with a JSON body containing the run_id on success.
func handleStartRun(
	svc *Service, w http.ResponseWriter, r *http.Request,
) {
	var req startRunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(),
			http.StatusBadRequest)
		return
	}
	runID, err := svc.StartRun(r.Context(), req.Workflow, req.Input)
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
		svc.tel.Logger.Error("encode response", encErr)
	}
}

// handleGetRun extracts the run ID from the URL path and returns the
// current run snapshot as JSON. Returns 404 when run does not exist.
func handleGetRun(
	svc *Service, w http.ResponseWriter, r *http.Request,
) {
	parts := strings.Split(
		strings.TrimPrefix(r.URL.Path, "/api/runs/"), "/",
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
		svc.tel.Logger.Error("encode response", encErr)
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
	Jaeger string      `json:"jaeger"`
}

// streamInfo carries TELEMETRY stream usage stats.
type streamInfo struct {
	Messages uint64  `json:"messages"`
	Bytes    uint64  `json:"bytes"`
	Percent  float64 `json:"percent"`
}

// handleHealth returns service health and optional telemetry stream
// status. Never fails the health check -- telemetry is informational.
func handleHealth(svc *Service, w http.ResponseWriter) {
	resp := healthResponse{Status: "healthy"}
	info, err := svc.js.StreamInfo("TELEMETRY")
	if err == nil && info != nil {
		resp.Telemetry = buildTelemetryInfo(info)
	}
	w.Header().Set("Content-Type", "application/json")
	encErr := json.NewEncoder(w).Encode(resp)
	if encErr != nil {
		svc.tel.Logger.Error("encode health response", encErr)
	}
}

// handleGetRunEvents returns the event history for a run as JSON.
func handleGetRunEvents(
	svc *Service, w http.ResponseWriter, r *http.Request,
) {
	path := strings.TrimPrefix(r.URL.Path, "/api/runs/")
	runID := strings.TrimSuffix(path, "/events")
	if runID == "" {
		http.Error(w, "missing run ID", http.StatusBadRequest)
		return
	}
	events, err := svc.GetRunEvents(
		context.Background(), runID,
	)
	if err != nil {
		http.Error(w, err.Error(),
			http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	encErr := json.NewEncoder(w).Encode(events)
	if encErr != nil {
		svc.tel.Logger.Error("encode events", encErr)
	}
}

// handleListWorkflows returns all registered workflow definitions
// as a JSON array. Delegates to Service.ListWorkflows.
func handleListWorkflows(svc *Service, w http.ResponseWriter) {
	defs, err := svc.ListWorkflows(context.Background())
	if err != nil {
		http.Error(w, err.Error(),
			http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	encErr := json.NewEncoder(w).Encode(defs)
	if encErr != nil {
		svc.tel.Logger.Error("encode workflows", encErr)
	}
}

// handleGetWorkflow returns a single workflow definition by name.
func handleGetWorkflow(
	svc *Service, w http.ResponseWriter, r *http.Request,
) {
	name := strings.TrimPrefix(r.URL.Path, "/api/workflows/")
	if name == "" {
		http.Error(w, "missing workflow name",
			http.StatusBadRequest)
		return
	}
	def, err := svc.GetWorkflow(name)
	if err != nil {
		http.Error(w, "workflow not found",
			http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	encErr := json.NewEncoder(w).Encode(def)
	if encErr != nil {
		svc.tel.Logger.Error("encode workflow", encErr)
	}
}

// handleListRuns returns all workflow run snapshots as a JSON array.
// Supports optional ?workflow= and ?status= query filters.
func handleListRuns(
	svc *Service, w http.ResponseWriter, r *http.Request,
) {
	workflow := r.URL.Query().Get("workflow")
	status := r.URL.Query().Get("status")
	runs, err := svc.ListRuns(
		context.Background(), workflow, status,
	)
	if err != nil {
		http.Error(w, err.Error(),
			http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	encErr := json.NewEncoder(w).Encode(runs)
	if encErr != nil {
		svc.tel.Logger.Error("encode runs", encErr)
	}
}

// buildTelemetryInfo constructs telemetry info from JetStream stream
// metadata. Calculates usage percent from Bytes/MaxBytes.
func buildTelemetryInfo(info *nats.StreamInfo) *telemetryInfo {
	if info == nil {
		panic("buildTelemetryInfo: info must not be nil")
	}
	jaeger := "disabled"
	if os.Getenv("JAEGER_ENDPOINT") != "" {
		jaeger = "enabled"
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
		Jaeger: jaeger,
	}
}
