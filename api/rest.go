// api/rest.go
// REST HTTP handlers for the DagNats control plane.
// Routes delegate to Service methods; transport concerns (status codes, JSON
// encoding) live here so Service stays transport-agnostic.
package api

import (
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
	if svc.tel == nil {
		panic("NewRESTHandler: svc.tel must not be nil")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/workflows", svc.routeWorkflows)
	mux.HandleFunc("/runs", svc.routeRuns)
	mux.HandleFunc("/runs/", svc.routeRunByID)
	mux.HandleFunc("/health/telemetry", svc.routeHealth)
	return mux
}

// routeWorkflows dispatches POST /workflows to handleRegisterWorkflow.
func (s *Service) routeWorkflows(
	w http.ResponseWriter, r *http.Request,
) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	handleRegisterWorkflow(s, w, r)
}

// routeRuns dispatches POST /runs to handleStartRun.
func (s *Service) routeRuns(
	w http.ResponseWriter, r *http.Request,
) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	handleStartRun(s, w, r)
}

// routeRunByID dispatches GET /runs/{id} to handleGetRun.
func (s *Service) routeRunByID(
	w http.ResponseWriter, r *http.Request,
) {
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
	handleHealth(s, w)
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
		svc.tel.Logger.Error("encode response", encErr)
	}
}

// handleStartRun decodes a startRunRequest and calls StartRun.
// Returns 201 with a JSON body containing the run_id on success.
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
	if svc == nil {
		panic("handleHealth: svc must not be nil")
	}
	if w == nil {
		panic("handleHealth: w must not be nil")
	}
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

// buildTelemetryInfo constructs telemetry info from JetStream stream
// metadata. Calculates usage percent from Bytes/MaxBytes.
func buildTelemetryInfo(info *nats.StreamInfo) *telemetryInfo {
	if info == nil {
		panic("buildTelemetryInfo: info must not be nil")
	}
	if info.Config.Name == "" {
		panic("buildTelemetryInfo: stream name must not be empty")
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
