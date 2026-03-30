// api/rest.go
// REST HTTP handlers for the DagNats control plane.
// Routes delegate to Service methods; transport concerns (status codes, JSON
// encoding) live here so Service stays transport-agnostic.
package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/engine"
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
	mux.HandleFunc("/workflows", svc.routeWorkflows)
	mux.HandleFunc("/runs", svc.routeRuns)
	mux.HandleFunc("/runs/", svc.routeRunByID)
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
