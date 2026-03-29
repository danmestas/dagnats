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

// NewRESTHandler returns an http.Handler that routes the DagNats control-plane
// REST API. Panics if svc is nil — callers must pass a fully initialised Service.
func NewRESTHandler(svc *Service) http.Handler {
	if svc == nil {
		panic("NewRESTHandler: svc must not be nil")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/workflows", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handleRegisterWorkflow(svc, w, r)
	})
	mux.HandleFunc("/runs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handleStartRun(svc, w, r)
	})
	mux.HandleFunc("/runs/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handleGetRun(svc, w, r)
	})
	return mux
}

// handleRegisterWorkflow decodes a WorkflowDef from the request body and
// persists it via Service.RegisterWorkflow. Returns 201 on success.
func handleRegisterWorkflow(svc *Service, w http.ResponseWriter, r *http.Request) {
	var def dag.WorkflowDef
	if err := json.NewDecoder(r.Body).Decode(&def); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := svc.RegisterWorkflow(def); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "registered", "name": def.Name})
}

// handleStartRun decodes a startRunRequest and calls Service.StartRun.
// Returns 201 with a JSON body containing the run_id on success.
func handleStartRun(svc *Service, w http.ResponseWriter, r *http.Request) {
	var req startRunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	runID, err := svc.StartRun(req.Workflow, req.Input)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"run_id": runID})
}

// handleGetRun extracts the run ID from the URL path and returns the current
// run snapshot as JSON. Returns 404 when the run does not exist.
func handleGetRun(svc *Service, w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/runs/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "missing run ID", http.StatusBadRequest)
		return
	}
	runID := parts[0]
	run, err := svc.GetRun(runID)
	if err != nil {
		if errors.Is(err, engine.ErrRunNotFound) {
			http.Error(w, "run not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(run)
}
