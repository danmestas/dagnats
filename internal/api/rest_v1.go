// api/rest_v1.go
// Control-plane routes registered under the top-level /v1 namespace.
//
// Ownership note (see server/server.go startHTTP): the bridge package
// owns the "/v1/" catch-all (workers/connect, tasks/poll, tasks/{id}/
// resolve). MountV1 registers more-specific /v1 patterns on the SAME
// top-level mux; Go 1.22+ ServeMux picks the most specific pattern, so
// "GET /v1/workers" wins over the bridge's "/v1/" for that exact path
// while every other /v1/* path still falls through to the bridge.
package api

import (
	"encoding/json"
	"net/http"

	"github.com/danmestas/dagnats/worker"
)

// listWorkersV1Response is the JSON body for GET /v1/workers.
type listWorkersV1Response struct {
	Workers []worker.WorkerRegistration `json:"workers"`
	Count   int                         `json:"count"`
}

// MountV1 registers the control plane's /v1 routes on mux. Panics if
// mux or svc is nil. Future /v1 control-plane routes (#627 tokens, #632
// queue, #633 ci) should be added here alongside GET /v1/workers.
func MountV1(mux *http.ServeMux, svc *Service) {
	if mux == nil {
		panic("MountV1: mux must not be nil")
	}
	if svc == nil {
		panic("MountV1: svc must not be nil")
	}
	mux.HandleFunc("GET /v1/workers", svc.handleListWorkersV1)
}

// handleListWorkersV1 serves GET /v1/workers. Method mismatches never
// reach this handler -- the "GET /v1/workers" mux pattern rejects other
// methods with 405 before dispatch.
func (s *Service) handleListWorkersV1(
	w http.ResponseWriter, r *http.Request,
) {
	if s == nil {
		panic("handleListWorkersV1: s must not be nil")
	}
	if r == nil {
		panic("handleListWorkersV1: r must not be nil")
	}
	workers, err := s.ListWorkers(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if workers == nil {
		workers = []worker.WorkerRegistration{}
	}
	resp := listWorkersV1Response{Workers: workers, Count: len(workers)}
	w.Header().Set("Content-Type", "application/json")
	encErr := json.NewEncoder(w).Encode(resp)
	if encErr != nil {
		http.Error(w, encErr.Error(), http.StatusInternalServerError)
	}
}
