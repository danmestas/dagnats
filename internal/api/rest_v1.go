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

	"github.com/danmestas/dagnats/internal/workertoken"
	"github.com/danmestas/dagnats/worker"
)

// listWorkersV1Response is the JSON body for GET /v1/workers.
type listWorkersV1Response struct {
	Workers []worker.WorkerRegistration `json:"workers"`
	Count   int                         `json:"count"`
}

// MountV1 registers the control plane's /v1 routes on mux. Panics if
// mux, svc, or tokenStore is nil -- production always has one Store
// shared with the bridge (server/server.go); a caller with no worker-
// token feature to test can still construct one via workertoken.Open
// against a real (even throwaway) NATS connection. Future /v1 control-
// plane routes (#633 ci) should be added here alongside GET /v1/workers,
// GET /v1/queue, and the /v1/tokens routes.
func MountV1(mux *http.ServeMux, svc *Service, tokenStore *workertoken.Store) {
	if mux == nil {
		panic("MountV1: mux must not be nil")
	}
	if svc == nil {
		panic("MountV1: svc must not be nil")
	}
	if tokenStore == nil {
		panic("MountV1: tokenStore must not be nil")
	}
	mux.HandleFunc("GET /v1/workers", svc.handleListWorkersV1)
	mux.HandleFunc("GET /v1/queue", svc.handleGetQueueV1)
	mountTokenRoutes(mux, tokenStore)
	mux.HandleFunc("POST /v1/ci/compile", func(w http.ResponseWriter, r *http.Request) {
		handleCICompile(svc, w, r)
	})
	mux.HandleFunc("POST /v1/ci/validate", func(w http.ResponseWriter, r *http.Request) {
		handleCIValidate(svc, w, r)
	})
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
