// internal/api/rest_fork.go
// The fork endpoints (#810): POST /runs/{run}/fork and api.runs.fork.
package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

// forkBodyBytesMax bounds a fork request body: a step and a sequence.
const forkBodyBytesMax = 64 << 10

// handleForkRun serves POST /runs/{run}/fork with a ForkRequest body:
// 201 and a ForkResponse, 404 for an unknown run or step, 400 for a
// request that names no fork point.
func handleForkRun(svc *Service, w http.ResponseWriter, r *http.Request, runID string) {
	if svc == nil {
		panic("handleForkRun: svc must not be nil")
	}
	if r == nil || runID == "" {
		panic("handleForkRun: request and run must be set")
	}
	var req ForkRequest
	body := http.MaxBytesReader(w, r.Body, forkBodyBytesMax)
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	resp, err := svc.ForkRun(r.Context(), runID, req)
	if err != nil {
		http.Error(w, err.Error(), forkErrorStatus(err))
		return
	}
	writeJSON(w, http.StatusCreated, resp)
}

// forkErrorStatus maps a ForkRun error to its HTTP status.
func forkErrorStatus(err error) int {
	if err == nil {
		panic("forkErrorStatus: err must not be nil")
	}
	var forkErr *ForkError
	switch {
	case errors.Is(err, engine.ErrRunNotFound):
		return http.StatusNotFound
	case errors.As(err, &forkErr) && forkErr.NotFound:
		return http.StatusNotFound
	case errors.As(err, &forkErr):
		return http.StatusBadRequest
	}
	return http.StatusInternalServerError
}

// handleForkRunNATS serves api.runs.fork: {"run_id", "step", "seq"}
// answered by a ForkResponse or {"error": "..."}.
func (n *NATSAPI) handleForkRunNATS(req micro.Request) {
	if req == nil {
		panic("handleForkRunNATS: req must not be nil")
	}
	if n.svc == nil {
		panic("handleForkRunNATS: svc must not be nil")
	}
	var r struct {
		RunID string `json:"run_id"`
		ForkRequest
	}
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		n.reply(req, map[string]string{"error": err.Error()})
		return
	}
	ctx := observe.ExtractTraceContextHeader(nats.Header(req.Headers()))
	resp, err := n.svc.ForkRun(ctx, r.RunID, r.ForkRequest)
	if err != nil {
		n.reply(req, map[string]string{"error": err.Error()})
		return
	}
	n.reply(req, resp)
}
