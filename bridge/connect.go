package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/danmestas/dagnats/internal/workertoken"
	"github.com/danmestas/dagnats/worker"
)

// connectRequest is the JSON body for POST /v1/workers/connect.
type connectRequest struct {
	WorkerID  string   `json:"worker_id"`
	TaskTypes []string `json:"task_types"`
	MaxTasks  int      `json:"max_tasks"`
}

// heartbeatIntervalMs controls how often SSE heartbeats are sent.
// 25 seconds keeps the connection alive through most proxies
// (which typically timeout at 30-60s).
const heartbeatIntervalMs = 25_000

// heartbeatInterval is the ticker period sendHeartbeatLoop uses.
// Kept as a variable (rather than deriving the duration inline from
// heartbeatIntervalMs on every call) so tests can shrink it far below
// the real 25s cadence and observe several ticks inside a bounded
// wait (#650 round 3).
var heartbeatInterval = time.Duration(heartbeatIntervalMs) * time.Millisecond

// handleConnect registers an HTTP worker and maintains an SSE
// heartbeat stream. On disconnect the worker is deregistered.
func (b *Bridge) handleConnect(
	w http.ResponseWriter, r *http.Request,
) {
	if b.nc == nil {
		panic("handleConnect: nc must not be nil")
	}
	if b.js == nil {
		panic("handleConnect: js must not be nil")
	}
	ctx, span := b.tracer.Start(r.Context(), "bridge.connect")
	defer span.End()

	req, err := parseConnectRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	slog.InfoContext(ctx, "worker connected",
		"worker_id", req.WorkerID,
		"max_tasks", req.MaxTasks,
	)

	claims := claimsFromContext(ctx)
	dir := worker.NewDirectory(b.js)
	reg := worker.WorkerRegistration{
		WorkerID:  req.WorkerID,
		TaskTypes: req.TaskTypes,
		Language:  "http",
		Transport: "bridge",
		MaxTasks:  req.MaxTasks,
		TokenID:   registrationTokenID(claims),
	}
	if !registerOwnedOrReject(w, dir, reg, claims) {
		return
	}
	defer deregisterOnDisconnect(ctx, dir, req.WorkerID, claims)

	writeSSEHeaders(w)
	sendHeartbeatLoop(ctx, w, r, reg, dir, claims)
}

// registrationTokenID is the token_id RegisterOwned stores for a
// registration (#650 round 3). An admin caller -- the env bearer, or
// any caller in dev mode, both of which produce claims.Admin==true --
// writes the reserved worker.AdminTokenID marker instead of the empty
// string claims.TokenID carries for them: an admin/dev-mode entry
// with an empty token_id was indistinguishable from a genuinely
// unowned one and so claimable by the next bridge token to connect,
// silently undoing an admin takeover. A Store-issued worker token
// (#627) always has a non-empty claims.TokenID and is written as-is.
func registrationTokenID(claims workertoken.Claims) string {
	if claims.Admin {
		return worker.AdminTokenID
	}
	return claims.TokenID
}

// registerOwnedOrReject performs the ownership-guarded write for
// connect -- and, on rejection, responds to the request. Returns true
// when the caller may proceed to open the SSE stream; false means the
// response has already been written and the handler must return.
func registerOwnedOrReject(
	w http.ResponseWriter, dir *worker.Directory,
	reg worker.WorkerRegistration, claims workertoken.Claims,
) bool {
	if w == nil {
		panic("registerOwnedOrReject: w must not be nil")
	}
	if dir == nil {
		panic("registerOwnedOrReject: dir must not be nil")
	}
	err := dir.RegisterOwned(reg, claims.TokenID, claims.Admin)
	if err == nil {
		return true
	}
	if errors.Is(err, worker.ErrWorkerIDOwned) {
		http.Error(
			w, "worker_id is registered to another token",
			http.StatusConflict,
		)
		return false
	}
	http.Error(w, "register failed", http.StatusInternalServerError)
	return false
}

// deregisterOnDisconnect removes the worker's directory entry when
// its connection closes, scoped to the same ownership rule enforced
// on connect (#650): a stale disconnect from a token that no longer
// owns the worker_id (e.g. after an admin takeover while this
// connection stayed open) must not delete the current owner's entry.
func deregisterOnDisconnect(
	ctx context.Context, dir *worker.Directory,
	workerID string, claims workertoken.Claims,
) {
	if dir == nil {
		panic("deregisterOnDisconnect: dir must not be nil")
	}
	if workerID == "" {
		panic("deregisterOnDisconnect: workerID must not be empty")
	}
	err := dir.DeregisterOwned(workerID, claims.TokenID, claims.Admin)
	switch {
	case errors.Is(err, worker.ErrWorkerIDOwned):
		slog.DebugContext(ctx,
			"stale disconnect for worker_id owned by another token",
			"worker_id", workerID,
		)
	case err != nil:
		slog.ErrorContext(ctx, "deregister failed",
			"worker_id", workerID, "error", err,
		)
	default:
		slog.InfoContext(ctx, "worker disconnected",
			"worker_id", workerID,
		)
	}
}

// parseConnectRequest validates the connect JSON body.
func parseConnectRequest(
	r *http.Request,
) (connectRequest, error) {
	if r == nil {
		panic("parseConnectRequest: r must not be nil")
	}
	if r.Body == nil {
		panic("parseConnectRequest: r.Body must not be nil")
	}
	var req connectRequest
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		return req, fmt.Errorf("invalid JSON: %w", err)
	}
	if req.WorkerID == "" {
		return req, fmt.Errorf("worker_id is required")
	}
	if len(req.TaskTypes) == 0 {
		return req, fmt.Errorf("task_types is required")
	}
	if req.MaxTasks <= 0 {
		req.MaxTasks = 1
	}
	return req, nil
}

// writeSSEHeaders sets the headers for Server-Sent Events.
func writeSSEHeaders(w http.ResponseWriter) {
	if w == nil {
		panic("writeSSEHeaders: w must not be nil")
	}
	if w.Header() == nil {
		panic("writeSSEHeaders: w.Header() must not be nil")
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

// sendHeartbeatLoop sends periodic SSE heartbeats and re-registers
// the worker until the client disconnects.
func sendHeartbeatLoop(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	reg worker.WorkerRegistration,
	dir *worker.Directory,
	claims workertoken.Claims,
) {
	if w == nil {
		panic("sendHeartbeatLoop: w must not be nil")
	}
	if r == nil {
		panic("sendHeartbeatLoop: r must not be nil")
	}
	flusher, _ := w.(http.Flusher)
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			_, err := fmt.Fprintf(
				w, "event: heartbeat\ndata: ok\n\n",
			)
			if err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			if !heartbeatReregister(ctx, dir, reg, claims) {
				return
			}
		}
	}
}

// heartbeatReregister re-registers reg through the same ownership-
// guarded write path as connect (#650 round 3 blocker): an earlier
// version called Directory.Register directly here, an unguarded Put
// that replayed this connection's TokenID every tick with no
// ownership check, no revision guard, and a swallowed error --
// resurrecting a worker_id an admin had already taken over the next
// time this heartbeat fired. Returns false when the caller must stop
// heartbeating: ErrWorkerIDOwned means another owner now holds this
// worker_id and this connection must never fight it back. Any other
// error is logged and treated as best-effort -- the KV bucket's TTL
// still governs liveness if a single re-register fails transiently.
func heartbeatReregister(
	ctx context.Context, dir *worker.Directory,
	reg worker.WorkerRegistration, claims workertoken.Claims,
) bool {
	if dir == nil {
		panic("heartbeatReregister: dir must not be nil")
	}
	if reg.WorkerID == "" {
		panic("heartbeatReregister: reg.WorkerID must not be empty")
	}
	err := dir.RegisterOwned(reg, claims.TokenID, claims.Admin)
	switch {
	case err == nil:
		return true
	case errors.Is(err, worker.ErrWorkerIDOwned):
		slog.WarnContext(ctx,
			"worker_id taken over; stopping heartbeat",
			"worker_id", reg.WorkerID,
		)
		return false
	default:
		slog.ErrorContext(ctx, "heartbeat re-register failed",
			"worker_id", reg.WorkerID, "error", err,
		)
		return true
	}
}
