package bridge

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

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
	req, err := parseConnectRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	dir := worker.NewDirectory(b.js)
	reg := worker.WorkerRegistration{
		WorkerID:  req.WorkerID,
		TaskTypes: req.TaskTypes,
		Language:  "http",
		Transport: "bridge",
		MaxTasks:  req.MaxTasks,
	}
	if err := dir.Register(reg); err != nil {
		http.Error(
			w, "register failed", http.StatusInternalServerError,
		)
		return
	}
	defer dir.Deregister(req.WorkerID)

	writeSSEHeaders(w)
	sendHeartbeatLoop(w, r, reg, dir)
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
	w http.ResponseWriter,
	r *http.Request,
	reg worker.WorkerRegistration,
	dir *worker.Directory,
) {
	if w == nil {
		panic("sendHeartbeatLoop: w must not be nil")
	}
	if r == nil {
		panic("sendHeartbeatLoop: r must not be nil")
	}
	flusher, _ := w.(http.Flusher)
	interval := time.Duration(heartbeatIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)
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
			// Re-register to refresh KV TTL
			_ = dir.Register(reg)
		}
	}
}
