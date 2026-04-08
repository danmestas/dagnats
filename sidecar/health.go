package sidecar

import (
	"encoding/json"
	"net/http"
	"time"
)

const (
	maxProcessCount = 100
)

// newHealthHandler creates an HTTP handler serving the
// /healthz endpoint with live supervisor state as JSON.
func newHealthHandler(s *Supervisor) http.Handler {
	if s == nil {
		panic("newHealthHandler: supervisor is nil")
	}
	if s.cfg == nil {
		panic("newHealthHandler: supervisor config is nil")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		resp := buildHealthResponse(s)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	return mux
}

// buildHealthResponse reads live supervisor state with proper
// locking on each process to build a health snapshot.
func buildHealthResponse(s *Supervisor) map[string]any {
	if s == nil {
		panic("buildHealthResponse: supervisor is nil")
	}
	if s.cfg == nil {
		panic("buildHealthResponse: config is nil")
	}

	now := time.Now()
	uptimeSeconds := now.Sub(s.startedAt).Seconds()

	allOK := true
	procs := make([]map[string]any, 0, len(s.processes))

	for i, p := range s.processes {
		if i >= maxProcessCount {
			break
		}
		info := buildProcessStatus(p, now)
		if info["status"] != "running" {
			allOK = false
		}
		procs = append(procs, info)
	}

	status := "ok"
	if !allOK {
		status = "degraded"
	}

	return map[string]any{
		"status":         status,
		"uptime_seconds": uptimeSeconds,
		"processes":      procs,
		"storage": map[string]any{
			"path": s.cfg.Storage.LocalPath,
			"type": s.cfg.Storage.Type,
		},
	}
}

// buildProcessStatus snapshots a single process under its
// mutex. Name is read outside the lock (immutable).
func buildProcessStatus(
	p *Process, now time.Time,
) map[string]any {
	if p == nil {
		panic("buildProcessStatus: process is nil")
	}
	if now.IsZero() {
		panic("buildProcessStatus: now is zero")
	}

	name := p.Name

	p.mu.Lock()
	running := p.isRunningLocked()
	pid := 0
	if running && p.cmd != nil && p.cmd.Process != nil {
		pid = p.cmd.Process.Pid
	}
	startedAt := p.startedAt
	restarts := p.restarts
	p.mu.Unlock()

	status := "stopped"
	procUptime := 0.0
	if running {
		status = "running"
		procUptime = now.Sub(startedAt).Seconds()
	}

	return map[string]any{
		"name":           name,
		"status":         status,
		"pid":            pid,
		"restarts":       restarts,
		"uptime_seconds": procUptime,
	}
}
