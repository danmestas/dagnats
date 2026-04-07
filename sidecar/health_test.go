// Methodology: integration tests using real subprocesses and
// httptest to verify the health endpoint returns correct JSON
// schema and status values. All tests use bounded timeouts.

package sidecar

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type healthResponse struct {
	Status        string            `json:"status"`
	UptimeSeconds float64           `json:"uptime_seconds"`
	Processes     []processStatus   `json:"processes"`
	Storage       healthStorageInfo `json:"storage"`
}

type processStatus struct {
	Name          string  `json:"name"`
	Status        string  `json:"status"`
	PID           int     `json:"pid"`
	Restarts      int     `json:"restarts"`
	UptimeSeconds float64 `json:"uptime_seconds"`
}

type healthStorageInfo struct {
	Path string `json:"path"`
	Type string `json:"type"`
}

func TestHealthHandler_Running(t *testing.T) {
	t.Parallel()

	procs := []*Process{
		{Name: "alpha", Bin: "sleep", Args: []string{"60"}},
		{Name: "beta", Bin: "sleep", Args: []string{"60"}},
		{Name: "gamma", Bin: "sleep", Args: []string{"60"}},
	}
	sup := testSupervisor(procs)
	defer sup.Stop()

	if err := sup.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	handler := newHealthHandler(sup)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodGet, "/healthz", nil,
	)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var body healthResponse
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	// Positive: overall status should be ok when all running.
	if body.Status != "ok" {
		t.Fatalf("expected status ok, got %q", body.Status)
	}

	// Positive: correct process count.
	if len(body.Processes) != 3 {
		t.Fatalf(
			"expected 3 processes, got %d",
			len(body.Processes),
		)
	}

	// Verify each process has a nonzero PID and running status.
	for _, p := range body.Processes {
		if p.PID == 0 {
			t.Fatalf(
				"expected nonzero PID for %s", p.Name,
			)
		}
		if p.Status != "running" {
			t.Fatalf(
				"expected running for %s, got %q",
				p.Name, p.Status,
			)
		}
	}

	// Negative: uptime must be non-negative.
	if body.UptimeSeconds < 0 {
		t.Fatalf(
			"expected non-negative uptime, got %f",
			body.UptimeSeconds,
		)
	}

	// Verify storage info is populated from config.
	if body.Storage.Type != "local" {
		t.Fatalf(
			"expected storage type local, got %q",
			body.Storage.Type,
		)
	}
}

func TestHealthHandler_Stopped(t *testing.T) {
	t.Parallel()

	// Never start the process — it stays in stopped state.
	procs := []*Process{
		{Name: "ghost", Bin: "sleep", Args: []string{"60"}},
	}
	sup := testSupervisor(procs)

	// Set startedAt so uptime is computable.
	sup.startedAt = time.Now()

	handler := newHealthHandler(sup)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodGet, "/healthz", nil,
	)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var body healthResponse
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	// Positive: status should be degraded when process stopped.
	if body.Status != "degraded" {
		t.Fatalf(
			"expected status degraded, got %q", body.Status,
		)
	}

	if len(body.Processes) != 1 {
		t.Fatalf(
			"expected 1 process, got %d", len(body.Processes),
		)
	}

	// Negative: stopped process has PID 0 and status stopped.
	p := body.Processes[0]
	if p.PID != 0 {
		t.Fatalf("expected PID 0 for stopped process, got %d", p.PID)
	}
	if p.Status != "stopped" {
		t.Fatalf(
			"expected status stopped, got %q", p.Status,
		)
	}
}

func TestHealthServer_Lifecycle(t *testing.T) {
	t.Parallel()

	procs := []*Process{
		{Name: "test", Bin: "sleep", Args: []string{"60"}},
		{Name: "test2", Bin: "sleep", Args: []string{"60"}},
		{Name: "test3", Bin: "sleep", Args: []string{"60"}},
	}
	sup := testSupervisor(procs)
	sup.cfg.Supervisor.Listen = "localhost:0"

	if err := sup.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if err := sup.startHealthServer(); err != nil {
		sup.Stop()
		t.Fatalf("startHealthServer failed: %v", err)
	}

	url := "http://" + sup.healthAddr + "/healthz"
	resp, err := http.Get(url)
	if err != nil {
		sup.Stop()
		t.Fatalf("health endpoint unreachable: %v", err)
	}
	defer resp.Body.Close()

	// Positive: endpoint returns 200.
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var body healthResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode health response: %v", err)
	}

	if len(body.Processes) != 3 {
		t.Fatalf(
			"expected 3 processes, got %d",
			len(body.Processes),
		)
	}

	sup.Stop()

	// Negative: endpoint should be down after Stop.
	_, err = http.Get(url)
	if err == nil {
		t.Fatal("health endpoint should be down after Stop")
	}
}
