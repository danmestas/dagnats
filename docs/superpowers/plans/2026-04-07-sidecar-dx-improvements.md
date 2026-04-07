# Sidecar DX Improvements Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Improve sidecar OTel collector DX from 5.3/10 to ~8/10 with 8 targeted fixes.

**Architecture:** Two phases. Phase 1: four single-line-scale fixes in `cli/sidecar.go` and `sidecar/config.go`. Phase 2: supervisor health endpoint, init command, MCP binary install, dry-run flag. All changes follow existing patterns — `exitFunc` for testable exits, `captureSidecarOutput` for stdout capture, `t.TempDir()` for filesystem tests.

**Tech Stack:** Go, YAML (gopkg.in/yaml.v3), net/http (health endpoint), os/exec (go build for local binary)

**Spec:** `docs/superpowers/specs/2026-04-06-sidecar-dx-improvements-design.md`

---

## Chunk 1: Phase 1 — Trivial Fixes

### Task 1: Error on unknown sidecar subcommand

**Files:**
- Modify: `cli/sidecar.go:37-46` (runSidecarCmd default case)
- Test: `cli/sidecar_test.go`

- [ ] **Step 1: Write failing test for unknown subcommand**

In `cli/sidecar_test.go`, add:

```go
func TestSidecarCmdUnknownSubcommand(t *testing.T) {
	var exitCode int
	oldExit := exitFunc
	exitFunc = func(code int) { exitCode = code }
	defer func() { exitFunc = oldExit }()

	output := captureSidecarOutput(func() {
		runSidecarCmd([]string{"bogus"})
	})

	// Positive: should exit with code 1.
	if exitCode != 1 {
		t.Fatalf("expected exit 1, got %d", exitCode)
	}

	// Negative: should not attempt start (no config error).
	if strings.Contains(output, "error: load config") {
		t.Fatal("should not attempt start for unknown subcommand")
	}
}
```

Add `"strings"` to imports if not present.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestSidecarCmdUnknownSubcommand -v`

Expected: FAIL — currently the default case falls through to `runSidecarStartCmd` which tries to load config, not exit with the expected error pattern.

- [ ] **Step 3: Fix the default case in runSidecarCmd**

In `cli/sidecar.go`, replace lines 44-46:

```go
	default:
		runSidecarStartCmd(args)
	}
```

with:

```go
	default:
		fmt.Fprintf(os.Stderr,
			"unknown sidecar command: %s\n", args[0])
		printSidecarUsage()
		exitFunc(1)
	}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestSidecarCmdUnknownSubcommand -v`

Expected: PASS

- [ ] **Step 5: Run full sidecar test suite**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestSidecar -v`

Expected: all existing tests still pass. The `TestSidecarCmdDispatchUnknown` test will need updating — it currently expects exit code 1 because the default case tries start, which fails on missing config. Now it exits 1 directly. The test assertion `exitCode != 1` still passes, but the reason changed. Verify the test still passes as-is.

- [ ] **Step 6: Commit**

```bash
git add cli/sidecar.go cli/sidecar_test.go
git commit -m "fix(sidecar): error on unknown subcommand instead of falling through to start"
```

---

### Task 2: Reject unknown YAML config keys

**Files:**
- Modify: `sidecar/config.go:86-92` (LoadConfig YAML parsing)
- Test: `sidecar/config_test.go`

- [ ] **Step 1: Write failing test for unknown YAML key**

In `sidecar/config_test.go`, add:

```go
func TestLoadConfig_UnknownKey(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "bad.yaml")
	content := `listen: "0.0.0.0:4318"
unknown_field: true
`
	if err := os.WriteFile(
		cfgPath, []byte(content), 0o600,
	); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	_, err := LoadConfig(cfgPath)

	// Positive: error returned for unknown key.
	if err == nil {
		t.Fatal("LoadConfig() should fail for unknown key")
	}

	// Positive: error message mentions the field.
	if !strings.Contains(err.Error(), "unknown_field") {
		t.Errorf("error = %q, want mention of 'unknown_field'",
			err.Error())
	}
}
```

Add `"strings"` to imports if not present.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -run TestLoadConfig_UnknownKey -v`

Expected: FAIL — `yaml.Unmarshal` silently ignores unknown fields.

- [ ] **Step 3: Switch to yaml.NewDecoder with KnownFields**

In `sidecar/config.go`, replace the `yaml.Unmarshal` call in `LoadConfig`:

```go
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
```

with:

```go
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
```

Add `"bytes"` to imports.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -run TestLoadConfig_UnknownKey -v`

Expected: PASS

- [ ] **Step 5: Run full config test suite**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -run TestLoadConfig -v`

Expected: all existing LoadConfig tests still pass. The `TestLoadConfig_YAML` test uses only known fields so it should be unaffected.

- [ ] **Step 6: Commit**

```bash
git add sidecar/config.go sidecar/config_test.go
git commit -m "fix(sidecar): reject unknown YAML config keys via KnownFields"
```

---

### Task 3: Print env var export hint in startup banner

**Files:**
- Modify: `cli/sidecar.go:231-247` (printStartBanner)
- Test: `cli/sidecar_test.go`

- [ ] **Step 1: Write failing test for env var in banner**

In `cli/sidecar_test.go`, add:

```go
func TestPrintStartBannerExportHint(t *testing.T) {
	cfg := sidecar.DefaultConfig()

	output := captureSidecarOutput(func() {
		printStartBanner(cfg)
	})

	// Positive: should contain the export env var.
	if !strings.Contains(output,
		"OTEL_EXPORTER_OTLP_ENDPOINT") {
		t.Fatalf(
			"expected OTEL env var in banner, got:\n%s",
			output)
	}

	// Positive: should use localhost, not 0.0.0.0.
	if !strings.Contains(output, "localhost") {
		t.Fatalf(
			"expected localhost in export hint, got:\n%s",
			output)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestPrintStartBannerExportHint -v`

Expected: FAIL — banner doesn't contain `OTEL_EXPORTER_OTLP_ENDPOINT`.

- [ ] **Step 3: Add export hint to printStartBanner**

In `cli/sidecar.go`, in `printStartBanner`, after the `fmt.Printf("  Collector:` line (line 242), add:

```go
	exportAddr := strings.Replace(
		cfg.Listen, "0.0.0.0", "localhost", 1,
	)
	fmt.Printf("  Export:      export "+
		"OTEL_EXPORTER_OTLP_ENDPOINT=http://%s\n",
		exportAddr)
```

Add `"strings"` to the import block if not present.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestPrintStartBannerExportHint -v`

Expected: PASS

- [ ] **Step 5: Run full banner tests**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestPrintStartBanner -v`

Expected: all existing banner tests still pass.

- [ ] **Step 6: Commit**

```bash
git add cli/sidecar.go cli/sidecar_test.go
git commit -m "feat(sidecar): print OTEL env var export hint in startup banner"
```

---

### Task 4: Show backend forwarding in banner

**Files:**
- Modify: `cli/sidecar.go:231-247` (printStartBanner)
- Test: `cli/sidecar_test.go`

- [ ] **Step 1: Write failing test for backend line in banner**

In `cli/sidecar_test.go`, add:

```go
func TestPrintStartBannerBackend(t *testing.T) {
	cfg := sidecar.DefaultConfig()
	cfg.Backend = &sidecar.BackendConfig{
		Endpoint: "https://otel.prod.example.com",
	}

	output := captureSidecarOutput(func() {
		printStartBanner(cfg)
	})

	// Positive: should contain backend endpoint.
	if !strings.Contains(output,
		"https://otel.prod.example.com") {
		t.Fatalf(
			"expected backend endpoint in banner, got:\n%s",
			output)
	}

	// Positive: should indicate forwarding.
	if !strings.Contains(output, "forwarding") {
		t.Fatalf(
			"expected 'forwarding' in banner, got:\n%s",
			output)
	}
}

func TestPrintStartBannerNoBackend(t *testing.T) {
	cfg := sidecar.DefaultConfig()
	// Backend is nil by default.

	output := captureSidecarOutput(func() {
		printStartBanner(cfg)
	})

	// Negative: should not contain Backend line.
	if strings.Contains(output, "Backend:") {
		t.Fatalf(
			"should not show Backend when nil, got:\n%s",
			output)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestPrintStartBannerBackend -v`

Expected: FAIL — no backend line in banner.

- [ ] **Step 3: Add backend line to printStartBanner**

In `cli/sidecar.go`, in `printStartBanner`, after the DuckDB MCP line, add:

```go
	if cfg.Backend != nil {
		fmt.Printf("  Backend:     %s (forwarding)\n",
			cfg.Backend.Endpoint)
	}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestPrintStartBannerBackend -v`

Expected: PASS (both test functions)

- [ ] **Step 5: Commit**

```bash
git add cli/sidecar.go cli/sidecar_test.go
git commit -m "feat(sidecar): show backend forwarding endpoint in startup banner"
```

---

## Chunk 2: Phase 2 — SupervisorConfig and Process Fields

### Task 5: Add SupervisorConfig to config and update defaults/validation

**Files:**
- Modify: `sidecar/config.go` (new struct, update SidecarConfig, DefaultConfig, Validate)
- Test: `sidecar/config_test.go`

- [ ] **Step 1: Write failing test for SupervisorConfig defaults**

In `sidecar/config_test.go`, add to `TestDefaultConfig`:

```go
	// Positive: supervisor listen defaults to localhost:4320.
	if cfg.Supervisor.Listen != "localhost:4320" {
		t.Errorf("Supervisor.Listen = %q, want %q",
			cfg.Supervisor.Listen, "localhost:4320")
	}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -run TestDefaultConfig -v`

Expected: FAIL — `Supervisor` field doesn't exist yet.

- [ ] **Step 3: Add SupervisorConfig struct and field**

In `sidecar/config.go`, after the `MCPConfig` struct, add:

```go
// SupervisorConfig controls the supervisor health endpoint.
type SupervisorConfig struct {
	Listen string `yaml:"listen"`
}
```

Add the field to `SidecarConfig`:

```go
Supervisor SupervisorConfig `yaml:"supervisor"`
```

Update `DefaultConfig()` to set the default:

```go
Supervisor: SupervisorConfig{
	Listen: "localhost:4320",
},
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -run TestDefaultConfig -v`

Expected: PASS

- [ ] **Step 5: Write failing test for Validate rejecting empty supervisor listen**

In `sidecar/config_test.go`, add:

```go
func TestValidate_EmptySupervisorListen(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Supervisor.Listen = ""

	err := cfg.Validate()

	// Positive: error returned.
	if err == nil {
		t.Fatal("Validate() should fail for empty supervisor listen")
	}

	// Positive: error mentions supervisor.
	if !strings.Contains(err.Error(), "supervisor") {
		t.Errorf("error = %q, want mention of 'supervisor'",
			err.Error())
	}
}
```

- [ ] **Step 6: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -run TestValidate_EmptySupervisorListen -v`

Expected: FAIL — Validate doesn't check Supervisor.Listen yet.

- [ ] **Step 7: Add validation for Supervisor.Listen**

In `sidecar/config.go`, in `Validate()`, after the `listen address must not be empty` check, add:

```go
	if c.Supervisor.Listen == "" {
		return fmt.Errorf(
			"supervisor listen address must not be empty",
		)
	}
```

- [ ] **Step 8: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -run TestValidate_EmptySupervisorListen -v`

Expected: PASS

- [ ] **Step 9: Write failing test for YAML round-trip of supervisor config**

In `sidecar/config_test.go`, add:

```go
func TestLoadConfig_SupervisorListen(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "sup.yaml")
	content := `supervisor:
  listen: "localhost:9999"
`
	if err := os.WriteFile(
		cfgPath, []byte(content), 0o600,
	); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	cfg, err := LoadConfig(cfgPath)

	// Positive: no error.
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Positive: custom value parsed.
	if cfg.Supervisor.Listen != "localhost:9999" {
		t.Errorf("Supervisor.Listen = %q, want %q",
			cfg.Supervisor.Listen, "localhost:9999")
	}
}
```

- [ ] **Step 10: Run test to verify it passes** (struct tags already wired)

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -run TestLoadConfig_SupervisorListen -v`

Expected: PASS (yaml tags handle this automatically)

- [ ] **Step 11: Run full sidecar package tests**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -v`

Expected: all pass

- [ ] **Step 12: Commit**

```bash
git add sidecar/config.go sidecar/config_test.go
git commit -m "feat(sidecar): add SupervisorConfig with health endpoint listen address"
```

---

### Task 6: Add startedAt and restarts fields to Process

**Files:**
- Modify: `sidecar/process.go` (add fields, set in Start, increment in RestartWithBackoff)
- Test: `sidecar/process_test.go`

- [ ] **Step 1: Write failing test for startedAt**

In `sidecar/process_test.go`, add:

```go
func TestProcess_StartedAt(t *testing.T) {
	t.Parallel()

	p := &Process{
		Name: "test-started-at",
		Bin:  "sleep",
		Args: []string{"60"},
	}

	before := time.Now()
	if err := p.Start(t.Context()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = p.Stop(5 * time.Second) }()

	// Positive: startedAt should be set after Start.
	if p.startedAt.IsZero() {
		t.Fatal("expected startedAt to be set after Start")
	}

	// Positive: startedAt should be recent.
	if p.startedAt.Before(before) {
		t.Fatal("startedAt should be >= before Start call")
	}
}
```

Note: `startedAt` is accessed directly because tests are in the same package.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -run TestProcess_StartedAt -v`

Expected: FAIL — `startedAt` field doesn't exist.

- [ ] **Step 3: Add startedAt field and set it in Start**

In `sidecar/process.go`, add to the `Process` struct fields (after `failedAt`):

```go
	startedAt time.Time
	restarts  int
```

In `Process.Start()`, just before the `return nil` at the end (after `p.done = make(chan struct{})`), add:

```go
	p.startedAt = time.Now()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -run TestProcess_StartedAt -v`

Expected: PASS

- [ ] **Step 5: Write failing test for restarts counter**

In `sidecar/process_test.go`, add:

```go
func TestProcess_RestartsCounter(t *testing.T) {
	t.Parallel()

	p := &Process{
		Name: "test-restarts",
		Bin:  "sleep",
		Args: []string{"60"},
	}

	// Start, then restart once.
	if err := p.Start(t.Context()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Positive: restarts starts at 0.
	if p.restarts != 0 {
		t.Fatalf("expected 0 restarts, got %d", p.restarts)
	}

	if err := p.RestartWithBackoff(); err != nil {
		t.Fatalf("RestartWithBackoff failed: %v", err)
	}

	// Positive: restarts incremented to 1.
	if p.restarts != 1 {
		t.Fatalf("expected 1 restart, got %d", p.restarts)
	}

	_ = p.Stop(5 * time.Second)
}
```

- [ ] **Step 6: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -run TestProcess_RestartsCounter -v`

Expected: FAIL — `restarts` is never incremented.

- [ ] **Step 7: Increment restarts in RestartWithBackoff**

In `sidecar/process.go`, in `RestartWithBackoff()`, after `p.failures++` (line 173), add:

```go
	p.restarts++
```

- [ ] **Step 8: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -run TestProcess_RestartsCounter -v`

Expected: PASS

- [ ] **Step 9: Run full process test suite**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -run TestProcess -v`

Expected: all pass

- [ ] **Step 10: Commit**

```bash
git add sidecar/process.go sidecar/process_test.go
git commit -m "feat(sidecar): add startedAt and restarts fields to Process"
```

---

### Task 7: Add startedAt to Supervisor

**Files:**
- Modify: `sidecar/supervisor.go` (add field, set in Start)
- Test: `sidecar/supervisor_test.go`

- [ ] **Step 1: Write failing test for supervisor startedAt**

In `sidecar/supervisor_test.go`, add:

```go
func TestSupervisor_StartedAt(t *testing.T) {
	t.Parallel()

	procs := []*Process{
		{Name: "first", Bin: "sleep", Args: []string{"60"}},
		{Name: "second", Bin: "sleep", Args: []string{"60"}},
		{Name: "third", Bin: "sleep", Args: []string{"60"}},
	}

	sup := testSupervisor(procs)
	defer sup.Stop()

	before := time.Now()
	if err := sup.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Positive: startedAt should be set.
	if sup.startedAt.IsZero() {
		t.Fatal("expected startedAt to be set after Start")
	}

	// Positive: startedAt should be recent.
	if sup.startedAt.Before(before) {
		t.Fatal("startedAt should be >= before Start call")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -run TestSupervisor_StartedAt -v`

Expected: FAIL — `startedAt` field doesn't exist on Supervisor.

- [ ] **Step 3: Add startedAt to Supervisor and set in Start**

In `sidecar/supervisor.go`, add to the `Supervisor` struct:

```go
	startedAt time.Time
```

In `Supervisor.Start()`, at the beginning of the method (after the panic check), add:

```go
	s.startedAt = time.Now()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -run TestSupervisor_StartedAt -v`

Expected: PASS

- [ ] **Step 5: Run full supervisor test suite**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -run TestSupervisor -v`

Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add sidecar/supervisor.go sidecar/supervisor_test.go
git commit -m "feat(sidecar): add startedAt field to Supervisor"
```

---

## Chunk 3: Phase 2 — Health Endpoint

### Task 8: Implement supervisor health HTTP endpoint

**Files:**
- Create: `sidecar/health.go` (health handler, response types, server lifecycle)
- Create: `sidecar/health_test.go`
- Modify: `sidecar/supervisor.go` (start/stop health server in Run)

The health endpoint is its own file because it has clear boundaries: HTTP handler, response types, server lifecycle. The supervisor calls into it but doesn't own the HTTP details.

- [ ] **Step 1: Write failing test for health handler**

Create `sidecar/health_test.go`:

```go
// Methodology: Unit tests for the health HTTP handler. Uses
// httptest to verify JSON response schema, process state
// reporting, and bounded timeouts. No real child processes.

package sidecar

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// healthResponse mirrors the JSON schema from the spec.
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
		{Name: "proc-a", Bin: "sleep", Args: []string{"60"}},
		{Name: "proc-b", Bin: "sleep", Args: []string{"60"}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, p := range procs {
		if err := p.Start(ctx); err != nil {
			t.Fatalf("start %s: %v", p.Name, err)
		}
		defer func() { _ = p.Stop(5 * time.Second) }()
	}

	sup := &Supervisor{
		cfg:       DefaultConfig(),
		processes: procs,
		startedAt: time.Now().Add(-1 * time.Hour),
	}

	handler := newHealthHandler(sup)
	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Positive: status 200.
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp healthResponse
	if err := json.Unmarshal(
		w.Body.Bytes(), &resp,
	); err != nil {
		t.Fatalf("invalid JSON: %v\nbody: %s",
			err, w.Body.String())
	}

	// Positive: overall status ok.
	if resp.Status != "ok" {
		t.Fatalf("expected status ok, got %q", resp.Status)
	}

	// Positive: two processes reported.
	if len(resp.Processes) != 2 {
		t.Fatalf("expected 2 processes, got %d",
			len(resp.Processes))
	}

	// Positive: processes are running.
	for _, p := range resp.Processes {
		if p.Status != "running" {
			t.Fatalf("expected running, got %q for %s",
				p.Status, p.Name)
		}
	}

	// Positive: uptime is roughly 1 hour.
	if resp.UptimeSeconds < 3500 {
		t.Fatalf("expected ~3600s uptime, got %.0f",
			resp.UptimeSeconds)
	}

	// Positive: storage info present.
	if resp.Storage.Type != "local" {
		t.Fatalf("expected local storage, got %q",
			resp.Storage.Type)
	}

	// Negative: PID should be nonzero for running process.
	for _, p := range resp.Processes {
		if p.PID == 0 {
			t.Fatalf("PID should be nonzero for %s", p.Name)
		}
	}
}

func TestHealthHandler_Stopped(t *testing.T) {
	t.Parallel()

	// Process that is not started — simulates a crashed child.
	procs := []*Process{
		{Name: "dead-proc", Bin: "sleep", Args: []string{"60"}},
	}

	sup := &Supervisor{
		cfg:       DefaultConfig(),
		processes: procs,
		startedAt: time.Now(),
	}

	handler := newHealthHandler(sup)
	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var resp healthResponse
	if err := json.Unmarshal(
		w.Body.Bytes(), &resp,
	); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	// Positive: overall status degraded when a process is down.
	if resp.Status != "degraded" {
		t.Fatalf("expected degraded, got %q", resp.Status)
	}

	// Positive: process reported as stopped.
	if resp.Processes[0].Status != "stopped" {
		t.Fatalf("expected stopped, got %q",
			resp.Processes[0].Status)
	}

	// Negative: PID should be 0 for stopped process.
	if resp.Processes[0].PID != 0 {
		t.Fatal("PID should be 0 for stopped process")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -run TestHealthHandler -v`

Expected: FAIL — `newHealthHandler` doesn't exist.

- [ ] **Step 3: Implement health handler**

Create `sidecar/health.go`:

```go
// sidecar/health.go
// HTTP health endpoint for the supervisor. Serves GET /healthz
// with per-process status, uptime, and storage info as JSON.

package sidecar

import (
	"encoding/json"
	"net/http"
	"time"
)

const healthMaxResponseBytes = 64 * 1024

// newHealthHandler returns an http.Handler that reports
// supervisor health as JSON.
func newHealthHandler(s *Supervisor) http.Handler {
	if s == nil {
		panic("newHealthHandler: supervisor is nil")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(
		w http.ResponseWriter, r *http.Request,
	) {
		if r == nil {
			panic("healthz: request is nil")
		}
		resp := buildHealthResponse(s)
		w.Header().Set("Content-Type", "application/json")
		data, err := json.Marshal(resp)
		if err != nil {
			http.Error(w,
				`{"error":"marshal failed"}`,
				http.StatusInternalServerError)
			return
		}
		if len(data) > healthMaxResponseBytes {
			http.Error(w,
				`{"error":"response too large"}`,
				http.StatusInternalServerError)
			return
		}
		w.Write(data)
	})
	return mux
}

// buildHealthResponse reads live supervisor state into
// the JSON response structure. Acquires process locks
// for concurrency safety.
func buildHealthResponse(s *Supervisor) map[string]any {
	if s == nil {
		panic("buildHealthResponse: supervisor is nil")
	}

	now := time.Now()
	uptimeSeconds := now.Sub(s.startedAt).Seconds()

	allHealthy := true
	procs := make([]map[string]any, 0, len(s.processes))

	for _, p := range s.processes {
		p.mu.Lock()
		status := "stopped"
		pid := 0
		procUptime := 0.0

		if p.isRunningLocked() {
			status = "running"
			if p.cmd != nil && p.cmd.Process != nil {
				pid = p.cmd.Process.Pid
			}
			if !p.startedAt.IsZero() {
				procUptime = now.Sub(p.startedAt).Seconds()
			}
		} else {
			allHealthy = false
		}

		restarts := p.restarts
		p.mu.Unlock()

		procs = append(procs, map[string]any{
			"name":           p.Name,
			"status":         status,
			"pid":            pid,
			"restarts":       restarts,
			"uptime_seconds": procUptime,
		})
	}

	overallStatus := "ok"
	if !allHealthy {
		overallStatus = "degraded"
	}

	return map[string]any{
		"status":         overallStatus,
		"uptime_seconds": uptimeSeconds,
		"processes":      procs,
		"storage": map[string]any{
			"path": s.cfg.Storage.LocalPath,
			"type": s.cfg.Storage.Type,
		},
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -run TestHealthHandler -v`

Expected: PASS (both Running and Stopped tests)

- [ ] **Step 5: Commit**

```bash
git add sidecar/health.go sidecar/health_test.go
git commit -m "feat(sidecar): add health HTTP handler for supervisor status"
```

---

### Task 9: Wire health server into Supervisor.Run

**Files:**
- Modify: `sidecar/supervisor.go` (start/stop HTTP server in Run)
- Test: `sidecar/health_test.go` (integration test with real server)

- [ ] **Step 1: Write failing test for health server lifecycle**

In `sidecar/health_test.go`, add. Note: uses `localhost:0` for a dynamic port,
and reads `sup.healthAddr` (added in Step 3) for the actual address.

```go
func TestHealthServer_Lifecycle(t *testing.T) {
	t.Parallel()

	procs := []*Process{
		{Name: "test", Bin: "sleep", Args: []string{"60"}},
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

	// Positive: should return 200.
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var body healthResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode health response: %v", err)
	}

	// Positive: process reported.
	if len(body.Processes) != 1 {
		t.Fatalf("expected 1 process, got %d",
			len(body.Processes))
	}

	sup.Stop()

	// Negative: health endpoint should be down after Stop.
	_, err = http.Get(url)
	if err == nil {
		t.Fatal("health endpoint should be down after Stop")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -run TestHealthServer_Lifecycle -v`

Expected: FAIL — health server not started.

- [ ] **Step 3: Add health server start/stop to Supervisor**

In `sidecar/supervisor.go`, add a field to `Supervisor`:

```go
	healthSrv *http.Server
```

Add import `"net/http"`.

Add two new methods:

```go
const (
	healthReadTimeout  = 5 * time.Second
	healthWriteTimeout = 5 * time.Second
	healthShutTimeout  = 5 * time.Second
	healthMaxHeader    = 1 << 16
)

// startHealthServer launches the health HTTP server in
// a background goroutine. Must be called after Start().
func (s *Supervisor) startHealthServer() error {
	if s.cfg.Supervisor.Listen == "" {
		panic("startHealthServer: listen address is empty")
	}

	handler := newHealthHandler(s)
	s.healthSrv = &http.Server{
		Addr:           s.cfg.Supervisor.Listen,
		Handler:        handler,
		ReadTimeout:    healthReadTimeout,
		WriteTimeout:   healthWriteTimeout,
		MaxHeaderBytes: healthMaxHeader,
	}

	ln, err := net.Listen(
		"tcp", s.cfg.Supervisor.Listen,
	)
	if err != nil {
		return fmt.Errorf(
			"health listen %s: %w",
			s.cfg.Supervisor.Listen, err,
		)
	}

	go func() {
		if err := s.healthSrv.Serve(ln); err != nil &&
			err != http.ErrServerClosed {
			// Log but don't crash — health is advisory.
			fmt.Fprintf(os.Stderr,
				"health server error: %v\n", err)
		}
	}()

	return nil
}

// stopHealthServer gracefully shuts down the health server
// with a bounded timeout.
func (s *Supervisor) stopHealthServer() {
	if s.healthSrv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), healthShutTimeout,
	)
	defer cancel()
	_ = s.healthSrv.Shutdown(ctx)
}
```

Add `"net"` and `"os"` to imports.

Modify `Supervisor.Run()` to start the health server after `Start()`:

```go
func (s *Supervisor) Run() error {
	if err := s.Start(); err != nil {
		return fmt.Errorf("supervisor start: %w", err)
	}

	if err := s.startHealthServer(); err != nil {
		s.Stop()
		return fmt.Errorf("health server: %w", err)
	}
```

Modify `Supervisor.Stop()` to shut down the health server first:

```go
func (s *Supervisor) Stop() {
	s.stopHealthServer()
	s.stopUpTo(len(s.processes))
	s.cancel()
}
```

- [ ] **Step 4: Note on test port handling**

The test in Step 1 uses `localhost:0` so `net.Listen` picks a dynamic port. The
Supervisor stores the actual address in `healthAddr` (added in Step 3 via
`s.healthAddr = ln.Addr().String()` after `net.Listen`). Existing tests that
use `testSupervisor` but don't call `startHealthServer` are unaffected — the
health server only starts in `Run()` or explicitly.

- [ ] **Step 5: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -run TestHealthServer_Lifecycle -v`

Expected: PASS

- [ ] **Step 6: Run full sidecar package tests**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -v`

Expected: all pass

- [ ] **Step 7: Commit**

```bash
git add sidecar/supervisor.go sidecar/health.go sidecar/health_test.go
git commit -m "feat(sidecar): wire health HTTP server into supervisor lifecycle"
```

---

## Chunk 4: Phase 2 — CLI Commands

### Task 10: Update sidecar status to use health endpoint

**Files:**
- Modify: `cli/sidecar.go` (rewrite runSidecarStatusCmd)
- Test: `cli/sidecar_test.go`

- [ ] **Step 1: Write failing test for status with health endpoint**

In `cli/sidecar_test.go`, add:

```go
func TestSidecarStatusWithHealthEndpoint(t *testing.T) {
	// Start a mock health endpoint.
	handler := http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(
				"Content-Type", "application/json",
			)
			fmt.Fprint(w, `{
				"status": "ok",
				"uptime_seconds": 3621,
				"processes": [
					{
						"name": "otelcol",
						"status": "running",
						"pid": 12345,
						"restarts": 0,
						"uptime_seconds": 3621
					}
				],
				"storage": {
					"path": "./telemetry-data",
					"type": "local"
				}
			}`)
		},
	)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	output := captureSidecarOutput(func() {
		printHealthStatus(srv.URL)
	})

	// Positive: should show process name.
	if !strings.Contains(output, "otelcol") {
		t.Fatalf(
			"expected otelcol in status, got:\n%s", output)
	}

	// Positive: should show running.
	if !strings.Contains(output, "running") {
		t.Fatalf(
			"expected running in status, got:\n%s", output)
	}

	// Negative: should not say "not running".
	if strings.Contains(output, "not running") {
		t.Fatalf(
			"should not say not running, got:\n%s", output)
	}
}
```

Add `"fmt"`, `"net/http"`, `"net/http/httptest"` to imports.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestSidecarStatusWithHealthEndpoint -v`

Expected: FAIL — `printHealthStatus` doesn't exist.

- [ ] **Step 3: Implement the health status probe and display**

In `cli/sidecar.go`, add:

```go
const (
	healthProbeTimeout = 2 * time.Second
	healthMaxResponse  = 64 * 1024
)

// probeHealthEndpoint hits the supervisor /healthz endpoint.
// Returns the JSON body or an error if unreachable.
func probeHealthEndpoint(
	baseURL string,
) ([]byte, error) {
	if baseURL == "" {
		panic("probeHealthEndpoint: baseURL is empty")
	}

	client := &http.Client{Timeout: healthProbeTimeout}
	url := baseURL + "/healthz"

	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("probe: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(
		io.LimitReader(resp.Body, healthMaxResponse),
	)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"health returned %d", resp.StatusCode,
		)
	}

	return data, nil
}

// printHealthStatus fetches and displays health from
// the given supervisor URL.
func printHealthStatus(baseURL string) {
	if baseURL == "" {
		panic("printHealthStatus: baseURL is empty")
	}

	data, err := probeHealthEndpoint(baseURL)
	if err != nil {
		fmt.Println("Sidecar not running")
		printBinaryStatus()
		return
	}

	var resp struct {
		Status        string  `json:"status"`
		UptimeSeconds float64 `json:"uptime_seconds"`
		Processes     []struct {
			Name          string  `json:"name"`
			Status        string  `json:"status"`
			PID           int     `json:"pid"`
			Restarts      int     `json:"restarts"`
			UptimeSeconds float64 `json:"uptime_seconds"`
		} `json:"processes"`
		Storage struct {
			Path string `json:"path"`
			Type string `json:"type"`
		} `json:"storage"`
	}

	if err := json.Unmarshal(data, &resp); err != nil {
		fmt.Fprintf(os.Stderr,
			"error: parse health response: %v\n", err)
		exitFunc(1)
		return
	}

	uptime := time.Duration(
		resp.UptimeSeconds,
	) * time.Second
	fmt.Printf("Sidecar running (uptime: %s)\n",
		formatDuration(uptime))

	fmt.Printf("  %-20s %-8s %-6s %-10s %s\n",
		"NAME", "STATUS", "PID", "RESTARTS", "UPTIME")
	for _, p := range resp.Processes {
		procUptime := time.Duration(
			p.UptimeSeconds,
		) * time.Second
		fmt.Printf("  %-20s %-8s %-6d %-10d %s\n",
			p.Name, p.Status, p.PID,
			p.Restarts, formatDuration(procUptime))
	}

	fmt.Printf("Storage: %s (%s)\n",
		resp.Storage.Path, resp.Storage.Type)
}

// printBinaryStatus prints the binary-exists fallback
// (existing behavior extracted).
func printBinaryStatus() {
	names := []string{
		"otelcol", "otlp2parquet", "dagnats-mcp-duckdb",
	}
	for _, name := range names {
		path, err := sidecar.FindBinary(name)
		if err != nil {
			fmt.Printf("  %-20s not found\n", name)
			continue
		}
		fmt.Printf("  %-20s %s\n", name, path)
	}
}

// formatDuration renders a duration as a human-friendly
// string like "1h0m21s".
func formatDuration(d time.Duration) string {
	d = d.Truncate(time.Second)
	return d.String()
}
```

Add `"encoding/json"`, `"io"`, `"net/http"`, `"time"` to imports.

Update `runSidecarStatusCmd` to use the new functions:

```go
func runSidecarStatusCmd(args []string) {
	if args == nil {
		panic("runSidecarStatusCmd: args must not be nil")
	}
	if HasHelpFlag(args) {
		fmt.Println(
			"Usage: dagnats sidecar status [--json]",
		)
		fmt.Println()
		fmt.Println(
			"Checks sidecar health via the supervisor endpoint.",
		)
		fmt.Println(
			"Falls back to binary detection if sidecar is not running.",
		)
		return
	}

	jsonOutput := HasJSONFlag(args)

	configPath := extractConfigFlag(args)
	if configPath == "" {
		configPath = defaultConfigFileName
	}

	cfg, _ := sidecar.LoadConfig(configPath)
	if cfg == nil {
		cfg = sidecar.DefaultConfig()
	}

	baseURL := "http://" + cfg.Supervisor.Listen

	if jsonOutput {
		data, err := probeHealthEndpoint(baseURL)
		if err != nil {
			fmt.Fprintf(os.Stderr,
				"sidecar not running: %v\n", err)
			exitFunc(1)
			return
		}
		os.Stdout.Write(data)
		fmt.Println()
		return
	}

	printHealthStatus(baseURL)
}
```

Update `printSidecarUsage` to document the `--json` flag on status:

```go
fmt.Println(
	"  status   check sidecar health [--json]",
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestSidecarStatusWithHealthEndpoint -v`

Expected: PASS

- [ ] **Step 5: Write test for status fallback when unreachable**

In `cli/sidecar_test.go`, add:

```go
func TestSidecarStatusFallback(t *testing.T) {
	output := captureSidecarOutput(func() {
		// Use an address nothing is listening on.
		printHealthStatus("http://127.0.0.1:19999")
	})

	// Positive: should say not running.
	if !strings.Contains(output, "not running") {
		t.Fatalf(
			"expected 'not running', got:\n%s", output)
	}

	// Positive: should still show binary names.
	if !strings.Contains(output, "otelcol") {
		t.Fatalf(
			"expected otelcol in fallback, got:\n%s", output)
	}
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestSidecarStatusFallback -v`

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add cli/sidecar.go cli/sidecar_test.go
git commit -m "feat(sidecar): status command probes health endpoint with binary fallback"
```

---

### Task 11: Update banner to show health endpoint

**Files:**
- Modify: `cli/sidecar.go` (printStartBanner)
- Test: `cli/sidecar_test.go`

- [ ] **Step 1: Write failing test**

In `cli/sidecar_test.go`, add:

```go
func TestPrintStartBannerHealthLine(t *testing.T) {
	cfg := sidecar.DefaultConfig()

	output := captureSidecarOutput(func() {
		printStartBanner(cfg)
	})

	// Positive: should show health endpoint URL.
	if !strings.Contains(output, "/healthz") {
		t.Fatalf(
			"expected /healthz in banner, got:\n%s", output)
	}

	// Positive: should show supervisor listen address.
	if !strings.Contains(output, "localhost:4320") {
		t.Fatalf(
			"expected localhost:4320 in banner, got:\n%s",
			output)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestPrintStartBannerHealthLine -v`

Expected: FAIL

- [ ] **Step 3: Add health line to printStartBanner**

In `cli/sidecar.go`, in `printStartBanner`, after the backend line (or MCP line if no backend), add:

```go
	fmt.Printf("  Health:      http://%s/healthz\n",
		cfg.Supervisor.Listen)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestPrintStartBannerHealthLine -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add cli/sidecar.go cli/sidecar_test.go
git commit -m "feat(sidecar): show health endpoint URL in startup banner"
```

---

### Task 12: `sidecar init` command

**Files:**
- Modify: `cli/sidecar.go` (add init subcommand and handler)
- Test: `cli/sidecar_test.go`

- [ ] **Step 1: Write failing test for init creates file**

In `cli/sidecar_test.go`, add:

```go
func TestSidecarInitCreatesFile(t *testing.T) {
	dir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(oldDir)

	output := captureSidecarOutput(func() {
		runSidecarInitCmd([]string{})
	})

	// Positive: file should exist.
	cfgPath := filepath.Join(dir, "dagnats.yaml")
	if _, err := os.Stat(cfgPath); err != nil {
		t.Fatalf("expected dagnats.yaml to exist: %v", err)
	}

	// Positive: file should contain commented config.
	data, _ := os.ReadFile(cfgPath)
	content := string(data)
	if !strings.Contains(content, "# listen:") {
		t.Fatalf(
			"expected commented listen, got:\n%s", content)
	}

	// Positive: output should confirm creation.
	if !strings.Contains(output, "dagnats.yaml") {
		t.Fatalf(
			"expected confirmation, got:\n%s", output)
	}

	// Negative: should not be empty.
	if len(data) == 0 {
		t.Fatal("config file should not be empty")
	}
}
```

Add `"path/filepath"` to imports.

- [ ] **Step 2: Write failing test for init refuses overwrite**

In `cli/sidecar_test.go`, add:

```go
func TestSidecarInitRefusesOverwrite(t *testing.T) {
	dir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(oldDir)

	// Create existing file.
	cfgPath := filepath.Join(dir, "dagnats.yaml")
	os.WriteFile(cfgPath, []byte("existing"), 0o600)

	var exitCode int
	oldExit := exitFunc
	exitFunc = func(code int) { exitCode = code }
	defer func() { exitFunc = oldExit }()

	runSidecarInitCmd([]string{})

	// Positive: should exit with code 1.
	if exitCode != 1 {
		t.Fatalf("expected exit 1, got %d", exitCode)
	}

	// Negative: original file should be unchanged.
	data, _ := os.ReadFile(cfgPath)
	if string(data) != "existing" {
		t.Fatal("should not overwrite existing file")
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestSidecarInit -v`

Expected: FAIL — `runSidecarInitCmd` doesn't exist.

- [ ] **Step 4: Implement sidecar init**

In `cli/sidecar.go`, add the init command handler:

```go
const initConfigTemplate = `# Sidecar configuration — uncomment to override defaults.
# listen: 0.0.0.0:4318
# supervisor:
#   listen: localhost:4320
# storage:
#   type: local
#   local_path: ./telemetry-data
# backend:
#   endpoint: https://otel.example.com
#   headers:
#     Authorization: Bearer <token>
# mcp:
#   listen: ""  # empty = stdio
`

// runSidecarInitCmd scaffolds a dagnats.yaml config file.
func runSidecarInitCmd(args []string) {
	if args == nil {
		panic("runSidecarInitCmd: args must not be nil")
	}
	if HasHelpFlag(args) {
		fmt.Println(
			"Usage: dagnats sidecar init",
		)
		fmt.Println()
		fmt.Println(
			"Creates a dagnats.yaml config file in the " +
				"current directory.",
		)
		return
	}

	if _, err := os.Stat(defaultConfigFileName); err == nil {
		fmt.Fprintf(os.Stderr,
			"error: %s already exists\n",
			defaultConfigFileName)
		exitFunc(1)
		return
	}

	const filePerms = 0o644
	if err := os.WriteFile(
		defaultConfigFileName,
		[]byte(initConfigTemplate),
		filePerms,
	); err != nil {
		fmt.Fprintf(os.Stderr,
			"error: write config: %v\n", err)
		exitFunc(1)
		return
	}

	fmt.Printf("Created %s\n", defaultConfigFileName)
}
```

Add `"init"` to the switch in `runSidecarCmd`:

```go
	case "init":
		runSidecarInitCmd(args[1:])
```

Update `printSidecarUsage` to include init:

```go
	fmt.Println(
		"  init     scaffold a dagnats.yaml config file",
	)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestSidecarInit -v`

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add cli/sidecar.go cli/sidecar_test.go
git commit -m "feat(sidecar): add init command to scaffold dagnats.yaml"
```

---

### Task 13: `sidecar start --dry-run`

**Files:**
- Modify: `cli/sidecar.go` (parse --dry-run, print config, exit)
- Test: `cli/sidecar_test.go`

- [ ] **Step 1: Write failing test for dry-run**

In `cli/sidecar_test.go`, add:

```go
func TestSidecarStartDryRun(t *testing.T) {
	dir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(oldDir)

	output := captureSidecarOutput(func() {
		runSidecarStartCmd([]string{"--dry-run"})
	})

	// Positive: should contain OTel collector config YAML.
	if !strings.Contains(output, "receivers:") {
		t.Fatalf(
			"expected receivers: in dry-run output, got:\n%s",
			output)
	}

	// Positive: should contain the OTLP receiver.
	if !strings.Contains(output, "otlp:") {
		t.Fatalf(
			"expected otlp: in dry-run output, got:\n%s",
			output)
	}

	// Negative: should not contain supervisor started message.
	if strings.Contains(output, "Sidecar started") {
		t.Fatal("dry-run should not start the sidecar")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestSidecarStartDryRun -v`

Expected: FAIL — `--dry-run` is not handled.

- [ ] **Step 3: Implement dry-run in runSidecarStartCmd**

In `cli/sidecar.go`, add a flag helper:

```go
// hasDryRunFlag checks for --dry-run in args.
func hasDryRunFlag(args []string) bool {
	for _, a := range args {
		if a == "--dry-run" {
			return true
		}
	}
	return false
}
```

Replace the entire `runSidecarStartCmd` function with:

```go
func runSidecarStartCmd(args []string) {
	if args == nil {
		panic("runSidecarStartCmd: args must not be nil")
	}
	if HasHelpFlag(args) {
		printSidecarUsage()
		return
	}

	dryRun := hasDryRunFlag(args)

	configPath := extractConfigFlag(args)
	if configPath == "" {
		configPath = defaultConfigFileName
	}
	cfg := loadSidecarConfig(configPath)

	if dryRun {
		data, err := sidecar.GenerateCollectorConfig(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr,
				"error: generate config: %v\n", err)
			exitFunc(1)
			return
		}
		fmt.Print(string(data))
		return
	}

	ensureStorageDir(cfg)
	writeCollectorYAML(cfg)
	checkBinariesAvailable()
	startSupervisor(cfg)
}
```

Uses `sidecar.GenerateCollectorConfig` directly instead of writing to disk
first — dry-run generates the config in memory and prints it without
side effects.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestSidecarStartDryRun -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add cli/sidecar.go cli/sidecar_test.go
git commit -m "feat(sidecar): add --dry-run flag to print generated collector config"
```

---

### Task 14: Include dagnats-mcp-duckdb in install and status

**Files:**
- Modify: `sidecar/install.go` (add BuildLocal, add to InstallAll)
- Test: `sidecar/install_test.go`
- Modify: `cli/sidecar.go` (add to checkBinariesAvailable)

- [ ] **Step 1: Write failing test for BuildLocal**

In `sidecar/install_test.go`, add:

```go
func TestBuildLocal_GoNotFound(t *testing.T) {
	// Override PATH to hide go binary.
	t.Setenv("PATH", t.TempDir())

	err := BuildLocal("test", "./cmd/test/")

	// Positive: should return error about go.
	if err == nil {
		t.Fatal("expected error when go not on PATH")
	}

	// Positive: error should mention go.
	if !strings.Contains(err.Error(), "go") {
		t.Errorf("error = %q, want mention of 'go'",
			err.Error())
	}
}
```

Add `"strings"` to imports.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -run TestBuildLocal -v`

Expected: FAIL — `BuildLocal` doesn't exist.

- [ ] **Step 3: Implement BuildLocal**

In `sidecar/install.go`, add:

```go
// localBinary describes a binary built from source in this repo.
type localBinary struct {
	Name string
	Pkg  string
}

// localBinaries lists binaries built from local source.
var localBinaries = []localBinary{
	{
		Name: "dagnats-mcp-duckdb",
		Pkg:  "./cmd/dagnats-mcp-duckdb/",
	},
}

// BuildLocal compiles a Go package and places the binary
// in ~/.dagnats/bin/. Requires the Go toolchain on PATH.
// Uses the full module path to avoid working-directory
// dependence on relative package paths.
func BuildLocal(name, pkg string) error {
	if name == "" {
		panic("BuildLocal: name is empty")
	}
	if pkg == "" {
		panic("BuildLocal: pkg is empty")
	}

	// Check for Go toolchain with a friendly error.
	if _, err := exec.LookPath("go"); err != nil {
		return fmt.Errorf(
			"go toolchain not found on PATH: "+
				"install Go to build %s", name,
		)
	}

	binDir, err := BinDir()
	if err != nil {
		return err
	}

	// Find the module root so relative package paths resolve
	// correctly regardless of the caller's working directory.
	modRoot, err := findModuleRoot()
	if err != nil {
		return fmt.Errorf("find module root: %w", err)
	}

	dest := filepath.Join(binDir, name)
	cmd := exec.Command(
		"go", "build", "-o", dest, pkg,
	)
	cmd.Dir = modRoot
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("go build %s: %w", name, err)
	}

	return nil
}

// findModuleRoot returns the directory containing go.mod
// by running `go env GOMOD` and taking its directory.
func findModuleRoot() (string, error) {
	out, err := exec.Command(
		"go", "env", "GOMOD",
	).Output()
	if err != nil {
		return "", fmt.Errorf("go env GOMOD: %w", err)
	}
	modFile := strings.TrimSpace(string(out))
	if modFile == "" || modFile == os.DevNull {
		return "", fmt.Errorf(
			"not inside a Go module")
	}
	return filepath.Dir(modFile), nil
}
```

Add local binaries to `InstallAll`, after the existing download loop:

```go
	// Build local binaries from source.
	for _, lb := range localBinaries {
		path, err := FindBinary(lb.Name)
		if err == nil {
			fmt.Fprintf(w, "✓ %s found at %s\n",
				lb.Name, path)
			continue
		}

		fmt.Fprintf(w, "🔨 building %s from source...\n",
			lb.Name)

		if err := BuildLocal(lb.Name, lb.Pkg); err != nil {
			return fmt.Errorf(
				"build %s: %w", lb.Name, err,
			)
		}

		fmt.Fprintf(w, "✓ %s built\n", lb.Name)
	}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -run TestBuildLocal -v`

Expected: PASS

- [ ] **Step 5: Update checkBinariesAvailable in cli/sidecar.go**

In `cli/sidecar.go`, in `checkBinariesAvailable()`, add `"dagnats-mcp-duckdb"` to the required slice:

```go
	required := []string{
		"otelcol", "otlp2parquet", "dagnats-mcp-duckdb",
	}
```

- [ ] **Step 6: Update sidecar status binary list**

The `printBinaryStatus` function (added in Task 10) already lists all three binaries. If the old `runSidecarStatusCmd` binary list is still separate, update it too. Verify the list includes `dagnats-mcp-duckdb`.

- [ ] **Step 7: Run full install test suite**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -run TestInstall -v && go test ./sidecar/ -run TestBuildLocal -v`

Expected: all pass

- [ ] **Step 8: Commit**

```bash
git add sidecar/install.go sidecar/install_test.go cli/sidecar.go
git commit -m "feat(sidecar): include dagnats-mcp-duckdb in install and status"
```

---

### Task 15: Update observe status to use health endpoint

**Files:**
- Modify: `cli/observe_status.go` (switch sidecar detection to /healthz)
- Test: `cli/observe_status_test.go`

- [ ] **Step 1: Write failing test for richer sidecar status**

In `cli/observe_status_test.go`, add:

```go
func TestCollectSidecarStatusFromHealth(t *testing.T) {
	// Mock health endpoint.
	handler := http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(
				"Content-Type", "application/json",
			)
			fmt.Fprint(w, `{
				"status": "ok",
				"processes": [
					{"name": "a", "status": "running"},
					{"name": "b", "status": "running"},
					{"name": "c", "status": "running"}
				]
			}`)
		},
	)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	// Extract host:port from test server URL.
	addr := strings.TrimPrefix(srv.URL, "http://")

	got := collectSidecarStatusFromAddr(addr)

	// Positive: status should mention running count.
	if !strings.Contains(got.Status, "3") {
		t.Fatalf("expected count in status, got %q",
			got.Status)
	}

	// Negative: should not be "not detected".
	if got.Status == "not detected" {
		t.Fatal("should detect running sidecar")
	}
}
```

Add `"fmt"`, `"net/http"`, `"net/http/httptest"` to imports.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestCollectSidecarStatusFromHealth -v`

Expected: FAIL — `collectSidecarStatusFromAddr` doesn't exist.

- [ ] **Step 3: Implement health-aware sidecar status collection**

In `cli/observe_status.go`, replace `collectSidecarStatus()` with a version that tries the health endpoint first:

```go
// collectSidecarStatusFromAddr tries the health endpoint,
// falls back to TCP probe.
func collectSidecarStatusFromAddr(
	addr string,
) *sidecarStatus {
	if addr == "" {
		panic(
			"collectSidecarStatusFromAddr: addr is empty",
		)
	}

	url := "http://" + addr + "/healthz"
	client := &http.Client{Timeout: sidecarProbeTimeout}

	resp, err := client.Get(url)
	if err != nil {
		// Fall back to TCP probe.
		return probeSidecarTCP(addr)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return probeSidecarTCP(addr)
	}

	var health struct {
		Status    string `json:"status"`
		Processes []struct {
			Status string `json:"status"`
		} `json:"processes"`
	}

	data, err := io.ReadAll(
		io.LimitReader(resp.Body, 64*1024),
	)
	if err != nil {
		return probeSidecarTCP(addr)
	}

	if err := json.Unmarshal(data, &health); err != nil {
		return probeSidecarTCP(addr)
	}

	running := 0
	for _, p := range health.Processes {
		if p.Status == "running" {
			running++
		}
	}

	return &sidecarStatus{
		Address: addr,
		Status: fmt.Sprintf("running, %d processes healthy",
			running),
	}
}

// probeSidecarTCP does the old TCP dial check.
func probeSidecarTCP(addr string) *sidecarStatus {
	conn, err := net.DialTimeout(
		"tcp", addr, sidecarProbeTimeout,
	)
	if err != nil {
		return &sidecarStatus{
			Address: addr,
			Status:  "not detected",
		}
	}
	conn.Close()
	return &sidecarStatus{
		Address: addr,
		Status:  "detected",
	}
}
```

Add `"encoding/json"`, `"io"`, `"net/http"` to imports.

Add a constant for the supervisor default address and update
`collectSidecarStatus()` to probe the health endpoint on the
supervisor port (4320), not the collector port (4318):

```go
const supervisorDefaultAddress = "localhost:4320"

func collectSidecarStatus() *sidecarStatus {
	return collectSidecarStatusFromAddr(
		supervisorDefaultAddress,
	)
}
```

Note: `sidecarDefaultAddress` (`localhost:4318`) was the collector port.
The health endpoint runs on the supervisor port (`localhost:4320`).
The `sidecarStatus.Address` field will now show the supervisor address.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestCollectSidecarStatusFromHealth -v`

Expected: PASS

- [ ] **Step 5: Run full observe test suite**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestObserve -v`

Expected: all existing tests still pass. `TestCollectSidecarStatusNotDetected` should still work because the fallback TCP probe handles the no-server case.

- [ ] **Step 6: Commit**

```bash
git add cli/observe_status.go cli/observe_status_test.go
git commit -m "feat(observe): probe health endpoint for richer sidecar status"
```

---

### Task 16: Final integration verification

- [ ] **Step 1: Run all sidecar package tests**

Run: `cd /Users/dmestas/projects/dagnats && go test ./sidecar/ -v`

Expected: all pass

- [ ] **Step 2: Run all CLI tests**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -v`

Expected: all pass

- [ ] **Step 3: Run vet and staticcheck**

Run: `cd /Users/dmestas/projects/dagnats && go vet ./... && staticcheck ./...`

Expected: clean

- [ ] **Step 4: Run full test suite**

Run: `cd /Users/dmestas/projects/dagnats && go test ./...`

Expected: all pass

- [ ] **Step 5: Commit any final fixes**

If any tests failed in the integration sweep, fix and commit.
