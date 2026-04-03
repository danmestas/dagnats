# Embedded Workers Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Embed worker handlers directly in `dagnats serve` via a Go API and config-driven handlers, eliminating the need for a separate worker process.

**Architecture:** `server.EmbeddedWorker(srv)` returns a `*WorkerShim` that records handler registrations before `Run()`. During `startComponents()`, shims are materialized into real `*worker.Worker` instances. Config-driven handlers (`exec`, `http`) are wired in `cli/serve.go` using the same shim API.

**Tech Stack:** Go, NATS JetStream (existing), `os/exec` for exec handlers, `net/http` for HTTP handlers.

**Spec:** `docs/superpowers/specs/2026-04-03-embedded-workers-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `server/embedded.go` | Create | `WorkerShim` struct, `EmbeddedWorker()` function, materialization logic |
| `server/embedded_test.go` | Create | Unit tests for shim registration, panics, bounds |
| `server/server.go` | Modify | Add `workerShims`, `workers`, `running` fields; start/stop workers in lifecycle |
| `server/config.go` | Modify | Add `WorkerConfig` type, parse `worker.*` keys, bump `maxConfigFileLines` |
| `server/config_test.go` | Modify | Tests for worker config parsing and validation |
| `cli/handlers.go` | Create | `buildHandler()` factory, `execHandler()`, `httpHandler()` |
| `cli/handlers_test.go` | Create | Unit tests for exec and HTTP handler factories |
| `cli/serve.go` | Modify | Wire config-driven handlers before `srv.Run()` |
| `server/server_test.go` | Modify | Integration test: embedded worker completes a workflow run |

---

## Chunk 1: WorkerShim and Server Lifecycle

### Task 1: WorkerShim, EmbeddedWorker, and Server struct fields

**Files:**
- Create: `server/embedded.go`
- Modify: `server/server.go:26-38` (Server struct — add fields needed by EmbeddedWorker)
- Test: `server/embedded_test.go`

> **Note:** Server struct fields (`workerShims`, `workers`, `running`) are added in this task
> because `EmbeddedWorker()` references them. Lifecycle wiring (start/stop) is Task 2.

- [ ] **Step 1: Write failing test for WorkerShim.Handle**

In `server/embedded_test.go`:

```go
// Methodology: Pure unit tests for WorkerShim. No NATS required.
// Tests verify registration recording, panic guards, and bounds.
package server

import (
	"fmt"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/worker"
)

func TestWorkerShim_Handle_RecordsRegistration(t *testing.T) {
	shim := &WorkerShim{}
	called := false
	handler := func(ctx worker.TaskContext) error {
		called = true
		return nil
	}

	shim.Handle("test-task", handler)

	// Positive: registration recorded
	if len(shim.registrations) != 1 {
		t.Fatalf("registrations = %d, want 1",
			len(shim.registrations))
	}
	if shim.registrations[0].taskType != "test-task" {
		t.Errorf("taskType = %q, want %q",
			shim.registrations[0].taskType, "test-task")
	}

	// Negative: handler must not be nil
	if shim.registrations[0].handler == nil {
		t.Fatal("handler is nil")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./server/ -run TestWorkerShim_Handle_RecordsRegistration -v`
Expected: FAIL — `WorkerShim` type not defined

- [ ] **Step 3: Add fields to Server struct**

In `server/server.go`, add to the `Server` struct (lines 26-38) and imports:

```go
import (
	// ... existing imports ...
	"github.com/danmestas/dagnats/worker"
)

type Server struct {
	// ... existing fields ...
	workerShims []*WorkerShim      // pre-Run registrations
	workers     []*worker.Worker   // live workers (post-start)
	running     atomic.Bool        // guards EmbeddedWorker
}
```

- [ ] **Step 4: Write minimal WorkerShim implementation**

Create `server/embedded.go`:

```go
package server

import (
	"github.com/danmestas/dagnats/worker"
)

const maxEmbeddedWorkers = 50

// registration pairs a task type with its handler function.
type registration struct {
	taskType string
	handler  worker.HandlerFunc
}

// WorkerShim collects handler registrations before the server
// starts. Returned by EmbeddedWorker(). The shim is materialized
// to a real *worker.Worker during startComponents().
type WorkerShim struct {
	registrations []registration
	groups        []string
	started       bool
}

// Handle registers a handler for a task type. Panics if called
// after Run(), if taskType is empty, or if handler is nil.
func (s *WorkerShim) Handle(
	taskType string, handler worker.HandlerFunc,
) {
	if s == nil {
		panic("WorkerShim.Handle: s is nil")
	}
	if s.started {
		panic("WorkerShim.Handle: called after Run()")
	}
	if taskType == "" {
		panic("WorkerShim.Handle: taskType is empty")
	}
	if handler == nil {
		panic("WorkerShim.Handle: handler is nil")
	}
	s.registrations = append(s.registrations, registration{
		taskType: taskType,
		handler:  handler,
	})
}

// WithGroups configures this embedded worker for specific worker
// groups. During materialization, translated to
// worker.WithGroups(groups...). Panics after Run().
func (s *WorkerShim) WithGroups(groups ...string) {
	if s == nil {
		panic("WorkerShim.WithGroups: s is nil")
	}
	if s.started {
		panic("WorkerShim.WithGroups: called after Run()")
	}
	if len(groups) == 0 {
		panic("WorkerShim.WithGroups: groups is empty")
	}
	for _, g := range groups {
		if g == "" {
			panic("WorkerShim.WithGroups: group name is empty")
		}
	}
	s.groups = groups
}

// EmbeddedWorker creates a WorkerShim bound to srv's lifecycle.
// Must be called before Run(). Panics if called after Run(), if
// srv is nil, or if the max embedded worker limit is exceeded.
func EmbeddedWorker(srv *Server) *WorkerShim {
	if srv == nil {
		panic("EmbeddedWorker: srv is nil")
	}
	if srv.running.Load() {
		panic("EmbeddedWorker: called after Run()")
	}
	if len(srv.workerShims) >= maxEmbeddedWorkers {
		panic("EmbeddedWorker: max embedded workers exceeded")
	}
	shim := &WorkerShim{}
	srv.workerShims = append(srv.workerShims, shim)
	return shim
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./server/ -run TestWorkerShim_Handle_RecordsRegistration -v`
Expected: PASS

- [ ] **Step 6: Write remaining shim unit tests**

Append to `server/embedded_test.go`:

```go
func TestWorkerShim_Handle_PanicsOnEmptyTaskType(t *testing.T) {
	shim := &WorkerShim{}
	defer func() {
		r := recover()
		// Positive: panic occurred
		if r == nil {
			t.Fatal("expected panic on empty taskType")
		}
		// Positive: message identifies the cause
		msg := fmt.Sprintf("%v", r)
		if !strings.Contains(msg, "taskType") {
			t.Errorf("panic = %q, want 'taskType'", msg)
		}
	}()
	shim.Handle("", func(ctx worker.TaskContext) error {
		return nil
	})
}

func TestWorkerShim_Handle_PanicsOnNilHandler(t *testing.T) {
	shim := &WorkerShim{}
	defer func() {
		r := recover()
		// Positive: panic occurred
		if r == nil {
			t.Fatal("expected panic on nil handler")
		}
		// Positive: message identifies the cause
		msg := fmt.Sprintf("%v", r)
		if !strings.Contains(msg, "handler") {
			t.Errorf("panic = %q, want 'handler'", msg)
		}
	}()
	shim.Handle("test-task", nil)
}

func TestWorkerShim_Handle_PanicsAfterStarted(t *testing.T) {
	shim := &WorkerShim{started: true}
	defer func() {
		r := recover()
		// Positive: panic occurred
		if r == nil {
			t.Fatal("expected panic after started")
		}
		// Positive: message identifies the cause
		msg := fmt.Sprintf("%v", r)
		if !strings.Contains(msg, "after Run") {
			t.Errorf("panic = %q, want 'after Run'", msg)
		}
	}()
	shim.Handle("test-task", func(ctx worker.TaskContext) error {
		return nil
	})
}

func TestWorkerShim_WithGroups_Records(t *testing.T) {
	shim := &WorkerShim{}
	shim.WithGroups("gpu", "cpu")

	// Positive: groups recorded
	if len(shim.groups) != 2 {
		t.Fatalf("groups = %d, want 2", len(shim.groups))
	}
	if shim.groups[0] != "gpu" {
		t.Errorf("groups[0] = %q, want %q",
			shim.groups[0], "gpu")
	}

	// Negative: registrations should be empty
	if len(shim.registrations) != 0 {
		t.Errorf("registrations = %d, want 0",
			len(shim.registrations))
	}
}

func TestWorkerShim_WithGroups_PanicsOnEmpty(t *testing.T) {
	shim := &WorkerShim{}
	defer func() {
		r := recover()
		// Positive: panic occurred
		if r == nil {
			t.Fatal("expected panic on empty groups")
		}
		// Positive: message identifies the cause
		msg := fmt.Sprintf("%v", r)
		if !strings.Contains(msg, "empty") {
			t.Errorf("panic = %q, want 'empty'", msg)
		}
	}()
	shim.WithGroups()
}

func TestEmbeddedWorker_PanicsOnNilServer(t *testing.T) {
	defer func() {
		r := recover()
		// Positive: panic occurred
		if r == nil {
			t.Fatal("expected panic on nil server")
		}
		// Positive: message identifies the cause
		msg := fmt.Sprintf("%v", r)
		if !strings.Contains(msg, "srv is nil") {
			t.Errorf("panic = %q, want 'srv is nil'", msg)
		}
	}()
	EmbeddedWorker(nil)
}

func TestEmbeddedWorker_TracksShimOnServer(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	srv := New(cfg)

	shim := EmbeddedWorker(srv)

	// Positive: shim is tracked
	if len(srv.workerShims) != 1 {
		t.Fatalf("workerShims = %d, want 1",
			len(srv.workerShims))
	}
	if srv.workerShims[0] != shim {
		t.Error("tracked shim does not match returned shim")
	}

	// Negative: workers not yet created
	if len(srv.workers) != 0 {
		t.Errorf("workers = %d, want 0 before Run()",
			len(srv.workers))
	}
}

func TestEmbeddedWorker_MultipleShims(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	srv := New(cfg)

	EmbeddedWorker(srv)
	EmbeddedWorker(srv)

	if len(srv.workerShims) != 2 {
		t.Fatalf("workerShims = %d, want 2",
			len(srv.workerShims))
	}
}
```

- [ ] **Step 7: Run all shim tests**

Run: `cd /Users/dmestas/projects/dagnats && go test ./server/ -run TestWorkerShim -v && go test ./server/ -run TestEmbeddedWorker -v`
Expected: all PASS

- [ ] **Step 8: Commit**

```bash
git add server/embedded.go server/embedded_test.go server/server.go
git commit -m "feat: add WorkerShim and EmbeddedWorker for embedded worker support"
```

### Task 2: Wire shims into Server lifecycle

**Files:**
- Modify: `server/server.go:53-75` (Run — set running flag)
- Modify: `server/server.go:79-144` (startComponents — materialize workers)
- Modify: `server/server.go:228-286` (shutdown — stop workers)

> **Note:** Server struct fields were already added in Task 1.

- [ ] **Step 1: Set running flag in Run()**

In `server/server.go` `Run()` method, add `s.running.Store(true)` before `s.startComponents()`:

```go
func (s *Server) Run() error {
	// ... existing assertions ...
	s.running.Store(true)

	if err := os.MkdirAll(s.cfg.DataDir, 0755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	// ... rest unchanged ...
}
```

- [ ] **Step 2: Materialize shims at end of startComponents()**

Add to the end of `startComponents()`, before `return nil`:

```go
	// Materialize embedded workers (must be after streams & KV exist)
	for _, shim := range s.workerShims {
		var opts []worker.WorkerOption
		if len(shim.groups) > 0 {
			opts = append(opts, worker.WithGroups(shim.groups...))
		}
		w := worker.NewWorker(s.nc, s.tel, opts...)
		for _, reg := range shim.registrations {
			w.Handle(reg.taskType, reg.handler)
		}
		if len(shim.registrations) > 0 {
			w.Start()
			s.workers = append(s.workers, w)
		}
		shim.started = true
	}
	if len(s.workers) > 0 {
		printStep(os.Stderr, "embedded workers started")
	}
	s.workerShims = nil // no ambiguous stale state
```

Also add cleanup in the error paths: if trigger start fails, stop any already-started workers.

- [ ] **Step 3: Add worker stop to shutdown()**

In `server/server.go` `shutdown()`, insert worker stop between triggers and orchestrator:

```go
		printStep(os.Stderr, "stopping triggers...")
		if s.trig != nil {
			s.trig.Stop()
		}

		// Stop embedded workers before orchestrator so in-flight
		// tasks can publish completion events.
		for _, w := range s.workers {
			w.Stop()
		}
		if len(s.workers) > 0 {
			printStep(os.Stderr,
				"embedded workers stopped")
		}

		printStep(os.Stderr, "stopping orchestrator...")
```

- [ ] **Step 4: Run existing server tests to verify no regression**

Run: `cd /Users/dmestas/projects/dagnats && go test ./server/ -v -timeout 60s`
Expected: all existing tests PASS

- [ ] **Step 5: Commit**

```bash
git add server/server.go
git commit -m "feat: wire WorkerShim lifecycle into Server start/stop"
```

### Task 3: Integration test — embedded worker completes a run

**Files:**
- Modify: `server/server_test.go`

- [ ] **Step 1: Write integration test**

Append to `server/server_test.go`:

```go
func TestServer_EmbeddedWorkerCompletesRun(t *testing.T) {
	cfg := testConfig(t)
	srv := New(cfg)

	if srv == nil {
		panic("New() returned nil")
	}

	// Register embedded handler that uppercases input
	w := EmbeddedWorker(srv)
	w.Handle("upper", func(ctx worker.TaskContext) error {
		input := string(ctx.Input())
		if input == "" {
			return ctx.Fail(fmt.Errorf("empty input"))
		}
		return ctx.Complete(
			[]byte(strings.ToUpper(input)),
		)
	})

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run() }()

	// Wait for ready
	readyURL := fmt.Sprintf(
		"http://%s/ready", cfg.HTTPAddr,
	)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(readyURL)
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Register workflow via REST API
	wfBody := `{
		"name": "embedded-test",
		"version": "1.0",
		"steps": [{"id": "upper", "task": "upper"}]
	}`
	wfResp, err := http.Post(
		fmt.Sprintf("http://%s/workflows", cfg.HTTPAddr),
		"application/json",
		strings.NewReader(wfBody),
	)
	if err != nil {
		t.Fatalf("register workflow: %v", err)
	}
	if wfResp.StatusCode != http.StatusOK &&
		wfResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(wfResp.Body)
		t.Fatalf("register workflow: %d %s",
			wfResp.StatusCode, string(body))
	}
	wfResp.Body.Close()

	// Start a run
	runBody := `{"workflow": "embedded-test", "input": "hello"}`
	runResp, err := http.Post(
		fmt.Sprintf("http://%s/runs", cfg.HTTPAddr),
		"application/json",
		strings.NewReader(runBody),
	)
	if err != nil {
		t.Fatalf("start run: %v", err)
	}
	runRespBody, _ := io.ReadAll(runResp.Body)
	runResp.Body.Close()

	if runResp.StatusCode != http.StatusOK &&
		runResp.StatusCode != http.StatusCreated {
		t.Fatalf("start run: %d %s",
			runResp.StatusCode, string(runRespBody))
	}

	// Poll run status until completed (bounded 15s)
	// Extract run_id from response
	var startResult struct {
		RunID string `json:"run_id"`
	}
	if err := json.Unmarshal(
		runRespBody, &startResult,
	); err != nil {
		t.Fatalf("unmarshal run response: %v", err)
	}
	if startResult.RunID == "" {
		t.Fatal("run_id is empty in response")
	}

	runURL := fmt.Sprintf(
		"http://%s/runs/%s", cfg.HTTPAddr, startResult.RunID,
	)
	pollDeadline := time.Now().Add(15 * time.Second)
	completed := false

	for time.Now().Before(pollDeadline) && !completed {
		resp, err := http.Get(runURL)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var runState struct {
			Status int `json:"status"`
		}
		if err := json.Unmarshal(body, &runState); err == nil {
			// dag.RunStatusCompleted == 2
			if runState.Status == 2 {
				completed = true
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Positive: run completed
	if !completed {
		t.Fatal("run did not complete within 15s")
	}

	// Clean shutdown
	srv.Stop()
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("Run() returned error: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Run() did not return within 20s")
	}
}
```

Add `"encoding/json"` to imports if not present, and `"github.com/danmestas/dagnats/worker"`.

- [ ] **Step 2: Run the integration test**

Run: `cd /Users/dmestas/projects/dagnats && go test ./server/ -run TestServer_EmbeddedWorkerCompletesRun -v -timeout 60s`
Expected: PASS — workflow registers, run starts, embedded handler executes, run completes

- [ ] **Step 3: Run full server test suite**

Run: `cd /Users/dmestas/projects/dagnats && go test ./server/ -v -timeout 120s`
Expected: all PASS

- [ ] **Step 4: Commit**

```bash
git add server/server_test.go
git commit -m "test: integration test for embedded worker completing a run"
```

---

## Chunk 2: Config Parsing and Built-in Handlers

### Task 4: Add WorkerConfig to config parser

**Files:**
- Modify: `server/config.go:1-20` (constants)
- Modify: `server/config.go:22-29` (Config struct)
- Modify: `server/config.go:133-224` (loadConfigFile, applyConfigValue)
- Modify: `server/config.go:98-127` (applyEnvOverrides)
- Test: `server/config_test.go`

- [ ] **Step 1: Write failing test for worker config parsing**

Append to `server/config_test.go`:

```go
func TestLoadConfigFile_ParsesWorkerEntries(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "dagnats.yaml")
	content := "worker.run-tests.exec: go test ./...\n" +
		"worker.notify.http: https://example.com/hook\n" +
		"worker.check.http: https://example.com/check\n" +
		"worker.check.http_method: PUT\n"
	if err := os.WriteFile(
		cfgPath, []byte(content), 0644,
	); err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	if err := loadConfigFile(cfgPath, &cfg); err != nil {
		t.Fatalf("loadConfigFile: %v", err)
	}

	// Positive: 3 workers parsed
	if len(cfg.Workers) != 3 {
		t.Fatalf("Workers = %d, want 3", len(cfg.Workers))
	}

	// Positive: exec worker correct
	found := false
	for _, w := range cfg.Workers {
		if w.Task == "run-tests" {
			found = true
			if w.Exec != "go test ./..." {
				t.Errorf("Exec = %q, want %q",
					w.Exec, "go test ./...")
			}
			if w.HTTP != "" {
				t.Errorf("HTTP = %q, want empty", w.HTTP)
			}
		}
	}
	if !found {
		t.Error("worker 'run-tests' not found")
	}

	// Positive: http worker with method
	for _, w := range cfg.Workers {
		if w.Task == "check" {
			if w.HTTPMethod != "PUT" {
				t.Errorf("HTTPMethod = %q, want %q",
					w.HTTPMethod, "PUT")
			}
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./server/ -run TestLoadConfigFile_ParsesWorkerEntries -v`
Expected: FAIL — `WorkerConfig` type not defined, `Workers` field not on Config

- [ ] **Step 3: Implement WorkerConfig and parsing**

In `server/config.go`:

Add constants:
```go
const (
	// ... existing constants ...
	maxConfigFileLines   = 300  // bumped from 100
	maxWorkerConfigs     = 50
)
```

Add type and field:
```go
// WorkerConfig defines a config-driven embedded worker handler.
type WorkerConfig struct {
	Task       string
	Exec       string
	HTTP       string
	HTTPMethod string // default: POST
}

type Config struct {
	// ... existing fields ...
	Workers []WorkerConfig `json:"workers"`
}
```

In `applyConfigValue`, add before the `default` case:

```go
	default:
		if strings.HasPrefix(key, "worker.") {
			return applyWorkerConfigValue(key, val, cfg)
		}
		return fmt.Errorf("unknown config key: %s", key)
	}
```

Add new function:

```go
// applyWorkerConfigValue parses a worker.{task}.{field} key.
func applyWorkerConfigValue(
	key, val string, cfg *Config,
) error {
	if cfg == nil {
		panic("applyWorkerConfigValue: cfg is nil")
	}
	if key == "" {
		panic("applyWorkerConfigValue: key is empty")
	}

	// Parse worker.{task}.{field}
	parts := strings.SplitN(key, ".", 3)
	if len(parts) != 3 || parts[0] != "worker" {
		return fmt.Errorf("invalid worker key format: %s", key)
	}

	task := parts[1]
	field := parts[2]

	if task == "" {
		return fmt.Errorf("worker key has empty task: %s", key)
	}

	// Find or create worker entry
	idx := -1
	for i := range cfg.Workers {
		if cfg.Workers[i].Task == task {
			idx = i
			break
		}
	}
	if idx == -1 {
		if len(cfg.Workers) >= maxWorkerConfigs {
			return fmt.Errorf(
				"max worker configs (%d) exceeded",
				maxWorkerConfigs,
			)
		}
		cfg.Workers = append(cfg.Workers, WorkerConfig{
			Task: task,
		})
		idx = len(cfg.Workers) - 1
	}

	switch field {
	case "exec":
		cfg.Workers[idx].Exec = val
	case "http":
		cfg.Workers[idx].HTTP = val
	case "http_method":
		cfg.Workers[idx].HTTPMethod = val
	default:
		return fmt.Errorf("unknown worker field: %s", field)
	}

	return nil
}
```

Add `"strings"` import if not already present.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./server/ -run TestLoadConfigFile_ParsesWorkerEntries -v`
Expected: PASS

- [ ] **Step 5: Write validation test**

Append to `server/config_test.go`:

```go
func TestValidateWorkerConfigs_RejectsDuplicates(t *testing.T) {
	cfg := Config{
		DataDir:       t.TempDir(),
		HTTPAddr:      ":8080",
		NATSPort:      4222,
		MaxStoreBytes: defaultMaxStoreBytes,
		Workers: []WorkerConfig{
			{Task: "dup", Exec: "echo a"},
			{Task: "dup", Exec: "echo b"},
		},
	}
	err := validateWorkerConfigs(cfg.Workers)
	if err == nil {
		t.Fatal("expected error for duplicate task names")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("error = %q, want 'duplicate'", err.Error())
	}
}

func TestValidateWorkerConfigs_RejectsBothExecAndHTTP(t *testing.T) {
	workers := []WorkerConfig{
		{Task: "bad", Exec: "echo", HTTP: "http://x"},
	}
	err := validateWorkerConfigs(workers)
	if err == nil {
		t.Fatal("expected error for both exec and http")
	}
}

func TestValidateWorkerConfigs_RejectsNeitherExecNorHTTP(t *testing.T) {
	workers := []WorkerConfig{
		{Task: "empty"},
	}
	err := validateWorkerConfigs(workers)
	if err == nil {
		t.Fatal("expected error for neither exec nor http")
	}
}
```

- [ ] **Step 6: Implement validateWorkerConfigs**

In `server/config.go`:

```go
// validateWorkerConfigs checks worker config consistency.
// Returns error on first violation. Panics if len > max.
func validateWorkerConfigs(workers []WorkerConfig) error {
	if len(workers) > maxWorkerConfigs {
		panic("validateWorkerConfigs: exceeds max bound")
	}

	seen := make(map[string]bool, len(workers))
	for _, w := range workers {
		if w.Task == "" {
			return fmt.Errorf(
				"worker config: task name is empty",
			)
		}
		if seen[w.Task] {
			return fmt.Errorf(
				"worker config: duplicate task %q", w.Task,
			)
		}
		seen[w.Task] = true

		hasExec := w.Exec != ""
		hasHTTP := w.HTTP != ""
		if hasExec && hasHTTP {
			return fmt.Errorf(
				"worker %q: cannot have both exec and http",
				w.Task,
			)
		}
		if !hasExec && !hasHTTP {
			return fmt.Errorf(
				"worker %q: must have exec or http",
				w.Task,
			)
		}
	}
	return nil
}
```

Call it from `ConfigFromEnv()`, after `applyEnvOverrides(&cfg)`:

```go
	if len(cfg.Workers) > 0 {
		if err := validateWorkerConfigs(cfg.Workers); err != nil {
			log.Fatalf("invalid worker config: %v", err)
		}
	}
```

- [ ] **Step 7: Run all config tests**

Run: `cd /Users/dmestas/projects/dagnats && go test ./server/ -run TestLoadConfigFile -v && go test ./server/ -run TestValidateWorkerConfigs -v`
Expected: all PASS

- [ ] **Step 8: Add env var override for workers**

In `applyEnvOverrides`, after existing overrides:

```go
	// Override worker configs from env vars
	for i := range cfg.Workers {
		envTask := strings.ToUpper(
			strings.ReplaceAll(cfg.Workers[i].Task, "-", "_"),
		)
		if val := os.Getenv(
			"DAGNATS_WORKER_" + envTask + "_EXEC",
		); val != "" {
			cfg.Workers[i].Exec = val
		}
		if val := os.Getenv(
			"DAGNATS_WORKER_" + envTask + "_HTTP",
		); val != "" {
			cfg.Workers[i].HTTP = val
		}
		if val := os.Getenv(
			"DAGNATS_WORKER_" + envTask + "_HTTP_METHOD",
		); val != "" {
			cfg.Workers[i].HTTPMethod = val
		}
	}
```

- [ ] **Step 9: Run full config test suite**

Run: `cd /Users/dmestas/projects/dagnats && go test ./server/ -run TestConfig -v && go test ./server/ -run TestLoad -v && go test ./server/ -run TestValidate -v`
Expected: all PASS

- [ ] **Step 10: Commit**

```bash
git add server/config.go server/config_test.go
git commit -m "feat: parse worker.* config keys and validate worker configs"
```

### Task 5: Exec handler implementation

**Files:**
- Create: `cli/handlers.go`
- Create: `cli/handlers_test.go`

- [ ] **Step 1: Write failing test for exec handler success**

Create `cli/handlers_test.go`:

```go
// Methodology: Unit tests for built-in handler factories.
// Uses real os/exec commands (echo, false) and a local HTTP
// server for HTTP handler tests. No NATS required.
package cli

import (
	"fmt"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/server"
)

func TestBuildHandler_Exec_Success(t *testing.T) {
	cfg := server.WorkerConfig{
		Task: "test",
		Exec: "echo hello",
	}

	handler := buildHandler(cfg)
	if handler == nil {
		t.Fatal("buildHandler returned nil")
	}

	tc := &fakeTaskContext{input: []byte("ignored")}
	err := handler(tc)

	// Positive: no error
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}

	// Positive: Complete called with stdout
	if !tc.completed {
		t.Fatal("Complete was not called")
	}
	output := strings.TrimSpace(string(tc.output))
	if output != "hello" {
		t.Errorf("output = %q, want %q", output, "hello")
	}

	// Negative: Fail was not called
	if tc.failed {
		t.Error("Fail was called unexpectedly")
	}
}
```

- [ ] **Step 2: Write fakeTaskContext test helper**

Add to `cli/handlers_test.go`:

```go
// fakeTaskContext is a minimal TaskContext for testing
// built-in handlers. Only Complete and Fail are used.
type fakeTaskContext struct {
	input      []byte
	output     []byte
	completed  bool
	failed     bool
	failErr    error
	runID      string
	stepID     string
	retryCount int
}

func (f *fakeTaskContext) Input() []byte        { return f.input }
func (f *fakeTaskContext) RunID() string         { return f.runID }
func (f *fakeTaskContext) StepID() string        { return f.stepID }
func (f *fakeTaskContext) RetryCount() int       { return f.retryCount }

func (f *fakeTaskContext) Complete(
	output []byte,
) error {
	f.completed = true
	f.output = output
	return nil
}

func (f *fakeTaskContext) Fail(err error) error {
	f.failed = true
	f.failErr = err
	return nil
}

func (f *fakeTaskContext) Continue(
	output []byte,
) error {
	return fmt.Errorf("Continue not expected")
}

func (f *fakeTaskContext) PutStream(
	data []byte,
) error {
	return nil
}

func (f *fakeTaskContext) Heartbeat() error {
	return nil
}

func (f *fakeTaskContext) Checkpoint(
	state []byte,
) error {
	return nil
}

func (f *fakeTaskContext) LoadCheckpoint() (
	[]byte, error,
) {
	return nil, nil
}

func (f *fakeTaskContext) WaitForSignal(
	name string, timeout time.Duration,
) ([]byte, error) {
	return nil, fmt.Errorf("not implemented")
}

func (f *fakeTaskContext) SendSignal(
	runID, name string, data []byte,
) error {
	return nil
}
```

Add `"time"` to imports.

- [ ] **Step 3: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestBuildHandler_Exec_Success -v`
Expected: FAIL — `buildHandler` not defined

- [ ] **Step 4: Implement buildHandler and execHandler**

Create `cli/handlers.go`:

```go
package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"github.com/danmestas/dagnats/server"
	"github.com/danmestas/dagnats/worker"
)

const (
	handlerOutputMaxBytes = 10 << 20 // 10 MB
	execDefaultTimeout    = 5 * time.Minute
	httpDefaultTimeout    = 60 * time.Second
)

// buildHandler returns a HandlerFunc for the given WorkerConfig.
// Panics if config has neither exec nor http (should be caught
// by validation).
func buildHandler(cfg server.WorkerConfig) worker.HandlerFunc {
	if cfg.Task == "" {
		panic("buildHandler: task is empty")
	}
	if cfg.Exec != "" {
		return execHandler(cfg.Exec)
	}
	if cfg.HTTP != "" {
		method := cfg.HTTPMethod
		if method == "" {
			method = "POST"
		}
		return httpHandler(cfg.HTTP, method)
	}
	panic("buildHandler: no exec or http configured")
}

// execHandler returns a HandlerFunc that runs a shell command.
// Command string is split on spaces. Stdin receives task input.
// Stdout becomes output on success. Stderr is included in error.
func execHandler(command string) worker.HandlerFunc {
	if command == "" {
		panic("execHandler: command is empty")
	}

	parts := strings.Fields(command)
	if len(parts) == 0 {
		panic("execHandler: command splits to zero parts")
	}

	return func(ctx worker.TaskContext) error {
		if ctx == nil {
			panic("execHandler: ctx is nil")
		}
		if len(parts) == 0 {
			panic("execHandler: parts is empty")
		}

		execCtx, cancel := context.WithTimeout(
			context.Background(), execDefaultTimeout,
		)
		defer cancel()

		cmd := exec.CommandContext(execCtx, parts[0], parts[1:]...)
		cmd.Stdin = bytes.NewReader(ctx.Input())
		cmd.Env = append(cmd.Environ(),
			"DAGNATS_RUN_ID="+ctx.RunID(),
			"DAGNATS_STEP_ID="+ctx.StepID(),
			fmt.Sprintf("DAGNATS_RETRY_COUNT=%d",
				ctx.RetryCount()),
		)

		var stdout, stderr bytes.Buffer
		cmd.Stdout = io.LimitWriter(&stdout, handlerOutputMaxBytes)
		cmd.Stderr = io.LimitWriter(&stderr, handlerOutputMaxBytes)

		err := cmd.Run()
		if err != nil {
			var exitErr *exec.ExitError
			code := -1
			if errors.As(err, &exitErr) {
				code = exitErr.ExitCode()
			}
			errMsg := stderr.String()
			if errMsg == "" {
				errMsg = err.Error()
			}
			return ctx.Fail(fmt.Errorf(
				"exit %d: %s",
				code, strings.TrimSpace(errMsg),
			))
		}

		return ctx.Complete(stdout.Bytes())
	}
}
```

Note: `io.LimitWriter` doesn't exist in stdlib. We need a helper:

```go
// limitWriter wraps a writer with a byte limit.
type limitWriter struct {
	w       io.Writer
	limit   int64
	written int64
}

func (lw *limitWriter) Write(p []byte) (int, error) {
	total := len(p)
	if lw.written >= lw.limit {
		return total, nil // discard silently
	}
	remaining := lw.limit - lw.written
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := lw.w.Write(p)
	lw.written += int64(n)
	if err != nil {
		return n, err
	}
	return total, nil // report full len to avoid short-write errors
}
```

Replace `io.LimitWriter` calls with:
```go
cmd.Stdout = &limitWriter{w: &stdout, limit: handlerOutputMaxBytes}
cmd.Stderr = &limitWriter{w: &stderr, limit: handlerOutputMaxBytes}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestBuildHandler_Exec_Success -v`
Expected: PASS

- [ ] **Step 6: Write remaining exec tests**

Append to `cli/handlers_test.go`:

```go
func TestBuildHandler_Exec_NonZeroExit(t *testing.T) {
	cfg := server.WorkerConfig{
		Task: "test",
		Exec: "false",
	}

	handler := buildHandler(cfg)
	tc := &fakeTaskContext{input: []byte("")}
	err := handler(tc)

	// Positive: Fail was called (handler returns nil,
	// calls ctx.Fail internally)
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if !tc.failed {
		t.Fatal("Fail was not called")
	}

	// Negative: Complete was not called
	if tc.completed {
		t.Error("Complete was called unexpectedly")
	}
}

func TestBuildHandler_Exec_StdinReceivesInput(t *testing.T) {
	cfg := server.WorkerConfig{
		Task: "test",
		Exec: "cat",
	}

	handler := buildHandler(cfg)
	tc := &fakeTaskContext{input: []byte("input data")}
	err := handler(tc)

	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if !tc.completed {
		t.Fatal("Complete was not called")
	}
	if string(tc.output) != "input data" {
		t.Errorf("output = %q, want %q",
			string(tc.output), "input data")
	}
}

func TestBuildHandler_Exec_SetsEnvVars(t *testing.T) {
	cfg := server.WorkerConfig{
		Task: "test",
		Exec: "env",
	}

	handler := buildHandler(cfg)
	tc := &fakeTaskContext{
		input:      []byte(""),
		runID:      "run-123",
		stepID:     "step-abc",
		retryCount: 2,
	}
	err := handler(tc)

	if err != nil {
		t.Fatalf("handler error: %v", err)
	}

	output := string(tc.output)
	if !strings.Contains(output, "DAGNATS_RUN_ID=run-123") {
		t.Error("missing DAGNATS_RUN_ID in env")
	}
	if !strings.Contains(output, "DAGNATS_STEP_ID=step-abc") {
		t.Error("missing DAGNATS_STEP_ID in env")
	}
	if !strings.Contains(output, "DAGNATS_RETRY_COUNT=2") {
		t.Error("missing DAGNATS_RETRY_COUNT in env")
	}
}
```

- [ ] **Step 7: Run all exec tests**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestBuildHandler_Exec -v`
Expected: all PASS

- [ ] **Step 8: Commit**

```bash
git add cli/handlers.go cli/handlers_test.go
git commit -m "feat: add exec handler for config-driven embedded workers"
```

### Task 6: HTTP handler implementation

**Files:**
- Modify: `cli/handlers.go`
- Modify: `cli/handlers_test.go`

- [ ] **Step 1: Write failing test for HTTP handler**

Append to `cli/handlers_test.go`:

```go
import (
	"net/http/httptest"
)

func TestBuildHandler_HTTP_Success(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			// Positive: received input as body
			if string(body) != "test input" {
				t.Errorf("body = %q, want %q",
					string(body), "test input")
			}
			// Positive: method is POST
			if r.Method != "POST" {
				t.Errorf("method = %q, want POST", r.Method)
			}
			w.WriteHeader(200)
			w.Write([]byte("response"))
		}),
	)
	defer ts.Close()

	cfg := server.WorkerConfig{
		Task: "test",
		HTTP: ts.URL,
	}
	handler := buildHandler(cfg)
	tc := &fakeTaskContext{input: []byte("test input")}
	err := handler(tc)

	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if !tc.completed {
		t.Fatal("Complete was not called")
	}
	if string(tc.output) != "response" {
		t.Errorf("output = %q, want %q",
			string(tc.output), "response")
	}
}

func TestBuildHandler_HTTP_NonSuccess(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(500)
			w.Write([]byte("internal error"))
		}),
	)
	defer ts.Close()

	cfg := server.WorkerConfig{
		Task: "test",
		HTTP: ts.URL,
	}
	handler := buildHandler(cfg)
	tc := &fakeTaskContext{input: []byte("")}
	err := handler(tc)

	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if !tc.failed {
		t.Fatal("Fail was not called for 500")
	}
	if tc.completed {
		t.Error("Complete should not be called on 500")
	}
}

func TestBuildHandler_HTTP_CustomMethod(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "PUT" {
				t.Errorf("method = %q, want PUT", r.Method)
			}
			w.WriteHeader(200)
			w.Write([]byte("ok"))
		}),
	)
	defer ts.Close()

	cfg := server.WorkerConfig{
		Task:       "test",
		HTTP:       ts.URL,
		HTTPMethod: "PUT",
	}
	handler := buildHandler(cfg)
	tc := &fakeTaskContext{input: []byte("")}
	err := handler(tc)

	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if !tc.completed {
		t.Fatal("Complete was not called")
	}
}
```

Add `"io"` and `"net/http"` to imports.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestBuildHandler_HTTP -v`
Expected: FAIL — `httpHandler` not defined

- [ ] **Step 3: Implement httpHandler**

Add to `cli/handlers.go`:

```go
// httpHandler returns a HandlerFunc that POSTs task input to a URL.
// Response body becomes output on 2xx, error on non-2xx.
func httpHandler(
	url string, method string,
) worker.HandlerFunc {
	if url == "" {
		panic("httpHandler: url is empty")
	}
	if method == "" {
		panic("httpHandler: method is empty")
	}

	client := &http.Client{Timeout: httpDefaultTimeout}

	return func(ctx worker.TaskContext) error {
		if ctx == nil {
			panic("httpHandler: ctx is nil")
		}
		if url == "" {
			panic("httpHandler: url is empty in closure")
		}

		req, err := http.NewRequest(
			method, url, bytes.NewReader(ctx.Input()),
		)
		if err != nil {
			return ctx.Fail(fmt.Errorf(
				"create request: %v", err,
			))
		}
		req.Header.Set(
			"Content-Type", "application/json",
		)

		resp, err := client.Do(req)
		if err != nil {
			return ctx.Fail(fmt.Errorf(
				"http request: %v", err,
			))
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(
			io.LimitReader(resp.Body, handlerOutputMaxBytes),
		)
		if err != nil {
			return ctx.Fail(fmt.Errorf(
				"read response: %v", err,
			))
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return ctx.Complete(body)
		}

		return ctx.Fail(fmt.Errorf(
			"HTTP %d: %s",
			resp.StatusCode,
			strings.TrimSpace(string(body)),
		))
	}
}
```

- [ ] **Step 4: Run HTTP tests**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestBuildHandler_HTTP -v`
Expected: all PASS

- [ ] **Step 5: Run all handler tests**

Run: `cd /Users/dmestas/projects/dagnats && go test ./cli/ -run TestBuildHandler -v`
Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add cli/handlers.go cli/handlers_test.go
git commit -m "feat: add HTTP handler for config-driven embedded workers"
```

### Task 7: Wire config-driven handlers in serve command

**Files:**
- Modify: `cli/serve.go`

- [ ] **Step 1: Modify runServeCmd to wire config workers**

Update `cli/serve.go`:

```go
package cli

import (
	"fmt"
	"os"

	"github.com/danmestas/dagnats/server"
)

func runServeCmd(args []string) {
	if HasHelpFlag(args) {
		fmt.Println("Usage: dagnats serve")
		fmt.Println("Starts embedded NATS server with" +
			" DagNats engine and API.")
		fmt.Println()
		fmt.Println("Config: dagnats.yaml" +
			" (optional, in current directory)")
		fmt.Println("Env:    DAGNATS_DATA_DIR," +
			" DAGNATS_HTTP_ADDR, DAGNATS_NATS_PORT")
		fmt.Println()
		fmt.Println("Run 'dagnats config show'" +
			" to see effective configuration.")
		return
	}

	cfg := server.ConfigFromEnv()
	srv := server.New(cfg)

	if len(cfg.Workers) > 0 {
		w := server.EmbeddedWorker(srv)
		for _, wc := range cfg.Workers {
			w.Handle(wc.Task, buildHandler(wc))
		}
	}

	if err := srv.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 2: Run full test suite**

Run: `cd /Users/dmestas/projects/dagnats && go test ./... -timeout 120s`
Expected: all PASS

- [ ] **Step 3: Run linting**

Run: `cd /Users/dmestas/projects/dagnats && make lint`
Expected: no errors

- [ ] **Step 4: Commit**

```bash
git add cli/serve.go
git commit -m "feat: wire config-driven workers in serve command"
```

### Task 8: Final verification

- [ ] **Step 1: Run full test suite**

Run: `cd /Users/dmestas/projects/dagnats && go test ./... -timeout 120s`
Expected: all PASS

- [ ] **Step 2: Run formatting and linting**

Run: `cd /Users/dmestas/projects/dagnats && gofmt -l . && go vet ./... && make lint`
Expected: no output from gofmt, no errors from vet/lint

- [ ] **Step 3: Manual smoke test**

Create a test config file:
```bash
cat > /tmp/dagnats-smoke.yaml << 'EOF'
worker.echo.exec: cat
EOF
```

Run:
```bash
cd /tmp && cp dagnats-smoke.yaml dagnats.yaml
dagnats serve
# In another terminal:
# dagnats workflow register (a workflow with task "echo")
# dagnats run start echo-workflow '"hello"'
# dagnats run output --last
# Expected output: "hello"
```

- [ ] **Step 4: Clean up and final commit if needed**

```bash
rm /tmp/dagnats.yaml /tmp/dagnats-smoke.yaml
```
