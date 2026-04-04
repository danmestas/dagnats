# Tier 1: Workflow Primitives Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add durable sleep, wait-for-event, rate limiting, worker directory, and HTTP-to-NATS bridge to DagNats using NATS-native patterns.

**Architecture:** Protocol-first (middle-out) approach. Wire format and registration protocol designed language-agnostic from day one. Go worker package becomes the reference SDK implementation. All primitives use NATS primitives (NakWithDelay, KV watches, pull consumers) instead of custom infrastructure.

**Tech Stack:** Go, NATS JetStream (streams, KV, pull consumers), net/http (bridge)

**Spec:** `docs/superpowers/specs/2026-04-04-tier1-workflow-primitives-design.md`

**Coding rules:** @tigerstyle @idiomatic-go — 70-line function limit, min 2 assertions per function, no recursion, bounded everything, all errors handled.

**Testing rules:** Red-green TDD. Real embedded NATS servers. Min 2 assertions per test. Bounded timeouts on all waits. No shared NATS servers between tests.

**Critical API signatures (from codebase, updated after merge with main 2026-04-04):**
- `Build() (WorkflowDef, error)` — returns error, not panic. Tests must check `err`.
- `SetupAll(nc *nats.Conn, opts ...SetupOption) error` — takes `*nats.Conn`, not `JetStreamContext`.
- `NewWorker(nc *nats.Conn, tel *observe.Telemetry, opts ...WorkerOption) *Worker` — no `JetStreamContext` param. Pass `nil` for tel in tests.
- `Worker.Start()` — returns nothing (panics on failure). Do not check `err`.
- `validateLoopConfig` (line 126), `validateSkipIf` (line 219), and `validateMapConfig` (line 155) all panic if `step.Task == ""`. Must guard these for Sleep/WaitForEvent steps (Task == "" by design).
- `DependsOn(ids ...string)` is on `*WorkflowBuilder` (line 108), NOT on `StepRef`. `StepRef` uses `.After(refs...)` for compile-time-safe deps. Tests should use `.After()` or chain via builder.
- `Task(id, task string)` does NOT accept variadic options. Rate limit config must use `StepRef` methods (e.g., `ref.WithRateLimit(...)`) not `Task("a", "b", WithRateLimit(...))`.
- **SLEEP_TIMERS stream already exists** (natsutil/conn.go:43-47) with subjects `sleep.>` and `scheduled.>`. Do NOT recreate — extend if needed.
- **api/timer.go already exists** — timer consumer for scheduled runs using NakWithDelay on `scheduled.>`. Our SleepTimer should follow the same pattern but on `sleep.>` subjects.
- **Typed generics** (`dag/typed.go`) and **Map step** already landed. StepDef now has 14 fields.
- **OnFailure/Compensate** fully implemented in engine.

---

## File Structure

### New Files
| File | Responsibility |
|------|---------------|
| `dag/sleep.go` | Sleep step validation helpers |
| `dag/waitforevent.go` | Match, ResolvedMatch, MatchOp, WaitForEventOpts types and validation |
| `dag/ratelimit.go` | RateLimit, KeyedRateLimit types and validation |
| `dag/dotpath.go` | Dot-path field extraction from JSON (shared by rate limit + correlator) |
| `engine/sleeptimer.go` | Sleep timer consumer: fetch, NakWithDelay, publish completion |
| `engine/correlator.go` | Event correlator: KV watch waiter index, match incoming events |
| `engine/ratelimit.go` | Token bucket: KV-backed, CAS loop, refill logic |
| `worker/directory.go` | Worker directory: KV registration, TTL heartbeat, deregistration |
| `bridge/bridge.go` | HTTP-to-NATS bridge server lifecycle |
| `bridge/connect.go` | POST /v1/workers/connect handler (SSE) |
| `bridge/poll.go` | POST /v1/tasks/poll handler (long-poll) |
| `bridge/resolve.go` | POST /v1/tasks/{id}/resolve handler (action discriminator) |
| `bridge/ackmap.go` | In-memory task_id -> nats.Msg map with expiry |
| `cli/workers.go` | `dagnats workers list` command |
| `docs/wire-protocol.md` | Reference doc for polyglot SDK authors |

### Modified Files (line numbers updated after main merge 2026-04-04)
| File | Changes |
|------|---------|
| `dag/types.go:13-19` | Add `StepTypeSleep`, `StepTypeWaitForEvent` after `StepTypeMap` |
| `dag/types.go:161-176` | Add `Duration`, `WaitForEvent`, `RateLimit`, `KeyedRateLimit` fields to StepDef |
| `dag/types.go:206-214` | Add `WakeAt *time.Time` field to StepState |
| `dag/builder.go` | Add `Sleep()`, `WaitForEvent()` builder methods (after existing `Map()` at line 82) |
| `dag/stepref.go` | Add `WithRateLimit()`, `WithKeyedRateLimit()` methods (after existing `Compensate()` at line 146) |
| `dag/validate.go:9` | Add sleep, wait-for-event, rate limit validation calls; guard `validateMapConfig` for empty Task |
| `dag/resolve.go` | No changes needed (sleep/wait steps resolve normally via deps) |
| `engine/orchestrator.go:28` | Add sleepTimer, correlator, rateLimiter fields |
| `engine/orchestrator.go:192` | Add new event type routing in isHandledEventType/dispatchEvent |
| `engine/orchestrator.go:1159` | Branch on StepTypeSleep/StepTypeWaitForEvent in enqueueReady |
| `worker/worker.go:18-32` | Add `Pause(name string, duration time.Duration) error` to TaskContext interface |
| `worker/context.go` | Implement Pause method |
| `protocol/protocol.go:24-45` | Add new EventType constants, TaskID field, TaskResolution struct, TimerMessage struct |
| `natsutil/conn.go:63` | Add workers/event_waiters/rate_limits KV buckets (SLEEP_TIMERS already exists) |
| `api/service.go` | Add ListWorkers method |
| `cli/root.go` | Add "workers" case to command switch |

---

## Chunk 1: Worker Directory

### Task 1.1: NATS Resources for Worker Directory

**Files:**
- Modify: `natsutil/conn.go:57-75`
- Test: `natsutil/conn_test.go`

- [ ] **Step 1: Write failing test for workers KV bucket creation**

```go
// natsutil/conn_test.go
// Methodology: integration test with real embedded NATS.
// Tests that SetupAll creates the workers KV bucket with TTL.

func TestSetupAllCreatesWorkersKV(t *testing.T) {
    s, nc, js := StartTestServer(t)
    defer s.Shutdown()
    defer nc.Close()

    SetupAll(nc)

    kv, err := js.KeyValue("workers")
    assert(t, err == nil, "workers KV bucket must exist: %v", err)
    assert(t, kv != nil, "workers KV bucket must not be nil")

    status, err := kv.Status()
    assert(t, err == nil, "status must succeed: %v", err)
    assert(t, status.TTL() == 60*time.Second,
        "workers TTL must be 60s, got %v", status.TTL())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./natsutil/ -run TestSetupAllCreatesWorkersKV -v`
Expected: FAIL — "workers" bucket does not exist

- [ ] **Step 3: Add workers KV bucket to SetupKVBuckets**

In `natsutil/conn.go`, add to `SetupKVBuckets` after the existing buckets:

```go
// Workers directory — TTL-based entries for heartbeat expiry
_, err = js.CreateKeyValue(&nats.KeyValueConfig{
    Bucket: "workers",
    TTL:    60 * time.Second,
})
if err != nil && !isAlreadyExists(err) {
    panic(fmt.Sprintf("create workers KV: %v", err))
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./natsutil/ -run TestSetupAllCreatesWorkersKV -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add natsutil/conn.go natsutil/conn_test.go
git commit -m "feat: add workers KV bucket with TTL for worker directory"
```

---

### Task 1.2: Worker Registration Types

**Files:**
- Create: `worker/directory.go`
- Test: `worker/directory_test.go`

- [ ] **Step 1: Write failing test for worker registration**

```go
// worker/directory_test.go
// Methodology: integration test with real embedded NATS.
// Tests that a worker can register itself and be retrieved from KV.

func TestWorkerDirectoryRegister(t *testing.T) {
    s, nc, js := natsutil.StartTestServer(t)
    defer s.Shutdown()
    defer nc.Close()
    natsutil.SetupAll(nc)

    dir := NewDirectory(js)
    reg := WorkerRegistration{
        WorkerID:  "worker-abc123",
        TaskTypes: []string{"send-email", "process-payment"},
        Language:  "go",
        Transport: "nats",
        MaxTasks:  10,
        Metadata:  map[string]string{"region": "us-east-1"},
    }

    err := dir.Register(reg)
    assert(t, err == nil, "register must succeed: %v", err)

    workers, err := dir.List()
    assert(t, err == nil, "list must succeed: %v", err)
    assert(t, len(workers) == 1, "expected 1 worker, got %d", len(workers))
    assert(t, workers[0].WorkerID == "worker-abc123",
        "expected worker-abc123, got %s", workers[0].WorkerID)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./worker/ -run TestWorkerDirectoryRegister -v`
Expected: FAIL — NewDirectory, WorkerRegistration undefined

- [ ] **Step 3: Implement Directory struct and Register/List methods**

Create `worker/directory.go`:

```go
package worker

import (
    "encoding/json"
    "fmt"

    "github.com/nats-io/nats.go"
)

// WorkerRegistration is the directory entry for a running worker.
type WorkerRegistration struct {
    WorkerID  string            `json:"worker_id"`
    TaskTypes []string          `json:"task_types"`
    Language  string            `json:"language"`
    Transport string            `json:"transport"`
    MaxTasks  int               `json:"max_tasks"`
    Metadata  map[string]string `json:"metadata,omitempty"`
}

// Directory provides worker visibility via NATS KV.
// It is an observability feature — the engine never reads it.
type Directory struct {
    kv nats.KeyValue
}

// NewDirectory creates a Directory backed by the workers KV bucket.
func NewDirectory(js nats.JetStreamContext) *Directory {
    if js == nil {
        panic("Directory: js must not be nil")
    }
    kv, err := js.KeyValue("workers")
    if err != nil {
        panic(fmt.Sprintf("Directory: workers KV: %v", err))
    }
    return &Directory{kv: kv}
}

// Register writes or refreshes a worker's directory entry.
// Call periodically to keep the TTL-based entry alive.
func (d *Directory) Register(reg WorkerRegistration) error {
    if reg.WorkerID == "" {
        panic("Directory.Register: worker_id must not be empty")
    }
    if len(reg.TaskTypes) == 0 {
        panic("Directory.Register: task_types must not be empty")
    }

    data, err := json.Marshal(reg)
    if err != nil {
        return fmt.Errorf("marshal registration: %w", err)
    }
    _, err = d.kv.Put(reg.WorkerID, data)
    if err != nil {
        return fmt.Errorf("put worker %s: %w", reg.WorkerID, err)
    }
    return nil
}

// Deregister removes a worker's directory entry on graceful shutdown.
func (d *Directory) Deregister(workerID string) error {
    if workerID == "" {
        panic("Directory.Deregister: workerID must not be empty")
    }
    return d.kv.Delete(workerID)
}

// List returns all currently registered workers.
func (d *Directory) List() ([]WorkerRegistration, error) {
    keys, err := d.kv.Keys()
    if err == nats.ErrNoKeysFound {
        return nil, nil
    }
    if err != nil {
        return nil, fmt.Errorf("list keys: %w", err)
    }

    workers := make([]WorkerRegistration, 0, len(keys))
    for _, key := range keys {
        entry, err := d.kv.Get(key)
        if err != nil {
            continue // TTL expiry race
        }
        var reg WorkerRegistration
        if err := json.Unmarshal(entry.Value(), &reg); err != nil {
            continue // corrupt entry
        }
        workers = append(workers, reg)
    }
    return workers, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./worker/ -run TestWorkerDirectoryRegister -v`
Expected: PASS

- [ ] **Step 5: Write test for deregistration**

```go
func TestWorkerDirectoryDeregister(t *testing.T) {
    s, nc, js := natsutil.StartTestServer(t)
    defer s.Shutdown()
    defer nc.Close()
    natsutil.SetupAll(nc)

    dir := NewDirectory(js)
    reg := WorkerRegistration{
        WorkerID:  "worker-xyz",
        TaskTypes: []string{"task-a"},
        Language:  "go",
        Transport: "nats",
        MaxTasks:  5,
    }

    err := dir.Register(reg)
    assert(t, err == nil, "register must succeed: %v", err)

    err = dir.Deregister("worker-xyz")
    assert(t, err == nil, "deregister must succeed: %v", err)

    workers, err := dir.List()
    assert(t, err == nil, "list must succeed: %v", err)
    assert(t, len(workers) == 0, "expected 0 workers after deregister, got %d", len(workers))
}
```

- [ ] **Step 6: Run test to verify it passes (already implemented)**

Run: `go test ./worker/ -run TestWorkerDirectoryDeregister -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add worker/directory.go worker/directory_test.go
git commit -m "feat: add worker directory with KV-based registration and TTL heartbeat"
```

---

### Task 1.3: Integrate Directory into Worker Lifecycle

**Files:**
- Modify: `worker/worker.go:42-56,135-193`
- Test: `worker/worker_test.go`

- [ ] **Step 1: Write failing test for automatic registration on Start**

```go
// worker/worker_test.go
func TestWorkerRegistersOnStart(t *testing.T) {
    s, nc, js := natsutil.StartTestServer(t)
    defer s.Shutdown()
    defer nc.Close()
    natsutil.SetupAll(nc)

    w := NewWorker(nc, nil)
    w.Handle("test-task", func(ctx TaskContext) error {
        return ctx.Complete(nil)
    })

    w.Start() // panics on failure — no error return
    defer w.Stop()

    dir := NewDirectory(js)
    workers, err := dir.List()
    assert(t, err == nil, "list must succeed: %v", err)
    assert(t, len(workers) == 1, "expected 1 registered worker, got %d", len(workers))
    assert(t, len(workers[0].TaskTypes) == 1, "expected 1 task type")
    assert(t, workers[0].TaskTypes[0] == "test-task",
        "expected test-task, got %s", workers[0].TaskTypes[0])
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./worker/ -run TestWorkerRegistersOnStart -v`
Expected: FAIL — worker does not register in directory

- [ ] **Step 3: Add directory field to Worker, register on Start, deregister on Stop**

In `worker/worker.go`, add `dir *Directory`, `workerID string`, and `stopHeartbeat chan struct{}` fields to the Worker struct. In `Start()`, after creating subscriptions:
1. Create directory and register
2. Start a heartbeat goroutine with `time.NewTicker(30 * time.Second)` that re-PUTs the registration entry to refresh the KV TTL (60s bucket TTL, 30s refresh = survives one missed heartbeat)
In `Stop()`, close `stopHeartbeat` channel to stop ticker, deregister before unsubscribing. Generate workerID via `fmt.Sprintf("worker-%s", generateID())` using a short random hex string.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./worker/ -run TestWorkerRegistersOnStart -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add worker/worker.go worker/worker_test.go
git commit -m "feat: auto-register workers in directory on Start, deregister on Stop"
```

---

### Task 1.4: API Endpoint and CLI Command for Worker List

**Files:**
- Modify: `api/service.go:28-41`
- Create: `cli/workers.go`
- Modify: `cli/root.go:22-50`

- [ ] **Step 1: Add ListWorkers to API service**

In `api/service.go`, add:

```go
func (s *Service) ListWorkers() ([]worker.WorkerRegistration, error) {
    dir := worker.NewDirectory(s.js)
    return dir.List()
}
```

- [ ] **Step 2: Create cli/workers.go**

```go
package cli

import (
    "encoding/json"
    "fmt"
    "os"

    "github.com/danmestas/dagnats/api"
)

func runWorkersCmd(svc *api.Service, args []string) {
    if len(args) == 0 {
        fmt.Fprintln(os.Stderr, "Usage: dagnats workers <list>")
        os.Exit(1)
    }
    switch args[0] {
    case "list":
        workers, err := svc.ListWorkers()
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error: %v\n", err)
            os.Exit(1)
        }
        data, _ := json.MarshalIndent(workers, "", "  ")
        fmt.Println(string(data))
    default:
        fmt.Fprintf(os.Stderr, "Unknown workers command: %s\n", args[0])
        os.Exit(1)
    }
}
```

- [ ] **Step 3: Add "workers" case to cli/root.go command switch**

Add `case "workers": runWorkersCmd(svc, args[2:])` to the command switch.

- [ ] **Step 4: Run existing tests to verify no regressions**

Run: `go test ./cli/ ./api/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add api/service.go cli/workers.go cli/root.go
git commit -m "feat: add 'dagnats workers list' command and API endpoint"
```

---

## Chunk 2: Durable Sleep (Step-Level)

### Task 2.0: Guard Validation Against Empty Task Field

Sleep and WaitForEvent steps have `Task == ""` by design. The existing `validateLoopConfig` (line 126), `validateSkipIf` (line 219), and `validateMapConfig` (line 155) all panic on empty Task. Must guard these.

**Files:**
- Modify: `dag/validate.go:68-108`
- Test: `dag/validate_test.go`

- [ ] **Step 1: Write failing test — sleep step passes validation**

```go
func TestValidateSleepStepDoesNotPanicOnEmptyTask(t *testing.T) {
    wf, err := NewWorkflow("test").
        Sleep("delay", 1*time.Hour).
        Build()
    assert(t, err == nil, "sleep step must pass validation: %v", err)
    assert(t, len(wf.Steps) == 1, "expected 1 step")
}
```

- [ ] **Step 2: Run test — expect panic from validateLoopConfig**

Run: `go test ./dag/ -run TestValidateSleepStepDoesNotPanic -v`
Expected: PANIC at validateLoopConfig: "step task is empty"

- [ ] **Step 3: Guard validateLoopConfig and validateSkipIf**

In `validateLoopConfig` (line 126), `validateMapConfig` (line 155), and `validateSkipIf` (line 219), replace the `step.Task == ""` panic with an early return for non-task step types:
```go
// Sleep and WaitForEvent steps have no Task — skip task-related checks.
if step.Type == StepTypeSleep || step.Type == StepTypeWaitForEvent {
    return nil
}
```

Add this guard at the top of all three functions, before the existing `step.Task == ""` panic.

- [ ] **Step 4: Run test — expect pass**

- [ ] **Step 5: Run all existing tests for regression**

Run: `go test ./dag/ -v`

- [ ] **Step 6: Commit**

```bash
git add dag/validate.go dag/validate_test.go
git commit -m "fix: guard validation against empty Task field for sleep/wait steps"
```

---

### Task 2.1: StepTypeSleep Type and Protocol Events

**Files:**
- Modify: `dag/types.go:13-18,149-163,183-190`
- Modify: `protocol/protocol.go:24-39`
- Test: `dag/types_test.go`

- [ ] **Step 1: Write failing test for StepTypeSleep string encoding**

```go
// dag/types_test.go
func TestStepTypeSleepString(t *testing.T) {
    assert(t, StepTypeSleep.String() == "sleep",
        "expected 'sleep', got '%s'", StepTypeSleep.String())

    var st StepType
    err := json.Unmarshal([]byte(`"sleep"`), &st)
    assert(t, err == nil, "unmarshal must succeed: %v", err)
    assert(t, st == StepTypeSleep, "expected StepTypeSleep, got %v", st)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./dag/ -run TestStepTypeSleepString -v`
Expected: FAIL — StepTypeSleep undefined

- [ ] **Step 3: Add StepTypeSleep constant and Duration field**

In `dag/types.go`:
- Add `StepTypeSleep` to the StepType iota block (after StepTypeAgent)
- Add `"sleep"` to `stepTypeStrings` array
- Add `Duration time.Duration` field to `StepDef` struct
- Add comment to StepDef: `// NOTE: Tier 2+ must refactor to step-type-specific config before adding fields.`
- Add `WakeAt *time.Time` field to `StepState` struct

In `protocol/protocol.go`:
- Add `EventStepSleepStarted EventType = "step.sleep.started"`
- Add `EventStepSleepCompleted EventType = "step.sleep.completed"`

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./dag/ -run TestStepTypeSleepString -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add dag/types.go protocol/protocol.go dag/types_test.go
git commit -m "feat: add StepTypeSleep type, Duration field, and sleep event types"
```

---

### Task 2.2: Sleep Builder Method

**Files:**
- Modify: `dag/builder.go:14-140`
- Test: `dag/builder_test.go`

- [ ] **Step 1: Write failing test for Sleep builder**

```go
// dag/builder_test.go
func TestBuilderSleep(t *testing.T) {
    wf, err := NewWorkflow("test").
        Task("a", "task-a").
        Sleep("wait-1h", 1*time.Hour).After(StepRef{/* "a" */}).
        Task("b", "task-b").After(StepRef{/* "wait-1h" */}).
        Build()
    assert(t, err == nil, "build must succeed: %v", err)
    assert(t, len(wf.Steps) == 3, "expected 3 steps, got %d", len(wf.Steps))

    sleepStep := wf.Steps[1]
    assert(t, sleepStep.ID == "wait-1h", "expected wait-1h, got %s", sleepStep.ID)
    assert(t, sleepStep.Type == StepTypeSleep, "expected StepTypeSleep")
    assert(t, sleepStep.Duration == 1*time.Hour,
        "expected 1h duration, got %v", sleepStep.Duration)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./dag/ -run TestBuilderSleep -v`
Expected: FAIL — Sleep method undefined

- [ ] **Step 3: Add Sleep method to WorkflowBuilder**

In `dag/builder.go`, add after the existing builder methods:

```go
// Sleep adds a durable delay step to the workflow.
// No worker is involved — the engine handles the timer.
func (b *WorkflowBuilder) Sleep(id string, duration time.Duration) StepRef {
    if id == "" {
        panic("Sleep: id must not be empty")
    }
    if duration <= 0 {
        panic("Sleep: duration must be positive")
    }

    b.steps = append(b.steps, StepDef{
        ID:       id,
        Task:     "", // no task type — engine handles directly
        Type:     StepTypeSleep,
        Duration: duration,
    })
    b.current = len(b.steps) - 1
    return StepRef{id: id, index: b.current, builder: b}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./dag/ -run TestBuilderSleep -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add dag/builder.go dag/builder_test.go
git commit -m "feat: add Sleep() builder method for durable delay steps"
```

---

### Task 2.3: Sleep Validation

**Files:**
- Create: `dag/sleep.go`
- Modify: `dag/validate.go:9-24`
- Test: `dag/validate_test.go`

- [ ] **Step 1: Write failing tests for sleep validation**

```go
// dag/validate_test.go
func TestValidateSleepDuration365DayMax(t *testing.T) {
    _, err := NewWorkflow("test").
        Sleep("too-long", 366*24*time.Hour).
        Build()
    assert(t, err != nil, "expected error for >365 day sleep")
    assert(t, strings.Contains(err.Error(), "exceeds max"),
        "error should mention max: %v", err)
}

func TestValidateSleepDurationPositive(t *testing.T) {
    // Duration <= 0 panics at builder time (precondition), not Build() time.
    defer func() {
        r := recover()
        assert(t, r != nil, "expected panic for zero duration sleep")
    }()

    NewWorkflow("test").
        Sleep("zero", 0)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./dag/ -run TestValidateSleep -v`
Expected: FAIL — no validation for sleep duration

- [ ] **Step 3: Create dag/sleep.go with validation, wire into Validate**

Create `dag/sleep.go`:

```go
package dag

import (
    "fmt"
    "time"
)

const maxSleepDuration = 365 * 24 * time.Hour

func validateSleepStep(step StepDef) error {
    if step.Type != StepTypeSleep {
        return nil
    }
    if step.Duration <= 0 {
        return fmt.Errorf(
            "step %q: sleep duration must be positive, got %v",
            step.ID, step.Duration)
    }
    if step.Duration > maxSleepDuration {
        return fmt.Errorf(
            "step %q: sleep duration %v exceeds max %v",
            step.ID, step.Duration, maxSleepDuration)
    }
    return nil
}
```

In `dag/validate.go`, add `validateSleepStep(step)` call inside the per-step validation loop in `Validate()`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./dag/ -run TestValidateSleep -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add dag/sleep.go dag/validate.go dag/validate_test.go
git commit -m "feat: validate sleep step duration (positive, max 365 days)"
```

---

### Task 2.4: ~~SLEEP_TIMERS Stream Setup~~ ALREADY EXISTS

**SKIP** — `SLEEP_TIMERS` stream already exists on main (natsutil/conn.go:43-47) with subjects `sleep.>` and `scheduled.>`. Created as part of the scheduled runs feature. Uses `LimitsPolicy` and `FileStorage`.

**Note:** The existing stream uses `LimitsPolicy`, not `WorkQueuePolicy`. This is acceptable — the scheduled runs timer consumer already works with this policy. Our SleepTimer consumer will follow the same pattern.

**Verify only:**
- [ ] **Step 1: Run existing test to confirm stream exists**

Run: `go test ./natsutil/ -run TestSetupAll -v`
Expected: PASS (SLEEP_TIMERS already created)

---

### Task 2.5: Sleep Timer Consumer

**Files:**
- Create: `engine/sleeptimer.go`
- Test: `engine/sleeptimer_test.go`

- [ ] **Step 1: Write failing test for sleep timer consumer**

```go
// engine/sleeptimer_test.go
// Methodology: integration test with real NATS.
// Publishes a timer message, verifies it NAKs with delay and eventually
// produces a step.sleep.completed event on the history stream.

func TestSleepTimerFiresCompletion(t *testing.T) {
    s, nc, js := natsutil.StartTestServer(t)
    defer s.Shutdown()
    defer nc.Close()
    natsutil.SetupAll(nc)

    timer := NewSleepTimer(nc, js)
    err := timer.Start()
    assert(t, err == nil, "start must succeed: %v", err)
    defer timer.Stop()

    // Subscribe to history to catch the completion event
    sub, err := js.SubscribeSync("history.run-1",
        nats.BindStream("WORKFLOW_HISTORY"))
    assert(t, err == nil, "subscribe must succeed: %v", err)

    // Schedule a short sleep (100ms for testing)
    err = timer.Schedule("run-1", "sleep-step", 100*time.Millisecond)
    assert(t, err == nil, "schedule must succeed: %v", err)

    // Wait for the completion event
    msg, err := sub.NextMsg(5 * time.Second)
    assert(t, err == nil, "must receive completion event: %v", err)

    var evt protocol.Event
    err = json.Unmarshal(msg.Data, &evt)
    assert(t, err == nil, "unmarshal event: %v", err)
    assert(t, evt.Type == protocol.EventStepSleepCompleted,
        "expected step.sleep.completed, got %s", evt.Type)
    assert(t, evt.StepID == "sleep-step",
        "expected sleep-step, got %s", evt.StepID)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./engine/ -run TestSleepTimerFiresCompletion -v`
Expected: FAIL — NewSleepTimer undefined

- [ ] **Step 3: Implement SleepTimer**

Create `engine/sleeptimer.go`. Follow the pattern established by `api/timer.go` (the scheduled runs timer consumer), which uses the same SLEEP_TIMERS stream with NakWithDelay:

```go
package engine

// SleepTimer manages durable sleep via NakWithDelay on SLEEP_TIMERS.
// Subscribes to "sleep.>" subjects (scheduled runs use "scheduled.>").
//
// Flow: Schedule() publishes a timer message. The consumer fetches it
// immediately and NAKs with the sleep duration. On redelivery, it
// dispatches based on the action field in the payload.

type SleepTimer struct {
    nc  *nats.Conn
    js  nats.JetStreamContext
    sub *nats.Subscription
}

func NewSleepTimer(nc *nats.Conn, js nats.JetStreamContext) *SleepTimer
func (st *SleepTimer) Start() error     // creates push subscriber on sleep.>, starts handling
func (st *SleepTimer) Stop()            // drains subscription
func (st *SleepTimer) Schedule(msg protocol.TimerMessage, duration time.Duration) error
```

The consumer handling (modeled after `api/timer.go:handleTimer`):
1. Fetch one message with short timeout
2. Check `Nats-Redelivered` header or `NumDelivered` metadata
3. First delivery (NumDelivered == 1): NAK with `NakWithDelay(duration)` where duration is read from message payload
4. Redelivery (NumDelivered > 1): publish `step.sleep.completed` event to `history.{runID}`, then ACK

Timer message payload includes an `action` discriminator so the consumer handles multiple concerns (durable sleep, wait-for-event timeout, rate-limit retry) without callers knowing about each other:
```json
{"action": "sleep_complete", "run_id": "run-1", "step_id": "sleep-step", "duration_ms": 100}
```

On redelivery, the consumer dispatches on `action`:
- `sleep_complete` → publish `step.sleep.completed` to `history.{runID}`
- `wait_timeout` → publish `step.wait.timeout` to `history.{runID}`
- `rate_retry` → re-publish task to `task.{taskType}.{runID}`

Define a `TimerMessage` struct in `protocol/protocol.go`:
```go
type TimerAction string

const (
    TimerSleepComplete TimerAction = "sleep_complete"
    TimerWaitTimeout   TimerAction = "wait_timeout"
    TimerRateRetry     TimerAction = "rate_retry"
)

type TimerMessage struct {
    Action     TimerAction     `json:"action"`
    RunID      string          `json:"run_id"`
    StepID     string          `json:"step_id"`
    DurationMs int64           `json:"duration_ms,omitempty"`
    TaskType   string          `json:"task_type,omitempty"`
    Input      json.RawMessage `json:"input,omitempty"`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./engine/ -run TestSleepTimerFiresCompletion -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add engine/sleeptimer.go engine/sleeptimer_test.go
git commit -m "feat: implement sleep timer consumer with NakWithDelay"
```

---

### Task 2.6: Integrate Sleep into Orchestrator

**Files:**
- Modify: `engine/orchestrator.go:151-239,928-989`
- Test: `engine/orchestrator_test.go`

- [ ] **Step 1: Write failing integration test for sleep workflow**

```go
// engine/orchestrator_test.go
func TestOrchestratorSleepStep(t *testing.T) {
    s, nc, js := natsutil.StartTestServer(t)
    defer s.Shutdown()
    defer nc.Close()
    natsutil.SetupAll(nc)

    // Register a workflow with a sleep step
    wf := dag.NewWorkflow("sleep-test").
        Task("before", "echo").
        Sleep("delay", 100*time.Millisecond).DependsOn("before").
        Task("after", "echo").DependsOn("delay").
        Build()

    // ... setup orchestrator, worker, start run
    // ... wait for run to complete with bounded timeout

    run, err := store.Load(runID)
    assert(t, err == nil, "load run: %v", err)
    assert(t, run.Status == dag.RunStatusCompleted,
        "expected completed, got %s", run.Status)
    assert(t, run.Steps["delay"].Status == dag.StepStatusCompleted,
        "sleep step must be completed")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./engine/ -run TestOrchestratorSleepStep -v -timeout 30s`
Expected: FAIL — orchestrator doesn't handle StepTypeSleep

- [ ] **Step 3: Add sleep step handling to orchestrator**

In `engine/orchestrator.go`:

1. Add `sleepTimer *SleepTimer` field to Orchestrator struct
2. In `NewOrchestrator`, create and start the SleepTimer
3. In `enqueueReady`, after `ResolveReady()` returns steps, branch on step type:
   - `StepTypeSleep`: publish `step.sleep.started` event, call `sleepTimer.Schedule()`
   - Other types: existing `publishTask` path
4. In `dispatchEvent`, add case for `EventStepSleepCompleted`: call `handleStepCompleted` (same handler — sleep completion is logically identical to step completion)
5. In `isHandledEventType`, add `EventStepSleepCompleted`

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./engine/ -run TestOrchestratorSleepStep -v -timeout 30s`
Expected: PASS

- [ ] **Step 5: Run all existing tests to verify no regressions**

Run: `go test ./... -timeout 120s`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add engine/orchestrator.go engine/orchestrator_test.go
git commit -m "feat: integrate durable sleep into orchestrator event loop"
```

---

## Chunk 3: Worker-Level Pause

### Task 3.1: Add Pause to TaskContext Interface

**Files:**
- Modify: `worker/worker.go:18-32`
- Modify: `worker/context.go:13-27`
- Test: `worker/context_test.go`

- [ ] **Step 1: Write failing test for Pause**

```go
// worker/context_test.go
// Methodology: integration test with real NATS.
// Tests that Pause checkpoints state, NAKs with delay, and resumes.

func TestTaskContextPause(t *testing.T) {
    s, nc, js := natsutil.StartTestServer(t)
    defer s.Shutdown()
    defer nc.Close()
    natsutil.SetupAll(nc)

    pauseCount := 0
    completed := make(chan struct{})

    w := NewWorker(nc, nil)
    w.Handle("pause-task", func(ctx TaskContext) error {
        cp, _ := ctx.LoadCheckpoint()
        if cp != nil {
            // Resumed after pause
            pauseCount++
            close(completed)
            return ctx.Complete([]byte(`{"resumed":true}`))
        }
        // First execution — pause
        return ctx.Pause("wait-a-bit", 100*time.Millisecond)
    })

    w.Start() // panics on failure — no error return
    defer w.Stop()

    // Publish a task message
    // ... (publish to task.pause-task.run-1)

    select {
    case <-completed:
        assert(t, pauseCount == 1, "expected 1 resume, got %d", pauseCount)
    case <-time.After(10 * time.Second):
        t.Fatal("timeout waiting for pause/resume")
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./worker/ -run TestTaskContextPause -v`
Expected: FAIL — Pause method undefined

- [ ] **Step 3: Add Pause to TaskContext interface and implement**

In `worker/worker.go`, add to `TaskContext` interface:
```go
Pause(name string, duration time.Duration) error
```

In `worker/context.go`, implement:

```go
// Pause checkpoints state, NAKs with delay, and resumes on redeliver.
// The engine is not involved — step stays StepStatusRunning throughout.
func (tc *taskContext) Pause(name string, duration time.Duration) error {
    if name == "" {
        panic("Pause: name must not be empty")
    }
    if duration <= 0 {
        panic("Pause: duration must be positive")
    }

    // Write checkpoint with pause marker
    checkpoint := map[string]any{
        "pause_resume": name,
    }
    data, err := json.Marshal(checkpoint)
    if err != nil {
        return fmt.Errorf("marshal pause checkpoint: %w", err)
    }
    if err := tc.Checkpoint(data); err != nil {
        return fmt.Errorf("save pause checkpoint: %w", err)
    }

    // NAK with delay — message redelivers after duration
    return tc.msg.NakWithDelay(duration)
}
```

Also update `handleMessage` in `worker/worker.go` to NOT increment attempt counter when checkpoint has `pause_resume` marker. Check checkpoint before calling handler — if pause marker present, pass it through to handler (handler checks `LoadCheckpoint` as shown in test).

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./worker/ -run TestTaskContextPause -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add worker/worker.go worker/context.go worker/context_test.go
git commit -m "feat: add Pause method to TaskContext for mid-task durable delay"
```

---

## Chunk 4: Rate Limiting

### Task 4.1: Rate Limit Types

**Files:**
- Create: `dag/ratelimit.go`
- Test: `dag/ratelimit_test.go`

- [ ] **Step 1: Write failing test for rate limit types**

```go
// dag/ratelimit_test.go
func TestRateLimitValidation(t *testing.T) {
    b := NewWorkflow("test")
    b.Task("a", "task-a").WithRateLimit(RateLimit{
        Limit:  0,
        Period: 0,
    })
    _, err := b.Build()
    assert(t, err != nil, "expected error for zero-period rate limit")
    assert(t, strings.Contains(err.Error(), "positive"),
        "error should mention positive: %v", err)
}

func TestKeyedRateLimitValidation(t *testing.T) {
    b := NewWorkflow("test")
    b.Task("a", "task-a").WithKeyedRateLimit(KeyedRateLimit{
        Key:    "",
        Limit:  10,
        Period: time.Minute,
        Units:  1,
    })
    _, err := b.Build()
    assert(t, err != nil, "expected error for empty key")
    assert(t, strings.Contains(err.Error(), "empty"),
        "error should mention empty: %v", err)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./dag/ -run TestRateLimit -v`
Expected: FAIL — types undefined

- [ ] **Step 3: Create dag/ratelimit.go**

```go
package dag

import (
    "fmt"
    "time"
)

// RateLimit configures global per-task-type rate limiting.
type RateLimit struct {
    Limit  int
    Period time.Duration
}

// KeyedRateLimit configures per-key rate limiting using a dot-path expression.
type KeyedRateLimit struct {
    Key    string
    Limit  int
    Period time.Duration
    Units  int
}

// No StepOption pattern — Task() doesn't accept variadic options.
// Rate limits are set via StepRef methods, matching the existing pattern
// (WithTimeout, WithMaxItems, OnFailure, etc. are all on StepRef).

func validateRateLimit(step StepDef) error {
    if step.RateLimit != nil {
        if step.RateLimit.Limit <= 0 {
            return fmt.Errorf("step %q: rate limit must be positive", step.ID)
        }
        if step.RateLimit.Period <= 0 {
            return fmt.Errorf("step %q: rate limit period must be positive", step.ID)
        }
    }
    if step.KeyedRateLimit != nil {
        if step.KeyedRateLimit.Key == "" {
            return fmt.Errorf("step %q: keyed rate limit key must not be empty", step.ID)
        }
        if step.KeyedRateLimit.Limit <= 0 {
            return fmt.Errorf("step %q: keyed rate limit must be positive", step.ID)
        }
        if step.KeyedRateLimit.Period <= 0 {
            return fmt.Errorf("step %q: keyed rate limit period must be positive", step.ID)
        }
        if step.KeyedRateLimit.Units <= 0 {
            return fmt.Errorf("step %q: keyed rate limit units must be positive", step.ID)
        }
    }
    return nil
}
```

Add `RateLimit *RateLimit` and `KeyedRateLimit *KeyedRateLimit` fields to `StepDef` in `dag/types.go`. Add `WithRateLimit(rl RateLimit)` and `WithKeyedRateLimit(krl KeyedRateLimit)` methods to `StepRef` in `dag/stepref.go` (following the pattern of existing `WithTimeout`, `WithMaxItems`, `OnFailure`, etc.). Wire `validateRateLimit` into `Validate()`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./dag/ -run TestRateLimit -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add dag/ratelimit.go dag/ratelimit_test.go dag/types.go dag/builder.go dag/validate.go
git commit -m "feat: add rate limit types, step options, and validation"
```

---

### Task 4.2: Dot-Path Field Extraction

**Files:**
- Create: `dag/dotpath.go`
- Test: `dag/dotpath_test.go`

- [ ] **Step 1: Write failing tests for dot-path extraction**

```go
// dag/dotpath_test.go
func TestDotPathExtract(t *testing.T) {
    data := []byte(`{"data":{"order_id":"ord-123","nested":{"value":42}}}`)

    val, err := ExtractDotPath("data.order_id", data)
    assert(t, err == nil, "extract must succeed: %v", err)
    assert(t, val == "ord-123", "expected ord-123, got %v", val)

    val, err = ExtractDotPath("data.nested.value", data)
    assert(t, err == nil, "extract nested: %v", err)
    assert(t, val == float64(42), "expected 42, got %v", val)
}

func TestDotPathExtractMissing(t *testing.T) {
    data := []byte(`{"data":{}}`)

    _, err := ExtractDotPath("data.nonexistent", data)
    assert(t, err != nil, "expected error for missing path")
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./dag/ -run TestDotPath -v`
Expected: FAIL — ExtractDotPath undefined

- [ ] **Step 3: Implement ExtractDotPath**

Create `dag/dotpath.go`:

```go
package dag

import (
    "encoding/json"
    "fmt"
    "strings"
)

// ExtractDotPath extracts a value from JSON data using a dot-separated path.
// Example: "data.order_id" from {"data":{"order_id":"abc"}} returns "abc".
func ExtractDotPath(path string, data []byte) (any, error) {
    if path == "" {
        panic("ExtractDotPath: path must not be empty")
    }
    if len(data) == 0 {
        return nil, fmt.Errorf("empty data")
    }

    parts := strings.Split(path, ".")
    var current any
    if err := json.Unmarshal(data, &current); err != nil {
        return nil, fmt.Errorf("unmarshal: %w", err)
    }

    for _, part := range parts {
        obj, ok := current.(map[string]any)
        if !ok {
            return nil, fmt.Errorf("path %q: expected object at %q, got %T",
                path, part, current)
        }
        current, ok = obj[part]
        if !ok {
            return nil, fmt.Errorf("path %q: key %q not found", path, part)
        }
    }
    return current, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./dag/ -run TestDotPath -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add dag/dotpath.go dag/dotpath_test.go
git commit -m "feat: add dot-path field extraction for rate limit keys and event matching"
```

---

### Task 4.3: KV Token Bucket and Rate Limit Setup

**Files:**
- Modify: `natsutil/conn.go`
- Create: `engine/ratelimit.go`
- Test: `engine/ratelimit_test.go`

- [ ] **Step 1: Write failing test for rate_limits KV bucket creation**

```go
// natsutil/conn_test.go
func TestSetupAllCreatesRateLimitsKV(t *testing.T) {
    s, nc, js := StartTestServer(t)
    defer s.Shutdown()
    defer nc.Close()

    SetupAll(nc)

    kv, err := js.KeyValue("rate_limits")
    assert(t, err == nil, "rate_limits KV must exist: %v", err)
    assert(t, kv != nil, "rate_limits KV must not be nil")
}
```

- [ ] **Step 2: Add rate_limits KV bucket to natsutil/conn.go**

- [ ] **Step 3: Write failing test for token bucket**

```go
// engine/ratelimit_test.go
func TestTokenBucketAllowsWithinLimit(t *testing.T) {
    s, nc, js := natsutil.StartTestServer(t)
    defer s.Shutdown()
    defer nc.Close()
    natsutil.SetupAll(nc)

    rl := NewRateLimiter(js)

    // 5 per minute, consume 1 unit
    allowed, delay, err := rl.Allow("send-sms", "_global", 5, time.Minute, 1)
    assert(t, err == nil, "allow must succeed: %v", err)
    assert(t, allowed, "first request must be allowed")
    assert(t, delay == 0, "no delay expected")
}

func TestTokenBucketDeniesWhenExhausted(t *testing.T) {
    s, nc, js := natsutil.StartTestServer(t)
    defer s.Shutdown()
    defer nc.Close()
    natsutil.SetupAll(nc)

    rl := NewRateLimiter(js)

    // Exhaust the bucket: 2 per minute
    for i := 0; i < 2; i++ {
        allowed, _, err := rl.Allow("send-sms", "_global", 2, time.Minute, 1)
        assert(t, err == nil, "allow must succeed: %v", err)
        assert(t, allowed, "request %d must be allowed", i)
    }

    // Next should be denied
    allowed, delay, err := rl.Allow("send-sms", "_global", 2, time.Minute, 1)
    assert(t, err == nil, "allow must succeed: %v", err)
    assert(t, !allowed, "request must be denied when exhausted")
    assert(t, delay > 0, "delay must be positive when denied")
}
```

- [ ] **Step 4: Implement RateLimiter with KV token bucket**

Create `engine/ratelimit.go`:

```go
package engine

// RateLimiter implements a KV-backed token bucket for rate limiting.
// Uses optimistic locking (CAS) via KV revision for concurrent safety.
// CAS loop bounded at 10 retries.

type RateLimiter struct {
    kv nats.KeyValue
}

type tokenBucket struct {
    Tokens     int       `json:"tokens"`
    LastRefill time.Time `json:"last_refill"`
    Limit      int       `json:"limit"`
    PeriodMs   int64     `json:"period_ms"`
}

func NewRateLimiter(js nats.JetStreamContext) *RateLimiter
func (rl *RateLimiter) Allow(taskType, key string, limit int, period time.Duration, units int) (allowed bool, retryAfter time.Duration, err error)
```

The `Allow` method:
1. Gets KV entry at `{taskType}.{key}`, creates if missing (full bucket)
2. Refills tokens based on elapsed time
3. If tokens >= units: decrement, CAS put, return allowed=true
4. If tokens < units: return allowed=false, retryAfter = time until next refill
5. CAS loop bounded at 10 retries

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./engine/ -run TestTokenBucket -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add natsutil/conn.go engine/ratelimit.go engine/ratelimit_test.go
git commit -m "feat: implement KV-backed token bucket rate limiter"
```

---

### Task 4.4: Integrate Rate Limiting into Task Dispatch

**Files:**
- Modify: `engine/orchestrator.go:1025-1065`
- Test: `engine/orchestrator_test.go`

- [ ] **Step 1: Write failing test for rate-limited task dispatch**

```go
// engine/orchestrator_test.go
func TestOrchestratorRateLimitDelaysTask(t *testing.T) {
    // Register workflow with rate limit of 1 per minute
    // Start 2 runs — first should dispatch immediately,
    // second should be delayed (NAK with delay)
    // Verify via timing that the second task was held
}
```

- [ ] **Step 2: Add rateLimiter field to orchestrator, check in publishTask**

In `engine/orchestrator.go`:
1. Add `rateLimiter *RateLimiter` to Orchestrator struct
2. In `publishTask`, before publishing to `task.>`:
   - If step has `RateLimit`: call `rateLimiter.Allow(step.Task, "_global", ...)`
   - If step has `KeyedRateLimit`: extract key via `dag.ExtractDotPath`, call `rateLimiter.Allow(step.Task, keyValue, ...)`
   - If not allowed: use `NakWithDelay(retryAfter)` pattern — publish timer to SLEEP_TIMERS that re-enqueues the task after the delay

- [ ] **Step 3: Run test to verify it passes**

Run: `go test ./engine/ -run TestOrchestratorRateLimitDelaysTask -v -timeout 30s`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add engine/orchestrator.go engine/orchestrator_test.go
git commit -m "feat: integrate rate limiting into task dispatch path"
```

---

## Chunk 5: Wait-for-Event

### Task 5.1: WaitForEvent Types and Match Struct

**Files:**
- Create: `dag/waitforevent.go`
- Modify: `dag/types.go:13-18`
- Modify: `protocol/protocol.go:24-39`
- Test: `dag/waitforevent_test.go`

- [ ] **Step 1: Write failing tests for WaitForEvent types**

```go
// dag/waitforevent_test.go
func TestResolvedMatchEvaluate(t *testing.T) {
    m := ResolvedMatch{
        Left:  "data.order_id",
        Op:    MatchOpEq,
        Right: "ord-123",
    }
    eventData := []byte(`{"data":{"order_id":"ord-123"}}`)

    matched, err := m.Evaluate(eventData)
    assert(t, err == nil, "evaluate must succeed: %v", err)
    assert(t, matched, "expected match for equal order_id")
}

func TestResolvedMatchEvaluateNoMatch(t *testing.T) {
    m := ResolvedMatch{
        Left:  "data.order_id",
        Op:    MatchOpEq,
        Right: "ord-999",
    }
    eventData := []byte(`{"data":{"order_id":"ord-123"}}`)

    matched, err := m.Evaluate(eventData)
    assert(t, err == nil, "evaluate must succeed: %v", err)
    assert(t, !matched, "expected no match for different order_id")
}

func TestMatchResolve(t *testing.T) {
    m := Match{
        Left:  "event.data.order_id",
        Op:    MatchOpEq,
        Right: "step.create-order.output.order_id",
    }
    stepOutputs := map[string][]byte{
        "create-order": []byte(`{"order_id":"ord-123"}`),
    }
    resolved, err := m.Resolve(stepOutputs, nil)
    assert(t, err == nil, "resolve must succeed: %v", err)
    assert(t, resolved.Right == "ord-123",
        "expected ord-123, got %v", resolved.Right)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./dag/ -run TestResolvedMatch -v`
Expected: FAIL — ResolvedMatch, MatchOpEq undefined

- [ ] **Step 3: Create dag/waitforevent.go**

```go
package dag

import (
    "fmt"
    "time"
)

type MatchOp string

const MatchOpEq MatchOp = "eq"

// Match is the builder-time type. Both sides are dot-path strings.
// Right references step outputs or workflow input (resolved at waiter creation).
type Match struct {
    Left  string  `json:"left"`   // dot-path: "event.data.X"
    Op    MatchOp `json:"op"`
    Right string  `json:"right"`  // dot-path: "step.{id}.output.Y" or "input.Z"
}

// ResolvedMatch is the runtime type stored in KV waiter entries.
// Right is resolved to a concrete value when the waiter is created.
type ResolvedMatch struct {
    Left  string  `json:"left"`
    Op    MatchOp `json:"op"`
    Right any     `json:"right"`  // concrete value, e.g., "ord-123"
}

// Evaluate checks if the match condition holds against event JSON data.
func (m ResolvedMatch) Evaluate(eventData []byte) (bool, error) {
    if m.Left == "" {
        panic("ResolvedMatch.Evaluate: Left must not be empty")
    }
    if m.Op == "" {
        panic("ResolvedMatch.Evaluate: Op must not be empty")
    }

    leftVal, err := ExtractDotPath(m.Left, eventData)
    if err != nil {
        return false, nil // missing field = no match, not error
    }

    switch m.Op {
    case MatchOpEq:
        return fmt.Sprintf("%v", leftVal) == fmt.Sprintf("%v", m.Right), nil
    default:
        return false, fmt.Errorf("unknown match op: %s", m.Op)
    }
}

// Resolve converts a builder-time Match to a runtime ResolvedMatch
// by evaluating the Right dot-path against step outputs and workflow input.
func (m Match) Resolve(stepOutputs map[string][]byte, workflowInput []byte) (ResolvedMatch, error) {
    if m.Right == "" {
        panic("Match.Resolve: Right must not be empty")
    }
    var data []byte
    if strings.HasPrefix(m.Right, "step.") {
        // "step.{id}.output.{path}" → extract from step outputs
        parts := strings.SplitN(m.Right, ".", 4) // step, id, "output", path
        if len(parts) < 4 {
            return ResolvedMatch{}, fmt.Errorf("invalid step path: %s", m.Right)
        }
        data = stepOutputs[parts[1]]
        val, err := ExtractDotPath(parts[3], data)
        if err != nil {
            return ResolvedMatch{}, fmt.Errorf("resolve %s: %w", m.Right, err)
        }
        return ResolvedMatch{Left: m.Left, Op: m.Op, Right: val}, nil
    }
    if strings.HasPrefix(m.Right, "input.") {
        path := strings.TrimPrefix(m.Right, "input.")
        val, err := ExtractDotPath(path, workflowInput)
        if err != nil {
            return ResolvedMatch{}, fmt.Errorf("resolve %s: %w", m.Right, err)
        }
        return ResolvedMatch{Left: m.Left, Op: m.Op, Right: val}, nil
    }
    return ResolvedMatch{}, fmt.Errorf("unknown path prefix in %s", m.Right)
}

// WaitForEventOpts configures a wait-for-event step.
type WaitForEventOpts struct {
    Event   string        // event type to match on EVENTS stream
    Match   Match         // correlation condition
    Timeout time.Duration // max time to wait
}

func validateWaitForEventStep(step StepDef, ids map[string]bool) error {
    if step.Type != StepTypeWaitForEvent {
        return nil
    }
    if step.WaitForEvent == nil {
        return fmt.Errorf("step %q: WaitForEvent opts required", step.ID)
    }
    opts := step.WaitForEvent
    if opts.Event == "" {
        return fmt.Errorf("step %q: WaitForEvent.Event must not be empty", step.ID)
    }
    if opts.Match.Left == "" {
        return fmt.Errorf("step %q: WaitForEvent.Match.Left must not be empty", step.ID)
    }
    if opts.Match.Op == "" {
        return fmt.Errorf("step %q: WaitForEvent.Match.Op must not be empty", step.ID)
    }
    if opts.Timeout <= 0 {
        return fmt.Errorf("step %q: WaitForEvent.Timeout must be positive", step.ID)
    }
    // Validate step.* dot-paths reference declared step IDs.
    // event.* and input.* paths cannot be validated at build time.
    right := fmt.Sprintf("%v", opts.Match.Right)
    if strings.HasPrefix(right, "step.") {
        parts := strings.SplitN(right, ".", 3) // "step", "{id}", "output..."
        if len(parts) >= 2 && !ids[parts[1]] {
            return fmt.Errorf(
                "step %q: Match.Right references step %q which does not exist",
                step.ID, parts[1])
        }
    }
    return nil
}
```

Add `StepTypeWaitForEvent` to `dag/types.go` constants. Add `WaitForEvent *WaitForEventOpts` to StepDef. Add event types to `protocol/protocol.go`: `EventStepWaitStarted`, `EventStepWaitMatched`, `EventStepWaitTimeout`. Wire `validateWaitForEventStep` into `Validate()`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./dag/ -run "TestResolvedMatch|TestMatchResolve" -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add dag/waitforevent.go dag/waitforevent_test.go dag/types.go protocol/protocol.go dag/validate.go
git commit -m "feat: add WaitForEvent types, Match evaluation, and validation"
```

---

### Task 5.2: WaitForEvent Builder Method

**Files:**
- Modify: `dag/builder.go`
- Test: `dag/builder_test.go`

- [ ] **Step 1: Write failing test**

```go
func TestBuilderWaitForEvent(t *testing.T) {
    wf, err := NewWorkflow("test").
        Task("create-order", "create-order").
        WaitForEvent("payment", WaitForEventOpts{
            Event: "payment.completed",
            Match: Match{
                Left:  "event.data.order_id",
                Op:    MatchOpEq,
                Right: "step.create-order.output.order_id",
            },
            Timeout: 48 * time.Hour,
        }).After(StepRef{/* "create-order" */}).
        Task("ship", "ship-order").After(StepRef{/* "payment" */}).
        Build()
    assert(t, err == nil, "build must succeed: %v", err)
    assert(t, len(wf.Steps) == 3, "expected 3 steps")
    waitStep := wf.Steps[1]
    assert(t, waitStep.Type == StepTypeWaitForEvent, "expected WaitForEvent type")
    assert(t, waitStep.WaitForEvent.Event == "payment.completed",
        "expected payment.completed event type")
}
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Add WaitForEvent builder method**

```go
func (b *WorkflowBuilder) WaitForEvent(id string, opts WaitForEventOpts) StepRef {
    if id == "" {
        panic("WaitForEvent: id must not be empty")
    }
    b.steps = append(b.steps, StepDef{
        ID:           id,
        Type:         StepTypeWaitForEvent,
        WaitForEvent: &opts,
    })
    b.current = len(b.steps) - 1
    return StepRef{id: id, index: b.current, builder: b}
}
```

- [ ] **Step 4: Run test to verify it passes**

- [ ] **Step 5: Commit**

```bash
git add dag/builder.go dag/builder_test.go
git commit -m "feat: add WaitForEvent() builder method"
```

---

### Task 5.3: Event Waiters KV Bucket

**Files:**
- Modify: `natsutil/conn.go`
- Test: `natsutil/conn_test.go`

- [ ] **Step 1: Write failing test**

```go
func TestSetupAllCreatesEventWaitersKV(t *testing.T) {
    s, nc, js := StartTestServer(t)
    defer s.Shutdown()
    defer nc.Close()
    SetupAll(nc)

    kv, err := js.KeyValue("event_waiters")
    assert(t, err == nil, "event_waiters must exist: %v", err)
    assert(t, kv != nil, "must not be nil")
}
```

- [ ] **Step 2: Add event_waiters KV bucket**

- [ ] **Step 3: Run test, verify pass**

- [ ] **Step 4: Commit**

```bash
git add natsutil/conn.go natsutil/conn_test.go
git commit -m "feat: add event_waiters KV bucket"
```

---

### Task 5.4: Event Correlator

**Files:**
- Create: `engine/correlator.go`
- Test: `engine/correlator_test.go`

- [ ] **Step 1: Write failing test for correlator**

```go
// engine/correlator_test.go
// Methodology: integration test with real NATS.
// Creates a waiter KV entry, publishes a matching event,
// verifies step.wait.matched appears on history stream.

func TestCorrelatorMatchesEvent(t *testing.T) {
    s, nc, js := natsutil.StartTestServer(t)
    defer s.Shutdown()
    defer nc.Close()
    natsutil.SetupAll(nc)

    // Create a waiter entry
    waiterKV, _ := js.KeyValue("event_waiters")
    waiter := EventWaiter{
        RunID:     "run-1",
        StepID:    "wait-payment",
        EventType: "payment.completed",
        Match: dag.ResolvedMatch{
            Left:  "data.order_id",
            Op:    dag.MatchOpEq,
            Right: "ord-123",
        },
    }
    data, _ := json.Marshal(waiter)
    waiterKV.Put("payment.completed.run-1.wait-payment", data)

    // Start correlator
    cor := NewCorrelator(nc, js)
    err := cor.Start()
    assert(t, err == nil, "start must succeed: %v", err)
    defer cor.Stop()

    // Give KV watch time to populate index
    time.Sleep(200 * time.Millisecond)

    // Subscribe to history for the match event
    sub, _ := js.SubscribeSync("history.run-1",
        nats.BindStream("WORKFLOW_HISTORY"))

    // Publish matching event to EVENTS stream
    eventPayload, _ := json.Marshal(map[string]any{
        "type": "payment.completed",
        "data": map[string]any{"order_id": "ord-123"},
    })
    js.Publish("event.payment.completed", eventPayload)

    // Wait for matched event
    msg, err := sub.NextMsg(5 * time.Second)
    assert(t, err == nil, "must receive match event: %v", err)

    var evt protocol.Event
    json.Unmarshal(msg.Data, &evt)
    assert(t, evt.Type == protocol.EventStepWaitMatched,
        "expected step.wait.matched, got %s", evt.Type)
    assert(t, evt.StepID == "wait-payment",
        "expected wait-payment, got %s", evt.StepID)
}

func TestCorrelatorIgnoresNonMatchingEvent(t *testing.T) {
    // Same setup but event has order_id "ord-999"
    // Verify no match event appears within 1 second
}
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Implement Correlator**

Create `engine/correlator.go`:

```go
package engine

// Correlator watches the EVENTS stream and matches incoming events
// against registered waiters using an in-memory index populated by
// KV watch on event_waiters.>.
//
// Not a separate component — runs as part of the orchestrator.
// But implemented in its own file for clarity.

type EventWaiter struct {
    RunID     string           `json:"run_id"`
    StepID    string           `json:"step_id"`
    EventType string           `json:"event_type"`
    Match     dag.ResolvedMatch `json:"match"`
}

type Correlator struct {
    nc       *nats.Conn
    js       nats.JetStreamContext
    waiterKV nats.KeyValue
    // In-memory index: eventType -> []EventWaiter
    mu      sync.RWMutex
    waiters map[string][]EventWaiter
    // Subscriptions
    kvWatch nats.KeyWatcher
    eventSub *nats.Subscription
}

func NewCorrelator(nc *nats.Conn, js nats.JetStreamContext) *Correlator
func (c *Correlator) Start() error  // starts KV watch + EVENTS consumer
func (c *Correlator) Stop()
func (c *Correlator) AddWaiter(w EventWaiter) error    // writes to KV
func (c *Correlator) RemoveWaitersForRun(runID string)  // cancellation cleanup
```

`Start()`:
1. Opens KV watch on `event_waiters.>` — populates in-memory index
2. Subscribes to `EVENTS` stream via pull consumer
3. Starts goroutine: for each event, lock read on waiters[eventType], evaluate matches, publish step.wait.matched, delete KV entry on match

Bounded: 10,000 waiters per event type checked in `AddWaiter`.

- [ ] **Step 4: Run tests to verify they pass**

- [ ] **Step 5: Commit**

```bash
git add engine/correlator.go engine/correlator_test.go
git commit -m "feat: implement event correlator with KV watch waiter index"
```

---

### Task 5.5: Integrate WaitForEvent into Orchestrator

**Files:**
- Modify: `engine/orchestrator.go`
- Test: `engine/orchestrator_test.go`

- [ ] **Step 1: Write failing integration test**

```go
func TestOrchestratorWaitForEventMatches(t *testing.T) {
    // Register workflow: task-a -> wait-for-event -> task-b
    // Start run, complete task-a
    // Publish matching event to EVENTS stream
    // Verify task-b runs and workflow completes
}

func TestOrchestratorWaitForEventTimeout(t *testing.T) {
    // Register workflow with short timeout (200ms)
    // Start run, complete predecessor
    // Don't publish matching event
    // Verify step.wait.timeout fires and workflow handles it
}
```

- [ ] **Step 2: Add correlator to orchestrator**

1. Add `correlator *Correlator` field
2. In `NewOrchestrator`, create correlator
3. In `Start`, start correlator
4. In `enqueueReady`, for `StepTypeWaitForEvent` steps:
   - Resolve the right-side dot-path to a concrete value from step outputs/input
   - Publish `step.wait.started` event
   - Call `correlator.AddWaiter()` with resolved match
   - Schedule timeout via `sleepTimer.Schedule()` — on fire, publish `step.wait.timeout`
5. In `dispatchEvent`, handle `EventStepWaitMatched` and `EventStepWaitTimeout` — treat like step completion
6. In `handleWorkflowCancelled`, call `correlator.RemoveWaitersForRun()`

- [ ] **Step 3: Run tests to verify they pass**

- [ ] **Step 4: Run all tests for regression**

Run: `go test ./... -timeout 120s`

- [ ] **Step 5: Commit**

```bash
git add engine/orchestrator.go engine/orchestrator_test.go
git commit -m "feat: integrate wait-for-event into orchestrator with timeout"
```

---

## Chunk 6: HTTP-to-NATS Bridge

### Task 6.1: Ack Map

**Files:**
- Create: `bridge/ackmap.go`
- Test: `bridge/ackmap_test.go`

- [ ] **Step 1: Write failing tests for ack map**

```go
// bridge/ackmap_test.go
func TestAckMapStoreAndRetrieve(t *testing.T) {
    am := NewAckMap()

    msg := &nats.Msg{} // mock enough for interface
    am.Store("run-1.step-a", msg)

    retrieved, ok := am.Load("run-1.step-a")
    assert(t, ok, "must find stored entry")
    assert(t, retrieved == msg, "must return same message")
}

func TestAckMapDeleteRemoves(t *testing.T) {
    am := NewAckMap()
    am.Store("run-1.step-a", &nats.Msg{})
    am.Delete("run-1.step-a")

    _, ok := am.Load("run-1.step-a")
    assert(t, !ok, "must not find deleted entry")
}
```

- [ ] **Step 2: Implement AckMap**

```go
package bridge

// AckMap holds in-flight task references for HTTP workers.
// Keys are {runID}.{stepID}. Thread-safe via sync.Map.

type AckMap struct {
    m sync.Map
}

func NewAckMap() *AckMap
func (am *AckMap) Store(taskID string, msg *nats.Msg)
func (am *AckMap) Load(taskID string) (*nats.Msg, bool)
func (am *AckMap) Delete(taskID string)
```

- [ ] **Step 3: Run tests, verify pass**

- [ ] **Step 4: Commit**

```bash
git add bridge/ackmap.go bridge/ackmap_test.go
git commit -m "feat: add ack map for HTTP bridge task tracking"
```

---

### Task 6.2: Bridge Server and Connect Endpoint

**Files:**
- Create: `bridge/bridge.go`
- Create: `bridge/connect.go`
- Test: `bridge/bridge_test.go`

- [ ] **Step 1: Write failing test for /v1/workers/connect SSE**

```go
// bridge/bridge_test.go
func TestBridgeConnect(t *testing.T) {
    s, nc, js := natsutil.StartTestServer(t)
    defer s.Shutdown()
    defer nc.Close()
    natsutil.SetupAll(nc)

    b := NewBridge(nc, js)
    srv := httptest.NewServer(b.Handler())
    defer srv.Close()

    body := `{"worker_id":"w-1","task_types":["echo"],"max_tasks":5}`
    resp, err := http.Post(srv.URL+"/v1/workers/connect",
        "application/json", strings.NewReader(body))
    assert(t, err == nil, "connect must succeed: %v", err)
    assert(t, resp.StatusCode == 200, "expected 200, got %d", resp.StatusCode)
    assert(t, resp.Header.Get("Content-Type") == "text/event-stream",
        "expected SSE content type")

    // Verify worker appears in directory
    dir := worker.NewDirectory(js)
    workers, _ := dir.List()
    assert(t, len(workers) == 1, "expected 1 worker")

    resp.Body.Close() // triggers deregistration
}
```

- [ ] **Step 2: Implement Bridge struct and connect handler**

`bridge/bridge.go`:
```go
package bridge

type Bridge struct {
    nc     *nats.Conn
    js     nats.JetStreamContext
    ackMap *AckMap
    dir    *worker.Directory
}

func NewBridge(nc *nats.Conn, js nats.JetStreamContext) *Bridge
func (b *Bridge) Handler() http.Handler  // returns mux with all routes
```

`bridge/connect.go`:
```go
// POST /v1/workers/connect
// Registers worker in directory, starts SSE heartbeat stream.
// On disconnect: deregisters worker after grace period.
func (b *Bridge) handleConnect(w http.ResponseWriter, r *http.Request)
```

- [ ] **Step 3: Run test, verify pass**

- [ ] **Step 4: Commit**

```bash
git add bridge/bridge.go bridge/connect.go bridge/bridge_test.go
git commit -m "feat: add HTTP bridge with /v1/workers/connect SSE endpoint"
```

---

### Task 6.3: Poll Endpoint

**Files:**
- Create: `bridge/poll.go`
- Test: `bridge/bridge_test.go`

- [ ] **Step 1: Write failing test for /v1/tasks/poll**

```go
func TestBridgePoll(t *testing.T) {
    // Setup bridge, connect a worker
    // Publish a task to task.echo.run-1
    // Poll via HTTP — verify task payload returned
    // Verify ack map contains the task
}

func TestBridgePollTimeout(t *testing.T) {
    // Setup bridge, connect a worker
    // Poll with short timeout, no tasks available
    // Verify empty array returned
}
```

- [ ] **Step 2: Implement poll handler**

`bridge/poll.go`:
```go
// POST /v1/tasks/poll
// Long-polls NATS consumers for tasks matching registered types.
// Returns array of task payloads. Stores nats.Msg in ack map.
func (b *Bridge) handlePoll(w http.ResponseWriter, r *http.Request)
```

- [ ] **Step 3: Run tests, verify pass**

- [ ] **Step 4: Commit**

```bash
git add bridge/poll.go bridge/bridge_test.go
git commit -m "feat: add /v1/tasks/poll long-poll endpoint"
```

---

### Task 6.4: Resolve Endpoint

**Files:**
- Create: `bridge/resolve.go`
- Test: `bridge/bridge_test.go`

- [ ] **Step 1: Write failing tests for each action**

```go
func TestBridgeResolveComplete(t *testing.T) {
    // Setup, connect, poll a task
    // POST /v1/tasks/{id}/resolve with action=complete
    // Verify step.completed event appears on history stream
}

func TestBridgeResolveFail(t *testing.T) {
    // POST /v1/tasks/{id}/resolve with action=fail
    // Verify step.failed event
}

func TestBridgeResolvePause(t *testing.T) {
    // POST /v1/tasks/{id}/resolve with action=pause
    // Verify checkpoint written and message NAKed with delay
}
```

- [ ] **Step 2: Implement resolve handler**

`bridge/resolve.go`:
```go
// POST /v1/tasks/{id}/resolve
// Single deep endpoint with action discriminator.
// Actions: complete, fail, pause, checkpoint
func (b *Bridge) handleResolve(w http.ResponseWriter, r *http.Request)
```

Action dispatch:
- `complete`: publish step.completed event via the held nats.Msg context, ACK
- `fail`: publish step.failed event, ACK
- `pause`: write checkpoint to KV, NakWithDelay
- `checkpoint`: write checkpoint to KV, InProgress (extend ack)

- [ ] **Step 3: Run tests, verify pass**

- [ ] **Step 4: Run full test suite**

Run: `go test ./... -timeout 120s`

- [ ] **Step 5: Commit**

```bash
git add bridge/resolve.go bridge/bridge_test.go
git commit -m "feat: add /v1/tasks/{id}/resolve endpoint with action discriminator"
```

---

**Note on authentication:** The spec requires credential validation on `/connect`. For Tier 1, implement a simple shared-secret token via `Authorization: Bearer <token>` header. The bridge checks against a configured secret. Full auth (per-worker scoped credentials) is deferred to Tier 2.

### Task 6.5: Bridge E2E Integration Test

**Files:**
- Test: `bridge/e2e_test.go`

- [ ] **Step 1: Write full E2E test: connect, poll, complete**

```go
// bridge/e2e_test.go
// Methodology: full lifecycle test with real NATS + HTTP bridge.
// Registers a workflow, starts a run, bridges a task via HTTP,
// completes it, verifies workflow completes.

func TestBridgeE2EWorkflowCompletion(t *testing.T) {
    // 1. Start embedded NATS, setup all resources
    // 2. Register workflow with one task
    // 3. Start orchestrator
    // 4. Start bridge HTTP server
    // 5. HTTP: connect worker
    // 6. HTTP: start workflow run via API
    // 7. HTTP: poll for task
    // 8. HTTP: resolve task with action=complete
    // 9. Wait for workflow to complete
    // 10. Assert run status is Completed
}
```

- [ ] **Step 2: Run test, verify pass**

- [ ] **Step 3: Commit**

```bash
git add bridge/e2e_test.go
git commit -m "test: add bridge E2E integration test for full workflow lifecycle"
```

---

## Chunk 7: Wire Protocol Types and Documentation

No separate `sdk/` package. Extend existing types to avoid duplication (Ousterhout: eliminate change amplification).

### Task 7.1: Extend protocol.TaskPayload with TaskID

**Files:**
- Modify: `protocol/protocol.go:12-18`
- Test: `protocol/protocol_test.go`

- [ ] **Step 1: Write failing test for TaskID field**

```go
func TestTaskPayloadIncludesTaskID(t *testing.T) {
    p := TaskPayload{
        TaskID: "run-1.step-a",
        RunID:  "run-1",
        StepID: "step-a",
        Input:  []byte(`{"key":"value"}`),
    }
    data, err := json.Marshal(p)
    assert(t, err == nil, "marshal: %v", err)
    assert(t, strings.Contains(string(data), `"task_id":"run-1.step-a"`),
        "expected task_id in JSON")
}
```

- [ ] **Step 2: Add TaskID and TaskResolution to protocol/protocol.go**

```go
// Add to TaskPayload struct:
TaskID string `json:"task_id"` // {runID}.{stepID} — canonical identity

// New type for HTTP bridge resolve actions:
type TaskResolution struct {
    Action     string          `json:"action"`
    Output     json.RawMessage `json:"output,omitempty"`
    Error      string          `json:"error,omitempty"`
    PauseName  string          `json:"name,omitempty"`
    PauseMs    int64           `json:"duration_ms,omitempty"`
    Checkpoint json.RawMessage `json:"checkpoint,omitempty"`
    Data       json.RawMessage `json:"data,omitempty"`
}
```

- [ ] **Step 3: Run tests, verify pass. Update bridge to use protocol types directly.**

- [ ] **Step 4: Commit**

```bash
git add protocol/protocol.go protocol/protocol_test.go
git commit -m "feat: add TaskID to TaskPayload and TaskResolution type for bridge"
```

---

### Task 7.2: Wire Protocol Documentation

**Files:**
- Create: `docs/wire-protocol.md`

- [ ] **Step 1: Write wire protocol reference**

Document for other language SDK authors:
- NATS transport: subjects, `protocol.TaskPayload` JSON schema, consumer patterns
- HTTP transport: three endpoints, request/response schemas, SSE heartbeat format
- `worker.WorkerRegistration` JSON schema (directory entries)
- `protocol.TaskResolution` JSON schema (resolve actions)
- Task lifecycle: poll -> execute -> resolve
- Pause/checkpoint semantics
- Heartbeat/TTL expectations (60s bucket TTL, 30s refresh)

Reference the Go types as canonical — other SDKs implement against the JSON schemas.

- [ ] **Step 2: Commit**

```bash
git add docs/wire-protocol.md
git commit -m "docs: add wire protocol reference for polyglot worker SDKs"
```

---

## Final Validation

### Task F.1: Full Regression Test Suite

- [ ] **Step 1: Run all tests**

Run: `go test ./... -timeout 120s -count=1`
Expected: All PASS

- [ ] **Step 2: Run linters**

Run: `go vet ./... && staticcheck ./...`
Expected: No issues

- [ ] **Step 3: Verify formatting**

Run: `gofmt -l .`
Expected: No output (all files formatted)

- [ ] **Step 4: Final commit if any remaining changes**

```bash
git add -A
git commit -m "chore: final cleanup for tier 1 workflow primitives"
```
