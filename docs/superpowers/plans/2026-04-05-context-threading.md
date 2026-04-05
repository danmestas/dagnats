# Context Threading Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development
> (if subagents available) or superpowers:executing-plans to implement this plan.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace all 217 `context.Background()` placeholders with properly
threaded contexts for cancellation, timeouts, and trace propagation.

**Architecture:** Pure threading — no new abstractions. The contexts already
exist at entry points (`handleEventJS`, `handleMessage`, `r.Context()`,
Service method params). The work is passing them through instead of dropping
them on the floor. No public API changes.

**Tech Stack:** Go `context.Context` — standard library only.

**Spec:** `docs/superpowers/specs/2026-04-05-context-threading-design.md`

---

## Chunk 1: Engine Orchestrator (19 replacements)

The orchestrator handler methods already receive `ctx context.Context`.
They just use `context.Background()` for JetStream calls within.

### Task 1: Thread ctx through orchestrator publish/KV calls

**Files:**
- Modify: `internal/engine/orchestrator.go`

The `dispatchEvent`, `handleWorkflowStarted`, `handleStepCompleted`,
`handleStepFailed`, and all other handler methods already receive `ctx`.
Inside each, replace `context.Background()` with `ctx`.

- [ ] **Step 1: Replace all `context.Background()` with `ctx` in methods
  that already have ctx as a parameter**

Search for all `context.Background()` in `orchestrator.go`. For each one,
check if the containing function already has `ctx` in scope. If yes,
replace. If no (e.g. constructor, `Start`, `extractTraceCtx`), leave it.

Specifically:
- Constructor (`NewOrchestrator`): `context.Background()` for KV binding
  at startup — **keep** (no parent ctx at construction time)
- `Start()`: `context.Background()` for consumer setup — **keep** (startup)
- `extractTraceCtxJS`: `context.Background()` as fallback when no trace
  headers — **keep** (this IS the context origin)
- All handler methods: **replace** with `ctx`

- [ ] **Step 2: Pass ctx to helper functions that don't have it yet**

Functions called from handlers that take no ctx but do I/O:
- `o.saveSnapshot(ctx, run)` — check if it takes ctx
- `o.completeWorkflow(ctx, run)` — check if it takes ctx
- `o.publishDeadLetter(...)` — needs ctx added to signature
- `o.publishCancelEvent(...)` — needs ctx added to signature
- `o.notifyParentIfChild(...)` — needs ctx added to signature

For each: add `ctx context.Context` as first param, update all callers.

- [ ] **Step 3: Run engine tests**

```bash
go test ./internal/engine/ -v -timeout 120s
```

- [ ] **Step 4: Commit**

```bash
git add internal/engine/orchestrator.go
git commit -m "refactor(engine): thread ctx through orchestrator methods"
```

### Task 2: Thread ctx through engine helper modules

**Files:**
- Modify: `internal/engine/task_publish.go` (~5 replacements)
- Modify: `internal/engine/snapshot.go` (~3 replacements)
- Modify: `internal/engine/concurrency.go` (~8 replacements)
- Modify: `internal/engine/ratelimit.go` (~4 replacements)
- Modify: `internal/engine/sleeptimer.go` (~9 replacements)
- Modify: `internal/engine/correlator.go` (~8 replacements)
- Modify: `internal/engine/approval.go` (~5 replacements)
- Modify: `internal/engine/sticky.go` (~5 replacements)
- Modify: `internal/engine/planner.go` (~2 replacements)
- Modify: `internal/engine/actor_orch.go` (~2 replacements)
- Modify: `internal/engine/workflow_actor.go` (callers)

For each file:
1. Add `ctx context.Context` as first param to functions that do I/O
2. Replace `context.Background()` with `ctx` inside those functions
3. Update all callers to pass `ctx`

**Signature changes (all internal, not public):**

```go
// task_publish.go
func enqueueReadySteps(ctx context.Context, js ...) error
func publishAtomicBatches(ctx context.Context, js ...) error
func publishWorkflowEvent(ctx context.Context, js ...) error
func publishIterationTask(ctx context.Context, js ...) error

// snapshot.go
func (s *SnapshotStore) Save(ctx context.Context, run ...) error
func (s *SnapshotStore) Load(ctx context.Context, runID ...) (...)

// concurrency.go
func (cm *ConcurrencyManager) AcquireRun(ctx context.Context, ...) (...)
func (cm *ConcurrencyManager) ReleaseRun(ctx context.Context, ...) error
func (cm *ConcurrencyManager) AcquireTask(ctx context.Context, ...) (...)
func (cm *ConcurrencyManager) ReleaseTask(ctx context.Context, ...) error

// ratelimit.go
func (rl *RateLimiter) Allow(ctx context.Context, ...) (...)

// sleeptimer.go — fire* methods use context.WithTimeout
//   (no parent ctx — they react to timer expiry, not events)
// correlator.go — deliver methods use context.WithTimeout
//   (no parent ctx — they react to KV watcher, not events)
// approval.go — store/consume token methods gain ctx
// sticky.go — create/get/delete binding methods gain ctx
```

- [ ] **Step 1: Migrate each file, compile after each**

Work file by file. After each file, run:
```bash
go build ./internal/engine/
```
Fix compilation errors (callers in other files).

- [ ] **Step 2: Run engine tests**

```bash
go test ./internal/engine/ -v -timeout 120s
```

- [ ] **Step 3: Run E2E tests**

```bash
go test ./e2e/features/ -v -timeout 180s
```

- [ ] **Step 4: Commit**

```bash
git add internal/engine/
git commit -m "refactor(engine): thread ctx through all engine modules"
```

---

## Chunk 2: Worker (16 replacements, no public API change)

### Task 3: Use tc.ctx in taskContext methods

**Files:**
- Modify: `worker/context.go` (~6 replacements)
- Modify: `worker/worker.go` (~10 replacements)
- Modify: `worker/directory.go` (~5 replacements)

The `taskContext` struct has a `ctx context.Context` field set from the
message handler's trace context. All internal JetStream/KV calls should
use `tc.ctx` instead of `context.Background()`.

- [ ] **Step 1: Replace in context.go**

In every method on `taskContext` (`Complete`, `Fail`, `FailPermanent`,
`Continue`, `PutStream`, `Heartbeat`, `Checkpoint`, `LoadCheckpoint`,
`Pause`, `WaitForSignal`, `SendSignal`, `publishEvent`):

Replace `context.Background()` with `c.ctx`.

- [ ] **Step 2: Replace in worker.go**

In `createConsumer`, `createStickyConsumer`, `createElasticConsumer`,
`bindOptionalKV`, `registerDirectory`, `newDirectoryOptional`:

Constructor/startup functions — **keep** `context.Background()` (startup,
no parent). But `createConsumer` and `createElasticConsumer` could take
a startup ctx. For now, keep them — they run once at start.

- [ ] **Step 3: Replace in directory.go**

In `Register`, `Deregister`, `List` KV operations: these are called from
the heartbeat loop (no parent ctx) and startup. Keep `context.Background()`
but wrap with a timeout:

```go
ctx, cancel := context.WithTimeout(
    context.Background(), 5*time.Second,
)
defer cancel()
```

- [ ] **Step 4: Run worker tests**

```bash
go test ./worker/ -v -timeout 60s
```

- [ ] **Step 5: Commit**

```bash
git add worker/
git commit -m "refactor(worker): use tc.ctx for JetStream operations"
```

---

## Chunk 3: API Service (29 replacements)

### Task 4: Thread passed ctx in Service methods

**Files:**
- Modify: `internal/api/service.go` (~20 replacements)
- Modify: `internal/api/scheduled.go` (~9 replacements)
- Modify: `internal/api/bulk_run.go`
- Modify: `internal/api/timer.go`
- Modify: `internal/api/task_check.go`
- Modify: `internal/api/rest.go`

All Service methods already take `ctx context.Context` as first param.
Replace `context.Background()` with `ctx` in their bodies.

- [ ] **Step 1: Replace in service.go**

Every `context.Background()` inside a method that has `ctx` → use `ctx`.
For `NewService` constructor: **keep** (startup, no parent).

- [ ] **Step 2: Replace in other api files**

Same pattern in `scheduled.go`, `bulk_run.go`, `timer.go`, `task_check.go`.
REST handlers: extract `r.Context()` and pass to Service methods.

- [ ] **Step 3: Run API tests**

```bash
go test ./internal/api/ -v -timeout 60s
```

- [ ] **Step 4: Commit**

```bash
git add internal/api/
git commit -m "refactor(api): thread ctx through Service methods"
```

---

## Chunk 4: Bridge (12 replacements)

### Task 5: Thread r.Context() through bridge handlers

**Files:**
- Modify: `bridge/bridge.go`
- Modify: `bridge/poll.go`
- Modify: `bridge/resolve.go`

- [ ] **Step 1: Add ctx to bridge helper functions**

`fetchForType`, `processPolledMsg`, `publishEvent`, `writeCheckpoint`,
`resolveSendSignal`, `resolveWaitSignal`, `watchForSignal` — add
`ctx context.Context` as first param.

In HTTP handlers (`handlePoll`, `handleResolve`), extract `r.Context()`
and pass to helpers.

Constructor (`NewBridge`): **keep** `context.Background()` (startup).

- [ ] **Step 2: Run bridge tests**

```bash
go test ./bridge/ -v -timeout 60s
```

- [ ] **Step 3: Commit**

```bash
git add bridge/
git commit -m "refactor(bridge): thread r.Context() through handlers"
```

---

## Chunk 5: Triggers + Observe (25 replacements)

### Task 6: Thread ctx through trigger and observe modules

**Files:**
- Modify: `internal/trigger/service.go`
- Modify: `internal/trigger/subject.go`
- Modify: `internal/trigger/debounce.go`
- Modify: `internal/trigger/scheduler.go`
- Modify: `internal/trigger/webhook.go`
- Modify: `internal/observe/simple/log_collector.go`
- Modify: `internal/observe/simple/metrics_collector.go`
- Modify: `internal/observe/simple/monitor.go`
- Modify: `internal/observe/simple/trace_collector.go`

Trigger fire methods are called from the scheduler tick — add ctx.
Observe publish methods are fire-and-forget telemetry — keep
`context.Background()` with a short timeout. Telemetry must not be
cancelled by the context it's observing — a failed request's telemetry
should still be recorded.

- [ ] **Step 1: Thread ctx through trigger fire/KV operations**
- [ ] **Step 2: Add timeouts to observe publish calls**

Observe calls use `context.Background()` with timeout, NOT a threaded
ctx from the caller — telemetry must outlive the request it records:

```go
ctx, cancel := context.WithTimeout(
    context.Background(), 2*time.Second,
)
defer cancel()
```

- [ ] **Step 3: Run tests**

```bash
go test ./internal/trigger/ -v -timeout 60s
go test ./internal/observe/simple/ -v -timeout 60s
```

- [ ] **Step 4: Commit**

```bash
git add internal/trigger/ internal/observe/
git commit -m "refactor: thread ctx through triggers, timeout observe publishes"
```

---

## Chunk 6: Setup + Final validation

### Task 7: Add timeouts to setup functions

**Files:**
- Modify: `internal/natsutil/conn.go` (~6 replacements)
- Modify: `server/server.go`

CLI commands correctly use `context.Background()` — no parent context
exists. No changes needed in `cli/`.

- [ ] **Step 1: Add bounded timeout to setup functions**

In `SetupStreams`, `SetupKVBuckets`, `SetupTelemetryStream`,
`SetupStickyStream`, `SetupAll`:

```go
ctx, cancel := context.WithTimeout(
    context.Background(), 30*time.Second,
)
defer cancel()
```

- [ ] **Step 2: Run full test suite**

```bash
go test ./... -timeout 300s
```

- [ ] **Step 3: Run vet**

```bash
go vet ./...
```

- [ ] **Step 4: Verify remaining context.Background() is justified**

```bash
grep -rn 'context\.Background()' --include="*.go" \
    | grep -v _test.go | grep -v vendor | grep -v cli/ \
    | grep -v '.worktrees/dx'
```

Every remaining instance should be: constructor/startup, CLI, test,
observe telemetry (fire-and-forget), or SleepTimer/Correlator fire
methods (no parent ctx). All others should use a threaded ctx or
a bounded timeout.

- [ ] **Step 5: Commit**

```bash
git add internal/natsutil/ server/
git commit -m "refactor: add timeouts to setup functions, finalize context threading"
```

### Task 8: Update ADR

**Files:**
- Modify: `docs/architecture/jetstream-api-migration.md`

Remove the "all ctx parameters use context.Background()" constraint.
Replace with: "Context is threaded from entry points. Setup functions
use context.WithTimeout(30s). CLI uses context.Background() with
optional signal handling."

- [ ] **Step 1: Update ADR**
- [ ] **Step 2: Commit**

```bash
git add docs/architecture/
git commit -m "docs: update ADR — context threading complete"
```
