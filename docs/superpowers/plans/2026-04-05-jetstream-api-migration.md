# JetStream API Migration Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development
> (if subagents available) or superpowers:executing-plans to implement this plan.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate DagNats from legacy `nats.JetStreamContext` to the new
`jetstream.JetStream` API, unblocking Orbit extensions (jetstreamext, pcgroups).

**Architecture:** Dual-interface migration. The Orchestrator and Worker hold both
`nats.JetStreamContext` (legacy) and `jetstream.JetStream` (new) during the
transition. Convert file by file, bottom-up. Each phase produces a working,
tested state. Remove legacy interface when no callers remain.

**Tech Stack:** Go, `github.com/nats-io/nats.go/jetstream`

**Spec:** `docs/superpowers/specs/2026-04-05-jetstream-api-migration-design.md`

**Scope:** ~20 functions in `internal/engine/`, ~5 in `worker/`, ~40 test
functions. The public API (`worker.TaskContext`, `worker.Worker`) is unchanged.

---

## Chunk 1: Engine Task Publishing (enables jetstreamext)

Migrate `internal/engine/task_publish.go` — the critical path for atomic
fan-out. After this chunk, `enqueueReadySteps` can use `jetstream.JetStream`.

### Task 1: Add jsNew to Orchestrator

**Files:**
- Modify: `internal/engine/orchestrator.go` (struct + constructor)

- [ ] **Step 1: Add `jsNew jetstream.JetStream` field to Orchestrator struct**

Import `"github.com/nats-io/nats.go/jetstream"`. Add the field alongside
the existing `js nats.JetStreamContext`. Initialize in the constructor:

```go
jsNew, err := jetstream.New(nc)
if err != nil {
    panic("Orchestrator: jetstream.New: " + err.Error())
}
```

- [ ] **Step 2: Run engine tests**

```bash
go test ./internal/engine/ -v -timeout 120s
```

Expected: all pass. New field added but not used yet.

- [ ] **Step 3: Commit**

```bash
git add internal/engine/orchestrator.go
git commit -m "refactor(engine): add jetstream.JetStream to Orchestrator"
```

### Task 2: Migrate task_publish.go

**Files:**
- Modify: `internal/engine/task_publish.go`
- Modify: `internal/engine/task_publish_test.go` (create if needed)

- [ ] **Step 1: Change `enqueueReadySteps` to accept `jetstream.JetStream`**

Add `jsNew jetstream.JetStream` as a parameter alongside the existing `js`.
The `publishWorkflowEvent` call still uses legacy `js` (migrated later).
The task publish loop uses `jsNew` via `jetstreamext.PublishMsgBatch`.

Update all callers of `enqueueReadySteps` (grep for it in the engine package).

- [ ] **Step 2: Change `publishTask` to use new API**

Replace `js.PublishMsg(msg)` with `jsNew.PublishMsg(ctx, msg)`. Or, if
replacing with `PublishMsgBatch` (jetstreamext), the individual
`publishTask` is no longer needed — `collectReadyMessages` + batch
replaces the per-step loop.

- [ ] **Step 3: Run engine tests**

```bash
go test ./internal/engine/ -v -timeout 120s
```

Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add internal/engine/task_publish.go
git commit -m "refactor(engine): migrate task_publish to jetstream.JetStream"
```

### Task 3: Migrate publishWorkflowEvent

**Files:**
- Modify: `internal/engine/task_publish.go`

- [ ] **Step 1: Change `publishWorkflowEvent` to use new API**

Replace `js.Publish(subject, data, nats.MsgId(id))` with
`jsNew.PublishMsg(ctx, &nats.Msg{...})` or the equivalent new-API publish.

- [ ] **Step 2: Run engine tests**

```bash
go test ./internal/engine/ -v -timeout 120s
```

- [ ] **Step 3: Commit**

```bash
git add internal/engine/task_publish.go
git commit -m "refactor(engine): migrate publishWorkflowEvent to new API"
```

---

## Chunk 2: Engine KV Operations

Migrate KV-dependent code: snapshot store, concurrency manager, rate limiter.

### Task 4: Migrate SnapshotStore

**Files:**
- Modify: `internal/engine/snapshot.go`
- Modify: `internal/engine/snapshot_test.go`

- [ ] **Step 1: Change `NewSnapshotStore` to accept `jetstream.JetStream`**

Replace `nats.KeyValue` with `jetstream.KeyValue`. All KV operations
gain a `context.Context` parameter. Use `context.Background()` initially.

```go
// Old: kv, err := js.KeyValue("workflow_runs")
// New: kv, err := jsNew.KeyValueStore(ctx, "workflow_runs")
```

- [ ] **Step 2: Update all KV calls in snapshot.go**

`kv.Get(key)` → `kv.Get(ctx, key)`
`kv.Put(key, data)` → `kv.Put(ctx, key, data)`
`kv.Create(key, data)` → `kv.Create(ctx, key, data)`
`kv.Update(key, data, rev)` → `kv.Update(ctx, key, data, rev)`

- [ ] **Step 3: Update snapshot_test.go**

Tests construct JetStream via `nc.JetStream()` — add `jetstream.New(nc)`
alongside. Update `NewSnapshotStore` calls.

- [ ] **Step 4: Run tests**

```bash
go test ./internal/engine/ -run TestSnapshot -v
```

- [ ] **Step 5: Commit**

```bash
git add internal/engine/snapshot.go internal/engine/snapshot_test.go
git commit -m "refactor(engine): migrate SnapshotStore to jetstream.KeyValue"
```

### Task 5: Migrate ConcurrencyManager

**Files:**
- Modify: `internal/engine/concurrency.go`
- Modify: `internal/engine/concurrency_test.go`
- Modify: `internal/engine/orchestrator_concurrency_test.go`

Same pattern: `nats.KeyValue` → `jetstream.KeyValue`, add `ctx` to all
KV operations.

- [ ] **Step 1: Update struct and constructors**
- [ ] **Step 2: Update all KV calls**
- [ ] **Step 3: Update tests**
- [ ] **Step 4: Run tests**

```bash
go test ./internal/engine/ -run TestConcurrency -v
```

- [ ] **Step 5: Commit**

```bash
git add internal/engine/concurrency.go internal/engine/concurrency_test.go \
    internal/engine/orchestrator_concurrency_test.go
git commit -m "refactor(engine): migrate ConcurrencyManager to jetstream.KeyValue"
```

### Task 6: Migrate RateLimiter, SleepTimer, Correlator

**Files:**
- Modify: `internal/engine/ratelimit.go`, `sleeptimer.go`, `correlator.go`
- Modify: corresponding test files

Same pattern for each. These are leaf modules — no cascading changes.

- [ ] **Step 1-3: Migrate each file** (same KV migration pattern)
- [ ] **Step 4: Run full engine tests**

```bash
go test ./internal/engine/ -v -timeout 120s
```

- [ ] **Step 5: Commit**

```bash
git add internal/engine/
git commit -m "refactor(engine): migrate ratelimit, sleeptimer, correlator to new API"
```

---

## Chunk 3: Worker Migration (enables pcgroups)

Migrate `worker/worker.go` — subscriptions and message handling.

### Task 7: Add jsNew to Worker

**Files:**
- Modify: `worker/worker.go` (struct + constructor)

- [ ] **Step 1: Add `jsNew jetstream.JetStream` to Worker struct**

Initialize in `NewWorker`:

```go
jsNew, err := jetstream.New(nc)
if err != nil {
    panic("NewWorker: jetstream.New: " + err.Error())
}
```

- [ ] **Step 2: Run worker tests**

```bash
go test ./worker/ -v -timeout 60s
```

- [ ] **Step 3: Commit**

```bash
git add worker/worker.go
git commit -m "refactor(worker): add jetstream.JetStream to Worker"
```

### Task 8: Migrate Worker subscriptions

**Files:**
- Modify: `worker/worker.go` (`Start` method)
- Modify: `worker/worker.go` (`handleMessage` — adapt to `jetstream.Msg`)

- [ ] **Step 1: Replace `js.Subscribe` with new consumer API**

```go
// Old
sub, err := w.js.Subscribe(subject, handler, nats.AckExplicit())

// New
stream, _ := w.jsNew.Stream(ctx, "TASK_QUEUES")
cons, _ := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
    FilterSubject: subject,
    AckPolicy:     jetstream.AckExplicitPolicy,
})
cc, _ := cons.Consume(func(msg jetstream.Msg) {
    w.handleMessage(tt, h, msg)
})
```

- [ ] **Step 2: Update `handleMessage` to accept `jetstream.Msg`**

`jetstream.Msg` has: `Data() []byte`, `Headers() nats.Header`,
`Ack() error`, `Nak() error`, `NakWithDelay(delay) error`. Map from
the current `*nats.Msg` usage.

- [ ] **Step 3: Update `taskContext` to work with `jetstream.Msg`**

The `taskContext` holds the message for ack/nak. Update the stored
message type and all ack/nak calls in `context.go`.

- [ ] **Step 4: Run worker tests**

```bash
go test ./worker/ -v -timeout 60s
```

- [ ] **Step 5: Run E2E tests**

```bash
go test ./e2e/features/ -v -timeout 180s
```

- [ ] **Step 6: Commit**

```bash
git add worker/
git commit -m "refactor(worker): migrate subscriptions to jetstream consumer API"
```

---

## Chunk 4: Cleanup

### Task 9: Remove legacy JetStreamContext

**Files:**
- Modify: `internal/engine/orchestrator.go` (remove `js` field)
- Modify: `worker/worker.go` (remove `js` field)
- Modify: All remaining callers

- [ ] **Step 1: Remove legacy `js` field from Orchestrator**
- [ ] **Step 2: Remove legacy `js` field from Worker**
- [ ] **Step 3: Update natsutil setup functions if needed**
- [ ] **Step 4: Run full test suite**

```bash
go test ./... -timeout 300s
```

- [ ] **Step 5: Commit**

```bash
git add .
git commit -m "refactor: remove legacy nats.JetStreamContext"
```

### Task 10: Final validation

- [ ] **Step 1: Full test suite**
- [ ] **Step 2: go vet + staticcheck**
- [ ] **Step 3: E2E tests**
