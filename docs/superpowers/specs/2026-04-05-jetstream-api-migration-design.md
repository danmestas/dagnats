# JetStream API Migration

**Status:** Design
**Date:** 2026-04-05
**Depends on:** Nothing (prerequisite for orbit extensions)

## Problem

The NATS Go client has two JetStream APIs:

1. **Legacy:** `nats.JetStreamContext` from `github.com/nats-io/nats.go`
2. **New:** `jetstream.JetStream` from `github.com/nats-io/nats.go/jetstream`

DagNats uses the legacy API everywhere. The Orbit extensions (jetstreamext,
pcgroups) require the new API. The legacy API is maintenance-only — new
features like `AllowAtomicPublish` and consumer groups only exist on the new
API.

Migrating incrementally: hold both interfaces, convert call sites file by
file, remove the legacy interface when no callers remain.

## Scope

Two packages need migration:

- **`internal/engine/`** — Orchestrator, task publishing, snapshot store,
  concurrency, rate limiter, correlator, sleep timer, workflow actor.
  ~20 functions take `nats.JetStreamContext`.
- **`worker/`** — Worker struct, subscriptions, directory, context.
  ~5 functions take `nats.JetStreamContext`.

Supporting packages (`internal/natsutil/`, `internal/api/`, `server/`) are
touched only where they construct or pass JetStream handles.

## Design

### 1. Dual Interface Period

The Orchestrator and Worker hold both interfaces during migration:

```go
type Orchestrator struct {
    nc    *nats.Conn
    js    nats.JetStreamContext  // legacy — removed when migration complete
    jsNew jetstream.JetStream   // new API
    // ...
}
```

`jetstream.New(nc)` creates the new interface from the existing connection.
No new connections, no config changes.

### 2. Migration Order

Migrate bottom-up (leaf functions first, aggregates last):

**Phase 1 — Engine internals:**
1. `task_publish.go` — `publishTask`, `enqueueReadySteps` (enables atomic publish)
2. `snapshot.go` — `SnapshotStore` (KV operations)
3. `concurrency.go` — `ConcurrencyManager` (KV operations)
4. `ratelimit.go` — `RateLimiter` (KV operations)
5. `sleeptimer.go`, `correlator.go`, `approval.go`

**Phase 2 — Engine aggregates:**
6. `orchestrator.go` — replace `js` field with `jsNew`, update constructor
7. `workflow_actor.go`, `actor_orch.go`

**Phase 3 — Worker:**
8. `worker.go` — subscriptions, message handling
9. `context.go` — task context (ack/nak/publish)

**Phase 4 — Cleanup:**
10. Remove legacy `js` field from Orchestrator and Worker
11. Update `internal/natsutil/` setup functions to return new interface
12. Update all test helpers

### 3. KV Migration

The new API uses `jetstream.KeyValue` instead of `nats.KeyValue`. The
interface is similar but not identical:

```go
// Legacy
kv, err := js.KeyValue("bucket_name")
entry, err := kv.Get("key")

// New
kv, err := jsNew.KeyValueStore(ctx, "bucket_name")  // note: ctx required
entry, err := kv.Get(ctx, "key")
```

Every KV operation gains a `context.Context` parameter. Functions that
currently don't take a context need one added.

### 4. Subscription Migration

The new API uses `jetstream.Consumer` instead of `*nats.Subscription`:

```go
// Legacy
sub, err := js.Subscribe("subject", handler, nats.AckExplicit())

// New
cons, err := jsNew.CreateOrUpdateConsumer(ctx, "STREAM", jetstream.ConsumerConfig{...})
cc, err := cons.Consume(func(msg jetstream.Msg) { ... })
```

The handler receives `jetstream.Msg` instead of `*nats.Msg`. The message
interface is similar (`Data()`, `Ack()`, `Nak()`, `Headers()`) but method
names may differ. The worker's `handleMessage` needs adaptation.

### 5. Bounds

- Max files changed per phase: ~10
- Each phase produces a working, tested state
- No phase changes public API (Worker, TaskContext interfaces unchanged)

### 6. Risks

- **KV API differences:** The new `KeyValue` may have subtle behavioral
  differences. Mitigated by running the full test suite after each file.
- **Context threading:** Adding `context.Context` to KV operations means
  every function in the call chain needs a context. Use `context.Background()`
  initially, then thread real contexts in a follow-up.
- **Test churn:** Every test that constructs a JetStreamContext needs updating.
  This is mechanical but voluminous (~40+ test functions).
