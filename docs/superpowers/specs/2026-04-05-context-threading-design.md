# Context Threading

**Status:** Design
**Date:** 2026-04-05
**Depends on:** JetStream API migration (completed in PR #59)

## Problem

The JetStream API migration (PR #59) added `context.Background()` to every
JetStream and KV call — 217 instances across 58 files. This was intentional
as a migration shortcut, but it creates three real problems:

1. **No cancellation propagation.** When the engine shuts down or a request
   times out, in-flight JetStream operations continue to completion instead
   of being cancelled. On shutdown, `Stop()` returns but KV writes and
   publishes may still be in-flight for up to the NATS timeout.

2. **No timeout scoping.** HTTP bridge requests have no per-request deadline
   on their JetStream operations. A slow KV read blocks the HTTP handler
   indefinitely rather than failing with a timeout.

3. **Broken trace context.** The orchestrator creates a traced `ctx` from
   message headers (`handleEventJS` → `extractTraceCtxJS` → `dispatchEvent`)
   but downstream functions ignore it and use `context.Background()` for
   JetStream calls. Spans from KV reads and publishes are disconnected from
   the parent trace.

## Where Contexts Originate

Four entry points create the contexts that should flow through the system:

| Entry Point | Context Source | Package |
|-------------|---------------|---------|
| `handleEventJS` | Trace context from message headers | `internal/engine/` |
| `handleMessage` | Trace context from message headers | `worker/` |
| HTTP handlers | `r.Context()` from net/http | `bridge/`, `internal/api/rest.go` |
| CLI commands | `context.Background()` (legitimate — no parent) | `cli/` |

CLI commands are the one place where `context.Background()` is correct — there
is no parent context. They may optionally add signal handling:
`signal.NotifyContext(context.Background(), os.Interrupt)`.

## Design

### Threading Strategy

Thread `ctx context.Context` as the first parameter through every function
that performs I/O (JetStream publish, KV read/write, stream operations).
This is the standard Go convention.

**Functions that need ctx added to their signature:**

```
internal/engine/: enqueueReadySteps, publishAtomicBatches,
  collectReadyMessages (no I/O but passes to publishAtomicBatches),
  publishWorkflowEvent, publishIterationTask, saveSnapshot,
  AcquireRun, ReleaseRun, AcquireTask, ReleaseTask,
  all Orchestrator methods that do I/O

worker/: Complete, Fail, FailPermanent, Continue, PutStream,
  Checkpoint, LoadCheckpoint, Pause, WaitForSignal, SendSignal,
  Heartbeat (all on taskContext — but TaskContext interface is public)

bridge/: fetchForType, processPolledMsg, publishEvent,
  writeCheckpoint, resolveSendSignal, resolveWaitSignal

internal/api/: all Service methods (StartRun, GetRun, ListRuns, etc.)

internal/trigger/: fire methods, KV operations
```

### Public Interface Impact

**`TaskContext` interface** is public. Adding `ctx` to `Complete(output)` →
`Complete(ctx, output)` is a breaking API change. Two options:

**Option A: Break the API.** `Complete(ctx context.Context, output []byte)`.
Callers must update. Clear, Go-idiomatic. Semver major bump.

**Option B: Use stored context.** The `taskContext` already receives a `ctx`
in its constructor (from the trace context). Use `tc.ctx` for all internal
JetStream operations. The public `Complete(output)` signature is unchanged —
the context is implicit from the message handler's trace context.

**Recommendation: Option B.** The stored `tc.ctx` already has the right trace
parent and the right lifecycle (scoped to the message handler). There's no
reason for the handler author to provide a different context — they don't
control the JetStream connection or the trace span. This is **pulling
complexity downward** (Ousterhout): the module uses the right context so the
caller doesn't have to.

### `api.Service` Methods

All Service methods take `ctx context.Context` as first parameter already:

```go
func (s *Service) StartRun(
    ctx context.Context, workflowName string, input []byte,
) (string, error)
```

But internally they call `context.Background()` for JetStream operations.
Fix: replace `context.Background()` with the passed `ctx` in all internal
calls. No signature change needed.

### Engine Internal Functions

Package-level functions like `enqueueReadySteps`, `publishAtomicBatches`,
`publishWorkflowEvent` need `ctx` added as first parameter. These are
internal (package `engine`) — no public API impact.

The `Orchestrator.dispatchEvent` already takes `ctx` and passes it to
most methods. The fix is threading it the rest of the way through methods
that currently create their own `context.Background()`.

### Bridge HTTP Handlers

Bridge handlers receive `http.Request` which has `r.Context()`. Thread
`r.Context()` through all bridge operations. The HTTP server cancels this
context when the client disconnects — JetStream operations cancel with it.

### KV Operations

Every `kv.Get(context.Background(), key)` becomes `kv.Get(ctx, key)`.
Mechanical replacement once `ctx` is available in the function.

### Setup Functions

`natsutil.SetupStreams`, `SetupKVBuckets`, `SetupAll` — these run at startup
and have no parent context. `context.Background()` is correct here. Add a
bounded timeout: `context.WithTimeout(context.Background(), 30*time.Second)`.

## Migration Order

Bottom-up, same as the JetStream API migration:

**Phase 1 — Engine internals** (biggest impact, most `context.Background()`):
1. Thread `ctx` through all `Orchestrator` methods
2. Thread `ctx` through `SnapshotStore`, `ConcurrencyManager`, `RateLimiter`
3. Thread `ctx` through `SleepTimer`, `Correlator`, `Approval`, `Sticky`
4. Thread `ctx` through `task_publish.go` functions

**Phase 2 — Worker** (no public API change):
5. Use `tc.ctx` in all `taskContext` methods instead of `context.Background()`
6. Thread `ctx` in `Directory` operations

**Phase 3 — API service** (signatures already have ctx):
7. Replace `context.Background()` with the passed `ctx` in all Service methods

**Phase 4 — Bridge** (thread `r.Context()`):
8. Thread `r.Context()` through bridge handlers and helper functions

**Phase 5 — Triggers + Observe**:
9. Thread `ctx` through trigger fire/KV operations
10. Thread `ctx` through telemetry publishing

**Phase 6 — Setup functions**:
11. Add `context.WithTimeout(context.Background(), 30s)` to setup functions

## Scope

- ~217 `context.Background()` replacements
- ~58 files
- ~40-50 function signatures gain `ctx` parameter (internal only)
- 0 public API changes (Option B for TaskContext)
- Tests: `context.Background()` in tests is correct — they have no parent

## Bounds

- Each phase produces a compiling, passing state
- No phase changes public API
- Setup functions keep `context.Background()` with timeout
- CLI keeps `context.Background()` (may add signal handling later)

## Observability

After threading:
- Trace spans from KV reads and publishes connect to parent event span
- Bridge request cancellation propagates to all JetStream operations
- Shutdown cancellation propagates through the engine
