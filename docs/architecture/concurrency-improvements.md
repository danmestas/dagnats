# Concurrency Improvements

## Design Decision: errgroup for I/O Fan-Out, Context for Shutdown

Parallelize I/O-bound fan-out operations using `golang.org/x/sync/errgroup`. Migrate scheduler shutdown from `stopChan` to `context.Context`. Fix `time.AfterFunc` context leak. No changes to the actor runtime — actors remain single-threaded with mailbox channels.

## Parallel KV Helper

`natsutil.ParallelGet(kv, keys, limit)` fetches multiple KV entries concurrently with a configurable concurrency limit (`DefaultParallelism = 16`). Keys deleted between `Keys()` and `Get()` are silently skipped. Used in 5+ locations — lives in `natsutil` (which already owns NATS operational concerns).

## Parallelized Operations

| Location | Before | After |
|----------|--------|-------|
| `orchestrator.publishReadyTasks` | Sequential publish loop | `errgroup` fan-out (independent steps) |
| `SnapshotStore.ListAll` | Sequential KV Get loop | `natsutil.ParallelGet` |
| `api.listWorkflowsInner` | Sequential KV Get loop | `natsutil.ParallelGet` |
| `api.listTriggersInner` | Sequential KV Get loop | `natsutil.ParallelGet` |
| `trigger.loadAllTriggers` | Sequential KV Get loop | `natsutil.ParallelGet` |
| `orchestrator.findOldestPendingRun` | Sequential KV Get loop | `natsutil.ParallelGet` |
| `Scheduler.Tick` | Sequential trigger evaluation | `errgroup` (independent triggers) |
| `Scheduler.Backfill` | Sequential backfill | `errgroup` (independent triggers) |

## Thread Safety Reasoning

- `dag.ResolveInput` is pure (reads `run.Steps` map, no mutation) — safe to call before goroutine
- NATS `PublishMsg` is thread-safe
- Each `ParallelGet` goroutine writes to a distinct index — no mutex needed
- Trigger `shouldFire` is read-only. `fireWorkflow` publishes by unique key per trigger

## Scheduler Shutdown: stopChan -> context.Context

`Scheduler.Start(ctx, interval)` replaces `Start(stopChan, interval)`. `TriggerService` holds `ctx`/`cancel` instead of `stopChan`. Aligns with observability layer shutdown pattern, enables deadline propagation.

## time.AfterFunc Context Leak Fix

The `time.AfterFunc` in `handleStepContinue` captures a context that may go stale. Replaced with a context-aware goroutine:

```go
go func() {
    timer := time.NewTimer(delay)
    defer timer.Stop()
    select {
    case <-ctx.Done():
        return
    case <-timer.C:
        o.publishIterationTask(ctx, runID, stepDef, input, iter)
    }
}()
```

## API Listing Timeout Fix

Extract `fetchMessages(sub, limit, deadline)` helper that owns timeout algebra. Both `listDeadLettersInner` and `listRunEventsInner` use a 10-second total deadline instead of unbounded per-message timeouts.

## Semantic Change: Tick Error Isolation

Sequential `Tick` returned on first error — if trigger 2 of 5 failed, triggers 3-5 were never evaluated. Concurrent `Tick` runs all triggers independently. `g.Wait()` returns the first error, but all triggers still attempt. One failing trigger no longer blocks the rest.

## What Is NOT Changed

- **Actor runtime** — single-threaded with mailbox channels (correct for actor semantics)
- **Worker system** — sequential per-subscription processing (intentional for task ordering)
- **Per-run mutex** — run-level serialization via `sync.Map` (prevents KV races)
- **ConcurrencyManager** — KV-based CAS (correct for distributed concurrency limits)
