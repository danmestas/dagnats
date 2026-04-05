# Singleton

**Status:** Design
**Date:** 2026-04-04
**Depends on:** Nothing (builds on existing concurrency infrastructure)

## Problem

Some workflows must never have more than one instance running at a time for a given
entity. Examples: one deploy per environment, one sync per user. The existing
`Concurrency.MaxRuns` limits parallelism globally but cannot scope per-entity, and
offers no control over what happens to the duplicate.

## Design

### 1. Concept

At most one active run per key. Two conflict modes:

- **Skip:** discard the new run silently.
- **Cancel:** cancel the existing run, start the new one ("last write wins").

### 2. Type Changes

```go
type SingletonMode int

const (
    SingletonModeSkip   SingletonMode = iota
    SingletonModeCancel
)

type SingletonConfig struct {
    Mode SingletonMode `json:"mode"`
    Key  string        `json:"key,omitempty"` // dot-path; empty = global
}

type WorkflowDef struct {
    // ... existing fields ...
    Singleton *SingletonConfig `json:"singleton,omitempty"`
}
```

### 3. KV Bucket

**`singleton_locks`** -- no TTL, explicitly managed.

Key: `{workflowName}` (global) or `{workflowName}.{keyValue}` (per-entity).

Value: `{run_id, started_at}`.

### 4. How It Works

In `handleWorkflowStarted`, the singleton check is part of the `admitRun` pipeline
(along with priority and concurrency):

```go
func (o *Orchestrator) admitRun(
    wfDef dag.WorkflowDef, run dag.WorkflowRun,
    input json.RawMessage,
) (admissionResult, error) {
    // 1. Singleton check
    // 2. Priority resolution
    // 3. Concurrency check
}
```

**Singleton check flow:**

1. Try CAS `Create` on `singleton_locks.{key}`. If success: proceed.
2. If key exists: load existing lock, verify run is actually active via KV snapshot.
3. If stale (run already terminal): reclaim lock via CAS `Update`.
4. If active + mode=skip: discard new run, return `admissionSkip`.
5. If active + mode=cancel: update lock to new run, publish cancel for existing.

### 5. Lock Release

On every terminal state (`completeWorkflow`, `failWorkflow`, `handleWorkflowCancelled`):
delete lock only if `lock.RunID == run.RunID` (prevents cancel-mode from deleting
the replacement's lock).

### 6. Builder API

```go
// Global: one deploy at a time
wf := dag.NewWorkflow("deploy").
    WithSingleton(dag.SingletonModeCancel).Build()

// Per-user: one sync per user
wf := dag.NewWorkflow("user-sync").
    WithSingletonKey(dag.SingletonModeSkip, "data.user_id").Build()
```

### 7. Validation

- `Singleton.Key` must be valid dot-path if non-empty.
- `Singleton.Mode` must be Skip or Cancel.
- Compatible with concurrency, debounce, throttling, priority, CancelOn.
- Incompatible with batching (batching assumes 1:1 event:run).

### 8. CLI

```bash
dagnats singleton list [--workflow=deploy]
dagnats singleton release deploy.production  # admin escape hatch
```

### 9. Bounds

- Max key length: 256 characters.
- CAS retry on lock acquisition: 3 attempts.
- Stale lock detection: loads run from `workflow_runs` KV.

### 10. Observability

- Metric: `workflow.singleton.skip` -- skipped duplicates.
- Metric: `workflow.singleton.cancel` -- cancelled-and-replaced.
- Metric: `workflow.singleton.stale_lock` -- stale locks cleaned.
- Span attribute: `singleton_key`.

### 11. Edge Cases

- **Stale lock:** Detected by loading run status. If terminal, reclaim via CAS.
- **Race (cancel mode):** CAS ensures one winner. Loser retries once.
- **Singleton + concurrency:** Singleton checks first. Run can be singleton-allowed
  but concurrency-queued (Pending). Lock held while pending.
- **Cancel during step execution:** Existing run cancels between steps. New run
  starts immediately (doesn't wait for cancellation).
- **Key extraction fails:** Fall back to global key. Warning logged.
