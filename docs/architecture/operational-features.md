# Operational Features

## Retry Policies

**Resolution order:** step override → workflow default → legacy `Retries` field → nil (no retry)

**Strategies:**
- `Fixed` — constant delay between attempts
- `Linear` — delay increases linearly (initialDelay * attempt)
- `Exponential` — delay doubles each attempt (initialDelay * 2^attempt), capped at MaxDelay

**Fields:**
- `WorkflowDef.DefaultRetry *RetryPolicy` — workflow-level default
- `StepDef.Retry *RetryPolicy` — per-step override
- `StepDef.Retries int` — legacy field, synthesized to fixed 5s delay policy

**Implementation:** Pure `dag/retry.go` — `ResolveRetryPolicy()` + `CalculateDelay()`. Engine calls `NakWithDelay()` with calculated delay.

## Workflow Cancel

- Event: `EventWorkflowCancelled` → orchestrator sets `RunStatusCancelled`
- Per-step: running steps set to `StepStatusCancelled`, concurrency counters decremented
- Worker notification: best-effort (current task finishes, AckWait expires)
- Agent loops: check cancellation before Continue()

**Bulk cancellation:** `POST /runs/cancel` cancels multiple runs in one call, filtered by workflow ID, status (`running`/`pending`/`all`), and time range. Supports `--dry-run` to preview. Capped at 1000 runs per call — sequential publish (~1-2ms each) avoids thundering herd on the orchestrator. Synchronous (completes in 1-2s), no async job tracking needed. CLI: `dagnats run cancel-all --workflow=<name> [--status=running] [--after=T] [--before=T] [--dry-run]`. Cancelled runs release concurrency slots and cascade to non-detached sub-workflows. Cancellation does NOT trigger compensation (intentional, not failure).

## Non-Retriable Errors

Workers signal permanent failures via `FailPermanent(err)` or explicit retry delays via `FailRetryAfter(err, duration)`. Wire protocol: `StepFailedPayload` with `failure_type` discriminator (`retriable`, `non_retriable`, `retry_after`). Orchestrator parses and branches: non-retriable skips retries → on-failure/compensation/fail. Retry-after schedules exact delay via `SLEEP_TIMERS` (`TimerActionRetryAfter`), bypassing backoff. Existing `Fail(err)` defaults to retriable. Backward compat: old raw-string payloads treated as retriable. Bounds: retry-after clamped to [100ms, 1h]. HTTP bridge: `failure_type` + `retry_after_ms` on fail action.

## Bulk Operations

**Bulk run** (`POST /runs/bulk`): Start up to 1000 runs of same workflow. Def loaded once. Atomic validation (first bad input fails batch). CLI: `dagnats run bulk --workflow=X '{"a":1}' '{"a":2}'` or `--from-file=inputs.jsonl`.

**Bulk retry** (`POST /runs/retry`): Retry up to 1000 failed runs. Mode `rerun` = fresh start with original input. Mode `replay` = re-publish DLQ task messages. CLI: `dagnats run retry-all --workflow=X --mode=rerun`.

**Bulk cancel** (already documented above): `POST /runs/cancel`. All three share: 1000-run cap, sequential publish, dry-run, time range filters.

## Singleton Workflows

At most one active run per key. Two conflict modes:
- **Skip:** discard the new run silently
- **Cancel:** cancel the existing run, start the new one ("last write wins")

**Builder API:**
- `WithSingleton(mode)` — global singleton (one run of this workflow at a time)
- `WithSingletonKey(mode, dotPath)` — per-entity (e.g., one sync per user)

**Mechanism:** `singleton_locks` KV bucket. CAS `Create` on `{workflow}` or `{workflow}.{keyValue}`. If key exists: load existing lock, verify run is active via KV snapshot. Stale locks (terminal runs) reclaimed via CAS `Update`. Lock released on every terminal state, guarded by `lock.RunID == run.RunID` to prevent cancel-mode from deleting the replacement's lock.

**CLI:**
- `dagnats singleton list [--workflow=X]` — shows active locks
- `dagnats singleton release <key>` — admin escape hatch for stuck locks

**Bounds:** Max key length 256 chars. CAS retry 3 attempts. Key extraction failure falls back to global key with warning.

## Concurrency Limits

**Three scopes:**
- Per-workflow: `WorkflowDef.Concurrency.MaxRuns` — cap concurrent runs of same workflow
- Per-task-type: `StepDef.MaxTaskConcurrency` — global cap across all runs for a task type
- Per-run: `ConcurrencyLimit.MaxSteps` — cap concurrent steps within a single run

**Implementation:** KV-based counters with optimistic locking (CAS loops, bounded at 10 retries)
- Acquire on workflow start, release on complete/fail/cancel
- Excess runs queued as `RunStatusPending`
- On release: auto-start next pending run

## Trigger System (`trigger/` package)

**Three types (mutually exclusive per trigger):**

| Type | Config | Mechanism |
|------|--------|-----------|
| Cron | Expression + timezone + backfill flag | Scheduler ticks every 30s, dedup via Nats-Msg-Id |
| Subject | NATS subject pattern | Subscribe, wrap message in envelope, publish workflow.started |
| Webhook | HTTP path + optional HMAC-SHA256 secret | 202 Accepted, 1MB body limit, async publish |

**All triggers produce `TriggerEnvelope`** as workflow input: trigger type, source, timestamp, data.

**Cron parser:** In-house ~100 LOC. 5-field standard (min hour dom month dow). Supports `*`, `*/N`, `N-M`, comma lists. `NextN(ref, n)` scans minute-by-minute to compute upcoming fire times.

**Cron validation:** `dagnats trigger test <expr> [--tz=TZ] [--count=N]` validates offline and previews next fire times.

**Live reload:** KV watcher detects trigger add/update/delete without restart.

**Debounce:** Subject triggers support `DebounceConfig` — collapses rapid events into a single fire. Uses `debounce_state` KV bucket. Mutual exclusion with batching.

**Batching:** Collects multiple trigger events into a single workflow run. Controlled by `MaxSize` (fire when N events accumulate, max 1000) and `Timeout` (fire after duration, max 5min) — whichever comes first. Optional `Key` (dot-path) groups events into per-entity batches. Workflow receives a JSON array in `TriggerEnvelope.Data`. Uses `batch_state` KV bucket with CAS updates. Timer via `SLEEP_TIMERS` with `batch_fire` action. Mutual exclusion with debounce and singleton. Max payload 10 MiB (splits into sub-batches if exceeded).

**Bounds:** Max 500 active triggers. Backfill capped at 100 per trigger on startup.

## Dead-Letter Queue

- Stream: `DEAD_LETTERS` (subjects: `dead.{taskName}.{runID}.{stepID}`)
- Payload: JSON with run_id, step_id, task, error, attempts
- 30-day retention
- CLI: `dagnats dlq list` (50 max), `dagnats dlq replay <seq>`

## Run Output

- `dagnats run output <run-id>` prints final output of terminal steps (steps with no dependents)
- Single terminal: raw output. Multiple terminals: `--- stepID ---` separator per step.
- Only works on completed runs; non-completed prints status warning.
- `dagnats run start <wf> --output` watches until completion then prints output (combines start + watch + output into one command). Non-completed runs print status to stderr.

## CLI Connection Handling

- `connectService()` recovers from `api.NewService` panics on missing NATS resources
- Prints friendly error with hint (`run 'dagnats serve'`) instead of raw stack trace
- All server-dependent CLI commands benefit (status, workflow list, run list, dlq, etc.)

## Signal API

- KV-based pull model (bucket: `signals`, key: `{runID}.{name}`)
- `WaitForSignal(name, timeout)` — KV watcher blocks until key appears (max 1 hour)
- `SendSignal(runID, name, data)` — write to KV
- REST: `POST /runs/{id}/signal/{name}`

## Worker Groups

- Field: `StepDef.WorkerGroup string`
- Subject routing: `task.{taskType}.{group}.{runID}`
- Worker option: `WithGroups(groups...)` subscribes to group-specific subjects

**Sticky workers:** Bind a workflow run to a specific worker after the first step. Subsequent steps route to the same worker for cache locality, local state, or GPU affinity. Two strategies:
- `soft` — prefer sticky worker; 5s fallback timer re-publishes to any worker if unclaimed
- `hard` — require sticky worker; queues until worker returns (needs workflow timeout)

Binding stored in `sticky_bindings` KV (TTL: workflow timeout + 1h). Workers subscribe to `task.{type}.{workerID}.>` in addition to normal subjects. Engine writes binding on first `step.completed` (from `WorkerID` in event payload) and routes via `publishStickyTask` helper. Workers don't know about sticky — engine owns all routing (information hiding). Incompatible with per-step `WorkerGroup` overrides.

## OnFailure Recovery

- `StepDef.OnFailure` — step ID to run when this step permanently fails
- OnFailure handler receives error context as input
- If handler succeeds: original step transitions to `StepStatusRecovered`, dependents skipped
- If handler fails: workflow fails normally
- OnFailure targets cannot have their own `DependsOn` (receive error context directly)
- `AuxSteps` map precomputed at `Build()` — auxiliary steps don't block `IsComplete()`

## Saga Compensation

- `StepDef.Compensate` — step ID to run for rollback
- Triggered on permanent step failure (after OnFailure, if present)
- Runs in reverse topological order via temporary `DependsOn` chain
- `RunStatusCompensated` / `RunStatusCompensateFailed` for outcome tracking
- Protocol events: `compensate.started`, `compensate.step.completed`, `compensate.failed`, `compensate.completed`

## Scheduled Runs

- API-level feature (not a trigger type): `ScheduleRun(ctx, workflow, input, runAt)`
- Stored in `scheduled_runs` KV bucket with `RunAt` timestamp
- Timer: `SLEEP_TIMERS` stream with `NakWithDelay(time.Until(runAt))` — fires on redeliver
- `api/timer.go` consumes `scheduled.>` subjects, publishes `workflow.started` on fire
- CLI: `dagnats run start <wf> --at "2026-04-05T10:00:00Z"`
- REST: `POST /runs/scheduled`, `GET /runs/scheduled/{id}`, `DELETE /runs/scheduled/{id}`
- Max 365 days ahead. Cancelable before fire.

## Idempotency

Prevents duplicate workflow runs by keying on a value extracted from input. `WorkflowDef.IdempotencyKey` is a dot-path expression (e.g., `"data.payment_id"`) evaluated at `StartRun` time.

**Mechanism:** SHA-256 hash of `{workflowName}.{keyValue}` stored in `idempotency_keys` KV bucket (default 24h TTL). If key exists, return existing run ID (201→200 status code change signals idempotent return). If key missing, create run then store mapping. Race condition defined out of existence: KV `Create` (not `Put`) is atomic — loser retries and returns winner's run ID.

**Soft failure:** Missing key path in input → run starts without idempotency, warning logged. KV unavailable → run starts normally. Completed run within TTL → returns existing ID (correct — operation already done).

## Input/Output Schemas

- Fields: `WorkflowDef.InputSchema`, `WorkflowDef.OutputSchema` (JSON Schema subset)
- In-house validator ~100 LOC: supports `type`, `required`, `properties` (recursive)
- Input validated on start (reject invalid). Output logged as warning (don't fail).
- Nil schema passes all inputs.

## Workflow Timeouts

- Field: `WorkflowDef.Timeout time.Duration`
- `WorkflowRun.Deadline *time.Time` set on start
- Check: piggybacks on event processing (no background timer)
- If `now > deadline` → cancel workflow

## Checkpointing

- `Checkpoint(state)` → write to `checkpoints` KV at `{runID}.{stepID}`
- `LoadCheckpoint()` → read from KV, returns nil on first run
- Use case: resume long-running agent work after restart

## Workflow JSON Schema

- `docs/workflow-schema.json` — JSON Schema (draft-07) for workflow definition files
- Enables IDE autocomplete and validation when editing `.json` workflow files
- Add `"$schema": "./path/to/workflow-schema.json"` to workflow files for IDE support
- Matches `dag/types.go` and validation rules in `docs/workflow-schema.md`
- Durations accept both Go string format (`"5m"`) and nanosecond numbers

## CI Pipeline

- `.github/workflows/ci.yml` — runs on push to main and PRs
- Steps: `gofmt` format check, `go vet`, `staticcheck`, `go test`
- Go version pinned via `go-version-file: go.mod`
