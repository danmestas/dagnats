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

## Concurrency Limits

**Two scopes:**
- Per-workflow: `WorkflowDef.Concurrency.MaxRuns` — cap concurrent runs of same workflow
- Per-step: not yet implemented (field exists)

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

## Compensation/Saga (Declared, Not Implemented)

- Fields: `StepDef.OnFailure`, `StepDef.Compensate` (step ID references)
- Validation: referenced step IDs must exist in workflow
- Intended flow: permanent failure → run OnFailure step → Compensate in reverse order

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
