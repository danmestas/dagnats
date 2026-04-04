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
