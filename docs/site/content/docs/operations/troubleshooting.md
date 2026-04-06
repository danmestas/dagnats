---
title: Troubleshooting
weight: 5
---

Common issues and how to diagnose them using built-in tools.

## Runs Stuck in Pending

A workflow run stays in `pending` status when the orchestrator has not yet processed its `workflow.started` event.

**Diagnosis:**

```bash
dagnats run inspect <run-id>
```

Check the run status and step states. If the run shows `pending` with no steps queued:

1. **Engine not running**: verify `dagnats serve` is up and the orchestrator started. Check logs for `ActorOrchestrator.Start` errors.
2. **Consumer lag**: the orchestrator's consumer on `WORKFLOW_HISTORY` may be behind. Check with `nats consumer info WORKFLOW_HISTORY` and look at `Num Pending`.
3. **NATS connectivity**: the engine may have lost its connection. Check `/health` -- a 503 response means NATS is disconnected.

**Resolution:** Restart `dagnats serve`. The orchestrator replays the `WORKFLOW_HISTORY` stream on startup, recovering all in-flight runs.

## Tasks Not Being Picked Up

Steps are stuck in `queued` status -- the engine published tasks but no worker is processing them.

**Diagnosis:**

```bash
dagnats workers list
```

If no workers appear, none are connected. If workers appear but tasks are stuck:

1. **Task type mismatch**: the worker must handle the exact task type defined in the step. Check `worker.Handle("task-type", ...)` matches the step's `TaskType` field.
2. **Consumer not created**: the worker creates a pull consumer on `TASK_QUEUES` filtered to `task.{taskType}`. Check `nats consumer ls TASK_QUEUES` for the expected consumer.
3. **Rate limit exhausted**: if the step has a rate limit configured, tasks queue until tokens refill. Check the `rate_limits` KV bucket.
4. **Concurrency limit reached**: per-task-type or per-run concurrency limits can hold tasks. Check `concurrency_tasks` KV bucket.

**Resolution:** Start a worker that handles the correct task type. If rate or concurrency limits are the cause, wait for them to clear or adjust the limits.

## Task Timeouts

A task is redelivered after `AckWait` expires, incrementing the retry counter.

**Common causes:**

- **Handler too slow**: the worker did not call `Complete()`, `Fail()`, or `Heartbeat()` before `AckWait` expired. Use `ctx.Heartbeat()` for long-running tasks -- it calls NATS `InProgress()` to extend the ack deadline.
- **Worker crashed mid-task**: NATS redelivers after `AckWait`. The task retries on another worker (or the same one after restart). If the handler uses [checkpoints](/docs/coordination/checkpoints), it resumes from the last checkpoint.
- **Network partition**: the worker lost its NATS connection. NATS redelivers to another consumer.

**Diagnosis:**

```bash
dagnats run inspect <run-id>
```

Check the step's `Attempts` count. If it is climbing toward `MaxAttempts`, the handler is consistently failing or timing out.

## Dead Letter Queue Growing

The `DEAD_LETTERS` stream receives tasks that exhausted all retry attempts.

**Diagnosis:**

```bash
dagnats dlq list
dagnats dlq inspect <message-id>
```

Each dead letter entry includes the original task payload, the error message, and the run/step IDs. Look at the error message for the root cause.

**Common causes:**

- **Permanent failure**: the handler called `FailPermanent(err)` to skip retries. This is intentional -- fix the underlying issue and use `dagnats run retry` to re-run.
- **Retry exhaustion**: `MaxAttempts` reached. Either increase retries in the retry policy or fix the handler.
- **Invalid input**: the task received data the handler cannot process. Check the workflow definition's input wiring.

**Resolution:**

```bash
# Retry a single failed run (fresh start with current workflow def)
dagnats run retry <run-id> --mode=rerun

# Retry all failed runs in a time window
dagnats run retry-all --after=2026-04-01 --before=2026-04-02 --mode=rerun

# Replay from DLQ (resume at failed step, requires < 30 days)
dagnats run retry <run-id> --mode=replay
```

## Worker Disconnects

Workers disappear from `dagnats workers list` and in-flight tasks eventually timeout.

**Diagnosis:**

Check worker logs for NATS connection errors. Common causes:

1. **NATS server unreachable**: network issue or NATS server restart. Workers should auto-reconnect if using the default NATS client options.
2. **Slow consumer**: the worker's subscription fell behind and NATS disconnected it. This happens with high message volume and slow processing. Increase the worker's pending message limit.
3. **Resource exhaustion**: the worker process ran out of memory or file descriptors.

The `workers` KV bucket has a 60-second TTL. If a worker misses two consecutive heartbeats (30s interval), its entry expires and it disappears from the list. The worker itself continues functioning -- the directory is observability-only.

**Resolution:** Fix the underlying connectivity or resource issue. In-flight tasks will timeout and be redelivered by NATS to healthy workers.

## Run Inspection

The `dagnats run inspect` command is the primary diagnostic tool:

```bash
dagnats run inspect <run-id>
```

It shows:
- Run status (pending, running, completed, failed, cancelled)
- Each step's status, attempts, output, and error
- Workflow definition name and version
- Timing information

For deeper investigation, query the event history directly:

```bash
nats stream get WORKFLOW_HISTORY --subject "history.<run-id>" --last
```

This shows the raw events on the `WORKFLOW_HISTORY` stream for that run, in order. Since DagNats is event-sourced, this stream contains the complete, authoritative history of every state change.

## Telemetry Export Failures

If spans are not appearing in your observability backend:

1. **Check `OTEL_EXPORTER_OTLP_ENDPOINT`**: must be a valid OTLP/HTTP base URL (e.g., `http://localhost:4318`)
2. **Check connectivity**: the telemetry exporter must reach `{endpoint}/v1/traces`
3. **Check TELEMETRY stream**: spans are always written to the NATS `TELEMETRY` stream regardless of export. If the stream has messages but exports fail, the issue is between DagNats and your backend.

Export failures never affect workflow execution. Telemetry is best-effort.
