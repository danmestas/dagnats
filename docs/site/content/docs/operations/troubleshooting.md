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
2. **Consumer lag**: the orchestrator's durable consumer on `WORKFLOW_HISTORY` may be behind. Check with `nats consumer info WORKFLOW_HISTORY orchestrator` and look at `Num Pending`.
3. **NATS connectivity**: the engine may have lost its connection. Check `/health` -- a 503 response means NATS is disconnected.

**Resolution:** Restart `dagnats serve`. The orchestrator keeps no state between events: its durable consumer continues from the last event it acknowledged, and run state is in the `workflow_runs` snapshots, so in-flight runs carry on.

## Tasks Not Being Picked Up

Steps are stuck in `queued` status -- the engine published tasks but no worker is processing them.

**Diagnosis:**

```bash
dagnats workers list
```

If no workers appear, none are connected. If workers appear but tasks are stuck:

1. **Task type mismatch**: the worker must handle the exact task type defined in the step. Check `worker.Handle("task-type", ...)` matches the step's `TaskType` field.
2. **Consumer not created**: the worker creates a pull consumer on `TASK_QUEUES` filtered to its task type. Check `nats consumer ls TASK_QUEUES` for the expected consumer.
3. **Rate limit exhausted**: if the step has a rate limit configured, tasks queue until tokens refill. Check the `rate_limits` KV bucket.
4. **Concurrency limit reached**: per-task-type or per-workflow concurrency limits can hold tasks. Check the `concurrency_tasks` and `concurrency_runs` KV buckets. A held task is retried through `SLEEP_TIMERS` every second.

**Resolution:** Start a worker that handles the correct task type. If rate or concurrency limits are the cause, wait for them to clear or adjust the limits.

## Task Timeouts

A step with a `Timeout` fails with a retriable `step timeout exceeded` error when its current attempt has run that long. The engine's watchdog timer does this, not NATS `AckWait`. See [Timeouts]({{< ref "/docs/reliability/timeouts" >}}).

**Common causes:**

- **Handler too slow**: the attempt ran past the step's `Timeout`. `ctx.Heartbeat()` keeps the claim on the task message (it calls NATS `InProgress()`), but it does not extend the step timeout.
- **Time spent paused**: time in `Pause()` counts toward the timeout, so a step that pauses repeatedly needs a `Timeout` that covers its whole run.
- **Worker crashed mid-task**: when the worker stops extending its claim, NATS redelivers the task after `AckWait` to another worker (or the same one after restart). The redelivery does not restart the step timeout. If the handler is an agent loop, the redelivered iteration starts from the last state committed by `Continue()`. If it calls `Journal()`, it also reuses the model responses and tool results the crashed worker recorded, and the engine fails the step with `restart_limit` if the worker crashes on five deliveries in a row. See [Durable Agents](/docs/ai-patterns/durable-agents). Run `dagnats journal check <run-id>` to audit what a journal step recorded.
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
dagnats dlq list --run=<run-id>
```

Each dead letter entry includes the original task body, the error message, and the run and step IDs. Look at the `ERROR` column for the root cause.

**Common causes:**

- **Permanent failure**: the handler called `FailPermanent(err)` to skip retries. This is intentional -- fix the underlying issue and use `dagnats run retry <run-id>` to start it again.
- **Retry exhaustion**: `MaxAttempts` reached. Either increase retries in the retry policy or fix the handler.
- **Invalid input**: the task received data the handler cannot process. Check the workflow definition's input wiring.

**Resolution:**

```bash
# Re-run a single run (fresh start; the run's input is reused unless you pass new input)
dagnats run retry <run-id>

# Retry all failed runs of a workflow in a time window
dagnats run retry-all --workflow=my-workflow --mode=rerun \
  --after=2026-04-01T00:00:00Z --before=2026-04-02T00:00:00Z

# Replay from the DLQ (resume at the failed step, needs the entry to be under 30 days old)
dagnats run retry-all --workflow=my-workflow --mode=replay

# Replay one dead letter by its sequence number
dagnats dlq replay <sequence>
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

## Cluster Problems

For clusters, start with `GET /health/cluster` on each node. See [Clustering]({{< ref "/docs/operations/clustering" >}}).

- **`cluster quorum did not form` at startup**: the node waited 60 seconds and no JetStream meta leader answered. Check that every node is running, that `nats_cluster_routes` lists the other nodes on the route port, that `nats_cluster_name` and `nats_cluster_auth_token` match everywhere, and that the route port is reachable between nodes.
- **`/health/cluster` returns 503**: a replicated stream or bucket lost quorum. The names are under `jetstream.unavailable`. Bring the missing node back.
- **`/health/cluster` returns 200 with `jetstream.degraded`**: only single-replica assets are unreachable. The cluster still has quorum.
- **A node left an old replica behind after a restart**: nodes before 0.0.27 named themselves from the process ID, so each restart joined as a new JetStream peer. Set `nats_server_name`, or upgrade; the name then stays the same across restarts.

## Telemetry Export Failures

If spans are not appearing in your observability backend:

1. **Check `OTEL_EXPORTER_OTLP_ENDPOINT`**: must be a valid OTLP/HTTP base URL (e.g., `http://localhost:4318`)
2. **Check connectivity**: the telemetry exporter must reach `{endpoint}/v1/traces`
3. **Check TELEMETRY stream**: spans are always written to the NATS `TELEMETRY` stream regardless of export. If the stream has messages but exports fail, the issue is between DagNats and your backend.

Export failures never affect workflow execution. Telemetry is best-effort.
