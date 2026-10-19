---
title: Dead Letter Queue
weight: 4
---

The **dead letter queue** (DLQ) captures task messages that have exhausted all retries, providing a 30-day window for inspection and replay. The stream also has a byte ceiling of 5% of `max_store_bytes`, and the oldest entries go first if it fills.

## DEAD_LETTERS Stream

When a step fails permanently -- either by exceeding its retry limit or by calling `FailPermanent()` -- the engine publishes the failed task message to the `DEAD_LETTERS` stream on `dead.{taskType}.{runID}.{stepID}`. Messages are retained for **30 days**. Publishing is deduplicated for 24 hours on the run, step and attempt count, so a redelivered failure event does not create a second entry.

The DLQ keeps the original task body, and carries the run ID, step ID, error and delivery count as headers. This gives operators everything needed to diagnose the failure and decide whether to replay.

## Inspecting the DLQ

List dead-lettered tasks with the CLI:

```bash
# List DLQ entries (up to 500 by default)
dagnats dlq list

# Entries for one run
dagnats dlq list --run=<run-id>

# Everything, or a specific count
dagnats dlq list --all
dagnats dlq list --limit 50

# JSON output for scripting
dagnats dlq list --json
```

The columns are `SEQ`, `SUBJECT`, `RUN_ID`, `STEP_ID`, `TASK`, `BODY`, `DELIVERY`, `CONSUMER`, `ERROR` and `TIMESTAMP`. The `SEQ` is the stream sequence you pass to `dlq replay`. If the stream holds more entries than shown, a footer on stderr says so.

## Replaying Failed Tasks

Replay republishes a dead-lettered task message to its original task subject on the `TASK_QUEUES` stream, allowing workers to pick it up for another attempt:

```bash
# Replay one entry by its stream sequence
dagnats dlq replay <sequence>

# Replay every entry for a run
dagnats dlq replay --run=<run-id>

# Poll the DLQ and replay automatically (default every 30s)
dagnats dlq watch --interval=30s --max-replays=3

# Bulk retry all failed runs of a workflow
dagnats run retry-all --workflow=code-review-pipeline --mode=replay
```

An entry written before the DLQ kept task bodies cannot be replayed; `dlq replay` reports that it has no body.

### Replay vs. Rerun

`run retry-all` requires `--workflow` and `--mode`, and takes `--after` and `--before` as RFC 3339 timestamps. It supports two modes:

| Mode | Behavior |
|------|----------|
| `replay` | Re-publish original DLQ messages to resume at the failed step. Uses the existing run ID and state. Limited by 30-day DLQ retention. |
| `rerun` | Start a fresh run with the original input. New run ID, clean state, uses the current workflow definition. |

Both modes support `--dry-run` to preview what would be replayed without taking action.

## When Tasks Are Dead-Lettered

A task enters the DLQ when:

1. **Retries exhausted** -- the step has a retry policy and all attempts have failed with retriable errors
2. **Non-retryable failure** -- the worker called `FailPermanent()`, bypassing retries entirely
3. **Poison history event** -- the orchestrator could not handle a `WORKFLOW_HISTORY` event after its maximum number of deliveries. These entries land on `dead.orchestrator.{eventType}.{runID}` and hold the raw event, which you diagnose and replay to `history.{runID}` by hand once the cause is fixed.

Tasks that succeed, are cancelled, or are skipped never enter the DLQ.

## Related Pages

- [Retry Policies](/docs/reliability/retry-policies) -- controlling retry behavior before DLQ
- [Error Handling](/docs/reliability/error-handling) -- failure types that lead to DLQ
- [Idempotency](/docs/reliability/idempotency) -- safe replay of dead-lettered tasks
