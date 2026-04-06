---
title: Dead Letter Queue
weight: 4
---

The **dead letter queue** (DLQ) captures task messages that have exhausted all retries, providing a 30-day window for inspection and replay.

## DEAD_LETTERS Stream

When a step fails permanently -- either by exceeding its retry limit or by calling `FailPermanent()` -- the engine publishes the failed task message to the `DEAD_LETTERS` stream on `dead.>` subjects. Messages are retained for **30 days**.

The DLQ preserves the full task payload including run ID, step ID, attempt count, and original input. This gives operators everything needed to diagnose the failure and decide whether to replay.

## Inspecting the DLQ

List dead-lettered tasks with the CLI:

```bash
# List recent DLQ entries
dagnats dlq list

# Filter by workflow
dagnats dlq list --workflow code-review-pipeline

# Filter by time range
dagnats dlq list --after 2025-01-01 --before 2025-01-02

# JSON output for scripting
dagnats dlq list --json
```

Each entry shows the task ID, workflow name, step ID, failure reason, and timestamp.

## Replaying Failed Tasks

Replay republishes dead-lettered task messages back to the `TASK_QUEUES` stream, allowing them to be picked up by workers for another attempt:

```bash
# Replay a specific task
dagnats dlq replay <task-id>

# Bulk replay all failed tasks for a workflow
dagnats run retry-all code-review-pipeline --mode replay
```

### Replay vs. Rerun

The `retry-all` command supports two modes:

| Mode | Behavior |
|------|----------|
| `replay` | Re-publish original DLQ messages to resume at the failed step. Uses the existing run ID and state. Limited by 30-day DLQ retention. |
| `rerun` | Start a fresh run with the original input. New run ID, clean state, uses the current workflow definition. |

Both modes support `--dry-run` to preview what would be replayed without taking action.

## When Tasks Are Dead-Lettered

A task enters the DLQ when:

1. **Retries exhausted** -- the step has a retry policy and all attempts have failed with retriable errors
2. **Non-retryable failure** -- the worker called `FailPermanent()`, bypassing retries entirely
3. **MaxDeliver exceeded** -- NATS has redelivered the message the maximum number of times (timeout-driven failures)

Tasks that succeed, are cancelled, or are skipped never enter the DLQ.

## Related Pages

- [Retry Policies](/docs/reliability/retry-policies) -- controlling retry behavior before DLQ
- [Error Handling](/docs/reliability/error-handling) -- failure types that lead to DLQ
- [Idempotency](/docs/reliability/idempotency) -- safe replay of dead-lettered tasks
