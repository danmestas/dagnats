---
title: Idempotency
weight: 5
---

DagNats provides two layers of deduplication -- application-level idempotency keys for workflow runs and transport-level `Nats-Msg-Id` for message dedup -- ensuring safe retries and replay without duplicate side effects.

## Workflow Idempotency Keys

An **idempotency key** prevents duplicate workflow runs from the same logical request. When set on a `WorkflowDef`, the engine extracts a key value from the run input using a dot-path expression and checks it against the `idempotency_keys` KV bucket before creating the run.

```go
wf := dag.NewWorkflow("payment").
    WithIdempotencyKey("payment_id")
```

When a run starts with input `{"payment_id": "pay_123", "amount": 50}`, the engine:

1. Extracts `"pay_123"` from the input via the `payment_id` dot-path
2. Checks the `idempotency_keys` KV bucket for the SHA-256 hash of `payment.pay_123` (workflow name, a dot, then the key value)
3. If found, returns the existing run ID without creating a new run
4. If not found, creates the run and stores the mapping

The mapping is written with a create-if-absent after the run is published, and the check and the write are not one atomic step. Two requests with the same key that arrive at the same moment can both start a run; the first mapping stored is the one later requests see.

### Dot-Path Extraction

The key field supports nested dot-path expressions for deeply nested input structures:

```go
// Extracts from input.metadata.request_id
wf := dag.NewWorkflow("webhook-handler").
    WithIdempotencyKey("metadata.request_id")
```

The `dag.ExtractDotPath()` function walks nested JSON objects using dot-separated segments. A missing key is logged as an error, and the run starts without dedup protection.

### TTL

Idempotency key entries have a **24-hour TTL** by default. After expiry, the same key can create a new run. This balances dedup protection against storage growth -- most duplicate requests arrive within seconds or minutes, not days.

## NATS Message Deduplication

At the transport layer, NATS JetStream provides automatic message deduplication via the `Nats-Msg-Id` header. DagNats sets this header on all published messages to prevent duplicate events from being stored in streams.

### Message ID Format

| Message Type | ID Format | Example |
|-------------|-----------|---------|
| Workflow events | `{run_id}.{event_type}` | `run-1.workflow.started` |
| Step events | `{run_id}.{step_id}.{event_type}` | `run-1.fetch.step.completed` |
| Step events with an attempt | `{run_id}.{step_id}.{event_type}.{attempt}` | `run-1.fetch.step.failed.2` |
| Task messages | `{run_id}.{step_id}.queued` | `run-1.fetch.queued` |
| Dead letters | `dlq:{run_id}:{step_id}:{attempts}` | `dlq:run-1:fetch:3` |

### Dedup Window

The JetStream dedup window on `WORKFLOW_HISTORY` is **5 seconds** (stream-level configuration). `DEAD_LETTERS` uses 24 hours and `BUILD_LOGS` 2 minutes. Within the window, publishing a message with an already-seen `Nats-Msg-Id` is silently dropped. This handles scenarios like:

- Engine crashes mid-publish and replays on restart
- Network partitions causing duplicate deliveries
- Worker publishing a completion event twice

After the window, the same message ID can be published again. This is safe because events are idempotent by design -- replaying a `step.completed` event for an already-completed step is a no-op in the orchestrator.

### Engine Crash During Dispatch

The engine records a step as dispatched in the run snapshot first, and only then publishes the step's task (or, for a sleep or wait-for-event step, schedules its timer and registers its waiter). An engine that dies between the two leaves a step the snapshot calls queued that no worker will ever receive. To close that gap, the dispatch save also marks the step as not yet sent, and a second save clears the mark once the publish lands. Whenever the engine later handles an event for that run, including the redelivery of the event whose dispatch died, and on the reconciler's periodic pass, it publishes every step still marked again.

Sending again is safe when the first publish did land. A task still waiting in `TASK_QUEUES` under the same dispatch is found and not queued twice, even after the stream's two-minute dedup window has passed, and a step a worker has already started is not resent. Timers and waiters are keyed by run and step, so a repeated one changes nothing. The cost is one extra snapshot write per dispatch.

## Designing Idempotent Workers

While DagNats handles dedup at the platform level, workers should be idempotent at the application level when possible:

```go
w.Handle("charge", func(ctx worker.TaskContext) error {
    var in ChargeInput
    if err := json.Unmarshal(ctx.Input(), &in); err != nil {
        return ctx.FailPermanent(err)
    }

    // Use the payment ID as an idempotency key with Stripe
    result, err := stripe.Charge(in.Amount, stripe.WithIdempotencyKey(
        fmt.Sprintf("%s.%s", ctx.RunID(), ctx.StepID()),
    ))
    if err != nil {
        return ctx.Fail(err)
    }
    return ctx.Complete(result)
})
```

Using `{runID}.{stepID}` as an external idempotency key ensures that retries of the same step hit the same external dedup window.

## Related Pages

- [Retry Policies](/docs/reliability/retry-policies) -- retries that benefit from idempotency
- [Dead Letter Queue](/docs/reliability/dead-letter-queue) -- safe replay of failed tasks
- [Error Handling](/docs/reliability/error-handling) -- failure types and retry behavior
