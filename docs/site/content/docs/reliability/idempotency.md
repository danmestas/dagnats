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
2. Checks the `idempotency_keys` KV bucket for key `payment.pay_123`
3. If found, returns the existing run ID without creating a new run
4. If not found, creates the run and stores the mapping

### Dot-Path Extraction

The key field supports nested dot-path expressions for deeply nested input structures:

```go
// Extracts from input.metadata.request_id
wf := dag.NewWorkflow("webhook-handler").
    WithIdempotencyKey("metadata.request_id")
```

The `dag.ExtractDotPath()` function walks nested JSON objects using dot-separated segments. Missing keys produce an error, and the run proceeds without dedup protection.

### TTL

Idempotency key entries have a **24-hour TTL** by default. After expiry, the same key can create a new run. This balances dedup protection against storage growth -- most duplicate requests arrive within seconds or minutes, not days.

## NATS Message Deduplication

At the transport layer, NATS JetStream provides automatic message deduplication via the `Nats-Msg-Id` header. DagNats sets this header on all published messages to prevent duplicate events from being stored in streams.

### Message ID Format

| Message Type | ID Format | Example |
|-------------|-----------|---------|
| Step events | `{run_id}.{step_id}.{event_type}` | `run-1.fetch.step.completed` |
| Rate retries | `{run_id}.{step_id}.rate_retry` | `run-1.call-llm.rate_retry` |

### Dedup Window

The JetStream dedup window is **2 minutes** (stream-level configuration on `WORKFLOW_HISTORY`). Within this window, publishing a message with an already-seen `Nats-Msg-Id` is silently dropped. This handles scenarios like:

- Engine crashes mid-publish and replays on restart
- Network partitions causing duplicate deliveries
- Worker publishing a completion event twice

After the 2-minute window, the same message ID can be published again. This is safe because events are idempotent by design -- replaying a `step.completed` event for an already-completed step is a no-op in the orchestrator.

## Designing Idempotent Workers

While DagNats handles dedup at the platform level, workers should be idempotent at the application level when possible:

```go
w.Handle("charge", func(ctx worker.TaskContext) {
    var in ChargeInput
    json.Unmarshal(ctx.Input(), &in)

    // Use the payment ID as an idempotency key with Stripe
    result, err := stripe.Charge(in.Amount, stripe.WithIdempotencyKey(
        fmt.Sprintf("%s.%s", ctx.RunID(), ctx.StepID()),
    ))
    if err != nil {
        ctx.Fail(err)
        return
    }
    ctx.Complete(result)
})
```

Using `{runID}.{stepID}` as an external idempotency key ensures that retries of the same step hit the same external dedup window.

## Related Pages

- [Retry Policies](/docs/reliability/retry-policies) -- retries that benefit from idempotency
- [Dead Letter Queue](/docs/reliability/dead-letter-queue) -- safe replay of failed tasks
- [Error Handling](/docs/reliability/error-handling) -- failure types and retry behavior
