---
title: Error Reporting
weight: 4
---

DagNats has no error-tracker integration of its own. There is no Sentry, Bugsnag or Rollbar adapter and no error-reporting interface to implement. Failures surface through traces, logs, metrics and the dead letter queue, and you forward them to a tracker from there.

## Where errors show up

| Where | What you see |
|-------|--------------|
| Run state | `dagnats run inspect <run-id>` shows each step's status, attempts, output and error. |
| Traces | A handler error is recorded on its span with `RecordError` and an error status. `dagnats trace search --status=error` finds those traces. |
| Metrics | `step.retries`, `step.failure.non_retriable`, `step.failure.retry_after`, `workflow.runs.failed` and `dlq_entries_total`. See [Metrics]({{< ref "/docs/observability/metrics" >}}). |
| Dead letter queue | Steps that fail permanently are written to `DEAD_LETTERS`, with the error attached. `dagnats dlq list` shows them. See [Dead Letter Queue]({{< ref "/docs/reliability/dead-letter-queue" >}}). |
| Logs | Engine and worker errors are logged with `slog` at error level. See [Structured Logging]({{< ref "/docs/observability/structured-logging" >}}). |

```bash
dagnats trace search --status=error --since=1h
dagnats dlq list --run=<run-id>
```

## Sending errors to a tracker

Two routes work today.

**Through OpenTelemetry.** Set `OTEL_EXPORTER_OTLP_ENDPOINT` to a backend that turns error spans into issues, and the error spans from every process reach it. This needs no code.

**From your own worker.** A worker handler returns an error through `ctx.Fail(err)` or `ctx.FailPermanent(err)`. Call your tracker's SDK in the handler before you do, and keep the vendor import in your worker, not in shared code:

```go
w.Handle("send-email", func(ctx worker.TaskContext) error {
    if err := send(ctx.Input()); err != nil {
        tracker.Capture(err, map[string]string{
            "run_id":  ctx.RunID(),
            "step_id": ctx.StepID(),
        })
        return ctx.Fail(err)
    }
    return ctx.Complete(nil)
})
```

`tracker.Capture` stands for your tracker's call. DagNats does not define it.

## Related

- [Retry Policies](/docs/reliability/retry-policies) -- what happens after a failure
- [Error Handling](/docs/reliability/error-handling) -- failure types
- [Distributed Tracing](/docs/observability/distributed-tracing) -- span errors
