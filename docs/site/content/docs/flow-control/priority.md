---
title: Priority
weight: 4
---

Priority configuration controls the ordering of workflow runs when [concurrency limits](/docs/flow-control/concurrency-limits) create a backlog, using input-driven rules to promote or demote runs in the queue.

## PriorityConfig

The `PriorityConfig` on `WorkflowDef` maps input field values to priority offsets:

| Field | JSON Key | Type | Description |
|-------|----------|------|-------------|
| **Key** | `key` | `string` | Dot-path into run input for value extraction |
| **Rules** | `rules` | `map[string]int` | Value-to-offset mapping |
| **DefaultOffset** | `default_offset` | `int` | Offset when key is missing or value has no rule |

```go
wf := dag.NewWorkflow("deploy").
    WithConcurrency(1, 0).
    WithPriority(dag.PriorityConfig{
        Key: "environment",
        Rules: map[string]int{
            "production": 300,
            "staging":    100,
            "dev":        -100,
        },
        DefaultOffset: 0,
    })
```

## How Priority Works

Priority is implemented as a **time offset** on the run's creation timestamp. Higher offsets make a run appear "older" to the scheduler, causing it to be selected first when a concurrency slot opens.

The engine computes the effective queue position with:

```
effective_time = created_at - (priority_offset * 1 second)
```

A run with offset `300` appears 5 minutes older than its actual creation time. When the engine selects the oldest pending run to fill an open slot, higher-priority runs win.

### Offset Bounds

Priority offsets are **clamped to [-600, 600]** (plus or minus 10 minutes). This prevents extreme offsets from starving lower-priority runs indefinitely.

## Resolution

`ResolvePriority` computes the offset from the run's input data:

1. If no `PriorityConfig` is set, offset is 0
2. If `Key` is empty, `DefaultOffset` is used
3. Otherwise, extract the value at `Key` using dot-path, look up in `Rules`
4. If the value has no matching rule, `DefaultOffset` is used

The resolved offset is stored on `WorkflowRun.PriorityOffset` and used by `EffectiveTime()` for queue ordering.

## JSON Schema Example

```json
{
  "name": "deploy-pipeline",
  "version": "1",
  "concurrency": {"max_runs": 1},
  "priority": {
    "key": "environment",
    "rules": {
      "production": 300,
      "staging": 100
    },
    "default_offset": 0
  },
  "steps": [
    {
      "id": "deploy",
      "task": "deploy",
      "timeout": "10m",
      "type": "normal"
    }
  ]
}
```

With `max_runs: 1`, only one deploy runs at a time. If a production deploy and a staging deploy are both pending, the production deploy (offset 300) is selected first.

## NATS Consumer Priority

At the NATS transport layer, task message ordering is determined by JetStream consumer configuration. DagNats uses pull consumers with `AckWait`-based timeout, and the engine controls dispatch ordering through the `EffectiveTime()` calculation rather than NATS-level priority. This keeps priority logic in the application layer where it can be customized per-workflow.

## Related Pages

- [Concurrency Limits](/docs/flow-control/concurrency-limits) -- the limits that create backlogs
- [Rate Limiting](/docs/flow-control/rate-limiting) -- throttling dispatch frequency
- [CLI and API](/docs/triggers/cli-and-api) -- starting runs with input that drives priority
