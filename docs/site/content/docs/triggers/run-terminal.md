---
title: Run Terminal Trigger
weight: 6
---

`run_terminal` triggers start a workflow when ANOTHER workflow's run reaches
a terminal state — general workflow chaining. It reacts to the terminal
run-lifecycle events published on `event.run.{workflow}.{runID}.{status}`
(see [Consumer contract: run lifecycle events](/docs/reference/wire-protocol)).

The first user is log offload (`examples/log-offload/`): dagnats owns the
`BUILD_LOGS` hot lane for the duration of a run but is deliberately ignorant
of long-term storage — a `run_terminal` trigger is how an operator hands a
finished run's logs to a workflow that copies them somewhere durable.

## Configuration

```json
{
  "id": "log-offload-on-my-build",
  "workflow_id": "log-offload",
  "enabled": true,
  "run_terminal": {
    "workflow": "my-build-workflow",
    "statuses": ["completed", "failed", "cancelled"]
  }
}
```

- `workflow_id` (top-level, standard on every `TriggerDef`) — the workflow
  this trigger **starts**.
- `run_terminal.workflow` — the workflow this trigger **watches**. Must name
  exactly one workflow; empty or wildcard (`*`, `>`) values are rejected.
- `run_terminal.statuses` — which terminal statuses fire the trigger. Any
  subset of `completed`, `failed`, `cancelled`. Defaults to all three when
  omitted.

Register it by writing the definition to the `triggers` KV bucket (there is
no flag-driven `dagnats trigger create` support for `run_terminal` yet):

```bash
nats kv put triggers log-offload-on-my-build "$(cat trigger.json)"
```

## Started run input

The started run's `Input` is exactly:

```json
{
  "run_id": "<the source run's ID>",
  "workflow_id": "<the source run's workflow>",
  "status": "completed",
  "labels": { "...": "copied from the source run, if any" }
}
```

## Loop guards

A trigger graph that starts workflows in response to OTHER workflows
finishing can cycle. `run_terminal` guards against both shapes the issue
identified, designed in rather than patched on:

### Self-trigger (register-time)

A `run_terminal` trigger whose `run_terminal.workflow` filter equals its own
`workflow_id` is rejected at registration, naming both workflows in the
error. Without this, a workflow that reacts to its own completion would
re-trigger itself every time it finishes.

### Cross-workflow cycles (runtime)

The trigger graph is mutable after registration (A→B and, later, someone
registers B→A), so a register-time check cannot see every cycle. Every
`dag.WorkflowRun` carries a `TriggerDepth`:

- `0` for every manually, HTTP-, or cron-started run.
- `source run's TriggerDepth + 1` for a run started by a `run_terminal`
  trigger.

Past a depth of **8** (`TriggerDepthMax`), the engine refuses to start the
chained run: it logs a warning naming both the source and the
would-be-started run, increments the
`trigger.run_terminal.depth_refusals` counter, and acknowledges the
triggering event without firing — the source run's completion is not
affected, only the chain stops there.

## Delivery and dedup

Each `run_terminal` trigger owns a durable JetStream consumer on `EVENTS`,
filtered to `event.run.{sanitized watched workflow}.*.*` — the server does
the filtering, not the trigger. At-least-once: a redelivered `RunEvent` (for
example after an engine restart) does not double-start the target workflow.
The started run's `Nats-Msg-Id` is `trig-{triggerID}-{sourceRunID}`, so a
retry collapses to the same run via JetStream's publish dedup window.

## The TTL constraint (for log offload specifically)

If you use `run_terminal` to trigger a log-offload workflow against
`BUILD_LOGS`, that stream's hot retention must exceed the offload workflow's
own retry horizon. A chronically failing offload run eventually gives up
retrying; if the hot TTL expires before that, the logs are gone before
anything durable could copy them. See `examples/log-offload/README.md` for
the concrete numbers.

## Related Pages

- [Event Triggers](/docs/triggers/event-triggers) — triggers driven by
  externally-published events, a different mechanism from `run_terminal`'s
  internal run-lifecycle events.
- `examples/log-offload/` — the reference workflow + worker.
