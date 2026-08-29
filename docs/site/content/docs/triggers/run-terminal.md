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

A sub-workflow spawned by a run inherits that run's `TriggerDepth` (the loop
guard follows the trigger-chain lineage through a spawn, not just the
top-level run), so a cycle routed through a sub-workflow still hits the cap.
A DETACHED sub-workflow does not inherit it — same as it does not inherit
`RootRunID` — a detached spawn is a deliberately new, independent lineage.

**Operator-initiated resets are out of scope for the cap.** A bulk retry or
manual rerun of a chained run starts a fresh top-level run, which always
gets `TriggerDepth = 0` — the depth resets. This is intentional: the cap
exists to stop an unattended trigger graph from looping forever, not to
limit how many times an operator may deliberately re-run something.

## Delivery and dedup

Each `run_terminal` trigger owns a durable JetStream consumer on `EVENTS`,
filtered to `event.run.{sanitized watched workflow}.*.*` — the server does
the filtering, not the trigger (an exact-match check on `WorkflowID` runs
after that, since two distinct workflow names can sanitize to the same
subject token; see below). The consumer is created with `DeliverNewPolicy`,
not `DeliverAllPolicy`: at REGISTRATION time it must not replay `EVENTS`'
retention window and start one target run per historical terminal event that
predates the trigger. Once created, the durable consumer resumes from its
own ack floor on every restart regardless of that initial policy — `New`
only governs where a brand-new consumer starts reading.

At-least-once: a redelivered `RunEvent` (for example after an engine
restart, possibly minutes later) does not double-start the target workflow.
This does NOT rely on JetStream's publish dedup window (`Nats-Msg-Id`,
a few seconds) — that window cannot survive a restart gap. Instead:

- The started run's ID is **deterministic**: a SHA-256 of
  `{triggerID}|{sourceRunID}`, not a freshly minted ID. Every redelivery of
  the same (trigger, source run) pair names the identical target run.
- The engine claims that run ID with an atomic KV `Create` (not `Put`)
  before doing anything else — the first caller to successfully create wins,
  every other caller (including one racing at the same instant, not just one
  arriving later) is told the run already exists and acks without starting
  anything. This is a durable, no-expiry guard, not a time-bounded window.

The `Nats-Msg-Id` (`trig-{triggerID}-{sourceRunID}`) is still set on the
publish as a cheap fast-path that avoids a wasted publish attempt within the
short window — it is a minor optimization on top of the guarantees above,
not what makes redelivery safe.

Each trigger's durable consumer is named from a hash of the trigger ID
(not the sanitized ID directly), so two trigger IDs that happen to sanitize
to the same token still get independent consumers instead of one silently
overwriting the other's filter.

Deleting a `run_terminal` trigger deletes its durable consumer; disabling one
(or any other edit that re-registers it) does not — the consumer is reused
so re-enabling resumes from where it left off instead of losing position.

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
