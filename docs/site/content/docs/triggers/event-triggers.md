---
title: Event Triggers
weight: 3
---

Event triggers start workflow runs in response to messages published on a NATS subject, enabling reactive workflows driven by external systems.

## Overview

Any system that can publish to NATS can trigger a DagNats workflow. A trigger binds a subject pattern to a workflow. The server subscribes to that subject with a plain NATS subscription (not a JetStream consumer), so a message is delivered to the trigger only while the server is running. By convention events use the `event.>` subjects, which the `EVENTS` stream captures, but the trigger accepts any subject.

## Creating an Event Trigger

Pass the workflow name, then `--subject`:

```bash
dagnats trigger create deploy-pipeline --subject "event.deploy.production"
```

This starts a `deploy-pipeline` run for every message that arrives on `event.deploy.production`. The CLI prints the new trigger's ID.

The run's input is a trigger envelope, and the message payload is its `data` field:

```json
{
  "trigger": "subject",
  "source": "trig-0123456789abcdef",
  "workflow_id": "deploy-pipeline",
  "timestamp": "2026-10-19T09:00:00Z",
  "data": { "repo": "acme/api", "branch": "main" }
}
```

`data` is the raw payload bytes; it is omitted when the message is empty. A worker handler that only wants `data` can use `worker.UnwrapTrigger()` (see [HTTP Trigger](/docs/triggers/http#reading-the-request-inside-a-worker)).

### Subject Wildcards

NATS subject wildcards work in trigger definitions:

| Pattern | Matches |
|---------|---------|
| `event.deploy.*` | Any deploy event (`event.deploy.staging`, `event.deploy.production`) |
| `event.git.>` | All git events at any depth (`event.git.push`, `event.git.pr.opened`) |

## Publishing Events

External systems publish with any NATS client:

```go
nc, _ := nats.Connect("nats://localhost:4222")
js, _ := jetstream.New(nc)

payload := []byte(`{"repo": "acme/api", "branch": "main"}`)
js.Publish(ctx, "event.deploy.production", payload)
```

## Filtering

Event triggers have no payload filter. Every message on the subject starts a run. To start runs for only some messages, publish those messages on a narrower subject, or let the first step read `data` and complete early for the messages you want to ignore.

## Debounce

A trigger definition accepts a `debounce` block:

| Field | Meaning |
|-------|---------|
| `period` | Quiet time before the run starts. Required, up to 7 days. |
| `timeout` | Hard cap on how long a window can keep extending. Optional. Must be at least `period` and at most 7 days. |
| `key` | Dot-path into the event payload. Events with different values get separate windows. |

Debounce cannot be combined with a cron trigger, and `trigger create` has no flag for it; it is set in a `dagnats.yaml` trigger or in the trigger JSON.

**The server does not apply debounce to subject triggers today.** The definition is validated and stored, but no debouncer is attached when the trigger activates, so every message starts a run. Do not rely on it to collapse bursts.

## Event Correlation vs. Event Triggers

**Event triggers** start new workflow runs. They live in the `triggers` KV bucket and are held by the trigger service inside `dagnats serve`.

**Wait-for-event steps** pause an existing run until a matching event arrives on the `EVENTS` stream. They use the `event_waiters` KV bucket and are evaluated by the correlator inside the orchestrator.

These are distinct mechanisms. See [Wait for Event](/docs/step-types/wait-for-event) for the step-level pattern.

## Related Pages

- [Cron Schedules](/docs/triggers/cron-schedules) -- time-based triggers
- [Webhooks](/docs/triggers/webhooks) -- HTTP-based event ingestion
- [Run Terminal Trigger](/docs/triggers/run-terminal) -- start a run when another run finishes
- [Wait for Event](/docs/step-types/wait-for-event) -- mid-workflow event correlation
