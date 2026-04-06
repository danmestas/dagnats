---
title: Event Triggers
weight: 3
---

Event triggers start workflow runs in response to messages published to the NATS `EVENTS` stream, enabling reactive workflows driven by external systems.

## Overview

Any system that can publish to NATS can trigger a DagNats workflow. The `EVENTS` stream accepts messages on `event.>` subjects. A trigger definition binds a subject pattern to a workflow, optionally filtering on message content.

## Creating an Event Trigger

```bash
dagnats trigger create on-deploy \
  --workflow deploy-pipeline \
  --subject "event.deploy.production"
```

This fires a `deploy-pipeline` run whenever a message arrives on `event.deploy.production`. The event payload becomes the run's input.

### Subject Wildcards

NATS subject wildcards work in trigger definitions:

| Pattern | Matches |
|---------|---------|
| `event.deploy.*` | Any deploy event (`event.deploy.staging`, `event.deploy.production`) |
| `event.git.>` | All git events at any depth (`event.git.push`, `event.git.pr.opened`) |

## Publishing Events

External systems publish events to the `EVENTS` stream using any NATS client:

```go
nc, _ := nats.Connect("nats://localhost:4222")
js, _ := nc.JetStream()

payload := []byte(`{"repo": "acme/api", "branch": "main"}`)
js.Publish("event.deploy.production", payload)
```

## Filtering

Triggers can filter events by content to avoid unnecessary runs. The filter evaluates fields in the event payload before starting a workflow.

```bash
dagnats trigger create on-main-push \
  --workflow ci-pipeline \
  --subject "event.git.push" \
  --filter-field "branch" \
  --filter-value "main"
```

Only events where `branch == "main"` will trigger a run.

## Debounce

High-frequency events can be debounced to collapse rapid-fire events into a single run. Debounce state is tracked in the `debounce_state` KV bucket.

```bash
dagnats trigger create on-file-change \
  --workflow rebuild \
  --subject "event.fs.changed" \
  --debounce "5s"
```

When the first event arrives, a timer starts. Subsequent events within the 5-second window reset the timer. The run fires once the window expires with the **last** event's payload. The timer is implemented via `SLEEP_TIMERS` with a `debounce_fire` action.

## Batching

Batch mode accumulates events over a time window or until a count threshold is reached, then fires a single run with all accumulated payloads as an array input.

State is tracked in the `batch_state` KV bucket (TTL: 2x max timeout). The timer uses `SLEEP_TIMERS` with a `batch_fire` action.

## Event Correlation vs. Event Triggers

**Event triggers** start new workflow runs. They live in the `triggers` KV bucket and are evaluated by the scheduler.

**Wait-for-event steps** pause an existing run until a matching event arrives. They use the `event_waiters` KV bucket and are evaluated by the correlator inside the orchestrator.

These are distinct mechanisms. See [Wait for Event](/docs/step-types/wait-for-event) for the step-level pattern.

## Related Pages

- [Cron Schedules](/docs/triggers/cron-schedules) -- time-based triggers
- [Webhooks](/docs/triggers/webhooks) -- HTTP-based event ingestion
- [Wait for Event](/docs/step-types/wait-for-event) -- mid-workflow event correlation
