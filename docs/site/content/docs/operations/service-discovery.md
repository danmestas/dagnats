---
title: "Service Discovery (nats-micro)"
weight: 6
---

# Service Discovery (nats-micro)

DagNats's internal control-plane endpoints run as **[nats-micro](https://github.com/nats-io/nats.go/tree/main/micro)
services**. That means dagnats is **discoverable and observable with standard
NATS tooling** out of the box — no extra wiring — and the console Services page
shows **live** instance/version/health data sourced from the `$SRV.*` discovery
protocol rather than a static registry.

This is purely additive: the request/reply **subjects are unchanged**, so every
existing caller keeps working. Wrapping them in `micro.Service` only adds the
discovery + per-endpoint statistics surface.

## The services

| Service | Endpoints (subjects) |
|---|---|
| `dagnats-api` | `api.workflows.register`, `api.runs.start`, `api.runs.get`, `api.runtimes.register`, `api.runs.spawn`, `api.runtimes.budget` |
| `dagnats-trigger` | the trigger-type registration ack (`_REGISTRY.trigger_types.ack`) — present only when external-trigger support is enabled |

Each service reports the build version stamped into the binary (an un-stamped dev
build reports `0.0.0-dev`).

## Inspecting with the `nats` CLI

```bash
# List discoverable dagnats services and their instances
nats micro ls

# Full info for one service (endpoints, subjects, metadata)
nats micro info dagnats-api

# Per-endpoint request/error counts + processing time
nats micro stats dagnats-api
```

Under the hood these use the reserved `$SRV.PING`, `$SRV.INFO`, and `$SRV.STATS`
subjects that `micro.Service` answers automatically. Discovery is
**request-many-reply**: every running instance responds, so `nats micro ls`
shows the instance count for a horizontally-scaled deployment.

## In the console

**Console → Services** renders a live roster: it issues a bounded `$SRV` discovery
request per page load (a short, bounded timeout so a slow or absent responder can
never stall the page) and shows, per service:

- **Instances** — count of `$SRV.PING` responders.
- **Version** — from the ping response.
- **Status** — `online` (responding, no errors), `degraded` (errors reported),
  `unknown` (reachable but stats unavailable), or `stale` (registered but not
  responding). Anything unbacked is dashed, never fabricated.

The page is a **union** of the `services` KV roster and live-discovered services,
so `dagnats-api` and `dagnats-trigger` appear even though they don't self-register
in the KV bucket.

## Notes

- **Fan-out preserved.** The endpoints run **without** a micro queue group, matching
  the pre-micro plain-subscribe behavior — every instance receives each message
  (no load-balancing was silently introduced).
- **Error envelope unchanged.** Handlers reply with the same JSON error shape as
  before (they do not use `micro`'s `Nats-Service-Error` headers), so existing
  request/reply clients parse responses identically.
