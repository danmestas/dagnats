---
title: Deployment Models
weight: 1
---

DagNats supports a single-binary server for simplicity, a self-clustered set of those servers for high availability, and a distributed deployment for scale.

## Single Binary (`dagnats serve`)

The `dagnats serve` command starts everything in one process: an embedded NATS server, the workflow engine, the API, triggers, and an HTTP server. Zero configuration required.

```bash
dagnats serve
```

This is the recommended model for development, staging, and small-to-medium production workloads. The embedded NATS server binds to `127.0.0.1` by default, so only local connections are accepted.

**What runs inside `dagnats serve`:**

| Component | Role |
|-----------|------|
| Embedded NATS | JetStream streams, KV buckets, pub/sub |
| ActorOrchestrator | Per-workflow actors, event processing |
| API Service | REST + NATS micro control plane |
| TriggerService | Cron, subject, and webhook triggers |
| HTTP Server | REST endpoints, health checks, webhooks |
| Telemetry | Tracing, metrics, structured logging |

Your workers are separate processes. They connect to the embedded NATS server over the network. The only handlers that run inside `dagnats serve` are the config-driven `exec` and `http` workers declared in `dagnats.yaml`.

### Startup Order

1. Resolve data directory (platform default if unset, created if missing)
2. Start embedded NATS server
3. Connect internal client to `nats://localhost:{port}`
4. Create all JetStream streams and KV buckets (`natsutil.SetupAll`), within a 30-second budget standalone or 3 minutes in a cluster
5. Initialize telemetry pipeline
6. Start API service, actor orchestrator, HTTP bridge, trigger service
7. Start HTTP server
8. Block on SIGINT/SIGTERM

### Shutdown Order

Shutdown reverses startup with a 15-second deadline:

1. HTTP server graceful shutdown (5s timeout)
2. Stop the config watcher, triggers, the token store and the NATS API service
3. Stop embedded config-driven workers, so in-flight tasks can publish completion events
4. Stop orchestrator (unsubscribe from history stream, stop all actors)
5. Stop the metrics and queue snapshot publishers, then flush telemetry
6. Drain NATS connection (flush pending messages)
7. Stop embedded NATS server

If shutdown has not finished when the deadline passes, `dagnats serve` returns a `shutdown timeout` error instead of waiting.

## Self-Clustered Deployment

Three or five `dagnats serve` nodes can form their own NATS cluster, with JetStream replicating state across them. There is no external NATS to run. See [Clustering]({{< ref "/docs/operations/clustering" >}}).

## Distributed Deployment

For larger workloads or multi-machine setups, run components as separate binaries connecting to an external NATS cluster. `dagnats-engine` and `dagnats-api` read the server URL from `DAGNATS_NATS_URL` (the older `NATS_URL` still works), defaulting to `nats://127.0.0.1:4222`:

```bash
# Machine 1: NATS cluster (or use NATS Cloud)
nats-server -js -c nats.conf

# Machine 2: Engine + API
DAGNATS_NATS_URL=nats://nats-host:4222 dagnats-engine
DAGNATS_NATS_URL=nats://nats-host:4222 dagnats-api

# Machine 3+: Workers (your own binaries, connecting with the worker SDK)
```

In this model, you manage the NATS infrastructure yourself. Each component connects to the external NATS server. The engine, API, and workers can scale independently. `dagnats-api` listens on `DAGNATS_LISTEN_ADDR` (default `:8080`) and serves the REST API only: `/health`, `/ready` and `/health/cluster` belong to `dagnats serve`. `dagnats-engine` and `dagnats-api` run `SetupAll` on start, which creates every stream and bucket the engine, API, triggers and workers use. They take no cluster settings, so they provision at a single replica, and pointed at a replicated cluster they rewrite existing streams and buckets to one replica..

## Leaf Node Topology

The embedded NATS server in `dagnats serve` can connect to a hub cluster as a **leaf node**. This gives you single-binary simplicity with multi-cluster reach.

```yaml
# dagnats.yaml
leaf_remotes: nats://hub1:7422, nats://hub2:7422
```

When leaf remotes are configured:
- The embedded NATS server binds to `0.0.0.0` (instead of `127.0.0.1`) for hub communication
- NATS handles message routing transparently between the leaf and hub
- All internal components still connect to `localhost:{port}`
- Workers can connect to either the leaf or the hub -- same code path

Leaf node mode enables geographic distribution, multi-team isolation, or connecting edge deployments to a central cluster. A maximum of 10 leaf remotes can be configured.

## When to Use Each Model

| Scenario | Model |
|----------|-------|
| Development and testing | `dagnats serve` (standalone) |
| Small production (< 50 workers) | `dagnats serve` (standalone) |
| Multi-region or multi-team | `dagnats serve` (leaf node) |
| High availability without external NATS | `dagnats serve` (self-clustered) |
| High availability with an external NATS cluster | Distributed |
| Independent scaling of engine vs workers | Distributed |

## Health Checks

`dagnats serve` exposes health endpoints on its HTTP server:

| Endpoint | Behavior |
|----------|----------|
| `GET /health` | 200 if NATS connected and JetStream available, 503 otherwise |
| `GET /ready` | 200 only after all components have started |
| `GET /health/cluster` | Cluster membership and per-stream replica state; `{"mode":"standalone","ok":true}` without cluster routes. See [Clustering]({{< ref "/docs/operations/clustering" >}}) |

Use `/health` for liveness probes and `/ready` for readiness probes in container orchestrators.

## Scaling Workers

Workers scale horizontally by running more instances. Each worker connects to NATS and pulls tasks from JetStream consumers. NATS handles load distribution automatically via **pull consumers** with `MaxAckPending`.

```bash
# Run 5 instances of the same worker
for i in $(seq 1 5); do
  my-worker --nats-url nats://localhost:4222 &
done
```

The engine does not need to know how many workers exist. Worker discovery is observability-only via the `workers` KV bucket (60s TTL heartbeat). The engine never reads it.
