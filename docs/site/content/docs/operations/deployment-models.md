---
title: Deployment Models
weight: 1
---

DagNats supports two deployment models: a single-binary server for simplicity and a distributed deployment for scale.

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

Workers are always separate processes. They connect to the embedded NATS server over the network and are never embedded inside `dagnats serve`.

### Startup Order

1. Resolve data directory (platform default if unset, created if missing)
2. Start embedded NATS server
3. Connect internal client to `nats://localhost:{port}`
4. Create all JetStream streams and KV buckets (`natsutil.SetupAll`)
5. Initialize telemetry pipeline
6. Start API service, actor orchestrator, trigger service
7. Start HTTP server
8. Block on SIGINT/SIGTERM

### Shutdown Order

Shutdown reverses startup with a hard 15-second deadline:

1. HTTP server graceful shutdown (5s timeout)
2. Stop triggers (unsubscribe, stop scheduler)
3. Stop orchestrator (unsubscribe from history stream, stop all actors)
4. Flush telemetry (cancel exporter, flush pending spans)
5. Drain NATS connection (flush pending messages)
6. Stop embedded NATS server

If any step hangs past the deadline, the process force-exits. No goroutine leaks.

## Distributed Deployment

For larger workloads or multi-machine setups, run components as separate binaries connecting to an external NATS cluster:

```bash
# Machine 1: NATS cluster (or use NATS Cloud)
nats-server -js -c nats.conf

# Machine 2: Engine + API
dagnats-engine --nats-url nats://nats-host:4222
dagnats-api --nats-url nats://nats-host:4222

# Machine 3+: Workers
my-worker --nats-url nats://nats-host:4222
```

In this model, you manage the NATS infrastructure yourself. Each component connects to the external NATS server. The engine, API, and workers can scale independently.

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
| High availability with NATS clustering | Distributed |
| Independent scaling of engine vs workers | Distributed |

## Health Checks

Both models expose health endpoints on the HTTP server:

| Endpoint | Behavior |
|----------|----------|
| `GET /health` | 200 if NATS connected and JetStream available, 503 otherwise |
| `GET /ready` | 200 only after all components have started |

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
