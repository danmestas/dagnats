# Production Deployment Guide

Operational guidance for running DagNats in production. For config
keys, environment variables, and file format details, see
[configuration.md](configuration.md).

## Deployment Topologies

| Topology | Hub shape | When to use |
|---|---|---|
| **Leaf → clustered hub** | 3+ NATS servers in one DC | the production default |
| **Leaf → single-node hub** | one NATS server | small prod or hobby; HA between dagnats leaves but hub is a SPOF |
| **Leaf → supercluster** | multiple clusters, gateway-connected, multi-region | global / multi-DC, regional failover, edge |
| **Single binary** | none (embedded only) | dev / eval / CI / single-machine non-critical |
| **Distributed** | external cluster, dagnats components split | rare — only when `dagnats-engine` and `dagnats-api` need independent scaling |

Workers always run as separate processes connecting to NATS, regardless of topology.

### Leaf node — production

The hub (cluster or supercluster) is an external NATS deployment — `nats-server` processes you run separately, or Synadia Cloud. dagnats's embedded NATS does not run in cluster or supercluster mode itself; it only acts as a leaf connecting outward.

In all three leaf flavors, dagnats binds NATS to `0.0.0.0` because hub communication requires external connectivity. Restrict the port via firewall — see [Network Isolation](#network-isolation). Maximum 10 remotes per instance.

```bash
DAGNATS_LEAF_REMOTES=nats://hub1:7422,nats://hub2:7422 \
DAGNATS_LEAF_CREDENTIALS=/etc/dagnats/hub.creds \
  dagnats serve
```

The hub shape determines what failure modes you've actually defended against.

#### Clustered hub — the default

Two or more dagnats instances connect to a 3-node NATS cluster (Synadia Cloud or self-hosted). State lives in the hub via JetStream R=3 replication. Optimistic locking on the `concurrency_runs` KV bucket ensures only one instance is the active orchestrator for any given run, so you get HA without split-brain.

What this gets you:

- **No data loss on host failure.** State lives in the hub cluster, not in `data_dir` on the dying machine.
- **Zero-downtime upgrades.** Roll dagnats instances one at a time; in-flight runs migrate.
- **Horizontal scale.** Add another dagnats node and it joins the consumer pool automatically.
- **Tenancy on existing NATS.** If your team already runs NATS for messaging, dagnats becomes another consumer for free.

The honest tradeoff: a 3-node NATS cluster is real ops surface. Synadia Cloud removes that and turns it into a vendor decision. Self-hosting is straightforward (`nats-server` is a single ~15 MB Go binary), but it's still three more processes to monitor. Compared to Temporal's Postgres + Cassandra requirement, it's the lighter end of the spectrum — but it's not zero.

#### Single-node hub — cheap, with a SPOF

One NATS server with one or more dagnats leaves. You get HA *between the leaves* (any can fail and the others continue), but the hub itself is a single point of failure — if the hub box dies, your runs stall until it comes back.

```bash
# on the hub box
nats-server -js -p 4222 --cluster_name=dagnats-hub
```

When this is the right answer:

- Hobby and personal projects where downtime is annoying but not page-worthy.
- Small production deployments where you can tolerate hub-restart blips.
- Migration step toward a clustered hub — the dagnats config doesn't change, you just point `DAGNATS_LEAF_REMOTES` at three nats-server instances later.

When it's the wrong answer: anything with an SLO. You haven't actually bought HA, you've just spread the leaves around. Skip this and run a 3-node cluster.

#### Supercluster hub — multi-region

Multiple NATS clusters in different regions, connected via gateway connections. dagnats leaves connect to their *local* cluster for low-latency operation, and the supercluster's global subject routing lets workflows triggered in one region reach workers in another.

```
                ┌──────────────┐  gateway  ┌──────────────┐
                │  Cluster US  │◄─────────►│  Cluster EU  │
                │  (3 nodes)   │           │  (3 nodes)   │
                └──────┬───────┘           └──────┬───────┘
                       │ leaves                   │ leaves
                ┌──────┴───────┐           ┌──────┴───────┐
                │ dagnats × N  │           │ dagnats × N  │
                └──────────────┘           └──────────────┘
```

The hard part is **JetStream replication strategy**. Vanilla supercluster gives you global *subject* routing, but JetStream streams and KV buckets are *cluster-local* by default. For workflow state to survive a region loss you need one of:

- **Stream mirroring** — passive replication from the primary cluster's `WORKFLOW_HISTORY` / `TASK_QUEUES` / KV streams into a mirror in the secondary cluster. Simple to operate. The mirror is read-only; failover means promoting the mirror.
- **Stream sourcing** — active-active replication where each cluster sources from the others. Supports concurrent writes across regions, requires careful subject-namespace partitioning to avoid conflicts on the `concurrency_runs` KV.

Pick mirroring for failover-only setups; pick sourcing for true active-active multi-region writes. The dagnats engine itself doesn't need to know which you chose — it sees one logical NATS surface.

When this is the right answer:

- **Regional data residency.** EU data must stay in the EU; US must stay in the US. Run regional dagnats clusters and route workflows by tenant.
- **Edge / latency-sensitive workloads.** Workers and triggers physically close to the request origin; central coordination via gateway.
- **Multi-DC failover.** Region A goes dark; region B continues serving runs from the mirrored streams.

When it's the wrong answer: anything a single-region clustered hub solves. Supercluster is the highest ops surface in the table, demands genuine NATS expertise, and turns failover testing into a real engineering project. Reach for it because you have a multi-region requirement nothing simpler satisfies, not because it sounds cool.

### Single binary — dev, eval, small

One process runs everything — embedded NATS, orchestrator, API, triggers — all local. The right shape for development, evaluation, CI / ephemeral environments, and personal deployments where a host failure is a shrug. Don't ship this for production unless you can articulate exactly why your downtime budget tolerates a single point of failure.

```yaml
# dagnats.yaml
data_dir: /var/lib/dagnats
http_addr: :8080
nats_port: 4222
max_store_bytes: 10737418240
```

### Distributed

Rare. Run if and only if you have a specific operational reason to scale `dagnats-engine` and `dagnats-api` independently. Most teams that think they need this end up needing more workers, which are already separate processes in any topology.

Install the standalone binaries:

```bash
go install github.com/danmestas/dagnats/cmd/dagnats-engine@latest
go install github.com/danmestas/dagnats/cmd/dagnats-api@latest
```

Run against an external cluster:

```bash
nats-server -js
NATS_URL=nats://cluster:4222 dagnats-engine
NATS_URL=nats://cluster:4222 dagnats-api
```

## Security

### Network Isolation

In leaf node mode (production), NATS binds to `0.0.0.0` because the
hub cluster needs to reach it. Use firewall rules to restrict which
hosts can connect to the NATS port — only the hub cluster's egress
should be allowed. The HTTP API port (`http_addr`) always binds to
the address you configure; restrict it with a firewall or bind to a
specific interface:

```yaml
http_addr: 127.0.0.1:8080
```

In single-binary mode (dev / eval), the embedded NATS server binds
to `127.0.0.1`. Only local processes can connect. No additional
network configuration is required — but this is also why this mode
is not appropriate for production.

### Authentication (Leaf Node)

Leaf node connections authenticate using NATS credentials files.
Provide the path via `leaf_credentials` in the config file or
`DAGNATS_LEAF_CREDENTIALS` as an environment variable.

For CI/CD environments where mounting a file is inconvenient, pass
inline PEM content. DagNats detects the `-----BEGIN` prefix and
writes the content to a secure temp file (mode 0600) automatically:

```bash
DAGNATS_LEAF_CREDENTIALS="-----BEGIN NATS USER JWT-----
eyJ0eXAiOiJK...
------END NATS USER JWT------
-----BEGIN USER NKEY SEED-----
SUAM...
------END USER NKEY SEED------" dagnats serve
```

The temp file is cleaned up on shutdown.

### TLS

DagNats does not currently expose TLS configuration for the
embedded NATS server or the HTTP API. For TLS termination, place a
reverse proxy (Caddy, nginx, or a cloud load balancer) in front of
both the HTTP port and the NATS port.

### Webhook Secrets

Webhook triggers use HMAC-SHA256 for request verification. Set a
default secret via `DAGNATS_WEBHOOK_SECRET`. Per-trigger secrets
can override this with the `--secret` flag on `dagnats trigger
create`.

Keep secrets out of shell history by using the environment variable
rather than CLI flags.

## Storage and Backup

### Data Directory

All persistent state lives under `data_dir`. The embedded NATS
server stores JetStream data in `{data_dir}/jetstream/`.

| Platform | Default path                                  |
|----------|-----------------------------------------------|
| macOS    | `~/Library/Application Support/dagnats`       |
| Linux    | `~/.local/share/dagnats` (or `$XDG_DATA_HOME/dagnats`) |

For production, set an explicit path on a dedicated volume:

```yaml
data_dir: /var/lib/dagnats
```

### Sizing `max_store_bytes`

The default is 10 GiB. This caps total JetStream storage across all
streams (WORKFLOW_HISTORY, TASK_QUEUES, EVENTS, DEAD_LETTERS,
TELEMETRY) and KV buckets.

Sizing depends on:

- **Workflow volume:** each event is a small JSON message (typically
  1-10 KB). A workflow run with 10 steps generates roughly 20-40
  events.
- **Retention:** DEAD_LETTERS retains for 30 days. TELEMETRY is
  capped at 1 GB with 7-day retention.
- **Checkpoint data:** workers can store checkpoint blobs in the
  `checkpoints` KV bucket. Large checkpoints consume storage fast.

For a rough estimate: 1000 runs/day with 10 steps each and no large
checkpoints uses approximately 200-400 MB/day before stream
compaction.

Monitor JetStream storage usage via the NATS monitoring port or the
`/health/telemetry` endpoint.

```yaml
# 50 GiB for high-volume deployments
max_store_bytes: 53687091200
```

### Backup and Restore

DagNats stores all state in the `data_dir` directory. To back up:

1. Stop the server (`SIGTERM` triggers graceful shutdown)
2. Copy the entire `data_dir`
3. Restart the server

For zero-downtime backup of the NATS JetStream data, use the
`nats` CLI with stream backup commands against the running server.
KV buckets are backed by streams named `KV_<bucket>`, so the same
`nats stream backup` command works for both:

```bash
# Back up a stream
nats stream backup WORKFLOW_HISTORY /backup/workflow-history/

# Back up a KV bucket (the underlying stream is KV_<bucket>)
nats stream backup KV_workflow_runs /backup/kv-workflow-runs/
nats stream backup KV_checkpoints /backup/kv-checkpoints/
```

Restore by placing the backed-up data directory at the configured
`data_dir` path before starting the server, or by using
`nats stream restore <name> <path>` against a running server.

## Tuning

### Consumer Settings

DagNats creates JetStream consumers internally. The key knobs that
affect throughput and reliability are configured through NATS-native
patterns:

- **Retry backoff:** workers call `NakWithDelay` to retry failed
  tasks. Backoff policy (fixed, linear, exponential) is set per
  workflow step or as a workflow default.
- **Step timeouts:** controlled by `AckWait` on the consumer. If a
  worker does not ack within the timeout, NATS redelivers.
- **Max retries:** `MaxDeliver` on the consumer bounds redelivery
  attempts. After exhaustion, messages go to the DEAD_LETTERS
  stream.

These are set through workflow definitions, not server config. See
the workflow schema documentation for retry policy options.

### Concurrency

DagNats uses KV-based counters with optimistic locking for
per-workflow concurrency limits. Configure concurrency limits in
workflow definitions, not server config. Excess runs queue as
pending and auto-start when slots open.

### Embedded Workers

The config file supports `worker.{task}.exec` and
`worker.{task}.http` keys for config-driven embedded workers that
run inside the server process. Maximum 50 worker configs.

```yaml
worker.summarize.exec: /usr/local/bin/summarize
worker.validate.http: http://localhost:9000/validate
worker.validate.http_method: POST
```

These are convenience shims. For production workloads with
independent scaling needs, run workers as separate processes.

### Monitor Port

Enable the NATS built-in monitoring HTTP endpoint for operational
visibility:

```yaml
monitor_port: 8222
```

This exposes NATS server stats at `http://localhost:8222/varz`,
connection info at `/connz`, JetStream info at `/jsz`, and more.
Useful for Prometheus scraping or manual debugging.

## Observability

For deployment modes (embedded sidecar, distributed S3, external collector) and direct telemetry consumption (`nats sub "telemetry.spans.>"`), see [observability.md](observability.md). The two production-specific notes are:

- **Export failures never affect workflow execution.** The internal `TELEMETRY` stream always receives data regardless of OTLP export status.
- The `TELEMETRY` stream is capped at 7-day retention and 1 GB — size externals accordingly if you need longer history.

### Instrumentation Points

DagNats instruments the following components:

| Component | Spans |
|-----------|-------|
| Engine    | handleEvent, advanceDAG, enqueueTask, saveSnapshot |
| Worker    | executeTask, complete, fail, continue |
| API       | registerWorkflow, startRun, getRun |

Trace context propagates via W3C `traceparent` headers in NATS
messages and is persisted in event payloads for correlation.

### Health Endpoints

| Endpoint             | Purpose                             |
|----------------------|-------------------------------------|
| `GET /health`        | NATS + JetStream connectivity check |
| `GET /ready`         | Server fully started                |
| `GET /health/telemetry` | Telemetry stream usage stats     |

Use `/health` for load balancer health checks and `/ready` for
deployment orchestrators (Kubernetes readiness probes, etc.).

## Graceful Shutdown

The server shuts down cleanly on `SIGINT` or `SIGTERM`. Shutdown
sequence (bounded at 15 seconds total):

1. HTTP server graceful shutdown (5s timeout)
2. Trigger service stops
3. Embedded workers stop (in-flight tasks complete)
4. Orchestrator stops
5. Telemetry flushes
6. NATS client drains
7. Embedded NATS server stops
8. Temp credential files cleaned up

If any step hangs past the 15-second deadline, the process
force-exits.

## Startup Checklist

Before going to production, verify:

- [ ] `data_dir` points to a dedicated volume with enough space
- [ ] `max_store_bytes` is sized for your expected volume
- [ ] Firewall rules restrict access to NATS and HTTP ports
- [ ] Webhook secrets are set via `DAGNATS_WEBHOOK_SECRET`
- [ ] OTLP endpoint is configured if you use external monitoring
- [ ] `monitor_port` is set for NATS server metrics
- [ ] Backup strategy covers the `data_dir` directory
- [ ] Shutdown signals (SIGTERM) reach the process (no PID 1 issues
  in containers)
- [ ] `/health` and `/ready` are wired to your health check system

## Viewing Effective Config

Before starting, verify the resolved configuration:

```bash
dagnats config show
dagnats config show --json
```

This loads all three tiers (defaults, config file, env vars) and
prints the merged result. Useful for debugging config precedence
issues.
