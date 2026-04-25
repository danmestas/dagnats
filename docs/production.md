# Production Deployment Guide

Operational guidance for running DagNats in production. For config
keys, environment variables, and file format details, see
[configuration.md](configuration.md).

## Deployment Topologies

DagNats supports three deployment models. Choose based on scale and
isolation requirements.

### Single Binary (recommended for most deployments)

```bash
dagnats serve
```

One process runs the embedded NATS server, orchestrator, API, trigger
service, and HTTP server. All components connect to the embedded NATS
on localhost. This is the simplest deployment and the right starting
point for most teams.

**When to use:** single machine, moderate throughput, simplest
operations. Workers still run as separate processes connecting via
NATS.

**Config example:**

```yaml
# dagnats.yaml
data_dir: /var/lib/dagnats
http_addr: :8080
nats_port: 4222
max_store_bytes: 10737418240
```

### Leaf Node (hub-and-spoke)

The embedded NATS server connects to an external NATS hub cluster as
a leaf node. NATS handles message routing transparently. The DagNats
process still runs all components locally.

**When to use:** you already operate a NATS cluster and want DagNats
traffic to flow through it, or you need multiple DagNats instances
sharing state via a central hub.

```bash
DAGNATS_LEAF_REMOTES=nats://hub1:7422,nats://hub2:7422 \
DAGNATS_LEAF_CREDENTIALS=/etc/dagnats/hub.creds \
  dagnats serve
```

In leaf mode the embedded NATS binds to `0.0.0.0` instead of
`127.0.0.1` because hub communication requires external
connectivity. Standalone mode binds to localhost only.

**Leaf remote limit:** maximum 10 remotes per instance.

### Distributed (separate processes)

Run the engine, API, and workers as separate processes against a
shared NATS cluster. Use this only when you need independent scaling
of components across machines.

Install the standalone binaries (separate from `dagnats serve`):

```bash
go install github.com/danmestas/dagnats/cmd/dagnats-engine@latest
go install github.com/danmestas/dagnats/cmd/dagnats-api@latest
```

Then run them against your cluster:

```bash
nats-server -js
NATS_URL=nats://cluster:4222 dagnats-engine
NATS_URL=nats://cluster:4222 dagnats-api
```

Workers are always separate processes regardless of topology.

## Security

### Network Isolation

In standalone mode (no `leaf_remotes`), the embedded NATS server
binds to `127.0.0.1`. Only local processes can connect. This is the
default and requires no additional network configuration.

In leaf node mode, NATS binds to `0.0.0.0`. Use firewall rules to
restrict which hosts can reach the NATS port. The HTTP API port
(`http_addr`) always binds to the address you configure -- restrict
it with a firewall or bind to a specific interface:

```yaml
http_addr: 127.0.0.1:8080
```

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

### OTLP Export

DagNats writes all telemetry (traces, metrics, logs) to an internal
NATS `TELEMETRY` stream. When `OTEL_EXPORTER_OTLP_ENDPOINT` is
set, the server also exports spans to the specified OTLP/HTTP
endpoint:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318 dagnats serve
```

Spans are batched and sent to `{endpoint}/v1/traces`. This works
with any OTLP/HTTP-compatible backend: SigNoz, Grafana Tempo,
Jaeger, or any OpenTelemetry Collector.

**Export failures never affect workflow execution.** The TELEMETRY
stream always receives data regardless of export status.

You can also set this in the config file:

```yaml
otlp_endpoint: http://collector:4318
```

### Internal Telemetry Stream

Even without OTLP export, all telemetry flows to the NATS
`TELEMETRY` stream (7-day retention, 1 GB cap). You can consume
it directly:

```bash
nats sub "telemetry.spans.>"
nats sub "telemetry.metrics.>"
nats sub "telemetry.logs.>"
```

Subject hierarchy:
- `telemetry.spans.{service}.{run_id}`
- `telemetry.metrics.{service}.{metric_name}`
- `telemetry.logs.{service}.{level}`

All messages are JSON for human readability and `nats sub`
debugging.

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
