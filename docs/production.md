# Production Deployment Guide

Operational guidance for running DagNats in production. For config
keys, environment variables, and file format details, see
[configuration.md](configuration.md).

## Deployment Topologies

| Topology | Hub shape | When to use |
|---|---|---|
| **Self-clustered** | none — dagnats nodes form their own cluster | self-contained HA without external NATS infrastructure |
| **Leaf → clustered hub** | 3+ NATS servers in one DC | the production default |
| **Leaf → single-node hub** | one NATS server | small prod or hobby; HA between dagnats leaves but hub is a SPOF |
| **Leaf → supercluster** | multiple clusters, gateway-connected, multi-region | global / multi-DC, regional failover, edge |
| **Single binary** | none (embedded only) | dev / eval / CI / single-machine non-critical |
| **Distributed** | external cluster, dagnats components split | rare — only when `dagnats-engine` and `dagnats-api` need independent scaling |

Workers always run as separate processes connecting to NATS, regardless of topology.

### Self-clustered — embedded HA

Three or five dagnats instances each run their own embedded NATS server in cluster configuration, connecting directly to each other via NATS cluster routes. JetStream replicates state across nodes (R=3 or R=5). Failover is automatic; rolling upgrades work without an external NATS deployment.

**Config (each node):**
```yaml
nats_cluster_name: dagnats-prod
nats_cluster_routes: nats://node-1.dagnats.svc.cluster.local:6222, nats://node-2.dagnats.svc.cluster.local:6222
```

Server keys in `dagnats.yaml` are read one `key: value` line at a time, so routes are comma-separated on one line, and values are not `${VAR}`-expanded. Keep the route token out of the file by setting `DAGNATS_NATS_CLUSTER_AUTH_TOKEN` in each node's environment.

**What you get:**
- No external NATS to operate.
- JetStream replicates at R=3 (auto-derived from a 3-node cluster). Streams and KV buckets survive any single-host failure.
- Zero-downtime rolling upgrades: drain and restart nodes one at a time.
- Explicit override available via `nats_jetstream_replicas` for the rare case of intentionally choosing R<cluster_size.

**What you don't get (in v1):**
- Dynamic membership. Adding or removing nodes requires planned reconfiguration on all nodes plus a rolling restart.
- TLS for cluster routes. Route TLS is not yet supported: routes use the shared token only, and traffic between nodes is unencrypted. Run the cluster on a private network or tailnet you trust; do not expose the route port (`nats_cluster_port`, default 6222) to the internet.
- Multi-region. Use leaf → supercluster for that.

**Migration from single-binary:** add the four cluster fields to each node's config, restart all nodes within the 60-second quorum-wait window. Streams auto-update from R=1 to R=3 in place via JetStream's `CreateOrUpdateStream` semantics. Reversible by removing the cluster fields and restarting.

**Bound:** `nats_cluster_routes` cap is 10 entries (11-node maximum). Quorum-wait at startup is bounded at 60 seconds; if peers don't connect in time, `SetupAll` returns an error and the process exits.

**Route port:** nodes listen for routes on `nats_cluster_port` (default `6222`, env `DAGNATS_NATS_CLUSTER_PORT`). Give each node on the same host a distinct port; nodes on separate hosts can all use the default. The value must be 1-65535 and must differ from `nats_port`, `monitor_port`, `nats_ws_port` and the `http_addr` port; config load fails otherwise.

**Bind address and identity:** set `nats_cluster_host` to the node's private or tailnet IP to keep the route listener off public interfaces. Each node's server name defaults to `<cluster>-<hostname>-<cluster port>`, which stays the same across restarts. JetStream records cluster peers by that name, so a node that came back under a new name would leave its old identity behind as a replica that never returns. Set `nats_server_name` explicitly if a host may be renamed.

**Observing the cluster.** `GET /health/cluster` reports live state read from the embedded server, not config intent:

```json
{
  "mode": "cluster",
  "ok": true,
  "expected_peers": 2,
  "connected_peers": 2,
  "nodes_expected": 3,
  "nodes_up": 3,
  "node": "dagnats-prod-4021",
  "leader": "dagnats-prod-4021",
  "members": [
    {"name": "dagnats-prod-4021", "self": true, "route_connected": true, "meta_leader": true, "current": true},
    {"name": "dagnats-prod-4033", "route_connected": true, "current": true},
    {"name": "dagnats-prod-4047", "route_connected": true, "current": true}
  ],
  "jetstream": {
    "leader_elected": true,
    "streams": {"TASK_QUEUES": {"replicas": 3, "in_sync": 3}},
    "kv_buckets": {"workflow_runs": 3}
  }
}
```

- `node` is the server that answered; `members` lists every node the JetStream meta group knows, as seen from that node. `route_connected` is whether it has a live route to the member; `meta_leader` marks the JetStream meta leader (`leader` repeats its name); `offline` means the member has not been seen recently.
- `current` and `lag_ops` (caught up, operations behind) are reported only when the answering node is the meta leader. A follower sees the other followers as not current, so it omits the fields rather than report them wrongly. To read them, ask the node named in `leader`.
- `nodes_up` counts members that are route-connected and not offline (including the reporting node). A dead node drops `nodes_up` to 2 of 3 and `connected_peers` to 1 of 2.
- `streams` and `kv_buckets` cover every stream and bucket `SetupAll` provisions (the list comes from `natsutil`, so it cannot drift). `in_sync` is the leader plus followers that are current.
- Status is `200` while the cluster has quorum. `jetstream.unavailable` lists replicated streams or buckets that do not answer (quorum lost for them) and the response becomes `503`. `jetstream.degraded` lists unreachable single-replica assets (for example when `nats_jetstream_replicas: 1` is set on purpose); they are reported but stay `200`, so a per-node load-balancer check does not drain the survivors when one node goes down. Unavailable assets never hide the rest of the report.
- Every node answers for itself, so poll each node (or a load balancer health check per node) rather than one address.

The same report is served over NATS request/reply on the control API: send an empty request to `api.cluster.health` (service `dagnats-api`) and read the JSON body. The endpoint has no queue group, so every node is subscribed and you get whichever node replies first; read `node` in the body to see which one answered. Failure is `"ok": false` in the body; NATS replies carry no status code.

```bash
nats request api.cluster.health ''
```

**Known gap — assets outside `SetupAll`:** every stream and bucket `SetupAll` provisions uses the cluster's replication factor (including `TELEMETRY`, `STICKY_TASKS`, `TRIGGER_HISTORY` and `leases`; an existing deployment is upgraded in place on restart). A few buckets are created by individual components instead and are created at R=1 in a cluster. They are not in the health report, and losing the node that holds one makes that component's data unavailable until the node returns. Replicating them is not done yet.

**Testing.** `make test-cluster-e2e` (`DAGNATS_E2E_CLUSTER=1 go test ./e2e/cluster/`) starts three `dagnats serve` processes on localhost, waits for `/health/cluster` to show three nodes, runs a workflow, kills one node, and checks that health stays `200` with 2 of 3 nodes up and that a second workflow completes. It runs in the dedicated `Cluster E2E` CI job. This proves process-level clustering on one host; it does not test cross-machine networking, partitions, or clock skew, which need real hosts.

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
DAGNATS_NATS_URL=nats://cluster:4222 dagnats-engine
DAGNATS_NATS_URL=nats://cluster:4222 dagnats-api
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

Webhook triggers use HMAC-SHA256 for request verification. When you
create a trigger with `dagnats trigger create`, the secret comes from
`DAGNATS_WEBHOOK_SECRET`, unless you pass `--secret`, which takes
precedence. The secret is stored with the trigger, so changing the
environment variable later does not change existing triggers.

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

When `max_store_bytes` is unset, it defaults to half of the free disk
under `data_dir`, capped at 10 GiB. This is the JetStream storage
budget. Each stream gets a share of it (for example WORKFLOW_HISTORY
25%, EVENTS 15%, TASK_QUEUES 12%, TELEMETRY 10%, DEAD_LETTERS 5%).

Sizing depends on:

- **Workflow volume:** each event is a small JSON message (typically
  1-10 KB). A workflow run with 10 steps generates roughly 20-40
  events.
- **Retention:** DEAD_LETTERS retains for 30 days and TELEMETRY for
  7 days, each also bounded by its share of the budget. Terminal runs
  are pruned from the `workflow_runs` bucket after `runs_max_age`
  (30 days by default). BUILD_LOGS keeps task log lines for
  `DAGNATS_BUILD_LOGS_TTL` (168h by default).
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

- **Retry backoff:** the engine schedules each retry as a durable
  timer in `SLEEP_TIMERS`. Backoff policy (fixed, linear,
  exponential) is set per workflow step or as a workflow default.
- **Step timeouts:** the engine arms a watchdog timer per attempt.
  A worker's `AckWait` (default 5 minutes, per handler via
  `worker.WithAckWait`) only bounds how long it holds its claim;
  redelivery does not restart the step timeout.
- **Max retries:** the step's retry policy bounds attempts. After
  exhaustion the run fails and the task goes to the DEAD_LETTERS
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
- The `TELEMETRY` stream keeps 7 days and is bounded by its share of `max_store_bytes` — size externals accordingly if you need longer history.

### Instrumentation Points

DagNats instruments the following components:

| Component | Spans |
|-----------|-------|
| Engine    | `dagnats.engine handleEvent`, `dagnats.engine enqueueReady`, `enqueueTask <task>` |
| Worker    | `executeTask <task>`, `worker.complete`, `worker.fail`, `worker.failPermanent`, `worker.failRetryAfter`, `worker.continue` |
| API       | `dagnats.api <call>` per service call, such as `registerWorkflow`, `listWorkflows`, `createTrigger` |

Trace context propagates via W3C `traceparent` headers in NATS
messages and is persisted in event payloads for correlation.

### Health Endpoints

| Endpoint             | Purpose                             |
|----------------------|-------------------------------------|
| `GET /health`        | NATS + JetStream connectivity check |
| `GET /ready`         | Server fully started                |
| `GET /health/telemetry` | Telemetry stream usage stats     |
| `GET /health/cluster` | Cluster membership and JetStream replica health (see above) |

Use `/health` for load balancer health checks and `/ready` for
deployment orchestrators (Kubernetes readiness probes, etc.).

## Graceful Shutdown

The server shuts down cleanly on `SIGINT` or `SIGTERM`. Shutdown
sequence (bounded at 15 seconds total):

1. HTTP server graceful shutdown (5s timeout)
2. Config file watcher and trigger service stop
3. Embedded workers stop (in-flight tasks complete)
4. Orchestrator stops
5. Metrics aggregator, queue snapshot publisher and telemetry stop
6. NATS client drains
7. Embedded NATS server stops
8. Temp credential files cleaned up

If shutdown has not finished by the 15-second deadline, `serve`
exits with a non-zero status and a shutdown timeout error.

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
