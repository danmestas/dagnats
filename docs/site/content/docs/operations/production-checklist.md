---
title: Production Checklist
weight: 4
---

A pre-launch checklist covering NATS tuning, monitoring, backup, scaling, and health checks.

## NATS Configuration

### JetStream Storage

Set `max_store_bytes` based on your expected event volume. The default is 10 GiB. For high-throughput deployments, increase it:

```yaml
# dagnats.yaml
max_store_bytes: 53687091200  # 50 GiB
```

JetStream data lives in `{data_dir}/jetstream/`. Use fast storage (SSD or NVMe) for this directory. Spinning disks create latency spikes during fsync.

### Stream Retention

- **WORKFLOW_HISTORY**: grows indefinitely by default. Set a `MaxAge` or `MaxBytes` on the stream if you have retention requirements. Events older than your recovery window are safe to discard.
- **DEAD_LETTERS**: 30-day retention. Increase if you need longer post-mortem windows.
- **TELEMETRY**: 7-day retention, 1 GiB cap. Increase `MaxBytes` if you export slowly or want deeper history.

### Dedup Window

`WORKFLOW_HISTORY` and `TELEMETRY` use a 5-second dedup window via `Nats-Msg-Id`. This prevents duplicate events during retries. Do not reduce this below 5 seconds unless you understand the implications for at-least-once delivery.

## Monitoring

### Health Endpoints

| Endpoint | Use |
|----------|-----|
| `GET /health` | Liveness probe -- 200 if NATS connected and JetStream available |
| `GET /ready` | Readiness probe -- 200 only after all components started |

Wire `/health` to your container orchestrator's liveness check and `/ready` to the readiness check.

### NATS Monitoring

Enable the NATS monitoring port for direct server metrics:

```yaml
monitor_port: 8222
```

This exposes the standard NATS monitoring endpoints (`/varz`, `/jsz`, `/connz`, `/subsz`) on the specified port. Use these for:

- **Stream health** (`/jsz`): message counts, consumer lag, storage usage
- **Connection health** (`/connz`): connected workers, subscription counts
- **Server health** (`/varz`): memory, CPU, goroutines

### Key Metrics to Watch

| Metric | Warning Threshold | Action |
|--------|-------------------|--------|
| Consumer pending count | > 1000 | Add workers or check for stuck tasks |
| Dead letter stream size | Growing steadily | Investigate failing tasks |
| JetStream storage usage | > 80% of `max_store_bytes` | Increase limit or add retention policies |
| Worker heartbeat gaps | > 60s | Worker likely crashed |
| `WORKFLOW_HISTORY` consumer lag | > 500 | Engine falling behind event processing |

### Status Command

```bash
dagnats status
```

Shows connection state, stream info, active workers, and pending tasks at a glance.

## Backup

### JetStream Snapshots

NATS JetStream supports stream snapshots for backup:

```bash
nats stream backup WORKFLOW_HISTORY /backups/history-$(date +%Y%m%d).tar
nats stream backup TASK_QUEUES /backups/tasks-$(date +%Y%m%d).tar
```

Back up at minimum:
- `WORKFLOW_HISTORY` -- your source of truth for all workflow state
- KV buckets (`workflow_defs`, `workflow_runs`) -- quick recovery without full replay

KV buckets are stored as JetStream streams internally (prefixed `KV_`), so you can back them up the same way:

```bash
nats stream backup KV_workflow_defs /backups/defs-$(date +%Y%m%d).tar
nats stream backup KV_workflow_runs /backups/runs-$(date +%Y%m%d).tar
```

### Recovery

Restore from snapshots:

```bash
nats stream restore WORKFLOW_HISTORY /backups/history-20260401.tar
```

After restoring `WORKFLOW_HISTORY`, the orchestrator replays the stream on next startup and rebuilds in-memory state. KV snapshots are a convenience -- the event log is the authoritative record.

## Scaling Workers

Workers scale horizontally. Each worker instance creates a pull consumer on the `TASK_QUEUES` stream filtered to its task types. NATS distributes work automatically.

**Guidelines:**

- Start with 1 worker per task type, add more when consumer pending count rises
- Each worker process handles one task at a time per task type by default
- For CPU-bound tasks, match worker count to available cores
- For I/O-bound tasks (API calls, LLM inference), run more workers than cores
- Maximum `MaxAckPending` on the consumer limits in-flight parallelism

### Worker Affinity

For stateful workers (large model caches, local file context), use **sticky bindings**. The `sticky_bindings` KV bucket maps runs to specific workers. Tasks for that run route to the same worker via the `STICKY_TASKS` stream.

## Memory and Disk Sizing

### Memory

- **Engine**: ~2 KiB per active workflow actor. 10,000 concurrent runs needs ~20 MiB for actor state alone, plus overhead.
- **Workers**: depends on task payload size. Budget for the largest payload you expect times `MaxAckPending`.
- **NATS server**: JetStream uses memory-mapped files. Budget at least 256 MiB for the NATS process itself.

### Disk

- **WORKFLOW_HISTORY**: ~500 bytes per event. A workflow with 10 steps generates ~12 events. 1 million completed workflows = ~6 GiB.
- **TASK_QUEUES**: WorkQueue retention means completed tasks are deleted. Disk usage is proportional to in-flight tasks, not total tasks.
- **TELEMETRY**: capped at 1 GiB by default with 7-day retention.
- **KV buckets**: typically small. `workflow_runs` is the largest, proportional to active + recently completed runs.

### Recommended Minimums

| Deployment Size | CPU | Memory | Disk |
|----------------|-----|--------|------|
| Development | 1 core | 512 MiB | 1 GiB |
| Small (< 100 concurrent runs) | 2 cores | 2 GiB | 20 GiB SSD |
| Medium (< 10,000 concurrent runs) | 4 cores | 8 GiB | 100 GiB SSD |
| Large (> 10,000 concurrent runs) | 8+ cores | 16+ GiB | 500+ GiB NVMe |

## Security

### Network Isolation

In standalone mode, the embedded NATS server binds to `127.0.0.1` -- only local connections accepted. In leaf node mode, it binds to `0.0.0.0` for hub communication.

For production:
- Use NATS credentials (`leaf_credentials` config key) for leaf-to-hub authentication
- Place the HTTP API behind a reverse proxy with TLS
- Use `DAGNATS_WEBHOOK_SECRET` for webhook trigger authentication
- Use `DAGNATS_BRIDGE_TOKEN` for HTTP bridge authentication

### Data Directory Permissions

The `data_dir` contains JetStream storage including workflow definitions, run state, and task payloads. Restrict access:

```bash
chmod 700 /var/lib/dagnats
chown dagnats:dagnats /var/lib/dagnats
```
