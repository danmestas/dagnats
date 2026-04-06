# Production Configuration Guide

**Status:** Design
**Date:** 2026-04-06
**Depends on:** Nothing (documentation-only)

## Problem

The configuration reference (`docs/configuration.md`) documents what
each setting does, but there's no guidance on production-appropriate
values. Specific gaps:

1. **No TLS documentation.** NATS supports TLS natively but dagnats
   config doesn't expose TLS settings. Users must know to configure
   NATS leaf remotes with `tls://` URLs and provide credentials.
2. **No auth guidance.** The embedded NATS server has no authentication
   by default. No documentation on how to secure it.
3. **No backup/restore.** JetStream data in `data_dir` is the source of
   truth for all workflow state. No guidance on backup strategy.
4. **No tuning recommendations.** Default 10 GiB storage may be too much
   or too little. No guidance on sizing `max_store_bytes`, consumer
   settings, or monitoring thresholds.
5. **No deployment topology guidance.** When to use embedded `serve` vs
   distributed `dagnats-engine` + `dagnats-api` + external NATS.

## Design

### 1. New Document: `docs/production.md`

Structure:

```markdown
# Production Deployment Guide

## Deployment Topologies

### Single-Binary (Recommended for Most Cases)

When: fewer than ~50 concurrent runs, single-machine deployment.

    dagnats serve --config production.yaml

Advantages: zero operational overhead, single process to monitor.

### Distributed

When: high availability, horizontal worker scaling, existing NATS
cluster.

    # Existing NATS cluster (managed separately)
    DAGNATS_NATS_URL=nats://cluster:4222 dagnats-engine
    DAGNATS_NATS_URL=nats://cluster:4222 dagnats-api

Workers connect independently to the same NATS cluster.

### Leaf Node (Hub-and-Spoke)

When: edge processing, multi-region, or connecting to a central
NATS cluster without exposing it directly.

    DAGNATS_LEAF_REMOTES=nats://hub:7422 dagnats serve

## Security

### Network Isolation

The embedded NATS server listens on all interfaces by default.
In production, bind to a specific interface or use a firewall.

### Authentication

The embedded server does not enable auth by default. For production:

1. Use leaf nodes with credentials:

       leaf_remotes: nats://hub:7422
       leaf_credentials: /etc/dagnats/nkey.seed

2. Or run an external NATS server with full auth config and connect
   dagnats components as clients.

### TLS

Leaf node connections support TLS automatically when using `tls://`:

    leaf_remotes: tls://hub:7422

For the embedded server's client port, TLS requires running an
external NATS server with TLS configured.

### Webhook Security

Always set a webhook secret for trigger webhooks:

    DAGNATS_WEBHOOK_SECRET=<random-secret>

Or per-trigger: `dagnats trigger create ... --secret=<secret>`

## Storage & Backup

### Data Directory

All JetStream state lives in `data_dir`:

- Stream data (workflow history, task queues, DLQ)
- KV buckets (workflow defs, run state, triggers)

### Sizing `max_store_bytes`

Rule of thumb: 1 GB per 10,000 completed workflows (varies by
payload size). Monitor with `dagnats status --detail`.

### Backup

JetStream stores data as files in `data_dir`. Options:

1. **Filesystem snapshot**: Stop dagnats, snapshot `data_dir`, restart.
   Safe but requires downtime.
2. **NATS stream mirror**: Run a mirror cluster that replicates
   streams. Zero downtime but requires a second NATS cluster.
3. **Periodic export**: Use `nats stream backup` from the NATS CLI
   for individual stream backup without stopping the server.

### Restore

1. Stop dagnats
2. Replace `data_dir` contents with backup
3. Start dagnats — JetStream recovers from on-disk state

## Tuning

### Consumer Settings

DagNats configures JetStream consumers with these defaults:
- AckWait: 30s (task must complete or heartbeat within 30s)
- MaxDeliver: varies by retry policy

For long-running tasks, use `ctx.Heartbeat()` in workers rather
than increasing AckWait globally.

### Concurrency

- `concurrency.max_runs`: limits parallel runs per workflow
- `concurrency.max_steps`: limits parallel steps within a run
- Task-level: JetStream consumer max_ack_pending controls per-worker
  parallelism

### Monitoring Thresholds

Watch these via `dagnats status --detail` or the `/health/telemetry`
endpoint:
- TELEMETRY stream > 80% of max bytes → increase or reduce retention
- DLQ message count growing → investigate failed tasks
- Consumer pending count high → workers are falling behind

## Observability

### OTLP Export

    otlp_endpoint: http://your-collector:4318

Or via environment:

    JAEGER_ENDPOINT=http://collector:4318 dagnats serve

Traces, metrics, and logs are exported via OTLP/HTTP.

### Internal Telemetry

Even without OTLP export, all telemetry is stored in the NATS
TELEMETRY stream (7-day retention, 1 GB cap). Use:

    dagnats trace <run-id>
    dagnats logs search --level=error
    dagnats metrics show
```

### 2. Files Changed

| File | Change |
|------|--------|
| `docs/production.md` (new) | Full production guide |
| `docs/getting-started.md` | Add "Going to Production" link at bottom |
| `README.md` | Add link to production guide |
