---
title: Configuration
weight: 2
---

DagNats uses a three-tier configuration system where each tier overrides the previous.

## Resolution Order

1. **Built-in defaults** -- hardcoded, platform-appropriate values
2. **Config file** (`dagnats.yaml`) -- optional file in the working directory
3. **Environment variables** (`DAGNATS_*`) -- highest priority

Zero-config starts everything on defaults. Environment variables always win.

## Config Keys

| Key | Type | Default (macOS) | Default (Linux) |
|-----|------|-----------------|-----------------|
| `data_dir` | string | `~/Library/Application Support/dagnats` | `~/.local/share/dagnats` |
| `http_addr` | string | `:8080` | `:8080` |
| `nats_port` | int | `4222` | `4222` |
| `leaf_remotes` | []string | (none) | (none) |
| `leaf_credentials` | string | (none) | (none) |
| `monitor_port` | int | (none) | (none) |
| `max_store_bytes` | int64 | `10737418240` (10 GiB) | `10737418240` (10 GiB) |

On Linux, `data_dir` respects `XDG_DATA_HOME` if set.

## Environment Variables

### Core

| Variable | Overrides | Notes |
|----------|-----------|-------|
| `DAGNATS_DATA_DIR` | `data_dir` | |
| `DAGNATS_HTTP_ADDR` | `http_addr` | |
| `DAGNATS_NATS_PORT` | `nats_port` | Must be a valid integer |
| `DAGNATS_LEAF_REMOTES` | `leaf_remotes` | Comma-separated, max 10 entries |
| `DAGNATS_LEAF_CREDENTIALS` | `leaf_credentials` | Path to NATS credentials file |
| `DAGNATS_MONITOR_PORT` | `monitor_port` | NATS monitoring HTTP port |
| `DAGNATS_MAX_STORE_BYTES` | `max_store_bytes` | Must be a positive integer |

### Triggers

| Variable | Default | Notes |
|----------|---------|-------|
| `DAGNATS_WEBHOOK_SECRET` | (none) | Default HMAC secret for webhook triggers |

When `DAGNATS_WEBHOOK_SECRET` is set and no `--secret` flag is provided to `dagnats trigger create`, the env var value is used. The `--secret` flag always takes precedence. This keeps secrets out of shell history.

### Observability

| Variable | Default | Notes |
|----------|---------|-------|
| `JAEGER_ENDPOINT` | (none) | OTLP/HTTP base URL for span export |

When `JAEGER_ENDPOINT` is set, DagNats subscribes to the internal `TELEMETRY` span stream and batches spans to `{endpoint}/v1/traces` via OTLP/HTTP JSON. Works with any OTLP/HTTP-compatible backend (Jaeger, SigNoz, Grafana Tempo). When unset, spans are still written to the NATS `TELEMETRY` stream but not exported externally. Export failures never affect workflow execution.

```bash
JAEGER_ENDPOINT=http://localhost:4318 dagnats serve
```

### Workers (Config-Driven)

Worker handlers can be defined in the config file and overridden per-task via environment variables:

| Variable Pattern | Notes |
|-----------------|-------|
| `DAGNATS_WORKER_{TASK}_EXEC` | Shell command to execute |
| `DAGNATS_WORKER_{TASK}_HTTP` | HTTP endpoint URL |
| `DAGNATS_WORKER_{TASK}_HTTP_METHOD` | HTTP method (default: POST) |

Task names are uppercased with hyphens replaced by underscores. For example, a task named `call-claude` uses `DAGNATS_WORKER_CALL_CLAUDE_EXEC`.

### Deprecated Variables

The following environment variables still work but produce a warning on stderr. Migrate to the new names.

| Old Name | New Name |
|----------|----------|
| `NATS_URL` | `DAGNATS_NATS_URL` |
| `LISTEN_ADDR` | `DAGNATS_LISTEN_ADDR` |

## Config File

Place a `dagnats.yaml` file in the working directory. Format is simple `key: value` pairs, one per line. Lines starting with `#` are comments. Maximum 300 lines.

```yaml
# dagnats.yaml
data_dir: /var/lib/dagnats
http_addr: :9090
nats_port: 4333
leaf_remotes: nats://hub1:7422, nats://hub2:7422
max_store_bytes: 5368709120

# Config-driven workers
worker.summarize.exec: python3 /opt/workers/summarize.py
worker.call-api.http: http://localhost:3000/tasks
worker.call-api.http_method: POST
```

Unknown keys produce a warning but do not cause an error.

### Worker Config in YAML

Each worker needs a task name and either an `exec` command or an `http` endpoint (not both):

```yaml
worker.my-task.exec: /usr/local/bin/my-handler
worker.my-task.http: http://localhost:3000/handle
worker.my-task.http_method: PUT
```

Maximum of 50 worker configs. Duplicate task names are rejected. Having both `exec` and `http` for the same task is rejected.

## Viewing Effective Config

```bash
dagnats config show
dagnats config show --json
```

The `config show` command loads the resolved configuration (all three tiers merged) and prints it. Use `--json` for machine-readable output. This is the fastest way to verify which values are active.
