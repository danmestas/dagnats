# Configuration Reference

DagNats uses a three-tier configuration system. Each tier overrides the previous:

1. **Built-in defaults** -- hardcoded platform-appropriate values
2. **Config file** (`dagnats.yaml`) -- optional file in the working directory
3. **Environment variables** (`DAGNATS_*`) -- highest priority

## Config Keys

| Key               | Type     | Default (macOS)                                    | Default (Linux)                          |
|-------------------|----------|----------------------------------------------------|------------------------------------------|
| `data_dir`        | string   | `~/Library/Application Support/dagnats`            | `~/.local/share/dagnats`                 |
| `http_addr`       | string   | `:8080`                                            | `:8080`                                  |
| `nats_port`       | int      | `4222`                                             | `4222`                                   |
| `leaf_remotes`    | []string | (none)                                             | (none)                                   |
| `leaf_credentials`| string   | (none)                                             | (none)                                   |
| `monitor_port`    | int      | (none)                                             | (none)                                   |
| `max_store_bytes` | int64    | `10737418240` (10 GiB)                             | `10737418240` (10 GiB)                   |

On Linux, `data_dir` respects `XDG_DATA_HOME` if set.

## Environment Variables

| Variable                  | Overrides         | Notes                            |
|---------------------------|-------------------|----------------------------------|
| `DAGNATS_DATA_DIR`        | `data_dir`        |                                  |
| `DAGNATS_HTTP_ADDR`       | `http_addr`       |                                  |
| `DAGNATS_NATS_PORT`       | `nats_port`       | Must be a valid integer          |
| `DAGNATS_LEAF_REMOTES`    | `leaf_remotes`    | Comma-separated, max 10 entries  |
| `DAGNATS_LEAF_CREDENTIALS`| `leaf_credentials`| Path to creds file or inline PEM |
| `DAGNATS_MONITOR_PORT`    | `monitor_port`    | NATS monitoring HTTP port        |
| `DAGNATS_MAX_STORE_BYTES` | `max_store_bytes` | Must be a positive integer       |

### Triggers

| Variable                  | Default  | Notes                                    |
|---------------------------|----------|------------------------------------------|
| `DAGNATS_WEBHOOK_SECRET`  | (none)   | Default HMAC secret for webhook triggers |

When `DAGNATS_WEBHOOK_SECRET` is set and no `--secret` flag is provided
to `dagnats trigger create`, the env var value is used. The `--secret`
flag always takes precedence. This keeps secrets out of shell history.

### Observability

| Variable                       | Default  | Notes                              |
|--------------------------------|----------|------------------------------------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | (none)   | OTLP/HTTP base URL for telemetry   |

When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, DagNats subscribes to
the internal `TELEMETRY` span stream and batches spans to
`{endpoint}/v1/traces` via OTLP/HTTP JSON. Example:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 dagnats serve
```

This works with any OTLP/HTTP-compatible backend (SigNoz, Grafana
Tempo, Jaeger). When unset, spans are still written to the NATS
`TELEMETRY` stream but not exported externally. Export failures
never affect workflow execution.

### Deprecated Variables

The following environment variables are deprecated but still supported.
They produce a warning on stderr when used. Migrate to the new names.

| Old Name        | New Name              |
|-----------------|-----------------------|
| `NATS_URL`      | `DAGNATS_NATS_URL`    |
| `LISTEN_ADDR`   | `DAGNATS_LISTEN_ADDR` |

## Config File

Place a `dagnats.yaml` file in the working directory. Format is simple
`key: value` pairs, one per line. Lines starting with `#` are comments.
Maximum 100 lines.

```yaml
# dagnats.yaml
data_dir: /var/lib/dagnats
http_addr: :9090
nats_port: 4333
leaf_remotes: nats://remote1:7422, nats://remote2:7422
max_store_bytes: 5368709120
```

Unknown keys produce a warning but do not cause an error.

### Inline Leaf Credentials

`DAGNATS_LEAF_CREDENTIALS` accepts either a file path or inline PEM
content. If the value starts with `-----BEGIN`, DagNats writes it to a
secure temp file automatically. This is useful in CI/CD where mounting
a credentials file is inconvenient:

```bash
DAGNATS_LEAF_CREDENTIALS="-----BEGIN NATS USER JWT-----
eyJ0eXAiOiJK...
------END NATS USER JWT------
-----BEGIN USER NKEY SEED-----
SUAM...
------END USER NKEY SEED------" dagnats serve
```

## Viewing Effective Config

```bash
dagnats config show
dagnats config show --json
```

The `config show` command loads the resolved configuration (all three tiers
merged) and prints it. Use `--json` for machine-readable output.

Example output:

```
data_dir:        /Users/you/Library/Application Support/dagnats
http_addr:       :8080
nats_port:       4222
leaf_remotes:    (none)
leaf_credentials:(none)
monitor_port:    (none)
max_store_bytes: 10737418240
otlp_endpoint:   (none)
```
