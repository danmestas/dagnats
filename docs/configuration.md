# Configuration Reference

DagNats uses a three-tier configuration system. Each tier overrides the previous:

1. **Built-in defaults** -- hardcoded platform-appropriate values
2. **Config file** (`dagnats.yaml`) -- optional file in the working directory
3. **Environment variables** (`DAGNATS_*`) -- highest priority

## Config Keys

| Key                       | Type     | Default (macOS)                                    | Default (Linux)                          |
|---------------------------|----------|----------------------------------------------------|------------------------------------------|
| `data_dir`                | string   | `~/Library/Application Support/dagnats`            | `~/.local/share/dagnats`                 |
| `http_addr`               | string   | `127.0.0.1:8080`                                   | `127.0.0.1:8080`                         |
| `nats_port`               | int      | `4222`                                             | `4222`                                   |
| `leaf_remotes`            | []string | (none)                                             | (none)                                   |
| `leaf_credentials`        | string   | (none)                                             | (none)                                   |
| `nats_cluster_name`       | string   | (none)                                             | (none)                                   |
| `nats_cluster_routes`     | []string | (none)                                             | (none)                                   |
| `nats_cluster_auth_token` | string   | (none)                                             | (none)                                   |
| `nats_cluster_port`       | int      | `6222`                                             | `6222`                                   |
| `nats_cluster_host`       | string   | `0.0.0.0`                                          | `0.0.0.0`                                |
| `nats_server_name`        | string   | `<cluster>-<hostname>-<cluster port>`              | `<cluster>-<hostname>-<cluster port>`    |
| `nats_jetstream_replicas` | int      | `0` (auto-derive)                                  | `0` (auto-derive)                        |
| `monitor_port`            | int      | (none)                                             | (none)                                   |
| `max_store_bytes`         | int64    | half of free disk, capped at 10 GiB                | half of free disk, capped at 10 GiB      |
| `max_memory_bytes`        | int64    | `1073741824` (1 GiB)                               | `1073741824` (1 GiB)                     |
| `max_payload`             | int32    | unset (NATS default, 1 MiB)                        | unset (NATS default, 1 MiB)              |
| `max_active_runs_per_root` | int     | `100`                                              | `100`                                    |
| `max_defs_per_root`       | int      | `500`                                              | `500`                                    |
| `max_generation_depth`    | int      | `3`                                                | `3`                                      |
| `max_registers_per_minute_per_root` | int | `60`                                         | `60`                                     |
| `runs_max_age`            | string   | unset (disabled)                                   | unset (disabled)                         |
| `otlp_endpoint`           | string   | (none)                                             | (none)                                   |
| `nats_ws_port`            | int      | `0` (disabled)                                     | `0` (disabled)                           |
| `nats_ws_no_tls`          | bool     | `false`                                            | `false`                                  |

### Embedded cluster mode

These fields enable self-clustered topology (see [`production.md`](production.md#self-clustered--embedded-ha) for the deployment model):

- `nats_cluster_name` (string, default `""`) — Cluster name when running embedded cluster mode. Required when `nats_cluster_routes` is set.
- `nats_cluster_routes` (string list, default `[]`) — Peer URLs (e.g. `nats://node-2:6222`) for embedded cluster mode. Mutually exclusive with `leaf_remotes`. Cap 10 entries.
- `nats_cluster_auth_token` (string, default `""`) — Optional shared token for cluster route authentication. (Mapped to NATS `Cluster.Username` internally; functions as a shared secret across cluster peers.)
- `nats_cluster_port` (int, default `6222`) — Port the node listens on for cluster routes. Set a distinct value per node when several nodes share a host. Route traffic is not TLS-encrypted; keep it on a private network.
- `nats_cluster_host` (string, default `0.0.0.0`) — IP address the route listener binds to. Set it to the node's private or tailnet IP so routes are not reachable on public interfaces.
- `nats_server_name` (string, default `<cluster>-<hostname>-<cluster port>`) — The node's NATS server name. JetStream tracks cluster peers by this name, so it must stay the same across restarts; the default does. Override it if the hostname may change. Letters, digits, `_`, `-` and `.` only, max 64 bytes.
- `nats_jetstream_replicas` (int, default `0`) — JetStream replication factor override. Valid: `{0, 1, 3, 5}`. `0` means auto-derive from cluster size.

### Message size

`max_payload` (int, bytes) sets the embedded NATS server's `max_payload`, the largest message a client may publish. Unset keeps the nats-server default of 1 MiB (1048576). Raise it when producers publish larger messages, for example `max_payload: 4194304` for 4 MiB. An explicit value must be between 1 and 8388608 (8 MiB); zero, negative or larger values fail config load, because nats-server warns above 8 MB and advises against going higher. Clients learn the limit from the server's INFO, so nothing else needs changing. In a cluster or leaf topology, set the same value on every node.

### Memory and browser access

- `max_memory_bytes` caps the JetStream in-memory store and is also applied as the Go soft memory limit (GOMEMLIMIT), so the process collects garbage harder near the ceiling. `0` or negative disables both.
- `http_addr` binds to loopback by default, so the console and API are reachable only from the host. Set it to a non-loopback address such as `0.0.0.0:8080` for remote access.
- `nats_ws_port` starts an embedded NATS WebSocket listener for browser clients when greater than `0`. Until NATS TLS is wired, it also requires `nats_ws_no_tls: true`, an explicit opt-in to cleartext.

On Linux, `data_dir` respects `XDG_DATA_HOME` if set.

## Environment Variables

| Variable                  | Overrides         | Notes                            |
|---------------------------|-------------------|----------------------------------|
| `DAGNATS_DATA_DIR`        | `data_dir`        |                                  |
| `DAGNATS_HTTP_ADDR`       | `http_addr`       |                                  |
| `DAGNATS_NATS_PORT`       | `nats_port`       | Must be a valid integer          |
| `DAGNATS_LEAF_REMOTES`    | `leaf_remotes`    | Comma-separated, max 10 entries  |
| `DAGNATS_LEAF_CREDENTIALS`| `leaf_credentials`| Path to creds file or inline PEM |
| `DAGNATS_NATS_CLUSTER_NAME` | `nats_cluster_name` | Required with cluster routes |
| `DAGNATS_NATS_CLUSTER_ROUTES` | `nats_cluster_routes` | Comma-separated, max 10 entries |
| `DAGNATS_NATS_CLUSTER_AUTH_TOKEN` | `nats_cluster_auth_token` | Shared token across peers |
| `DAGNATS_NATS_CLUSTER_PORT` | `nats_cluster_port` | Route listener port; default 6222 |
| `DAGNATS_NATS_CLUSTER_HOST` | `nats_cluster_host` | Route listener bind IP; default 0.0.0.0 |
| `DAGNATS_NATS_SERVER_NAME` | `nats_server_name` | Stable server name; default derived from host |
| `DAGNATS_NATS_JETSTREAM_REPLICAS` | `nats_jetstream_replicas` | One of `{0,1,3,5}`; `0`=auto |
| `DAGNATS_MONITOR_PORT`    | `monitor_port`    | NATS monitoring HTTP port        |
| `DAGNATS_MAX_STORE_BYTES` | `max_store_bytes` | Must be a positive integer       |
| `DAGNATS_MAX_MEMORY_BYTES` | `max_memory_bytes` | Bytes; `0` or negative disables the cap |
| `DAGNATS_NATS_WS_PORT`    | `nats_ws_port`    | `0` disables the WebSocket listener |
| `DAGNATS_NATS_WS_NO_TLS`  | `nats_ws_no_tls`  | `1`/`true`/`yes`/`on` enables    |
| `DAGNATS_QUEUE_SNAPSHOT_INTERVAL` | (env only) | Queue snapshot publish cadence; Go duration in [1s, 5m], default `5s` |
| `DAGNATS_MAX_PAYLOAD`     | `max_payload`     | Bytes, 1 to 8388608 (8 MiB); 0, negative or larger is rejected at load |
| `DAGNATS_MAX_ACTIVE_RUNS_PER_ROOT` | `max_active_runs_per_root` | Must be a positive integer |
| `DAGNATS_MAX_DEFS_PER_ROOT` | `max_defs_per_root` | Must be a positive integer |
| `DAGNATS_MAX_GENERATION_DEPTH` | `max_generation_depth` | Must be a positive integer; clamped to engine ceiling |
| `DAGNATS_MAX_REGISTERS_PER_MINUTE_PER_ROOT` | `max_registers_per_minute_per_root` | Must be a positive integer |
| `DAGNATS_RUNS_MAX_AGE` | `runs_max_age` | Go duration format or unset; negatives rejected |

### Triggers

| Variable                  | Default  | Notes                                    |
|---------------------------|----------|------------------------------------------|
| `DAGNATS_WEBHOOK_SECRET`  | (none)   | Default HMAC secret for webhook triggers |

When `DAGNATS_WEBHOOK_SECRET` is set and no `--secret` flag is provided
to `dagnats trigger create`, the env var value is used. The `--secret`
flag always takes precedence. This keeps secrets out of shell history.

### Control plane policy

The `policy.control_plane` block gates which workflows can access the control plane. Two policy keys exist:

- `policy.control_plane.grant` (list of workflow names) — workflows allowed to register and spawn workflows at runtime
- `policy.control_plane.promote` (list of workflow names) — subset of `grant`; workflows that can spawn higher-privilege workflows

Both keys have no environment variable override. Empty or absent = deny-by-default (no workflow has access). The policy is hot-reloadable.

Example `dagnats.yaml`:

```yaml
policy:
  control_plane:
    grant:   [planner, supervisor]
    promote: [supervisor]
```

The constraint `promote ⊆ grant` is enforced at config load.

### Console, bridge, and logs

| Variable | Default | Notes |
|----------|---------|-------|
| `DAGNATS_CONSOLE_PASSWORD` | (unset) | Enables HTTP Basic auth on the console. See the console auth docs |
| `DAGNATS_CONSOLE_TRUST_FORWARDED_AUTH` | `false` | Trust `X-Forwarded-User` from a reverse proxy doing SSO |
| `DAGNATS_BRIDGE_TOKEN` | (unset) | Admin bearer token for the HTTP worker bridge; unset leaves the bridge open (dev mode) |
| `DAGNATS_BUILD_LOGS_TTL` | `168h` | Retention of the `BUILD_LOGS` stream; Go duration in [1h, 8760h] |

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
