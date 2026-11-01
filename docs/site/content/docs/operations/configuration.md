---
title: Configuration
weight: 2
---

DagNats uses a three-tier configuration system where each tier overrides the previous.

## Resolution Order

1. **Built-in defaults** -- hardcoded, platform-appropriate values
2. **Config file** (`dagnats.yaml`) -- optional; see [Config File](#config-file) for where it is searched for
3. **Environment variables** (`DAGNATS_*`) -- highest priority

Zero-config starts everything on defaults. Environment variables always win.

## Config Keys

| Key | Type | Default (macOS) | Default (Linux) |
|-----|------|-----------------|-----------------|
| `data_dir` | string | `~/Library/Application Support/dagnats` | `~/.local/share/dagnats` |
| `http_addr` | string | `127.0.0.1:8080` | `127.0.0.1:8080` |
| `nats_port` | int | `4222` | `4222` |
| `leaf_remotes` | []string | (none) | (none) |
| `leaf_credentials` | string | (none) | (none) |
| `leaf_user` | string | (none) | (none) |
| `leaf_password_file` | string | (none) | (none) |
| `leaf_port` | int | `0` (off) | `0` (off) |
| `leaf_host` | string | `127.0.0.1` | `127.0.0.1` |
| `leaf_listen_user` | string | (none) | (none) |
| `leaf_listen_password_file` | string | (none) | (none) |
| `nats_cluster_name` | string | (none) | (none) |
| `nats_cluster_routes` | []string | (none) | (none) |
| `nats_cluster_auth_token` | string | (none) | (none) |
| `nats_cluster_port` | int | `6222` | `6222` |
| `nats_cluster_host` | string | `0.0.0.0` | `0.0.0.0` |
| `nats_server_name` | string | `<cluster>-<hostname>-<cluster port>` | `<cluster>-<hostname>-<cluster port>` |
| `nats_jetstream_replicas` | int | `0` (auto-derive) | `0` (auto-derive) |
| `jetstream_domain` | string | (none) | (none) |
| `monitor_port` | int | (none) | (none) |
| `max_store_bytes` | int64 | half of free disk, capped at 10 GiB | half of free disk, capped at 10 GiB |
| `max_memory_bytes` | int64 | `1073741824` (1 GiB) | `1073741824` (1 GiB) |
| `nats_ws_port` | int | `0` (disabled) | `0` (disabled) |
| `nats_ws_no_tls` | bool | `false` | `false` |
| `max_payload` | int32 | unset (NATS default, 1 MiB) | unset (NATS default, 1 MiB) |
| `otlp_endpoint` | string | (none) | (none) |
| `legacy_step_events` | bool | `true` | `true` |
| `loop_state_max_bytes` | int | `262144` (256 KiB) | `262144` (256 KiB) |

`max_payload` sets the embedded NATS server's largest accepted message in bytes. Unset keeps the nats-server default of 1 MiB; raise it (for example to `4194304`, 4 MiB) when producers publish larger messages. Values must be between 1 and 8388608 (8 MiB). Clients learn the limit from the server's INFO. In a cluster or leaf topology, set the same value on every node.

`legacy_step_events` (`DAGNATS_LEGACY_STEP_EVENTS`) makes the engine accept `step.continue`, `step.completed` and `step.failed` events that carry no dispatch identity, from workers and bridge clients older than the fence. While it is on, a duplicate continue from such a worker can still advance a loop twice, and the engine logs a warning at startup. Set it to `false` once every worker is upgraded. `loop_state_max_bytes` (`DAGNATS_LOOP_STATE_MAX_BYTES`) caps the state an agent loop commits per iteration; it must stay below `max_payload`, and it does not apply to a step that uses the [agent journal](/docs/ai-patterns/durable-agents). Upgrade engines before workers; see [Durable Agents](/docs/ai-patterns/durable-agents#upgrading).

`leaf_port` (default `0`, off) opens a leafnode listener other servers dial with a `nats-leaf://` remote. `leaf_host` is its bind IP, `127.0.0.1` when unset; name a private or tailnet IP for a hub that leaves dial over the network. `leaf_listen_user` and `leaf_listen_password_file` are the login a leaf must present, and both are required whenever `leaf_port` is set, on loopback too: a linked leaf shares every subject and stream. The listener works in embedded cluster mode. On the dialing side, `leaf_user` and `leaf_password_file` log in to every leaf remote whose URL has no login of its own, with a plain user and password instead of the operator-mode creds file `leaf_credentials` takes; they are set together and exclude `leaf_credentials`. Password files are read at start, like `nats_password_file`. A leaf binds its client port to all interfaces unless `nats_host` is set, and a client there shares every subject of the hub, so on a laptop set `nats_host: 127.0.0.1` or `nats_user`; dagnats warns at start when neither is set, and when `leaf_host` is `0.0.0.0` or `::`, since the leaf login travels without TLS. A linked leaf has the hub's whole account, not only the streams it mirrors.

`jetstream_domain` names this server's JetStream domain, so a server linked to it over a leaf connection can reach its JetStream as `$JS.<domain>.API` (`jetstream.NewWithDomain(nc, "e-mac")`) while keeping its own `$JS.API`. Local clients, dagnats included, keep using plain `$JS.API`. Set it on a leaf node that runs its own JetStream, such as a laptop that works offline; leave it unset on the hub. The value must be one NATS subject token: no dots, spaces or wildcards, max 64 bytes. Non-ASCII letters are accepted, since they are legal in a NATS token. In embedded cluster mode the domain belongs to the whole cluster, so every node sets the same value.

On Linux, `data_dir` respects `XDG_DATA_HOME` if set.

The `nats_cluster_*`, `nats_server_name` and `nats_jetstream_replicas` keys are explained on the [Clustering]({{< ref "/docs/operations/clustering" >}}) page. `nats_cluster_routes` needs 2 to 10 entries, and `nats_cluster_name` is required whenever routes are set.

## Environment Variables

### Core

| Variable | Overrides | Notes |
|----------|-----------|-------|
| `DAGNATS_DATA_DIR` | `data_dir` | |
| `DAGNATS_HTTP_ADDR` | `http_addr` | |
| `DAGNATS_NATS_PORT` | `nats_port` | Must be a valid integer |
| `DAGNATS_LEAF_REMOTES` | `leaf_remotes` | Comma-separated, max 10 entries |
| `DAGNATS_LEAF_CREDENTIALS` | `leaf_credentials` | Path to NATS credentials file |
| `DAGNATS_LEAF_USER` | `leaf_user` | Login sent to leaf remotes; pairs with `DAGNATS_LEAF_PASSWORD_FILE` |
| `DAGNATS_LEAF_PASSWORD_FILE` | `leaf_password_file` | File holding the leaf remotes' password |
| `DAGNATS_LEAF_PORT` | `leaf_port` | Leafnode listener port; `0` (default) is off |
| `DAGNATS_LEAF_HOST` | `leaf_host` | Leafnode listener bind IP; default 127.0.0.1 |
| `DAGNATS_LEAF_LISTEN_USER` | `leaf_listen_user` | Login a leaf must present; required with `DAGNATS_LEAF_PORT` |
| `DAGNATS_LEAF_LISTEN_PASSWORD_FILE` | `leaf_listen_password_file` | File holding the listener's password |
| `DAGNATS_MONITOR_PORT` | `monitor_port` | NATS monitoring HTTP port |
| `DAGNATS_MAX_STORE_BYTES` | `max_store_bytes` | Must be a positive integer |
| `DAGNATS_MAX_MEMORY_BYTES` | `max_memory_bytes` | Bytes; also sets the Go soft memory limit; `0` or negative disables |
| `DAGNATS_NATS_CLUSTER_NAME` | `nats_cluster_name` | Required with cluster routes |
| `DAGNATS_NATS_CLUSTER_ROUTES` | `nats_cluster_routes` | Comma-separated, max 10 entries |
| `DAGNATS_NATS_CLUSTER_AUTH_TOKEN` | `nats_cluster_auth_token` | Shared token across peers |
| `DAGNATS_NATS_CLUSTER_PORT` | `nats_cluster_port` | Route listener port; default 6222 |
| `DAGNATS_NATS_CLUSTER_HOST` | `nats_cluster_host` | Route listener bind IP; default 0.0.0.0 |
| `DAGNATS_NATS_HOST` | `nats_host` | Client listener bind IP; default 127.0.0.1, or 0.0.0.0 in leaf mode |
| `DAGNATS_NATS_USER` | `nats_user` | The operator login on the client port and WebSocket listener; set with `nats_password_file`. Set: anonymous clients are the join user. Unset: anonymous is root. See [NATS users](#nats-users) |
| `DAGNATS_NATS_PASSWORD_FILE` | `nats_password_file` | File holding that user's password (chmod 600) |
| `DAGNATS_API_TOKEN_FILE` | `api_token_file` | File holding a bearer token every HTTP request must carry, except `/health`, `/health/cluster` and `/ready`. On the bridge's worker routes a token the bridge accepts also passes. Unset: open. Neither login covers `monitor_port` or cluster routes (use `nats_cluster_auth_token`) |
| `DAGNATS_NATS_SERVER_NAME` | `nats_server_name` | Stable server name; default derived from host |
| `DAGNATS_JETSTREAM_DOMAIN` | `jetstream_domain` | JetStream domain a leaf hub addresses this server by; default none |
| `DAGNATS_NATS_JETSTREAM_REPLICAS` | `nats_jetstream_replicas` | One of `{0,1,3,5}`; `0` = auto |
| `DAGNATS_NATS_WS_PORT` | `nats_ws_port` | Browser WebSocket listener; `0` disables |
| `DAGNATS_NATS_WS_NO_TLS` | `nats_ws_no_tls` | Required with `nats_ws_port` until NATS TLS is wired |
| `DAGNATS_QUEUE_SNAPSHOT_INTERVAL` | (env only) | Queue snapshot cadence; Go duration in [1s, 5m], default `5s` |
| `DAGNATS_MAX_PAYLOAD` | `max_payload` | Bytes, 1 to 8388608 (8 MiB); 0, negative or larger is rejected at load |

### Triggers

| Variable | Default | Notes |
|----------|---------|-------|
| `DAGNATS_WEBHOOK_SECRET` | (none) | Default HMAC secret for webhook triggers |

When `DAGNATS_WEBHOOK_SECRET` is set and no `--secret` flag is provided to `dagnats trigger create`, the env var value is used. The `--secret` flag always takes precedence. This keeps secrets out of shell history.

### Console, bridge, and logs

| Variable | Default | Notes |
|----------|---------|-------|
| `DAGNATS_CONSOLE_PASSWORD` | (unset) | Enables HTTP Basic auth on the console. See the console auth docs |
| `DAGNATS_CONSOLE_TRUST_FORWARDED_AUTH` | `false` | Trust `X-Forwarded-User` from a reverse proxy doing SSO |
| `DAGNATS_BRIDGE_TOKEN` | (unset) | Admin bearer token for the HTTP worker bridge; unset leaves the bridge open (dev mode) |
| `DAGNATS_BUILD_LOGS_TTL` | `168h` | Retention of the `BUILD_LOGS` stream; Go duration in [1h, 8760h] |

### Observability

| Variable | Default | Notes |
|----------|---------|-------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | (none) | OTLP/HTTP base URL for telemetry |

Spans, metrics and logs are always written to the NATS `TELEMETRY` stream. When `OTEL_EXPORTER_OTLP_ENDPOINT` is set (a non-empty `otlp_endpoint` config key also switches the exporters on, but the SDK takes the target from the environment variable), each DagNats process also exports its traces, metrics and logs directly over OTLP/HTTP. Where they go is then read by the OpenTelemetry SDK from its standard variables (`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_EXPORTER_OTLP_PROTOCOL` and the per-signal forms), so authentication headers and TLS settings come from those. This works with any OTLP/HTTP-compatible backend (SigNoz, Grafana Tempo, Jaeger). Export failures never affect workflow execution. See [Distributed Tracing]({{< ref "/docs/observability/distributed-tracing" >}}).

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 dagnats serve
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

`dagnats serve --config=PATH` reads that file. Without `--config`, it searches `./dagnats.yaml`, then `$XDG_CONFIG_HOME/dagnats/dagnats.yaml`, then (Linux only) `/etc/dagnats/dagnats.yaml`. Format is simple `key: value` pairs, one per line. Lines starting with `#` are comments. Maximum 300 lines.

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

### Inline Leaf Credentials

`DAGNATS_LEAF_CREDENTIALS` accepts either a file path or inline PEM content. If the value starts with `-----BEGIN`, DagNats writes it to a secure temp file automatically. This is useful in CI/CD where mounting a credentials file is inconvenient:

```bash
DAGNATS_LEAF_CREDENTIALS="-----BEGIN NATS USER JWT-----
eyJ0eXAiOiJK...
------END NATS USER JWT------
-----BEGIN USER NKEY SEED-----
SUAM...
------END USER NKEY SEED------" dagnats serve
```

## NATS users

dagnats' embedded NATS server always has these users:

- **Anonymous.** A client that presents no credentials is root when `nats_user` is unset (open mode); one that presents credentials the server does not know (an unregistered NKey, a wrong login) is rejected in either mode. When `nats_user` is set (required mode), it is the join user, which may only publish `api.programs.register` and subscribe to `_JOIN.>`, so it can register a program and do nothing else.
- **The operator login**, `nats_user`, when set. Unrestricted.
- **One NKey user per registered program**, with the permissions its manifest derives (`program.PermissionsFor`). Every node builds them from the `programs` KV bucket at start and reloads them whenever the bucket changes. A program whose key is unchanged stays connected across a reload; a removed or replaced key is disconnected.

> **Programs are trusted.** These permissions are guardrails against mistakes, not an isolation boundary. A program can still write arbitrary data into the account through JetStream reply reflection (nats-server does not check a JetStream reply subject against the requester's permissions), so run only trusted programs until per-program NATS accounts land ([#909](https://github.com/danmestas/dagnats/issues/909)).

**Proof of possession.** A registration only claims a public key, so a new program is held **pending** for 60 seconds: its NKey user may publish nothing but `api.programs.confirm.<its public key>`. The program logs in with its key (NATS's NKey challenge proves it holds the seed) and confirms there; only then does it get its full permissions. A manifest change of a registered program is applied only on that confirmation, never on the anonymous registration. An unconfirmed registration expires, its join-key use is refunded and its name is free again. `program.Run` does all of this.

In required mode a program needs a join key to register. In open mode the key is optional, because anonymous is root anyway; that includes reading other clients' registration requests, so join keys protect only a required-mode server.

**Updates.** A registered program's change is authenticated by its own key (the confirmation above), so a change within its own prefix (task types, workflows, its own streams and buckets) needs no join key: a short-lived, single-use key that admitted the program does not block its later releases, and revoking it does not either. A change that widens the program's reach, a new protocol or a new read of another program's bucket, needs a join key in that request, usable and with names covering the program, every new protocol's root and every new read's owner; it spends a use, and without one it is refused `join_key_required_to_widen`, naming what widened. An open-mode server takes a keyless widening as it takes a keyless registration. Every update is still checked for protocol clashes, version range, budget and destructive changes. Like every program permission, this is a guardrail for trusted programs, not an isolation boundary: a program can already write elsewhere in the account through JetStream reply reflection, so run only trusted programs until per-program NATS accounts land ([#909](https://github.com/danmestas/dagnats/issues/909)).

## Control plane policy

By default, task handlers cannot access the control plane. To allow a task to register and spawn workflows at runtime, declare `capabilities: ["control-plane"]` in the workflow definition and grant the workflow via deployment policy.

The `policy.control_plane.grant` and `policy.control_plane.promote` settings are hot-reloadable and default to deny-by-default (empty or absent = no workflow has control-plane access).

```yaml
policy:
  control_plane:
    grant:   [planner, supervisor]
    promote: [supervisor]
```

- `grant` — workflows allowed to access the control plane (list of workflow names)
- `promote` — subset of `grant` that can spawn higher-privilege workflows; must be a subset

See [Runtime-Generated Workflows]({{< ref "/docs/ai-patterns/runtime-generated-workflows" >}}) for agent-runtime patterns and [Service discovery]({{< ref "/docs/operations/service-discovery" >}}) for how the control plane is exposed.

## Agent-runtime limits & retention

Agent runtimes (runtime-generated workflows) are bounded per generation tree (root run). The following limits are enforced at capability boundaries and return errors the agent loop can handle:

| Config Key | Environment Variable | Default | Meaning |
|------------|----------------------|---------|---------|
| `max_active_runs_per_root` | `DAGNATS_MAX_ACTIVE_RUNS_PER_ROOT` | `100` | Maximum non-terminal runs per root |
| `max_defs_per_root` | `DAGNATS_MAX_DEFS_PER_ROOT` | `500` | Maximum ephemeral workflow definitions per root |
| `max_generation_depth` | `DAGNATS_MAX_GENERATION_DEPTH` | `3` | Maximum spawn nesting depth (clamped to engine ceiling) |
| `max_registers_per_minute_per_root` | `DAGNATS_MAX_REGISTERS_PER_MINUTE_PER_ROOT` | `60` | Registration rate-limit per root tree |
| `runs_max_age` | `DAGNATS_RUNS_MAX_AGE` | unset (disabled) | Optional run-retention window; Go duration format (e.g. `"720h"`); when set, terminal runs older than the window are pruned |

`runs_max_age` is opt-in — when unset, run retention is unlimited. Negative values are rejected at load time. The `max_generation_depth` limit is clamped to the engine ceiling and any value exceeding it is rejected at config load.

## Viewing Effective Config

```bash
dagnats config show
dagnats config show --json
```

The `config show` command loads the resolved configuration (all three tiers merged) and prints it. Use `--json` for machine-readable output. This is the fastest way to verify which values are active. It prints the core keys and the console variables; it does not print the cluster keys, so check those in your file or environment.

Example output:

```
config_file:      (defaults)
data_dir:         /Users/you/Library/Application Support/dagnats
http_addr:        127.0.0.1:8080
nats_port:        4222
monitor_port:     (disabled)
leaf_remotes:     (none)
leaf_credentials: (none)
max_store_bytes:  10737418240
max_payload:      (nats default, 1048576)

console env vars:
  CONSOLE_CSRF_SECRET                    (random per-restart) (default)
  CONSOLE_READ_ONLY                      false (default)
  DAGNATS_CONSOLE_PASSWORD               (unset → basic-auth disabled) (default)
  ...
```
