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
| `leaf_listen_allow` | []string | (none: every subject) | (none: every subject) |
| `nats_cluster_name` | string | (none) | (none) |
| `nats_cluster_routes` | []string | (none) | (none) |
| `nats_cluster_auth_token` | string | (none) | (none) |
| `nats_cluster_port` | int | `6222` | `6222` |
| `nats_cluster_host` | string | `0.0.0.0` | `0.0.0.0` |
| `nats_server_name` | string | `<cluster>-<hostname>-<cluster port>` | `<cluster>-<hostname>-<cluster port>` |
| `nats_jetstream_replicas` | int | `0` (auto-derive) | `0` (auto-derive) |
| `preferred_leaders` | []string | (none: off) | (none: off) |
| `retire_peers` | []string | (none: off) | (none: off) |
| `engine` | bool | `true` | `true` |
| `jetstream_domain` | string | (none) | (none) |
| `region` | string | (none) | (none) |
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

`leaf_port` (default `0`, off) opens a leafnode listener other servers dial with a `nats-leaf://` remote. `leaf_host` is its bind IP, `127.0.0.1` when unset; name a private or tailnet IP for a hub that leaves dial over the network. `leaf_listen_user` and `leaf_listen_password_file` are the login a leaf must present, and both are required whenever `leaf_port` is set, on loopback too: a linked leaf shares every subject and stream. Only that login can link; the anonymous user, `nats_user` and program keys cannot connect as a leaf. `leaf_listen_allow` (comma-separated subjects) limits what the link carries in both directions; unset carries every subject. A link between regions `eu` and `us` sets `$JS.eu.API.>, $JS.us.API.>, $JS.ACK.>, $JS.FC.>, _INBOX.>` on the listening side, so each region reaches the other's JetStream by domain while dagnats subjects stay in their region. The list restricts subjects, not trust: through `_INBOX.>` a client in one region can read the other's pull deliveries, and through `$JS.<domain>.API.>` it can read or delete the other's streams, so link only regions that trust each other ([#909](https://github.com/danmestas/dagnats/issues/909)). The listener works in embedded cluster mode. A cluster node may also set `leaf_remotes` to link two regions, if it has a `jetstream_domain` (or `region`) distinct from the other region's. The URLs in `leaf_remotes` are one hub cluster's leaf listeners: dagnats opens one leaf link to one of them and fails over to another when that node goes away. They are not parallel links; listing several hubs gives failover between them, not a link to each. They must all use one scheme, since the link has one TLS setting; a mix fails config load. On the dialing side, `leaf_user` and `leaf_password_file` log in to every leaf remote whose URL has no login of its own, with a plain user and password instead of the operator-mode creds file `leaf_credentials` takes; they are set together and exclude `leaf_credentials`. Password files are read at start, like `nats_password_file`. A leaf binds its client port to all interfaces unless `nats_host` is set, and a client there shares every subject of the hub, so on a laptop set `nats_host: 127.0.0.1` or `nats_user`; dagnats warns at start when neither is set, and when `leaf_host` is `0.0.0.0` or `::`, since the leaf login travels without TLS. A linked leaf has the hub's whole account, not only the streams it mirrors.

`jetstream_domain` names this server's JetStream domain, so a server linked to it over a leaf connection can reach its JetStream as `$JS.<domain>.API` (`jetstream.NewWithDomain(nc, "e-mac")`) while keeping its own `$JS.API`. Local clients, dagnats included, keep using plain `$JS.API`. Set it on a leaf node that runs its own JetStream, such as a laptop that works offline; leave it unset on the hub. The value must be one NATS subject token: no dots, spaces or wildcards, max 64 bytes. Non-ASCII letters are accepted, since they are legal in a NATS token. In embedded cluster mode the domain belongs to the whole cluster, so every node sets the same value.

`region` (`DAGNATS_REGION`) names a regional cluster. It sets both `nats_cluster_name` and `jetstream_domain` to its value, so the two cannot disagree; setting either explicitly to a different value fails config load. It follows `jetstream_domain`'s token rules. `/health/cluster` reports it as `region`, and the console header shows it.

On Linux, `data_dir` respects `XDG_DATA_HOME` if set.

The `nats_cluster_*`, `nats_server_name` and `nats_jetstream_replicas` keys are explained on the [Clustering]({{< ref "/docs/operations/clustering" >}}) page. `nats_cluster_routes` needs 2 to 10 entries, and `nats_cluster_name` is required whenever routes are set.

`engine: false` (`DAGNATS_ENGINE=false`) makes a node **replica-only**. It stays a JetStream voter and stream replica, but runs no dagnats work:
- **No work:** no orchestrator, no control plane (the `api.*` and `api.p.*` NATS services), no program registration, HTTP bridge, triggers, embedded workers or `dagnats.yaml` watcher.
- **Still runs:** its program users, so a program that dials it still logs in with its key; the leader rebalancer; and the disk monitor.
- **Requests go elsewhere:** a control-plane request sent through it is answered by an engine node in the same queue group.
- **HTTP:** it serves `/health`, `/health/cluster` (with `"replica_only": true`) and `/ready`. Every other route, the REST API and console included, answers 503 `replica-only node: engine off`.
- **Startup:** it logs `replica-only node: engine off`.

Use it for a node across a slow link, for example a home machine 150 ms to 1 s from two cloud nodes that are 1 ms apart. Its engine would otherwise pull a share of the cluster's work, and pay that link on every KV round trip. Pair it with `preferred_leaders` naming the other nodes. Keep at least one engine node.

`preferred_leaders` lists server names (`nats_server_name`) that should lead replicated streams. JetStream elects any in-sync replica, so after restarts and elections a stream's leader can land on any node, and every write to that stream goes through its leader. Set it when one node is far from the others, for example a laptop 160-700 ms from two cloud nodes that are 1 ms apart. Every write to a stream that node leads would cross the slow link. The node that is the JetStream meta leader checks 15 seconds after start and then every minute: a stream with more than one replica, whose leader is not preferred and which has an in-sync preferred replica, has its leader step down in favour of that replica. It moves at most 4 streams per check, rotates over the preferred servers, and leaves a moved stream alone for 5 minutes so nothing flaps. R1 streams are never touched.

The JetStream meta leader drifts the same way, and every stream and consumer create, update and info goes through it. So before the streams, a meta leader that is not preferred steps down in favour of a current, online preferred peer. That check ends there, and the new meta leader moves the streams on its next check. At most one meta move per check, and a node that moved the meta leader leaves it for 5 minutes. The meta group's API belongs to the system account, which clients cannot reach. dagnats moves it through a built-in system-account user that only dagnats itself dials, in process, with a random password per start.

Each move is logged at INFO and each failure at WARN. Empty, the default, leaves leaders where JetStream elects them.

`retire_peers` (string list, default none; env `DAGNATS_RETIRE_PEERS`) names servers (`nats_server_name`) leaving the cluster for good, for example a node moving to another region. Without it a switched-off node stays a member of the JetStream meta group: it still counts towards quorum, and every stream and consumer replica it held is lost redundancy. The node that is the JetStream meta leader checks 15 seconds after start and then every minute. A listed server that is still in the meta group and that this meta leader has seen offline, and not current, at every check for at least 2 minutes is removed with `$JS.API.SERVER.REMOVE`, by peer ID. The meta group keeps its members across restarts, so a meta leader that restarted after the server stopped still lists it, by peer ID alone, and removes it the same way. JetStream then re-places that server's replicas onto the remaining servers; if none fits, nats-server logs `could not replace peer` and the stream stays a replica short. One removal per check; an online server is never removed, and a change of meta leader restarts the 2 minutes. Each removal is logged at INFO and each refusal at WARN. nats-server answers only once the removal is applied, so a removal unanswered after 15 seconds is logged at INFO as sent, and the next check sees whether the server left. A server no longer in the meta group is logged once at INFO. Removal is through the same in-process system-account user as `preferred_leaders`. The list may hold at most 8 names, and may not name this node's `nats_server_name` or any `preferred_leaders` entry; either fails config load. Set it on every node before stopping the server: any node can be the meta leader, and only a meta leader with the list counts the 2 minutes. Remove the entry once the server has left.

## Environment Variables

### Core

| Variable | Overrides | Notes |
|----------|-----------|-------|
| `DAGNATS_DATA_DIR` | `data_dir` | |
| `DAGNATS_HTTP_ADDR` | `http_addr` | |
| `DAGNATS_NATS_PORT` | `nats_port` | Must be a valid integer |
| `DAGNATS_LEAF_REMOTES` | `leaf_remotes` | Comma-separated, max 10 entries; one link that fails over between them |
| `DAGNATS_LEAF_CREDENTIALS` | `leaf_credentials` | Path to NATS credentials file |
| `DAGNATS_LEAF_USER` | `leaf_user` | Login sent to leaf remotes; pairs with `DAGNATS_LEAF_PASSWORD_FILE` |
| `DAGNATS_LEAF_PASSWORD_FILE` | `leaf_password_file` | File holding the leaf remotes' password |
| `DAGNATS_LEAF_PORT` | `leaf_port` | Leafnode listener port; `0` (default) is off |
| `DAGNATS_LEAF_HOST` | `leaf_host` | Leafnode listener bind IP; default 127.0.0.1 |
| `DAGNATS_LEAF_LISTEN_USER` | `leaf_listen_user` | Login a leaf must present; required with `DAGNATS_LEAF_PORT` |
| `DAGNATS_LEAF_LISTEN_PASSWORD_FILE` | `leaf_listen_password_file` | File holding the listener's password |
| `DAGNATS_LEAF_LISTEN_ALLOW` | `leaf_listen_allow` | Subjects a linked leaf may publish and subscribe to; unset carries every subject |
| `DAGNATS_MONITOR_PORT` | `monitor_port` | NATS monitoring HTTP port |
| `DAGNATS_MAX_STORE_BYTES` | `max_store_bytes` | Must be a positive integer |
| `DAGNATS_MAX_MEMORY_BYTES` | `max_memory_bytes` | Bytes; also sets the Go soft memory limit; `0` or negative disables |
| `DAGNATS_NATS_CLUSTER_NAME` | `nats_cluster_name` | Required with cluster routes |
| `DAGNATS_NATS_CLUSTER_ROUTES` | `nats_cluster_routes` | Comma-separated, max 10 entries |
| `DAGNATS_NATS_CLUSTER_AUTH_TOKEN` | `nats_cluster_auth_token` | Shared token across peers |
| `DAGNATS_NATS_CLUSTER_PORT` | `nats_cluster_port` | Route listener port; default 6222 |
| `DAGNATS_NATS_CLUSTER_HOST` | `nats_cluster_host` | Route listener bind IP; default 0.0.0.0 |
| `DAGNATS_NATS_HOST` | `nats_host` | Client listener bind IP; default 127.0.0.1, or 0.0.0.0 on a leaf outside cluster mode |
| `DAGNATS_NATS_USER` | `nats_user` | The operator login on the client port and WebSocket listener; set with `nats_password_file`. Set: anonymous clients are the join user. Unset: anonymous is root. See [NATS users](#nats-users) |
| `DAGNATS_NATS_PASSWORD_FILE` | `nats_password_file` | File holding that user's password (chmod 600) |
| `DAGNATS_API_TOKEN_FILE` | `api_token_file` | File holding a bearer token every HTTP request must carry, except `/health`, `/health/cluster` and `/ready`. On the bridge's worker routes a token the bridge accepts also passes. Unset: open. Neither login covers `monitor_port` or cluster routes (use `nats_cluster_auth_token`) |
| `DAGNATS_NATS_SERVER_NAME` | `nats_server_name` | Stable server name; default derived from host |
| `DAGNATS_ENGINE` | `engine` | `false` makes the node replica-only (see below); default `true` |
| `DAGNATS_PREFERRED_LEADERS` | `preferred_leaders` | Comma-separated server names that should lead replicated streams; default none (off) |
| `DAGNATS_RETIRE_PEERS` | `retire_peers` | Comma-separated server names to remove from the JetStream meta group once offline 2 minutes; at most 8; default none (off) |
| `DAGNATS_JETSTREAM_DOMAIN` | `jetstream_domain` | JetStream domain a leaf hub addresses this server by; default none |
| `DAGNATS_REGION` | `region` | Region name; defaults `nats_cluster_name` and `jetstream_domain` |
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
- **One NKey user per registered program**, with the permissions its manifest derives (`program.PermissionsFor`, plus any `policy.archives` grant through `program.PermissionsGranted`). Every node builds them from the `programs` KV bucket at start and reloads them whenever the bucket changes. A program whose key is unchanged stays connected across a reload; a removed or replaced key is disconnected.
- **A system-account user**, `_dagnats_system`, with a random password per start that is never handed out. Only dagnats dials it, in process, to move the JetStream meta leader when `preferred_leaders` is set and to remove a server listed in `retire_peers`. No client reaches the system account.

> **A dagnats is one trust domain.** Every program on a dagnats is trusted by whoever runs it, so these permissions are guardrails against mistakes, not an isolation boundary. A program can still write arbitrary data into the account through JetStream reply reflection (nats-server does not check a JetStream reply subject against the requester's permissions), deliver a push consumer onto another program's subjects, or forge replies into another program's inboxes ([#909](https://github.com/danmestas/dagnats/issues/909), [#862](https://github.com/danmestas/dagnats/issues/862)). This is by design: parties that do not trust each other each run their own dagnats, rather than sharing one. A join key admits a program into that trust domain, so give one only to a party you would let run code on the host, and do not link (leaf) a dagnats to one whose operator you do not trust. Isolating untrusting programs on one dagnats would take one NATS account per program; that is out of scope for this model.

**Proof of possession.** A registration only claims a public key, so a new program is held **pending** for 60 seconds: its NKey user may publish nothing but `api.programs.confirm.<its public key>`. The program logs in with its key (NATS's NKey challenge proves it holds the seed) and confirms there; only then does it get its full permissions. A manifest change of a registered program is applied only on that confirmation, never on the anonymous registration. An unconfirmed registration expires, its join-key use is refunded and its name is free again. `program.Run` does all of this.

**Reading a bucket in another region.** A `Reads` entry may name the JetStream domain its bucket lives in, `<domain>:<bucket>` such as `eu:chat_cards` ([ADR-031](https://github.com/danmestas/dagnats/blob/main/docs/architecture/adr-031-supercluster-regions.md)). The program's user then gets the read crossings (stream info, direct get, consumer create, info and delete for a watch) as `$JS.<domain>.API.*`, which `jetstream.NewWithDomain(nc, "<domain>")` sends over the leaf link, instead of `$JS.API.*`, plus the same `$JS.FC.KV_<bucket>.>` flow-control grant; deliveries arrive on the program's own inbox. A bare entry reads the server's own domain. The domain must be one NATS token of at most 64 bytes; the bucket follows the bare rules, except that a domain-qualified read may name the program's own prefix, since its full access is local. `eu:chat_cards` and `chat_cards` are separate grants. The leaf link must carry `$JS.<domain>.API.>` and `$JS.FC.>`, as the region allow-list does.

In required mode a program needs a join key to register. In open mode the key is optional, because anonymous is root anyway; that includes reading other clients' registration requests, so join keys protect only a required-mode server.

**Archiving streams.** An `Archives` entry names a JetStream stream the program archives and purges. Declaring it grants nothing by itself: the operator must also grant it in `policy.archives` (see [Archive grants](#archive-grants)).

**Updates.** A registered program's change is authenticated by its own key (the confirmation above), so a change within its own prefix (task types, workflows, its own streams and buckets) needs no join key: a short-lived, single-use key that admitted the program does not block its later releases, and revoking it does not either. A change that widens the program's reach, a new protocol, a new read of another program's bucket or a new micro service in `Discovers`, needs a join key in that request, usable and with names covering the program, every new protocol's root and every new read's owner, the bucket's `<program>_` prefix in whatever domain the read names (a discovered service needs nothing more than the program: discovery reads only service metadata); it spends a use, and without one it is refused `join_key_required_to_widen`, naming what widened. An open-mode server takes a keyless widening as it takes a keyless registration. Every update is still checked for protocol clashes, version range, budget and destructive changes. Like every program permission, this is a guardrail, not an isolation boundary: a dagnats is one trust domain (see above), and a program can already write elsewhere in the account through JetStream reply reflection ([#909](https://github.com/danmestas/dagnats/issues/909)).

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

## Archive grants

A program that archives JetStream streams (lode, which drains streams into object storage and evicts what it has archived) declares them in its manifest's `Archives`, and the operator grants them to it in `policy.archives`. A program gets archive rights on a stream only when both name it: a declared stream the operator has not granted registers fine and grants nothing, a granted stream the program does not declare grants nothing, and the server logs a warning naming the declared, granted and effective streams whenever they differ. Absent = deny-by-default. The section is hot-reloadable: editing it reloads the program NATS users, and a connected program gains or loses the rights without reconnecting.

```yaml
policy:
  archives:
    lode: [AGENT_JOURNAL, WORKFLOW_HISTORY]
```

Each key is a program name, each value 1 to 32 distinct stream names of `A-Z`, `a-z`, `0-9`, `_` or `-`. On each effective stream `<S>` the program's user gets exactly: `$JS.API.STREAM.INFO.<S>` and `$JS.API.STREAM.PURGE.<S>`; `$JS.API.CONSUMER.LIST.<S>`, `$JS.API.CONSUMER.NAMES.<S>` and `$JS.API.CONSUMER.INFO.<S>.*` (every consumer, for ack floors); and `$JS.API.CONSUMER.CREATE.<S>.<program>_archive` (and `.>` after it, the filtered form), `$JS.API.CONSUMER.MSG.NEXT.<S>.<program>_archive` and `$JS.API.CONSUMER.DELETE.<S>.<program>_archive`. The program drains through that one consumer name (`program.ArchiveConsumer`), a plain named consumer with `AckNone` and an inactive threshold: a NATS permission can pin a whole name but not a name prefix, so nats.go's ordered consumer, whose name gains a new serial on every fetch, cannot be granted. There is no ack grant, no legacy `DURABLE.CREATE`, no publish on the stream's subjects and no stream update or delete.

> **Purge is powerful.** The purge request body (sequence, filter, keep) cannot be constrained by a subject, so an archive grant lets the program purge the whole stream, not only below the floors it read; the consumer create body likewise decides whether its one consumer is durable. Consumer info and list disclose every consumer on the stream, so never grant `STICKY_TASKS`, whose consumer names are capabilities. Grant only programs you trust with the stream's data. Like every program permission this is a guardrail, not isolation ([#909](https://github.com/danmestas/dagnats/issues/909)): the grant is declared so per-program accounts can enforce it later.

### Purging the agent journal: the protected floor

Agent steps re-read their journal on every delivery: `Open` replays the
current iteration's model and tool calls and starts from the last
committed state. Those reads hold no consumer, so ack floors do not
cover them. dagnats publishes the bound instead, as metadata on
`AGENT_JOURNAL` that `STREAM.INFO` returns:

| Key | Value |
|-----|-------|
| `dagnats.protected_floor` | Decimal stream sequence. Nothing at or above it may be purged. |
| `dagnats.protected_floor_at` | RFC3339 time the floor was computed. |

The floor is the lowest sequence a live run's agent step still reads:
for each non-terminal run, for each step journal, the commit of the
iteration before the step's current one (the state it starts from) or
the current iteration's first record, whichever is lower. A step with
no earlier commit is protected from its first record; a fork is
protected from its first record and so is each parent journal it
reads. With no live journal the floor is the stream's `LastSeq + 1`.
Records of earlier, finished iterations sit below it, so a long chat
does not pin the journal from its first turn.

The JetStream meta leader writes it. It is written when its value
changes, never more often than once a minute, and rewritten at least
every 5 minutes when unchanged, so `dagnats.protected_floor_at` stays
fresh during a long iteration or a quiet journal.

**The archiver's contract.** Never purge at or above
`dagnats.protected_floor`. Treat the floor as 0 (purge nothing) when
the key is missing or malformed, or when `dagnats.protected_floor_at`
is missing, malformed, in the future, or older than 10 minutes (twice
the refresh interval). A missing or stale floor is normal while the
writer is down, while a pass is over its budget (more than 10,000 live
runs, 1,024 journals in one run, or 500,000 journal records across live
runs: the pass is skipped and logged), or while an older dagnats binary
leads, so purging simply pauses. Provisioning keeps these keys (and any
other stream metadata it did not set) across restarts; an older binary's
provisioning may wipe them, which an archiver reads as "purge nothing"
until the leader rewrites them on its next pass.

### Reading purged journal records from the archive

Once an archiver purges `AGENT_JOURNAL` below what it archived, a journal read can need records that are no longer in JetStream. dagnats reads them back from the archive over NATS request/reply, so a step's replay is the same before and after the purge. Nothing purges `AGENT_JOURNAL` until the archiver is told to (lode keeps it off by default); until then this path is never taken.

**When the archive is read.** Every journal record carries `Nats-Expected-Last-Subject-Sequence`, the step's previous record, which the server enforced when it stored it. A read that finds its first record naming a predecessor it does not hold asks the archive for the step's subject from sequence 1 up to the stream's `FirstSeq` (exclusive), then continues from JetStream at `FirstSeq`. The same chain proves the two parts meet: every record must name the one before it, so a missing or duplicated record at the boundary fails the read. A journal JetStream still holds in full never reaches the archive. `Open` (and the relay serving a program's `Open`) also needs nothing before the newest commit of an earlier iteration, which is where the protected floor sits: when that commit is the first record JetStream holds, `Open` checks the chain from there and never asks the archive, so purging down to the floor costs a live step no archive read. A full read (`journal.Read`, the bridge's `journal.read`, `journal check`, `GET .../journal`) still reads the archive for everything below. Purging past the floor loses that commit, and `Open` then fails with `journal.ErrArchive` unless the archive has it.

**A step purged whole.** When JetStream holds no record of a step at all, no record is left to name a predecessor, so the read asks the archive only when the step can have had history: a later iteration or a retry (iteration above 0 or attempt above 1, from the dispatch or the run snapshot), or a fork's parent (the fork names one of its records). A brand-new step's first open never asks. Nothing is asked either when the stream was never purged past the step (`FirstSeq` is not beyond it). A later iteration or a fork's parent that the archive holds nothing of fails with `journal.ErrArchive`: a later iteration starts from an earlier commit, and replaying it from nothing would run its unsafe tools again. A retry of iteration 0 may have failed before it ever claimed, so an empty archive there is no history. The first record written after such a read carries `Dagnats-Journal-Prev-Seq`, the archived tail, because the server requires it to expect 0, and later reads follow the chain through it. The client read `GET .../journal` applies the same rule from the step's iterations and attempts.

Every reader does this: `journal.Open` and `journal.Read` (Go workers, `dagnats journal check`, forks, the engine's and the API's inputs checks), the bridge's `journal.view`, `journal.read` and `load_checkpoint`, the engine relay's `journal.view` for a program's worker, and `GET /runs/{run}/steps/{step}/journal` (pages and follows).

**Failure is loud.** The read fails, naming the run and step, with `journal.ErrArchive` when no archive answers, the archive answers `not_archived` (it cannot confirm continuity up to `FirstSeq`) or `unknown_epoch`, reports any gap in the range (a must-keep stream cannot replay across a loss), stays `busy` through its bounded retries (5, backing off 0.5s more each time), answers `not_indexed` (its subject index is still being built; not retried) or any other error code, or times out (15s a request, 2 minutes a read: object storage is slow, so the archive read has its own deadline, not the caller's; an explicit cancel still stops it). A broken chain is `journal.ErrDiscontinuous`. A worker never replays a journal with records missing. The bridge answers the action with an error, the program relay refuses with `archive`, and the client read answers 503. A step whose every record was purged reads as empty, because no record is left to name a predecessor, so an archiver must never purge all of a live step's records.

**The range protocol.** Subject `lode.stream.<domain>.<stream>.range`, where `<domain>` is the server's `jetstream_domain`, or `default` when it has none (`dagnatsext.ArchiveRangeSubject`). The server logs a warning at startup when `jetstream_domain` is literally `default`, since it then shares the subject with every server that has no domain. Request:

```json
{"epoch":1700000000123456789,"subject":"journal.<run>.<step>","from_seq":1,"to_seq":4812,"offset":0,"max_bytes":524288}
```

`epoch` is the stream's `StreamInfo.Created` in Unix nanoseconds, an integer, never a float. `to_seq` is exclusive and is the stream's `FirstSeq`. `offset` is the byte offset into the message at `from_seq`. Reply:

```json
{"messages":[{"seq":17,"subject":"journal.<run>.<step>","time":"2026-11-01T12:00:00.123456Z","headers":{"Dagnats-Journal-Type":["claim"]},"data":"<base64>"}],"next_seq":18,"next_offset":0,"more":false,"gaps":[],"done":false}
```

A page with no messages, `done:false` and a later `next_seq` is not the end: lode stops filling a page before the request timeout on a sparse subject, and dagnats keeps paging. A page that does not move `next_seq` (or `next_offset` inside a split message) fails the read. `more:true` means the last message is split: dagnats asks again at `next_seq` with `next_offset` and joins the parts. An error reply is `{"error":"...","code":"unknown_epoch|not_archived|not_indexed|busy|..."}`, and `not_archived` carries `"floor":N`. Message times are RFC3339: exact for messages lode archived from main-d047c52 on, microsecond precision for earlier ones. Go code uses the protocol through `dagnatsext.ArchiveReader` (`archivenats.NewReader`), which pages, joins split messages and retries `busy`, and returns complete messages in sequence order.

**Who may speak it.** Only dagnats asks, as its own root connection, after the read's ownership check (a program reads only its own step's journal through the relay; a bridge worker only its claimed task's). No program may publish under `lode.stream.>`: `program.PermissionsGranted` denies it to every program, whatever its name, protocols or `Archives`. A program with an effective archive grant on stream `<S>` may subscribe `lode.stream.<domain>.<S>.range` and answers on the requester's `_INBOX.>`, which every program may publish. A program with no effective archive grant is denied subscribe on `lode.stream.>`, so it cannot pose as an archive. lode, which runs as the program `lode`, also holds its own `lode.>`, which covers the range subjects of streams it was not granted; it is the archiver, already trusted with purge. A program's worker reads the archive through the relay one record per request, so a long purged journal replays slowly there.

## Agent-runtime limits & retention

Agent runtimes (runtime-generated workflows) are bounded per generation tree (root run). The following limits are enforced at capability boundaries and return errors the agent loop can handle:

| Config Key | Environment Variable | Default | Meaning |
|------------|----------------------|---------|---------|
| `max_active_runs_per_root` | `DAGNATS_MAX_ACTIVE_RUNS_PER_ROOT` | `100` | Maximum non-terminal runs per root |
| `max_defs_per_root` | `DAGNATS_MAX_DEFS_PER_ROOT` | `500` | Maximum ephemeral workflow definitions per root |
| `max_generation_depth` | `DAGNATS_MAX_GENERATION_DEPTH` | `3` | Maximum spawn nesting depth (clamped to engine ceiling) |
| `max_registers_per_minute_per_root` | `DAGNATS_MAX_REGISTERS_PER_MINUTE_PER_ROOT` | `60` | Registration rate-limit per root tree |
| `runs_max_age` | `DAGNATS_RUNS_MAX_AGE` | unset (no age pruning) | Optional age limit for terminal runs; Go duration (e.g. `"168h"`) or `30d`/`2w`; when set, terminal runs older than the window are pruned |

`runs_max_age` is opt-in. Whether or not it is set, the engine prunes the oldest terminal runs once `workflow_runs` passes 8% of `max_store_bytes`, so the bucket stays bounded. Negative values are rejected at load time. The `max_generation_depth` limit is clamped to the engine ceiling and any value exceeding it is rejected at config load.

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
