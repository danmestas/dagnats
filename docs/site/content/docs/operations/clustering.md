---
title: Clustering
weight: 3
---

Three or five `dagnats serve` nodes can form their own NATS cluster, with JetStream replicating every stream and bucket across them. There is no external NATS to run. This page covers the configuration, what happens at startup, how to read cluster health, and how to upgrade a running cluster.

## When to cluster

A single node loses its data if its disk is lost, and it stops serving while it restarts. A cluster keeps serving with one node down (three nodes) or two nodes down (five nodes), because JetStream needs a majority of the replicas to accept writes.

Clusters are fixed-size. You declare the other nodes in each node's config, and changing the size means changing every node's config and restarting them one at a time. There is no dynamic membership. Clusters and leaf nodes are mutually exclusive: setting both `nats_cluster_routes` and `leaf_remotes` is a config error.

## Configuration

Set these on every node. Each is a `dagnats.yaml` key and has an environment variable that overrides it.

| Key | Environment variable | Default | Meaning |
|-----|----------------------|---------|---------|
| `nats_cluster_name` | `DAGNATS_NATS_CLUSTER_NAME` | (none) | Cluster name. Required when routes are set. Must match on every node. |
| `nats_cluster_routes` | `DAGNATS_NATS_CLUSTER_ROUTES` | (none) | Comma-separated route URLs of the **other** nodes. 2 to 10 entries. Setting it turns cluster mode on. |
| `nats_cluster_port` | `DAGNATS_NATS_CLUSTER_PORT` | `6222` | Route listener port. 1 to 65535, and different from `nats_port`, `monitor_port`, `nats_ws_port` and the `http_addr` port. |
| `nats_cluster_host` | `DAGNATS_NATS_CLUSTER_HOST` | `0.0.0.0` | Bind address of the route listener. Must be an IP address. |
| `nats_cluster_auth_token` | `DAGNATS_NATS_CLUSTER_AUTH_TOKEN` | (none) | Shared secret for routes. Must match on every node. |
| `nats_server_name` | `DAGNATS_NATS_SERVER_NAME` | `<cluster>-<hostname>-<cluster port>` | The node's NATS server name. Up to 64 characters of `A-Z a-z 0-9 _ - .`. |
| `nats_jetstream_replicas` | `DAGNATS_NATS_JETSTREAM_REPLICAS` | `0` (auto) | Replication factor: `0`, `1`, `3` or `5`. |

A config that breaks these rules stops the node at startup, before it joins.

Example for the first node of three:

```yaml
# dagnats.yaml on node-1
nats_cluster_name: dagnats-prod
nats_cluster_routes: nats://node-2.internal:6222, nats://node-3.internal:6222
nats_cluster_host: 10.0.0.1
```

```bash
# the token stays out of the file
export DAGNATS_NATS_CLUSTER_AUTH_TOKEN=...
```

Notes:

- **Routes list the other nodes.** The expected cluster size is `len(routes) + 1`. Route URLs are `nats://host:port`. Server keys in `dagnats.yaml` are read one `key: value` line at a time, so routes go on one line, and values are not `${VAR}`-expanded.
- **The token** is added to every route URL that has no credentials of its own, so you only set it, not embed it in the URLs.
- **Replicas.** With `0`, the factor is derived from the cluster size: 5 for five or more nodes, 3 for three or four. An explicit value overrides it. Every stream and bucket that `SetupAll` creates uses this factor.
- **Server name.** JetStream tracks peers by server name, so the name has to survive restarts. The default is stable: the cluster name, the hostname (characters outside the allowed set become `-`) and the route port. Two nodes on one host need different route ports, or different names. Set `nats_server_name` yourself if a host may be renamed.
- **Route TLS is not supported.** Routes use the shared token only, and traffic between nodes is unencrypted. Run the cluster on a private network or tailnet, set `nats_cluster_host` to the node's private IP so the route listener stays off public interfaces, and never expose the route port to the internet.

### Moving to or from a cluster

To move a single node to a cluster, add the cluster keys to each node and restart all of them within the 60-second quorum wait described below. Existing streams are updated in place from one replica to three.

## Startup

A clustered node does these in order:

1. Starts its embedded NATS server and dials the routes.
2. Waits up to 60 seconds for JetStream to answer. A probe that gets no reply after 2 seconds is sent again. If nothing answers in time, startup fails with `cluster quorum did not form`.
3. Creates its streams and buckets within one shared time budget. The budget is **3 minutes** in a cluster and **30 seconds** standalone, where a server that does not answer is broken and should fail fast.

Nodes starting together wait on each other's meta leader, so some provisioning calls get no answer at first. These are retried, up to 5 attempts of 8 seconds each per resource, with a pause that grows from 500ms to 4s:

- timeouts;
- JetStream error 10008, "system temporarily unavailable" (no meta leader yet, or a re-election);
- JetStream error 10005, "no suitable peers for placement" (a meta leader exists but has not seen enough peers for a replicated stream).

Any other error fails startup at once, and the message names the stream or bucket. A replicated stream or bucket is created first and updated only if it already exists with a different config, because on nats-server 2.15 an update of a stream whose create is still electing a leader gets no reply.

## Health

`GET /health/cluster` on each node reports the cluster as that node sees it. The same report is served over NATS: send an empty request to `api.cluster.health` (service `dagnats-api`) and read the JSON reply.

```bash
curl -s http://node-1.internal:8080/health/cluster
nats request api.cluster.health ''
```

Without cluster routes the response is `{"mode":"standalone","ok":true}`.

```json
{
  "mode": "cluster",
  "ok": true,
  "expected_peers": 2,
  "connected_peers": 2,
  "nodes_expected": 3,
  "nodes_up": 3,
  "node": "dagnats-prod-node-1-6222",
  "leader": "dagnats-prod-node-1-6222",
  "members": [
    {"name": "dagnats-prod-node-1-6222", "self": true, "route_connected": true, "meta_leader": true, "current": true},
    {"name": "dagnats-prod-node-2-6222", "route_connected": true, "current": true},
    {"name": "dagnats-prod-node-3-6222", "route_connected": true, "current": true}
  ],
  "jetstream": {
    "leader_elected": true,
    "streams": {"TASK_QUEUES": {"replicas": 3, "in_sync": 3}},
    "kv_buckets": {"workflow_runs": 3}
  }
}
```

(`streams` and `kv_buckets` list every provisioned stream and bucket; the example shows one of each.)

| Field | Meaning |
|-------|---------|
| `mode` | `standalone` without routes, `cluster` with them. |
| `ok` | `false` when the response is a 503. |
| `node` | The server that answered. Every node answers for itself, so poll each node. A NATS request has no queue group and reaches whichever node replies first. |
| `leader` | The JetStream meta leader. |
| `nodes_expected`, `nodes_up` | Configured size, and members that are route-connected and not offline (the answering node included). |
| `expected_peers`, `connected_peers` | The same counts without the answering node. |
| `members[]` | `name`, `self`, `route_connected`, `meta_leader`, `offline`, and `current` and `lag_ops`. |
| `jetstream.leader_elected` | Whether a meta leader exists. |
| `jetstream.streams` | Per stream: `replicas` configured and `in_sync` (the leader plus followers that are current). |
| `jetstream.kv_buckets` | Per bucket: configured replicas. |
| `jetstream.unavailable` | Replicated streams or buckets that did not answer. Quorum is lost for them. |
| `jetstream.degraded` | Unreachable single-replica assets. |

`current` and `lag_ops` appear only when the answering node is the meta leader. A follower's view reports the other followers as not current, so it leaves the fields out rather than report them wrongly. To read them, ask the node named in `leader`.

### Status

- **200** while the cluster has quorum. If only single-replica assets are unreachable, the names are under `jetstream.degraded` and the status stays 200, so a per-node load-balancer check does not drain the survivors because one node's unreplicated asset went down with it.
- **503** when replicated streams or buckets lose quorum (they are listed under `jetstream.unavailable`), or when membership cannot be read. An unreachable asset never hides the rest of the report.

`nodes_up` below `nodes_expected` does not by itself change the status. With three nodes and one down, the cluster still has quorum, the status is 200, and `nodes_up` shows 2 of 3.

`dagnats status` prints a cluster summary from the same endpoint when the server reports `mode: cluster`. It reads `DAGNATS_HTTP_ADDR`, defaulting to `localhost:8080`.

### Assets outside SetupAll

Every stream and bucket `SetupAll` provisions uses the cluster's replication factor, including `TELEMETRY`, `STICKY_TASKS`, `TRIGGER_HISTORY` and `leases`. Before 0.0.27 the first three were single-replica in a cluster; an existing deployment is upgraded in place when its nodes restart.

The buckets `dagnats serve` adds itself (`triggers`, `trigger_state`, `signals`, `checkpoints`, `concurrency_runs`) and the console's `console_audit` bucket are created without a replica count, so they are single-replica. They are not in the health report, and losing the node that holds one makes that bucket unavailable until the node returns. See [NATS Infrastructure]({{< ref "/docs/operations/nats-infrastructure" >}}).

## Rolling upgrade

A cluster upgrades without downtime if you replace one node at a time and wait for it to rejoin before touching the next.

1. Check that the cluster is healthy: `/health/cluster` returns 200 on every node, `nodes_up` equals `nodes_expected`, and every stream shows `in_sync` equal to `replicas`.
2. Stop one node. `dagnats serve` shuts down within 15 seconds. The other nodes keep quorum.
3. Start the new binary on that node with the same config, including the same `nats_server_name` or the same hostname and route port.
4. Wait for that node's `/health/cluster` to return 200 with `nodes_up` equal to `nodes_expected`, and for every stream to show `in_sync` equal to `replicas`. Read `current` and `lag_ops` from the node named in `leader`, since only the meta leader reports them.
5. Only then move to the next node.

Do not stop a second node while the first is still catching up. With three nodes, that leaves one current replica and the streams stop accepting writes.

Run workers against any node. They reconnect on their own when the node they were using restarts, and unacknowledged tasks are redelivered.

### Upgrading from before 0.0.27

Before 0.0.27 a clustered node derived its NATS server name from the process ID, so every restart joined as a new JetStream peer and left the old name behind as a replica that never came back. From 0.0.27 the name is `<cluster>-<hostname>-<cluster port>` and stays the same across restarts. A node upgrading from an earlier version therefore changes its name once, during the upgrade. If you want to keep an existing name, set `nats_server_name` to it. Names left behind by earlier restarts can show up as `offline` members until you remove them from the JetStream meta group with the NATS tooling.

## Testing

`make test-cluster-e2e` starts three `dagnats serve` processes on localhost, waits for `/health/cluster` to show three nodes, runs a workflow, kills one node, and checks that health stays 200 with 2 of 3 nodes up and that a second workflow completes. It runs in the `Cluster E2E` CI job. It proves process-level clustering on one host. It does not test networking between machines, partitions or clock skew.
