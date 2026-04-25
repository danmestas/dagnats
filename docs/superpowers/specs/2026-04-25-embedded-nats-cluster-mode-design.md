# Embedded NATS Cluster Mode — Design Spec

**Date:** 2026-04-25
**Author:** Dan Mestas + Claude
**Status:** Draft (pending implementation)
**Scope:** v1 of a fifth deployment topology for dagnats: multiple dagnats instances form their own NATS cluster directly, without a separate external hub.

## Summary

Add embedded cluster mode to dagnats. Two or more dagnats instances each run their own embedded NATS server in cluster configuration, connecting directly to peers via NATS cluster routes. JetStream replicates state across nodes (R=3 or R=5). Failover and zero-downtime upgrades work without an external NATS deployment.

This complements the existing leaf-node modes documented in `docs/production.md`. It does not replace them — leaf modes remain the right answer when NATS is shared infrastructure used by other workloads. Embedded cluster mode is for teams that want self-contained HA with no external NATS to operate.

## Goals

1. Three or five dagnats instances form a NATS cluster automatically from config.
2. JetStream streams (`WORKFLOW_HISTORY`, `TASK_QUEUES`, etc.) and KV buckets created at the right replication factor without operator math.
3. Failover is automatic. Any node can fail; runs continue on the survivors.
4. Migration from existing single-binary deployments works in-place: edit config, restart, R bumps from 1 to 3.
5. The user-visible config surface stays small — four new fields, two of them optional.

## Non-goals (explicit, deferred to v1.1+)

- **Dynamic cluster membership.** v1 is fixed-size: declare 3 or 5 nodes at deploy time, planned rolling restart to change the count.
- **TLS / mTLS for cluster routes.** v1 ships token-based auth as the security knob.
- **`nats_cluster_advertise` for NAT'd Kubernetes deployments.** StatefulSet + headless service gives stable peer DNS without it. Add only if a real NAT'd use case surfaces.
- **Combined leaf + cluster mode.** A single dagnats process running in both modes simultaneously is technically possible in NATS but not exposed; operators wanting that hybrid use external `nats-server`.
- **OTel-emitted cluster lifecycle metrics** (peer connect/disconnect, leader change). Probably ADR-006 territory.
- **`dagnats nats migrate-replicas` explicit command.** Auto-derive plus `CreateOrUpdateStream` covers the migration path.

## User-visible surface

### Four new config fields

| Field | Type | Default | Env var | Required when |
|---|---|---|---|---|
| `nats_cluster_name` | string | `""` | `DAGNATS_NATS_CLUSTER_NAME` | clustering |
| `nats_cluster_routes` | `[]string` (comma-separated) | nil | `DAGNATS_NATS_CLUSTER_ROUTES` | clustering |
| `nats_cluster_auth_token` | string | `""` | `DAGNATS_NATS_CLUSTER_AUTH_TOKEN` | optional |
| `nats_jetstream_replicas` | int | `0` (auto) | `DAGNATS_NATS_JETSTREAM_REPLICAS` | optional override |

Two values fixed in v1, exposed only if real demand surfaces:

- **Cluster port:** hardcoded to `6222` (NATS convention). Operators don't need to know this exists for the common case.
- **Quorum-wait timeout:** hardcoded to 60 seconds. Long enough for cold-start DNS and peer connect; short enough that a misconfigured cluster fails fast.

### Validation rules (panic at startup, TigerStyle)

- Cluster mode is detected by `len(nats_cluster_routes) > 0`.
- When clustering: `nats_cluster_name` must be non-empty.
- `nats_cluster_routes` must have between 2 and 10 entries (3-node minimum, 11-node practical max).
- `nats_jetstream_replicas` must be in `{0, 1, 3, 5}`. Zero means auto-derive.
- Cluster mode and leaf mode are mutually exclusive: panic if both `nats_cluster_routes` and `leaf_remotes` are non-empty.

### Replica derivation

Auto-derived from cluster size when `nats_jetstream_replicas == 0`:

- Cluster size 3 (routes has 2 entries) → R=3
- Cluster size 4 (routes has 3 entries) → R=3 (round down to nearest odd)
- Cluster size 5 (routes has 4 entries) → R=5
- Cluster size 6+ → R=5 (NATS quorum cap)

Explicit override (`nats_jetstream_replicas: 3`) is for the rare case of intentionally choosing R<cluster_size to save disk on a 5-node deployment.

### Bind address logic

Three modes, one decision point in `server/nats.go`:

```
   cfg.NATSClusterRoutes set? ─── yes ─→ cluster mode (bind 0.0.0.0,
                              │           opts.Cluster set, no leaf)
                              │
                              └── no  ─→ cfg.LeafRemotes set?
                                              ┌── yes ─→ leaf mode (bind 0.0.0.0,
                                              │           opts.LeafNode set)
                                              │
                                              └── no  ─→ standalone mode
                                                          (bind 127.0.0.1, no extras)
```

### `/health/cluster` HTTP endpoint

Existing endpoints unchanged: `/health` (NATS connectivity), `/ready` (full startup), `/health/telemetry` (TELEMETRY stream). New `/health/cluster` reports cluster state:

```json
{
  "mode": "cluster",
  "expected_peers": 2,
  "connected_peers": 2,
  "leader": "nats://node-2:6222",
  "jetstream": {
    "leader_elected": true,
    "streams": {
      "WORKFLOW_HISTORY": { "replicas": 3, "in_sync": 3 },
      "TASK_QUEUES":      { "replicas": 3, "in_sync": 3 },
      "EVENTS":           { "replicas": 3, "in_sync": 3 },
      "DEAD_LETTERS":     { "replicas": 3, "in_sync": 3 },
      "SLEEP_TIMERS":     { "replicas": 3, "in_sync": 3 }
    },
    "kv_buckets": { "ok": 12, "lagging": 0 }
  },
  "ok": true
}
```

HTTP 200 when `ok: true`. HTTP 503 on degraded states (peer down, stream falling behind). For standalone and leaf modes, returns `{"mode": "standalone", "ok": true}` or `{"mode": "leaf", ...}` with HTTP 200 — useful for unconditional probes.

### `dagnats status` extension

Existing single-line status output gains cluster lines when applicable:

```
$ dagnats status
nats:        ok (jetstream ready)
mode:        cluster
peers:       2/2 connected
leader:      nats://node-2:6222
streams:     5/5 in-sync at R=3
kv buckets:  12/12 in-sync
```

`--json` prints the full `/health/cluster` payload. Standalone and leaf modes omit the cluster lines.

## Internal design

### Deep entry point: `natsutil.SetupAll`

The bootstrap dance — JetStream creation, optional quorum wait, R-derivation, stream creation, KV creation — all hides behind one call:

```go
package natsutil

type ClusterOptions struct {
    Routes           []string  // empty = not clustered, fall through to R=1 setup
    ReplicasOverride int       // 0 = auto-derive from len(Routes)+1
}

// SetupAll is the single entry point for NATS resource setup.
// When Routes is non-empty, blocks until cluster quorum forms (60s
// timeout) before creating streams at the derived replication factor.
// Callers do not need to know about cluster modes or R-derivation.
func SetupAll(ctx context.Context, nc *nats.Conn, cluster ClusterOptions) error
```

The caller in `server/server.go` collapses to:

```go
if err := natsutil.SetupAll(ctx, nc, natsutil.ClusterOptions{
    Routes:           cfg.NATSClusterRoutes,
    ReplicasOverride: cfg.NATSJetStreamReplicas,
}); err != nil {
    log.Fatalf("setup nats: %v", err)
}
```

`WaitForClusterQuorum`, `deriveReplicas`, and per-stream `Replicas` field plumbing are all hidden inside `natsutil`.

### Quorum-wait

`WaitForClusterQuorum(ctx, js, expectedSize)` polls every 500ms until:

1. NATS connection is healthy (`nc.IsConnected()`).
2. JetStream meta-leader is elected (`js.AccountInfo(ctx)` succeeds with no API errors).
3. Peer count equals `expectedSize - 1` (this node + N−1 peers).

Returns `context.DeadlineExceeded` if the bounded wait is exhausted, which the caller turns into `log.Fatalf`. The bounded timeout is 60s in `SetupAll`'s internal context.

### Stream and KV setup

`SetupStreams(js, replicas int)` and `SetupKVBuckets(js, replicas int)` take the derived replication factor and apply it to every `StreamConfig` and `KeyValueConfig`. `CreateOrUpdateStream` and `CreateOrUpdateKeyValue` are idempotent across the three lifecycle states:

- **Fresh-create** (no existing stream): creates at the requested R.
- **No-op** (existing stream already at requested R): no change.
- **Upgrade** (existing stream at R=1, requested R=3): updates Replicas in-place, JetStream replicates existing data to peers automatically.

This covers both cold-start of a fresh cluster and migration from an existing single-binary deployment without a separate code path.

### Bootstrap sequence in `server/server.go`

```
1. startNATS(cfg)                                   // existing, branches on mode
2. nc, _ := connectInternal(ns)                     // existing
3. natsutil.SetupAll(ctx, nc, ClusterOptions{...})  // new — all NATS setup hidden here
4. start orchestrator, API, triggers                // existing
```

Standalone and leaf modes pass `Routes: nil`; `SetupAll` skips the quorum-wait and uses R=1.

### NATS server cluster opts

In `server/nats.go`, when `len(cfg.NATSClusterRoutes) > 0`:

```go
opts.Cluster = natsserver.ClusterOpts{
    Name: cfg.NATSClusterName,
    Host: "0.0.0.0",
    Port: 6222,
    Routes: parsedRoutes,  // []*url.URL
}
if cfg.NATSClusterAuthToken != "" {
    opts.Cluster.AuthToken = cfg.NATSClusterAuthToken
}
```

Bind address resolution stays in the existing `host` variable: `127.0.0.1` for standalone, `0.0.0.0` for cluster or leaf.

### Test infrastructure

```go
package dagnatstest

// StartTestCluster starts n in-process NATS servers configured as a
// cluster, waits for quorum, returns a connection to peer 0 and a
// cleanup func registered with t.Cleanup. Panics if n < 3 or n > 5.
func StartTestCluster(t *testing.T, n int) (*nats.Conn, func())
```

Internals:

- Pre-allocate `2*n` free ports via `net.Listen(":0")` then close, gives N client ports + N cluster ports without port-collision races.
- Spin up N `natsserver.Server` instances with `Cluster.Routes` cross-referencing each other.
- Reuse the production `WaitForClusterQuorum` helper to confirm quorum before returning.
- Cleanup drains and shuts each peer in reverse order.

Used by `internal/natsutil/cluster_test.go` (cluster forms, R applied, restart preserves data), `server/cluster_test.go` (bootstrap end-to-end), `api/health_cluster_test.go` (endpoint shape).

### Code surface estimate

| Area | LoC |
|---|---|
| `server/config.go` — 4 new fields, env handling, validation | ~50 |
| `server/nats.go` — cluster opts when clustering | ~50 |
| `server/server.go` — collapse setup to single `SetupAll` call | ~5 (net negative) |
| `internal/natsutil/conn.go` — `SetupAll`, `WaitForClusterQuorum`, `deriveReplicas`, signature changes | ~120 |
| `api/health_cluster.go` — new endpoint | ~80 |
| `cli/status.go` — extend output for cluster | ~30 |
| `dagnatstest/cluster.go` — `StartTestCluster` | ~80 |
| **Runtime subtotal** | **~415** |
| `server/config_test.go` — validation tests | ~50 |
| `internal/natsutil/cluster_test.go` — quorum + R + migration | ~100 |
| `server/cluster_test.go` — bootstrap | ~80 |
| `api/health_cluster_test.go` — endpoint shape | ~50 |
| **Test subtotal** | **~280** |
| `docs/architecture/adr-005-embedded-nats-cluster-mode.md` | ~150 lines |
| `docs/production.md` — new "Self-clustered" topology row + section | ~80 lines |
| `docs/configuration.md` — four new fields | ~30 lines |
| **Docs subtotal** | **~260 lines** |
| **Total** | **~700 LoC + 260 lines docs** |

## Documentation changes

### New: `docs/architecture/adr-005-embedded-nats-cluster-mode.md`

Captures the rationale: why fixed-size for v1, why hybrid R-derivation, why optional auth, why disallow leaf+cluster combo, what's deferred to v1.1+. Decision-focused.

### Updated: `docs/production.md`

Adds a sixth topology row at the top of the table, distinguished from leaf modes:

| Topology | Hub shape | When to use |
|---|---|---|
| **Self-clustered** | none — dagnats nodes form their own cluster | self-contained HA without external NATS infrastructure |
| Leaf → clustered hub | 3+ external nats-servers in one DC | NATS is shared infra used by other workloads |
| Leaf → single-node hub | one external nats-server | small prod or hobby; hub is a SPOF |
| Leaf → supercluster | multi-cluster, gateway-connected | multi-region |
| Single binary | none (embedded only) | dev / eval / CI |
| Distributed | external cluster, components split | rare |

Plus a `### Self-clustered — embedded HA` subsection with config example, the R-derivation table, the quorum-wait behavior, and the auth token guidance.

### Updated: `docs/configuration.md`

Documents the four new config keys with defaults and validation rules, in line with existing config doc conventions.

## Backward compatibility

- **Existing single-binary deployments:** unaffected. No new fields are required; defaults preserve current behavior. Streams stay at R=1.
- **Existing leaf deployments:** unaffected. Cluster fields default to empty; the leaf+cluster mutual-exclusion validation only fires when both are explicitly set.
- **Existing API consumers:** `/health` shape unchanged; `/health/cluster` is new.
- **`SetupStreams` / `SetupKVBuckets` signature change:** internal-package only (`internal/natsutil/`). No external consumers to break.
- **Migration path single-binary → cluster:** edit config to add four cluster fields, restart all nodes within the 60s quorum-wait window, streams auto-update from R=1 to R=3 in place. Reversible by removing the cluster fields and restarting.

## Failure modes (explicitly tested)

| Scenario | Behavior | Test |
|---|---|---|
| Cold start, all peers up within 60s | Quorum forms, streams created at R=N | `cluster_test.go::TestColdStart` |
| Single node in cluster mode, peers never come | 60s timeout, `log.Fatalf`, exit | `cluster_test.go::TestQuorumTimeout` |
| Existing R=1 streams, cluster forms | Streams updated in-place, no data loss | `cluster_test.go::TestMigrateR1ToR3` |
| Network partition mid-startup | Minority half exits; majority half continues | `cluster_test.go::TestPartitionedStartup` |
| Operator typo: routes have 2 entries (3-node) but only 2 nodes deployed | Quorum never forms, timeout, exit | `cluster_test.go::TestUnderprovisioned` |
| Mismatched cluster names across nodes | Nodes can't connect, quorum-wait timeout | `cluster_test.go::TestMismatchedNames` |
| Auth token mismatch | NATS rejects route, quorum-wait timeout | `cluster_test.go::TestAuthTokenMismatch` |

## Open assumptions to verify in implementation

1. **`natsserver.Options.Cluster` API surface.** Skim of the package shows `Name`, `Host`, `Port`, `Routes` (`[]*url.URL`), `AuthToken` are all public. Need to confirm `WaitForClusterQuorum` can use `js.AccountInfo()` cluster info, or if a different API is needed.
2. **`CreateOrUpdateStream` for R upgrade preserves data.** Documented as idempotent; the migration test explicitly verifies "R=1 with messages → cluster forms → R=3, messages still readable, no duplicates."
3. **K8s StatefulSet + headless service works without `advertise`.** Pod DNS like `dagnats-0.dagnats-headless.default.svc.cluster.local` should resolve as cluster routes. Worth a kind-cluster smoke test before declaring v1.
4. **Quorum-wait against transient DNS failures.** During cold-start, peer DNS may not resolve immediately. The poll loop must distinguish transient (retry) from permanent (fail) errors.

## Implementation order (suggested PR breakdown)

Single PR with 5 commits, ordered for reviewability:

1. `feat(config)`: add the four new cluster fields, env handling, validation rules, `Config` tests
2. `feat(natsutil)`: introduce `SetupAll`, `WaitForClusterQuorum`, `deriveReplicas`; signature changes to `SetupStreams` / `SetupKVBuckets`
3. `feat(server)`: cluster opts in `server/nats.go`, collapse bootstrap to one `SetupAll` call in `server/server.go`
4. `feat(api+cli)`: `/health/cluster` endpoint, `dagnats status` extension
5. `feat(test+docs)`: `dagnatstest.StartTestCluster`, ADR-005, `production.md` and `configuration.md` updates

Each commit ships green tests; each is reviewable in isolation.
