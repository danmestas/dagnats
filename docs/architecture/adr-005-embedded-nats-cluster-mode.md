# ADR-005: Embedded NATS Cluster Mode

**Status:** Accepted (2026-04-25)
**Deciders:** Dan Mestas
**Spec:** [`docs/superpowers/specs/2026-04-25-embedded-nats-cluster-mode-design.md`](../superpowers/specs/2026-04-25-embedded-nats-cluster-mode-design.md)

## Context

DagNats currently supports two NATS topologies via its embedded server: standalone (single binary, single host) and leaf (the embedded NATS connects out to an external hub). Production deployments needing HA must run leaf mode against a separate NATS cluster they operate themselves, or pay for Synadia Cloud.

Some teams want HA without the operational burden of a separate NATS cluster. They want a single artifact (`dagnats serve`) that, when N copies are deployed and pointed at each other, forms its own NATS cluster directly.

## Decision

Add a third NATS topology — embedded cluster mode — exposed via four optional config fields: `nats_cluster_name`, `nats_cluster_routes`, `nats_cluster_auth_token`, and `nats_jetstream_replicas`. When `nats_cluster_routes` is non-empty, the embedded NATS server starts in cluster mode at port 6222, JetStream streams and KV buckets are created at the auto-derived (or explicitly overridden) replication factor, and the dagnats orchestrator waits for cluster quorum before accepting work.

Implementation hides behind one entry point in `internal/natsutil`: `SetupAll(nc, opts...SetupOption)` learned a new `WithCluster(ClusterOptions)` option. The caller in `server/server.go` does not branch on topology — `SetupAll` picks the right behavior internally.

V1 ships fixed-size clusters (3 or 5 nodes declared in config; planned rolling restart to change shape) with optional token auth. TLS, dynamic membership, and `advertise` for NAT'd K8s are explicit non-goals.

## Alternatives considered

**A. Recommend external `nats-server` cluster + leaf mode.** No new code; operators run their own cluster. Rejected because this is exactly what some teams want to avoid — the operational burden of a separate cluster is the friction we're addressing.

**B. Dynamic cluster membership in v1.** Nodes can join and leave after bootstrap. Rejected as YAGNI for v1; fixed-size covers the typical "3 or 5 nodes, never changes" deployment.

**C. Required TLS for cluster routes.** Forces operators to set up cert handling before clustering. Rejected for v1 because it's a meaningful adoption barrier; token-based auth ships in v1, TLS in v1.1.

**D. New `SetupAll(ctx, nc, ClusterOptions)` signature.** The original design had this. Rejected after `/ousterhout` audit and during implementation: the existing variadic-option pattern (`SetupAll(nc, ...SetupOption)`) accommodates `WithCluster` cleanly without breaking 22 existing call sites.

## Consequences

**Positive:**
- Self-contained HA with no external NATS to operate.
- Existing single-binary deployments can migrate by adding four config fields and rolling restart.
- The streams' R-factor change is automatic via `CreateOrUpdateStream` — no manual `nats stream update` step.

**Negative:**
- Adds ~700 LoC of code + tests. Maintenance surface grows.
- Cluster bootstrap reliability is a new failure class to debug.
- Operators get one more topology to choose from, increasing decision burden up front (mitigated by `docs/production.md` decision tree).

**Neutral:**
- Leaf mode is unchanged. Operators with existing leaf deployments are unaffected.

## API deviations from spec (worth noting)

Three deviations surfaced during implementation, all driven by the actual nats-server v2.12.6 Go API:

1. `Routes` lives on `Options`, not `Cluster.ClusterOpts`. Set `opts.Routes = parsedRoutes` rather than `opts.Cluster.Routes`.
2. `ClusterOpts` does not expose `AuthToken`. The shared-secret behavior is delivered via `Cluster.Username` instead. Functional equivalence; the config field name `nats_cluster_auth_token` is preserved for operator clarity.
3. JetStream cluster mode requires `ServerName`. Auto-derived as `<cluster-name>-<pid>` when not explicitly set. Operators can override via `Options.ServerName` if needed in v1.1.

## Out of scope (deferred to v1.1+)

Dynamic membership, TLS / mTLS for cluster routes, `advertise` address, combined leaf+cluster mode, OTel cluster lifecycle metrics, explicit `dagnats nats migrate-replicas` command.
