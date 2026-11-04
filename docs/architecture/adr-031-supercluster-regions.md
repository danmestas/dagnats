# ADR-031: Supercluster regions

**Status:** Accepted (2026-11-04)
**Deciders:** Dan Mestas
**Depends on:** ADR-005, ADR-030

## Context

ADR-030 made a cluster one region and named a second regional cluster as
the way to give a far site its own engine and quorum. This ADR designs
that for `dagnats-mesh`:

- **Region `eu`:** the existing cluster. `mesh-vps` and `mesh-eu`, two
  Hetzner hosts in Germany 1.3 ms apart, plus a new Hetzner server in
  Helsinki, which replaces `mesh-mini` as the third EU voter. Its round
  trip to the German pair is estimated at 20 to 30 ms, to be measured
  once it is up. That is inside ADR-030's bound of about 100 ms, though
  above the single-digit milliseconds ADR-030's migration hoped for; the
  leader placement below keeps it off the commit path.
- **Region `us`:** a new three-node cluster on the owner's machines over
  LAN and Tailscale. `mesh-mini` (Mac mini), the iMac (x86_64, macOS 13)
  and the owner's MacBook Pro (M4).

The starting brief joins the two with NATS gateways, making a
supercluster. Reading nats-server v2.15.2 shows that a gateway supercluster
is one JetStream system, and that this defeats the reason for having a
second region. The finding drives every decision below, so it comes
first.

### What a gateway supercluster means for JetStream

- **One meta group across every cluster.** The meta group has one fixed
  name, `_meta_` (`server/jetstream_cluster.go:770`). Its Raft traffic
  runs on `$NRG.V._meta_`, `$NRG.AE._meta_` and so on
  (`server/raft.go:2684-2690`) in the system account, which gateways carry
  like any other account. Nothing in `server/gateway.go` separates
  JetStream domains. The only place nats-server enforces a domain
  boundary is a leafnode connection, which denies `$NRG.>`, `$JSC.>`,
  `$JS.API.>`, `$KV.>` and `$OBJ.>` when the two sides' domains differ
  (`server/leafnode.go:2156-2180`, deny list at
  `server/jetstream_api.go:372`). So per-region JetStream domains are not
  available over gateways: a supercluster is one domain.
- **Names and subjects are unique per account across the supercluster.**
  Stream create refuses a name already assigned in the account
  (`server/jetstream_cluster.go:11058`) and a subject that overlaps another
  stream's (`server/jetstream_cluster.go:11073`). Both regions cannot each
  have a `WORKFLOW_HISTORY` bound to `history.>` in the same account.
  dagnats names its streams and buckets as constants
  (`internal/natsutil/conn.go:194-261`, `308-403`), so the regions would
  need separate NATS accounts or renamed streams.
- **Placement is per cluster, but metadata is not.** A stream without a
  placement lands in the cluster the request came from
  (`server/jetstream_cluster.go:10960-10969`), and `placement.cluster`
  pins it. Its Raft group then stays in that cluster, so stream writes and
  their quorum are local. Creating or changing a stream or a consumer is
  still a meta proposal (`server/jetstream_cluster.go:12756` for
  consumers), and the JetStream API refuses those requests while the meta
  group has had no leader for `lostQuorumInterval`, 10 s (stream create
  at `server/jetstream_api.go:1555`, consumer create at `5058-5075`,
  interval at `server/raft.go:322`).
- **Six meta voters split three and three.** A meta quorum is four. When
  the transatlantic link drops, neither region has four, so neither can
  create a stream or a consumer. dagnats creates consumers all the time,
  not only at startup: every KV watch and ordered consumer is an ephemeral
  consumer, recreated after each reset, and `SetupAll` creates or updates
  every stream when a node starts (`internal/natsutil/conn.go:660`). A
  node that restarts during the partition cannot come back.
- **The meta group is a stretched Raft group.** Its heartbeats and
  elections cross the home internet link, with the fixed timers ADR-030
  measured (`server/raft.go:317-323`). With `preferred_leaders` moving the
  meta leader to EU (#932), every US consumer create pays a transatlantic
  round trip, and a slow link causes meta elections. This is ADR-030
  symptom 3, confined to metadata but still present.

ADR-030 gave one reason for a second cluster: "a second region that must
keep working when the link to the first is down". A gateway supercluster
does not deliver that. Each region would keep its stream quorums, but lose
its ability to create consumers or restart nodes during a partition.

## Decision

**Each region is its own NATS cluster with its own JetStream domain.** The
cluster name and the JetStream domain are both the region name: `eu` and
`us`. Each region has its own meta group, streams, KV buckets, engine,
programs and join keys. A region keeps working, including restarts and
new consumers, when the other region or the link between them is gone.

**The regions are linked by a leafnode connection, not gateways.** This
replaces the gateway supercluster in the starting brief, for the reasons
in the Context. Every
US node dials the EU nodes' leafnode listeners. A leafnode link is the
mechanism nats-server provides for two JetStream domains to coexist
(`server/leafnode.go:2156-2180`). It maps `$JS.<domain>.API` to the remote
domain's API for non-system accounts (`server/leafnode.go:2208-2215`), so
one region can still reach the other's JetStream when it addresses it by
domain. The link is optional for correctness: either region works with it
down. dagnats needs no gateway support for this design.

**The link carries an allow-list, not everything.** A stream's ingest
subscription is an account subscription that propagates interest to
leafnodes and gateways like a client's (`server/client.go:3216-3223`).
With the same stream names and subjects on both sides, an unfiltered link
would store a `task.>` publish from one region in both regions' task
queues, and both engines would run it. The leaf login on the EU listener
therefore permits only:

- `$JS.eu.API.>`, `$JS.us.API.>` and replies on `_INBOX.>`, for explicit
  cross-domain JetStream access (operator inspection with a domain, and
  mirrors or sources if adopted later).
- The mirror and source delivery subjects those need, pinned by the PR
  that adds the allow-list.

No dagnats subject (`task.>`, `history.>`, `event.>`, `sleep.>`,
`api.>`, `$KV.>`) crosses the link. A test in that PR publishes on
`task.>` in one region and asserts the other region's `TASK_QUEUES` does
not store it.

### 1. Configuration

Per region, the existing keys:

| Key | eu nodes | us nodes |
|---|---|---|
| `nats_cluster_name` | `eu` | `us` |
| `jetstream_domain` | `eu` | `us` |
| `nats_cluster_routes` | the other EU nodes | the other US nodes |
| `nats_cluster_host` | tailnet IP | tailnet or LAN IP |
| `preferred_leaders` | `mesh-vps`, `mesh-eu` | `mesh-mini`, the iMac |
| `leaf_port`, `leaf_host`, `leaf_listen_user`, `leaf_listen_password_file` | set (#866) | unset |
| `leaf_remotes`, `leaf_user`, `leaf_password_file` | unset | the EU leaf listeners |

`leaf_host` binds the EU listener to the tailnet address, so the link is
encrypted by Tailscale and not exposed on the public interface.

New configuration:

- **`region`.** One key that defaults `nats_cluster_name` and
  `jetstream_domain` to the same value, so the two cannot disagree. Either
  key set explicitly to a different value is a load error. `/health/cluster`
  and the console report it.
- **`leaf_listen_allow`.** The publish and subscribe allow-list for the
  leaf login on the listener (nats-server `LeafNodeOpts.Users` with
  permissions, `server/opts.go:191`). Unset keeps today's behaviour, which
  ADR-030's single-machine leaf needs: core request/reply to the cluster.
  A region link sets it to the list above.
- **Clustered leaf.** `validateClusterConfig` panics when
  `nats_cluster_routes` and `leaf_remotes` are both set
  (`server/config.go:1435`). nats-server supports a cluster whose members
  each hold a leaf remote. The check is relaxed to allow both when
  `jetstream_domain` is set and differs from the remote's, which
  nats-server reports when the link comes up.

**EU leaders stay on the German pair.** Helsinki is a voter and holds a
replica of every R3 stream, but it is not in `preferred_leaders`, so the
rebalancer keeps stream leaders and the meta leader on `mesh-vps` and
`mesh-eu`. An R3 commit needs the leader plus one follower, and the other
German node acknowledges in about 1.3 ms, so Helsinki's round trip does
not sit on the commit path while both German nodes are up.

`preferred_leaders` keeps working per region: the rebalancer acts only
while its node leads its own domain's meta group (`server/leaders.go:17-40`),
and each domain has its own.

### 2. JetStream across regions

One domain per region, as decided above. Streams and buckets are created
by `SetupAll` exactly as today, with no `placement` set: each region's
nodes only see their own meta group, so placement is implicitly regional.
Replicas stay R3 in both regions (`internal/natsutil/cluster.go:42`).

A single meta group spanning both regions is not acceptable here, for the
partition and latency reasons in the Context. It would be acceptable for
two regions joined by a stable cloud backbone well under 100 ms, where a
partition is rare and a third region could hold the meta tie-break. Not
this deployment.

### 3. Engine per region

Each region's `dagnats serve` nodes run the orchestrator over their own
domain's streams. The engine, worker and program code use plain
`$JS.API` (`worker/worker.go:418`, `server/server.go:311`), which on a
node with a domain is that node's own domain. No code change is needed
for this. Work stealing between regions cannot happen: the task queues
are different streams, and `task.>` does not cross the link.

### 4. Workflow and run homing

**A run is homed in the region that started it, for its whole life.** Its
history, timers, task queue entries and state live in that region's
streams and buckets. Nothing moves a run between regions.

**A client reaches a region by connecting to it.** A CLI, program or HTTP
caller that starts or queries a run in `us` connects to a `us` node. A
query for a run ID in the wrong region returns not found, as for any
unknown run. Subject-level routing over the link (letting `api.>` queue
groups fall back across regions) is rejected for now: with the same
subjects on both sides, a request would be served by whichever region
has local interest, which hides the region from the caller and splits a
workflow's runs across regions by accident.

A region-addressed control plane, such as the API also answering
`region.<name>.api.>` and that prefix crossing the link, is deferred until
a workflow in one region needs to start or signal a run in the other.

### 5. Programs and registration

Per region. `KV_programs` and `KV_join_keys`
(`internal/natsutil/conn.go:322-325`) exist in each domain, so a program
registers with the region it runs in, using a join key minted there, over
`api.programs.register` (`program/register.go:10`). Its NKey users are
loaded only by that region's nodes. The scoped control plane
`api.p.<name>.*` and the engine relay (`internal/api/natsapi_programs.go:29-33`,
`internal/natsutil/relay.go:16-24`) are served by that region's API queue
group and do not cross the link. A program that runs in both regions
registers in both, with the same seed and separate grants.

Global programs or join keys are rejected: they need a single writer
across regions, which is the meta problem again.

### 6. Failure modes

- **Regions partitioned.** Both keep working. Each has its own meta
  group, stream quorums and engine. The leaf link reconnects when the
  network returns. Only cross-domain requests fail, with a timeout.
- **Helsinki down or slow.** EU keeps quorum on the German pair, and
  commits are unaffected because Helsinki is never the deciding ack while
  both German nodes are up. Its replicas catch up when it returns.
- **One German node down.** EU keeps quorum with the other German node
  and Helsinki. Commits then need Helsinki's acknowledgement, so EU write
  latency rises to roughly its round trip (estimated 20 to 30 ms). Leaders
  the lost node held re-elect onto the surviving German node, a 4 to 9 s
  pause (`server/raft.go:317-323`). If the election lands a leader on
  Helsinki, the rebalancer moves it back once a German node is available.
- **Both German nodes down.** EU loses quorum; Helsinki alone cannot
  commit. US is unaffected.
- **MacBook asleep.** US keeps two of three voters, so stream and meta
  quorums hold. Streams the MacBook led re-elect, a 4 to 9 s pause
  (`server/raft.go:317-323`); `preferred_leaders` naming only the mini and
  the iMac keeps leaders off it, so this is rare. When it wakes, its
  replicas catch up.
- **MacBook asleep and one more US node down.** US loses quorum. Writes
  stall and the US engine stops making progress until a second node
  returns. EU is unaffected. This is the main US risk.
- **MacBook away from home.** Over Tailscale from elsewhere it becomes a
  WAN voter inside US, with ADR-030's elections and slow quorum writes
  whenever its link is poor.

**Operating rule for the MacBook.** The MacBook Pro is the third US voter
on these terms:

1. It votes only while it is on AC power and on the home LAN.
2. Sleep is disabled on AC: `sudo pmset -c sleep 0 disksleep 0` and
   `sudo pmset -c tcpkeepalive 1`, with `dagnats serve` run by a launchd
   job wrapped in `caffeinate -s`.
3. Before it is unplugged or leaves the house, `dagnats serve` on it is
   stopped cleanly.

The risk this accepts: whenever the MacBook is away or stopped, US runs
on two of three voters, and an unplanned failure of the mini or the iMac
then stops US until one of them returns. A fixed always-on host can
replace the MacBook later without changing anything else in this
design.

### 7. Migration from dagnats-mesh

Today `mesh-mini` is the third EU voter, with `preferred_leaders` set and
`engine: false` replica-only being built (ADR-030 interim).

1. **Add the Helsinki node to EU.** Measure its round trip to
   `mesh-vps` and `mesh-eu` first and confirm it is well under 100 ms
   (the estimate is 20 to 30 ms). Join it to `nats_cluster_routes` on all
   EU nodes, rolling restart, and let R3 streams replicate to it. Set
   `preferred_leaders` on every EU node to `mesh-vps` and `mesh-eu` only,
   and leave Helsinki out. EU now has four voters, the mini included.
2. **Pause notify's EU triggers.** Notify runs on the mini against its
   local EU node. Disable its triggers and schedules in EU and let its
   in-flight runs finish (or cancel them). From here notify is not taking
   new work.
3. **Remove the mini from EU.** Drop it from every EU node's routes,
   rolling restart, and remove its peer from the EU meta group. EU is back
   to three voters: the German pair and Helsinki. Stop `dagnats serve` on
   the mini and move its data directory aside; its EU replicas are not
   reused.
4. **Stand up US.** The mini, iMac and MacBook with `region: us` (or
   `nats_cluster_name` and `jetstream_domain` set to `us` until `region`
   ships), routes to each other, `preferred_leaders` the mini and the
   iMac. The iMac takes the darwin/amd64 release build.
5. **Move notify to US.** Mint a US join key, point `NOTIFY_DAGNATS_URL`
   at the mini's local node, keep its seed, and start it. It registers in
   US and registers its workflows and triggers there. Enable its triggers
   in US. Then delete notify's program entry, workflows and triggers in
   EU. Notify's past EU runs stay in EU history.
6. **Link the regions.** Once the clustered-leaf and allow-list PRs ship,
   open `leaf_port` on the EU nodes with a leaf login and the allow-list,
   and set `leaf_remotes` on the US nodes. Verify with the cross-region
   `task.>` test above run against the live pair, and with
   `nats --js-domain us stream ls` from an EU node.

Steps 2 to 5 are a brief, planned notify outage. Notify starts fresh in
US: its past EU runs stay in EU and are neither migrated nor mirrored. Before step 3 every step is
reversible by undoing the config. After it, the mini can rejoin EU only
with an empty data directory.

### 8. Code changes implied

Rough PR breakdown, each small and independently useful:

1. **`region` key.** Defaults cluster name and domain, validates they
   agree, surfaces the region in `/health/cluster`, the cluster reporter
   and the console header.
2. **Clustered leaf.** Relax `server/config.go:1435` as above, wire
   `leaf_remotes` on a clustered node in `natsOptions`
   (`server/nats.go:97-106`), and extend `dagnatstest` to start two
   three-node clusters with distinct domains joined by leaf. Tests: both
   regions provision independently; one region keeps creating consumers
   while the link is down.
3. **Leaf link allow-list.** `leaf_listen_allow`, applied as permissions
   on the listener's user. Tests: `task.>` and `history.>` published in
   one region are not stored in the other; `$JS.<domain>.API` requests
   cross.
4. **`engine: false`.** Already in flight for ADR-030; used by the
   migration only if notify must keep running on the mini while it is
   still an EU voter.
5. **Docs.** `docs/production.md` and the clustering page: the region
   decision tree, the MacBook requirements, and the migration runbook.

No gateway code is in this list. dagnats has none today, and this design
does not need it.

## Alternatives considered

**A. Gateway supercluster, one domain, per-cluster placement.** The
starting brief. It needs gateway keys (`gateway_name`, which nats-server
requires to equal the cluster name, `server/server.go:1125-1133`;
`gateway_port`; `gateway_remotes`; a login on `GatewayOpts.Username` and
`Password`, `server/opts.go:143-158`), `placement.cluster` on every
stream and bucket, and separate NATS accounts per region because stream
names and subjects collide (`server/jetstream_cluster.go:11058`, `11073`).
In return it gives native geo-affinity for queue groups and one namespace.
Rejected: the shared meta group loses quorum on both sides during a
partition and stretches Raft across the home link, as set out in the
Context.

**B. Gateways with a seventh meta voter in a third location.** Breaks the
three-three tie so the majority side keeps its meta leader. The minority
side, almost always US behind a home link, still cannot create consumers
or restart. Rejected for the same reason as A.

**C. One stretched cluster.** ADR-030 rejected it.

**D. Two unlinked clusters.** Simplest, and the state after migration
step 5. It is not the end state only because cross-domain inspection and
later mirrors need the link. The link adds no coupling the regions depend
on.

**E. Global KV for programs and join keys.** One registration for both
regions. Needs a cross-region writer or a mirror with a single source of
truth. Rejected for now; per-region registration is one extra command.

## Consequences

**Positive:**

- Each region survives the loss of the other and of the link, including
  restarts and consumer creation.
- Engine, streams and KV code are unchanged; a region is a cluster plus a
  domain name.
- Turn latency in each region depends only on that region's links.

**Negative:**

- **No transparent cross-region calls.** Starting or querying a run in
  the other region means connecting to it. Cross-region workflows wait
  for the deferred region-addressed control plane.
- **Two of everything to operate:** join keys, program registrations,
  console URLs.
- **US quorum leans on a laptop** under the operating rule above.
- **The name "supercluster" no longer fits the mechanism.** The regions
  are a cluster pair linked by leafnodes. This ADR keeps the name for
  continuity with ADR-030, which should be read with this correction.

**Neutral:**

- ADR-030's single-machine leaf keeps working: `leaf_listen_allow` unset
  leaves the leaf login's permissions as they are.

## Out of scope and deferred

- Region-addressed control plane (`region.<name>.api.>`) and
  cross-region start, query and signal.
- Moving or failing over a run between regions.
- Mirrors or sources of one region's history into the other for
  disaster recovery.
- A console view across regions.
- Gateways. Revisit if a third region on stable cloud links appears and a
  single domain becomes acceptable.
- TLS on the leaf link; it runs over Tailscale.
