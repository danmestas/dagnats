# ADR-030: Regional clusters and far sites

**Status:** Accepted (2026-11-04)
**Deciders:** Dan Mestas
**Depends on:** ADR-005
**Issues:** #863, #866, #929, #932

## Context

ADR-005 lets three or five `dagnats serve` nodes form their own NATS
cluster. It says nothing about where those nodes may sit. One deployment
showed what happens when they sit far apart.

The mesh `dagnats-mesh` has three nodes:

- `mesh-vps` and `mesh-eu`, two Hetzner hosts in Germany, 1.3 ms apart.
- `mesh-mini`, a Mac mini in a US home, reached over Tailscale. Its
  round trip to the other two is 150 to 430 ms normally and 800 ms to
  1.1 s at worst.

Three symptoms followed:

1. **Leaders drifted onto the far node.** After restarts and elections,
   stream leaders landed on `mesh-mini`. Every write goes through its
   stream's leader and waits for a quorum, so every replicated write
   crossed the Atlantic. Pilot turns went from 2.5 s to 8 to 10 s.
   `preferred_leaders` (#929) and moving the JetStream meta leader too
   (#932) fixed this.
2. **The far node still does engine work.** With leaders kept off it,
   `mesh-mini`'s engine still pulls about a third of the shared work, and
   each of those steps pays the transatlantic round trip on its reads and
   writes. Turns swing between 3 and 9 s.
3. **Timeouts when the link degrades.** Bursts of `context deadline
   exceeded` appear whenever the link to `mesh-mini` slows down.

### What NATS does here

- **Raft timers are fixed.** The heartbeat is 1 s, the election timeout
  4 to 9 s, and a leader that loses quorum notices after 10 s. They are
  constants, not configuration: nats-server v2.15.2
  `server/raft.go:317-323`. A voter whose link stalls for seconds misses
  heartbeats and triggers elections that the operator cannot tune away.
- **Clusters are meant to be regional.** Synadia flags route RTT above
  about 100 ms, because elections become frequent, and recommends
  regional clusters:
  <https://www.synadia.com/insights/checks/nats-high-route-rtt>.
- **Leafnodes suit single-server edge sites**, where high RTT is
  expected: <https://www.synadia.com/insights/checks/nats-high-leaf-rtt>.
- **A stretch cluster needs three or more regions**, and its write
  latency is proportional to the latency between regions:
  <https://synadia.com/blog/multi-cluster-consistency-models>. Two
  regions cannot keep a majority if the bigger one fails.
- **Placement tags and `unique_tag`** spread replicas across zones
  within a cluster:
  <https://docs.nats.io/reference/config/jetstream/unique_tag>.
- **Topologies.** Clusters join into a supercluster through gateways,
  and leafnodes extend a cluster to an edge server. A leaf can mirror and
  source streams to and from the hub:
  <https://docs.nats.io/concepts/topologies>.
- **A JetStream leaf needs its own domain**, so its JetStream API does
  not collide with the hub's:
  <https://docs.nats.io/running-a-nats-service/configuration/leafnodes/jetstream_leafnodes>.
- **Queue groups prefer local members.** A message that arrives over a
  route goes to a local queue subscriber first, and a server prefers
  members in its own cluster over leaf members:
  <https://docs.nats.io/learn/core-nats/queue-groups>, and
  `server/client.go:5611-5646` and `5674-5700`. This does not help pull
  consumers. A fetch is a request to
  `$JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>`
  (`server/jetstream_api.go:158`), and only the consumer's leader
  subscribes to it (`server/consumer.go:1773`, `1840`). Every engine pulls
  from the same leader wherever it runs, so a far engine gets its share
  of the work and pays the round trip for each message.
- **A server with JetStream disabled is not a JetStream peer.** It is
  left out of the cluster's peer list (`server/jetstream_cluster.go:968`),
  and stream placement only picks from the meta group's peers
  (`server/jetstream_cluster.go:10680`). It routes messages but stores
  and votes on nothing.

## Decision

**A dagnats cluster is one region.** Every voter is within about 100 ms
round trip of the others, and ideally within single-digit milliseconds.
A cluster does not span an ocean or a home internet link.

**A far site joins in one of two ways:**

- **As a leafnode with its own `jetstream_domain`.** The far server is a
  standalone `dagnats serve` with `leaf_remotes` pointing at the
  cluster's leafnode listeners (`leaf_port`, #866) and its own
  `jetstream_domain` (#863). It does not vote in the cluster. Core NATS
  traffic (request/reply, publishes the cluster has interest in) crosses
  the leaf link, and its own JetStream stays local. This is the default
  for a single machine at a distance, such as a home server or a laptop.
- **As a separate regional cluster joined by gateways (a
  supercluster).** Use this when the far site needs its own engine and
  its own quorum, for example a second region that must keep working when
  the link to the first is down. dagnats does not configure gateways
  today; this option needs that work first.

**Interim, for an existing node that is already stretched across a slow
link:**

- Set `preferred_leaders` on every node to the well-connected nodes'
  server names (#929, #932), so stream leaders and the meta leader stay
  in the region.
- Run the far node as a replica only, with `engine: false`. It keeps
  voting and storing replicas, so the cluster keeps its size, but it
  does no engine work, which removes symptom 2. This role is being built
  now and is not released yet.

The interim does not remove symptom 3. While a WAN voter remains, a
degraded link can still stall elections and quorum writes for seconds.

## Alternatives considered

**A. Keep the stretched cluster and tune it.** The Raft timers that
matter are constants (`raft.go:317-323`), and leader placement only moves
the leader, not the quorum round trip on the far node's own work.
Rejected.

**B. Disable JetStream on the far node.** It would then route messages
only and hold no replicas. With three nodes, that leaves two JetStream
servers, too few to place three replicas. It also keeps the far node in the cluster's
route mesh for no benefit over a leaf. Rejected in favour of a leaf.

**C. Placement tags to keep replicas in one region.** Tags choose where
replicas go, but the far node still runs an engine pulling from the
same consumer leaders. They help a cluster spread across zones in one
region, not a cluster with one far member. Not adopted for this case.

## Consequences

**Positive:**

- Replicated writes and elections stay inside one region, so turn times
  depend on local latency, not on the worst link.
- The far site keeps working against its own JetStream when the link is
  down, and catches up through mirrors and sources.

**Negative:**

- **An extra host.** Removing `mesh-mini` from the cluster leaves two
  voters, so a third small regional host is needed to keep a
  three-node quorum.
- **Programs on a leaf reach the engine across the leaf link.** Only
  core NATS crosses it: request/reply and subscriptions the other side
  has interest in. A program on the leaf that uses the cluster's streams
  or buckets must connect to a cluster node or address the cluster's
  JetStream by domain; its local `$JS.API` is the leaf's own.
- **Data locality takes explicit mirrors and sources.** Nothing is
  replicated to the leaf by default. Streams the far site needs locally
  are mirrored or sourced into its domain, and the operator chooses
  which.
- **Elections can still stall** for as long as a WAN voter remains in
  the cluster, which is the case until migration step 3 below is done.

**Neutral:**

- ADR-005's fixed-size cluster model is unchanged. Clusters and
  `leaf_remotes` stay mutually exclusive on one node; a cluster node can
  still open a leafnode listener.

## Migration for dagnats-mesh

1. **Now.** `preferred_leaders` on all three nodes (done). `engine: false`
   on `mesh-mini` once the replica-only role ships (pending).
2. **Add a third regional voter.** A small Hetzner host near `mesh-vps`
   and `mesh-eu`, within single-digit milliseconds of both. Join it to
   the cluster's routes on all nodes and let streams replicate to it.
3. **Turn `mesh-mini` into a leaf.** Remove it from every node's
   `nats_cluster_routes` and restart the cluster one node at a time. Open
   `leaf_port` on the Hetzner nodes, with a leaf login. On `mesh-mini`,
   drop the cluster keys and set `leaf_remotes` to the Hetzner nodes'
   leafnode listeners, its own `jetstream_domain`, and the engine off.
   Then switch its local programs to the leaf connection, and add mirrors
   or sources for any streams it needs locally. Remove its old peer from
   the JetStream meta group.
4. **Later, optionally.** If the US side needs its own engine and quorum,
   build a second regional cluster there and join the two with gateways.
