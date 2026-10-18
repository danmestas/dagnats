# ADR-024: Coordination leases over request/reply

**Status:** Accepted (2026-10-18)
**Deciders:** Dan Mestas

## Context

Downstream services run several replicas and need to coordinate them: elect
one leader, split shards so each has exactly one owner, and learn when
ownership changes. Their boundary with dagnats is the wire (NATS and the HTTP
bridge). They never import dagnats Go code.

Nothing in dagnats does this today. The closest pieces are tied to runs and
task types, so they don't fit: `singleton_locks` (the AdmissionController),
`concurrency_tasks` and `rate_limits` (the ConcurrencyManager), and
`worker.HandleSingleton`.

NATS server 2.11+ supports a TTL on each KV key (`AllowMsgTTL`). With
`SubjectDeleteMarkerTTL` set, the server also writes a delete marker when a
key's TTL runs out. dagnats runs on 2.15.

## Decision

### Storage: the `leases` KV bucket

`natsutil.SetupKVBuckets` provisions a `leases` bucket with the same replica
count as the other buckets, so it is replicated in cluster mode. Its settings:

| Setting | Value | Why |
|---|---|---|
| History | 1 | Only the current grant matters. |
| `LimitMarkerTTL` | 60s | Turns on per-key TTL, and makes the server write a delete marker (`Nats-Marker-Reason: MaxAge`) when a key expires. The marker lasts 60s and then disappears, so dead keys do not pile up. |
| `MaxValueSize` | 1 KiB | Every value is a small JSON record. |
| `RePublish` | `$KV.leases.>` → `dagnats.lease.changed.>` | Every write, including the expiry marker, is republished as an event (see below). |

Each lease is one key. Its value is `{holder, acquired_at, ttl_ms}`. Each write
carries a `Nats-TTL`, so **the NATS server enforces expiry**, and clock skew
between holders cannot extend or shorten a lease.

**The key's KV revision (its stream sequence) is the fencing token.** It goes
up with every write to the key, and the next holder's revision is always
higher than the last holder's.

### Wire contract: four subjects on `dagnats-api`

| Subject | Request | Success reply |
|---|---|---|
| `api.leases.acquire` | `{key, holder, ttl_ms}` | `{granted, held, holder, revision, acquired_at, expires_at, ttl_ms}` |
| `api.leases.renew` | `{key, holder, revision, ttl_ms}` | same shape, new `revision` |
| `api.leases.release` | `{key, holder, revision}` | `{granted:false, held:false, key}` |
| `api.leases.owner` | `{key}` | the held shape, or `{held:false}` |

A fifth subject, `api.leases.forward`, was added later. See "Forwarding to
the owner" below.

A failed call replies `{error, kind}`. `kind` is one of:

- `invalid`: bad input.
- `lost`: the fence check failed.
- `transport`: the request body was not valid JSON.
- `internal`: a NATS error, such as a timeout.
- `unsupported`: the `leases` bucket does not exist. SetupAll skips it, logging a warning, on a NATS server older than 2.11, because per-key TTL needs 2.11. The error text names that requirement. The rest of dagnats starts normally.

`docs/wire-protocol.md` has the full contract.

Semantics:

- **acquire.** If nobody holds the key, the call writes the key with a CAS and
  grants it. If someone else holds it, the reply is `granted:false` and names
  the current holder. That is a normal answer, not an error. **If the caller
  already holds the key, acquire is idempotent:** it returns the existing lease
  with `granted:true` and the same revision, and the TTL is *not* extended.
  This makes it safe to retry after a lost reply. It also means acquire never
  writes on behalf of a holder that thinks it is still acquiring. To extend a
  lease, call renew. Making acquire act as renew would hide a gap: the caller
  could lose the lease, a contender could take it and then expire, and the
  caller would get a new grant without ever learning it had been displaced.
- **renew.** The holder must match, and the write is a CAS on the presented
  revision. A missing key, a different holder, or a stale revision returns
  `lost`.
- **release.** Same fence check as renew. It writes a KV purge marker, which
  itself expires after `LimitMarkerTTL`. Releasing a lease that already
  expired returns `lost`, so the caller learns it was not the owner when it
  thought it was.
- **owner.** Reads the key with no side effects.
- **Every read goes to the stream leader.** acquire, renew, release and owner read with a non-direct `STREAM.MSG.GET`. jetstream's `GetLastMsgForSubject` would use a direct get, because KV buckets set `AllowDirect`, and at R3 any replica can answer a direct get. A lagging replica could then show an old holder its own expired lease, and the idempotent acquire path would answer `granted:true` while another holder owns a newer revision. Leader reads close that hole. They also make `owner` exact, not merely best-effort.
- **Bounds.** Keys and holders must match `[A-Za-z0-9_-]` and be at most 128
  bytes. `ttl_ms` must be a whole number of seconds between 1000 and 600000.
  It must be whole seconds because the server truncates a message TTL to
  seconds, so 1.5s would quietly become 1s. Restricting the character set
  means a key *is* its subject token. No sanitizing step is needed, and two
  keys can never map to the same event subject (`a.b` and `a_b` would both
  sanitize to `a_b`).
- **`expires_at` is advisory.** Writes compute it from the dagnats node's
  clock just before the write, so it falls slightly before the server's
  actual expiry. Owner computes it from the stored message's server
  timestamp. In both cases the server TTL is what actually frees the key.

**The lease endpoints use queue group `dagnats-leases`.** The older
control-plane endpoints turn queue groups off, so every dagnats instance
receives every request. For leases that would be a correctness bug. N
instances would race the same acquire: one CAS wins and N−1 lose, and the
caller gets whichever reply arrives first. With the queue group, exactly one
instance handles each request.

**Leader election** is an acquire on a well-known key, for example
`notify-leader`. There is no separate API. **Shard ownership** is one lease
per shard key, for example `notify-shard-07`.

### Change events: server republish, no dagnats watcher

The bucket's `RePublish` config has the NATS server publish every stored
lease message to `dagnats.lease.changed.<key>`. Republishing is done **only
by the stream leader**. A cluster of N dagnats nodes therefore emits each
change once, not N times. This needs no dagnats goroutine, no election, and
no deduplication.

Each event carries the stored message's headers, plus `Nats-Sequence` (the
revision) and `Nats-Subject`:

| Event | How to recognise it | Body |
|---|---|---|
| acquired | `Dagnats-Lease-Event: acquired` | lease record JSON |
| renewed | `Dagnats-Lease-Event: renewed` | lease record JSON |
| released | `Dagnats-Lease-Event: released` | empty |
| expired | no `Dagnats-Lease-Event` header; `Nats-Marker-Reason: MaxAge` | empty |

The server writes the expiry marker itself, so dagnats cannot stamp a header
on it. That is why "expired" is recognised by a missing header.

Events are core NATS publishes, delivered at most once. A subscriber that was
down misses events. It should call `owner` to resync, or read the `KV_leases`
stream directly. A stream-leader change during a write can, rarely, drop or
repeat an event, so consumers should treat `Nats-Sequence` as the order and
ignore any sequence they have already seen.

### Forwarding to the owner: `api.leases.forward`

*Added 2026-10-18.* A downstream service with several replicas (notify)
needs to "send this request to whichever node owns key X". There were
two options:

- **(a)** A server endpoint, `api.leases.forward {key, payload,
  timeout_ms}`. It looks up the owner and requests the owner's inbox
  with the fence in a header.
- **(b)** Documentation only: a subject convention plus "look up the
  owner, then request it directly".

**We chose (a), and (b)'s convention is its public half.** The question
was whether the endpoint hides real complexity or is only an extra hop.
Looking up the owner and sending one request is two calls. Doing that
alone would be a pass-through. Forward is worth having because of what
happens when the first attempt fails:

- **Telling "not processed" from "maybe processed".** If nobody is
  subscribed on the owner's inbox, or the owner rejects the fence, the
  request was certainly not processed, so sending it again is safe. A
  timeout is different: the owner may have done the work. Forward retries
  only the first kind, and its error kinds tell the caller which one
  happened (`no_owner` and `owner_unavailable` mean not processed;
  `owner_timeout` means maybe). Each sender would otherwise have to get
  this distinction right itself, and getting it wrong delivers a
  non-idempotent request twice.
- **A handoff in flight.** The lease can move between the lookup and the
  delivery. Forward reads the lease again and retries only if the holder
  or revision changed. That ends after 3 attempts at most, and it never
  loops on an owner that keeps refusing.
- **The fence stays out of the sender's hands.** dagnats stamps the
  revision it read through the stream leader. The sender never handles a
  fence, so it cannot send a stale one from a cache.
- **A time budget.** `timeout_ms` (100 to 30000) bounds every lookup and
  delivery together. Each forward runs on its own goroutine, because a
  micro endpoint handles its requests on one goroutine. At most 256 run
  at once on an instance; past that the caller gets `busy` right away.
  Stopping the API service cancels forwards still in flight.
- **A reply that does not fit.** The owner's reply can fit its own hop
  but not dagnats' reply once base64-encoded. The owner has already
  processed the request, so the caller gets `reply_too_large` rather
  than a bare timeout.

The cost is an extra hop and a leader read for every forward, and dagnats
now carries the payload. The convention is public for that reason: a
sender that tracks the owner itself (from change events) can request the
inbox directly with the fence header. The owner-side rule protects both
paths.

**Convention.**

- The owner subscribes on `dagnats.lease.inbox.<holder>.>`. Holder IDs
  are unique per process, so this needs no queue group.
- A request for key `K` arrives on `dagnats.lease.inbox.<holder>.<K>`
  with header `Dagnats-Lease-Fence: <revision>`.
- Keys and holders are already single subject tokens, so the subject
  needs no escaping.

**Owner rule.** The owner remembers the revision its *acquire* returned,
which is the start of its tenure. It rejects a request when:

- it is not currently acting as owner;
- the fence header is missing or malformed; or
- the fence is lower than its tenure start.

To reject, it replies with an empty body and `Dagnats-Lease-Reject:
stale`. Comparing against the tenure start, not the latest revision,
matters. Renew raises the revision on every call, so "reject anything
below my latest revision" would refuse correct requests that were read
just before a routine renew. Any fence read during an earlier tenure,
this holder's or another's, is lower than the tenure start.

**Degraded mode.** Without the `leases` bucket, forward replies
`unsupported`, like the other lease subjects.

### Why request/reply and not a Go package

The owner chose request/reply because it keeps the downstream boundary a wire
contract. A Go client package would have pulled dagnats internals into
downstream builds, tied their release cadence to ours, and needed a separate
client for every language.

## Alternatives considered

- **Downstream services use the `leases` bucket directly through the `$KV`
  API.** This is possible on the wire, but every client would have to
  reimplement the fence check, holder validation, TTL bounds, and the
  marker-aware CAS on acquire. The endpoints keep those rules in one place.
  The bucket stays readable directly, and that is the documented way to
  consume events durably.
- **A dagnats-side KV watcher that publishes events.** This was the first
  plan. In cluster mode every node runs one, so every event goes out N times.
  Fixing that needs either an elected publisher or consumers deduplicating
  by revision. Server republish has neither problem and needs no code.
- **`kv.Create` / `kv.Update` for writes.** `Update` cannot set a TTL, and
  neither call can stamp the event-kind header. Writes go through
  `TracingPublisher.JSPublishMsg` with `ExpectLastSequencePerSubject` and
  `Nats-TTL`, which is the same CAS that `kv.Update` performs.
- **Forward that relays the client's reply subject** (dagnats publishes to
  the owner's inbox with the caller's reply subject, so the owner answers
  the caller directly). This saves a hop, but dagnats never sees the
  outcome. It cannot retry after a handoff, and it cannot turn "no
  responders" into an explicit error. Those two things are the reason to
  have the endpoint at all.
- **Sanitizing keys with `natsutil.SubjectToken`.** Rejected because of the
  collisions described above. Rejecting bad input defines the problem away.

## Consequences

- **Leases require nats-server 2.11 or newer.** On an older external server (`DAGNATS_NATS_URL`), dagnats still starts without the bucket, and every lease call returns `kind: "unsupported"`.
- **A lease is only as good as the holder's renew loop.** A holder should
  renew well before `expires_at`, for example every `ttl/3`. When renew
  returns `lost`, or the holder cannot reach dagnats before `expires_at`,
  **it must stop acting as owner immediately.**
- **Fence every downstream write with the revision.** A paused process (GC,
  a VM freeze, a partition) can wake up still believing it is the owner. The
  lease cannot stop that process's in-flight writes. Only the downstream
  store can, by rejecting any write whose revision is lower than the highest
  it has seen. If the downstream store cannot check a fence, the lease gives
  mutual exclusion only while every holder's renew loop is healthy.
- Holder IDs must be unique per process. Two processes that share a holder
  ID are, to this API, the same holder.
- Renew events fire on every renewal. Consumers that only care about
  ownership changes should filter on `Dagnats-Lease-Event`.
- Nothing else may subscribe under `dagnats.lease.inbox.>`. An extra
  subscriber (a monitoring tap, say) answers for "no responders", so a
  dead owner shows up as `owner_timeout` instead of `owner_unavailable`.
- An owner that accepts forwards must apply the owner rule itself.
  dagnats only stamps the fence. It cannot know whether the owner process
  still considers itself the owner.
- The `leases` bucket is not part of `dagnats clean --type runs`. Leases
  belong to the downstream services, not to workflow runs.
