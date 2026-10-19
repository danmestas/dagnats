---
title: Leases
weight: 4
---

Leases let the replicas of your own service elect a leader or split shard ownership, over plain NATS request/reply. They are not tied to workflow runs.

## Overview

A lease is a key that one holder owns for a bounded time. Your service asks dagnats to grant it, renews it while it is healthy, and stops acting as owner the moment it cannot. If the holder dies, the NATS server expires the key and another replica can take it.

Leases live in the `leases` KV bucket. The server enforces each lease's TTL, so clock skew between holders cannot extend or shorten one. The key's KV revision is the **fencing token**: it goes up with every write to the key, and a new holder always gets a higher revision than the previous one.

Leases are not part of the Go SDK. Your service talks to dagnats on the wire, so it does not import dagnats code. The design is in [ADR-024](https://github.com/danmestas/dagnats/blob/main/docs/architecture/adr-024-leases.md). The full wire contract is in `docs/wire-protocol.md`.

**Requirement:** leases need nats-server 2.11 or newer, because they rely on per-key TTL. On an older server dagnats still starts, but the `leases` bucket is not created and every lease subject replies `kind: "unsupported"`.

## Subjects

Five request/reply subjects belong to the `dagnats-api` service. They use the queue group `dagnats-leases`, so exactly one dagnats instance handles each request. Bodies are JSON.

| Subject | Request | Success reply |
|---|---|---|
| `api.leases.acquire` | `{key, holder, ttl_ms}` | `{granted, held, key, holder, revision, acquired_at, expires_at, ttl_ms}` |
| `api.leases.renew` | `{key, holder, revision, ttl_ms}` | same shape, with a new `revision` |
| `api.leases.release` | `{key, holder, revision}` | `{granted:false, held:false, key}` |
| `api.leases.owner` | `{key}` | the acquire shape with `held:true`, or `{granted:false, held:false, key}` |
| `api.leases.forward` | `{key, payload, timeout_ms}` | `{key, holder, revision, payload}` |

Input bounds:

- `key` and `holder` match `[A-Za-z0-9_-]` and are 1 to 128 bytes. There are no dots, so a key is exactly one subject token.
- `ttl_ms` is a whole number of seconds from 1000 (1s) to 600000 (10m). The server truncates a message TTL to whole seconds, so `1500` is rejected rather than quietly becoming 1s.
- `revision` on renew and release is the fence from your last reply. Leaving it out is `invalid`.
- `timeout_ms` on forward is 100 to 30000.

Times are RFC 3339 in UTC. `expires_at` is advisory: the server's TTL is what frees the key.

```json
// api.leases.acquire request
{"key": "notify-leader", "holder": "notify-7f3a", "ttl_ms": 15000}

// reply: granted
{"granted": true, "held": true, "key": "notify-leader", "holder": "notify-7f3a",
 "revision": 42, "acquired_at": "2026-10-18T12:00:00Z",
 "expires_at": "2026-10-18T12:00:15Z", "ttl_ms": 15000}

// reply: someone else holds it (a normal answer, not an error)
{"granted": false, "held": true, "key": "notify-leader", "holder": "notify-19bc",
 "revision": 40, "acquired_at": "...", "expires_at": "...", "ttl_ms": 15000}
```

### Behavior

- **acquire** grants the key if nobody holds it. If another holder has it, the reply is `granted:false` and names that holder. If the caller already holds it, acquire is idempotent: it returns the existing lease with `granted:true` and the same revision, and it does **not** extend the TTL. That makes it safe to retry after a lost reply. To extend a lease, call renew.
- **renew** extends the lease to `ttl_ms` from now. The holder must match and `revision` must be the current fence. The reply carries the new fence.
- **release** frees the key under the same fence check as renew. Releasing a lease that already expired returns `lost`.
- **owner** is a read with no side effects.
- Every call reads through the stream leader, so in a cluster a lagging replica cannot report a stale owner.

### Errors

A failed call replies `{"error": "...", "kind": "..."}`.

| `kind` | Meaning |
|---|---|
| `invalid` | Bad `key`, `holder`, `ttl_ms`, `timeout_ms`, or missing `revision`. |
| `lost` | renew or release only. The caller does not hold the key at that revision: it expired, was released, or belongs to someone else. Stop acting as owner now. |
| `transport` | The request body is not valid JSON. |
| `internal` | A NATS error, such as a timeout. Retry. |
| `unsupported` | The `leases` bucket does not exist (nats-server older than 2.11). Retrying will not help. |

## Change events

The server republishes every write to the bucket on `dagnats.lease.changed.<key>`. Only the stream leader republishes, so each change arrives once however many dagnats nodes run. Each event carries `Nats-Sequence` (the revision) and `Nats-Subject` headers.

| Event | How to recognise it | Body |
|---|---|---|
| acquired | `Dagnats-Lease-Event: acquired` | `{holder, acquired_at, ttl_ms}` |
| renewed | `Dagnats-Lease-Event: renewed` | same as acquired |
| released | `Dagnats-Lease-Event: released` | empty |
| expired | no `Dagnats-Lease-Event` header; `Nats-Marker-Reason: MaxAge` | empty |

Events are core NATS publishes, delivered at most once. A subscriber that was down should call `api.leases.owner` to resync. For durable delivery, consume the `KV_leases` stream directly. Use `Nats-Sequence` to order events and drop duplicates. Renew fires an event on every renewal, so filter on `Dagnats-Lease-Event` if you only care about ownership changes.

## Forwarding to the owner

`api.leases.forward` sends a request to whichever process holds a key, without the sender knowing who that is. dagnats reads the lease through the stream leader, then makes a NATS request to `dagnats.lease.inbox.<holder>.<key>` with the lease revision in the `Dagnats-Lease-Fence` header. The request body is the decoded `payload`. The owner's reply body comes back as `payload`; owner reply headers are not relayed. `payload` is base64 in JSON.

`timeout_ms` bounds the whole call, so set your own request timeout a little higher.

**Owner side.** A holder that accepts forwards subscribes on `dagnats.lease.inbox.<holder>.>` with a plain core subscription, not a queue group. Holder IDs are unique per process, so the subject names one process. Remember the revision your **acquire** returned: that is the start of your tenure. Renew raises the revision but the tenure stays the same. Reject a request when any of these is true:

- you are not currently acting as owner of the key (never acquired it, released it, renew returned `lost`, or you are past `expires_at` without a renewal);
- `Dagnats-Lease-Fence` is missing or not an unsigned integer;
- the fence is lower than your tenure's acquire revision.

To reject, reply with an empty body and the header `Dagnats-Lease-Reject: stale`, and do not process the request.

**Retry rule.** dagnats re-sends only after an outcome that proves the owner did not process the request: no subscriber on the inbox, or a `Dagnats-Lease-Reject` reply. It then re-reads the lease and, if the holder or revision changed, tries the new owner. A call makes at most 3 attempts. A timeout is never retried, because the owner may have processed the request.

Forward-only error kinds, in addition to the ones above:

| `kind` | Meaning | Processed? |
|---|---|---|
| `no_owner` | Nobody holds the key. | No |
| `owner_unavailable` | The holder had no inbox subscriber or refused the fence, and the lease did not move. The holder probably died; its lease expires within `ttl_ms`. | No |
| `busy` | This dagnats instance already has 256 forwards in flight. Retry shortly. | No |
| `owner_timeout` | The owner did not reply within `timeout_ms`. | Maybe. Retry only if the request is idempotent. |
| `reply_too_large` | The owner processed the request, but its reply does not fit the dagnats hop once base64-encoded. | Yes |

On an error, `holder` and `revision` in the reply name the owner dagnats last tried. A forward on a server without the `leases` bucket replies `unsupported`; a corrupt lease record or dagnats stopping mid-call is `internal`.

Payloads count against the NATS `max_payload` on both hops, and base64 adds a third on the dagnats hop. An owner reply of more than about 750 KB at the default 1 MB limit comes back as `reply_too_large`. Keep forwarded replies small, or reply with a reference.

Do not add other subscribers under `dagnats.lease.inbox.>`. A monitoring tap counts as a responder, so a dead owner looks like a hung one and you get `owner_timeout` instead of `owner_unavailable`.

## Example

Acquire a lease and renew it every third of the TTL. This uses only `nats.go`.

```go
package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

type leaseRequest struct {
	Key      string `json:"key"`
	Holder   string `json:"holder"`
	Revision uint64 `json:"revision,omitempty"`
	TTLMs    int64  `json:"ttl_ms,omitempty"`
}

type leaseReply struct {
	Granted  bool   `json:"granted"`
	Held     bool   `json:"held"`
	Holder   string `json:"holder"`
	Revision uint64 `json:"revision"`
	Error    string `json:"error"`
	Kind     string `json:"kind"`
}

func call(nc *nats.Conn, subject string, req leaseRequest) (leaseReply, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return leaseReply{}, err
	}
	msg, err := nc.Request(subject, body, 5*time.Second)
	if err != nil {
		return leaseReply{}, err
	}
	var reply leaseReply
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		return leaseReply{}, err
	}
	if reply.Error != "" {
		return reply, fmt.Errorf("%s: %s", reply.Kind, reply.Error)
	}
	return reply, nil
}

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		panic(err)
	}
	defer nc.Close()

	const ttl = 15 * time.Second
	req := leaseRequest{Key: "notify-leader", Holder: "notify-7f3a", TTLMs: ttl.Milliseconds()}
	got, err := call(nc, "api.leases.acquire", req)
	if err != nil {
		panic(err)
	}
	if !got.Granted {
		fmt.Println("held by", got.Holder)
		return
	}
	for range 3 {
		time.Sleep(ttl / 3)
		req.Revision = got.Revision
		got, err = call(nc, "api.leases.renew", req)
		if err != nil {
			// "lost" means stop acting as owner now.
			fmt.Println("lease lost:", err)
			return
		}
	}
	req.Revision = got.Revision
	if _, err := call(nc, "api.leases.release", req); err != nil {
		fmt.Println("release:", err)
	}
}
```

## Holder rules

- Renew about every `ttl/3`. When renew returns `lost`, or you cannot reach dagnats before `expires_at`, stop acting as owner immediately.
- Fence downstream writes. Send the lease `revision` with every write you make as owner, and have the downstream store reject any revision lower than the highest it has seen. A paused process (GC, a VM freeze, a partition) can wake up still believing it owns the lease, and only the downstream store can stop its writes.
- Holder IDs must be unique per process. Two processes with the same holder ID are the same holder to this API.
- Leader election is an acquire on one well-known key. Shard ownership is one lease per shard key.
- The `leases` bucket is not touched by `dagnats clean --type runs`. Leases belong to your services, not to workflow runs.
