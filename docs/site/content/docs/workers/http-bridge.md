---
title: HTTP Bridge
weight: 4
---

The HTTP bridge is an HTTP-to-NATS gateway that lets non-Go workers (Python, TypeScript, any language with an HTTP client) interact with DagNats over three REST endpoints.

## Architecture

The bridge runs as an HTTP server that translates REST calls into NATS JetStream operations. It maintains an in-memory **ack map** that tracks polled NATS messages so they can be acknowledged or NAK'd when the HTTP worker resolves a task.

```
HTTP Worker  -->  Bridge (HTTP)  -->  NATS JetStream
   poll      <--  task payload   <--  TASK_QUEUES
   resolve   -->  event publish  -->  WORKFLOW_HISTORY
```

The bridge provides **full capability parity** with Go native workers: completion, failure, retry, checkpointing, signals, and pause are all supported through the resolve endpoint.

## Endpoints

### POST /v1/workers/connect

Registers an HTTP worker and opens an SSE heartbeat stream. The connection stays open; the bridge sends `event: heartbeat` every 25 seconds to keep proxies and load balancers alive.

```json
{
    "worker_id": "python-worker-1",
    "task_types": ["extract-text", "classify"],
    "max_tasks": 3
}
```

The worker appears in the **workers** KV directory alongside Go native workers. On disconnect, the bridge deregisters the worker automatically.

`worker_id` is claimed by the first token that registers it: once an entry exists with a
non-empty token identity, only that same token (or the admin bearer) may re-register or
disconnect-clear that `worker_id` -- a restart or heartbeat re-register from the owning token
succeeds, but a different token attempting to take over the id gets `409 Conflict` and the
existing entry is left untouched. The connect handler, the periodic heartbeat re-register, and
disconnect cleanup all go through the same revision-guarded write, so two tokens racing an
unclaimed id can't both win it (exactly one gets `200`, the loser gets `409`), and a heartbeat
can never resurrect a `worker_id` an admin has since taken over. The admin bearer -- and every
caller in dev mode, which has no token identity to enforce -- can always take over or delete
any `worker_id`; those entries are written with the reserved `admin` token identity so a later
worker token can't reclaim them the way an unowned entry can. Entries with no token identity at
all -- a native Go worker, which never goes through the bridge -- are outside this scope
entirely: they're claimable, and deletable, by any bridge token, in both directions. That cuts
both ways: a native Go worker authenticates with NATS credentials, not a bridge token, and its
plain KV write carries no token identity at all, so if it registers a `worker_id` a bridge
token currently owns, the entry silently becomes unowned -- a different trust boundary than the
bridge's own ownership rule, worth knowing rather than discovering by surprise.

### POST /v1/tasks/poll

Long-polls for tasks from the TASK_QUEUES stream. Returns a JSON array of task payloads, or an empty array on timeout.

```json
{
    "task_types": ["extract-text"],
    "max_tasks": 1,
    "timeout_ms": 30000
}
```

Response:

```json
[
    {
        "task_id": "abc123.step-1",
        "run_id": "abc123",
        "step_id": "step-1",
        "iteration": 0,
        "attempt": 0,
        "input": {"url": "https://example.com/doc.pdf"},
        "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    }
]
```

The `timeout_ms` field controls how long the bridge waits for a task before returning empty. Maximum is 60 seconds.

`traceparent` (and `tracestate`, when present) carry W3C trace context for
that specific task. Start your execution span as a child of it and your
worker's spans join the run's trace instead of appearing as disconnected
roots. The fields are per task, not per response, because one poll can
return tasks belonging to different runs and therefore different traces.
Both are omitted entirely when the dispatch carried no trace context.

Go workers can convert the pair directly:

```go
ctx := observe.TraceContextFromTask(task)
ctx, span := tracer.Start(ctx, "my-worker.handle")
defer span.End()
```

### POST /v1/tasks/{id}/resolve

Resolves a polled task. The `action` field determines behavior:

| Action | Description |
|--------|-------------|
| `complete` | Publishes step.completed, acks the NATS message |
| `fail` | Publishes step.failed with configurable failure type |
| `pause` | Writes checkpoint, NAKs with delay for later retry |
| `checkpoint` | Saves state to KV, extends ack deadline |
| `send_signal` | Writes signal to KV for cross-step coordination |
| `wait_signal` | Blocks until signal arrives or timeout expires |

Complete example:

```json
{
    "action": "complete",
    "output": {"extracted_text": "Hello world"}
}
```

Fail with retry-after:

```json
{
    "action": "fail",
    "error": "rate limited by upstream API",
    "failure_type": "retry_after",
    "retry_after_ms": 5000
}
```

## Authentication

**No env token = open bridge (dev mode); set it and every worker needs
either the env token or a minted one.** `DAGNATS_BRIDGE_TOKEN` is the
sole switch: unset, every request is allowed unauthenticated (a
startup log warns `bridge auth disabled: DAGNATS_BRIDGE_TOKEN unset`
so this is never silent). Set it and every request must include:

```
Authorization: Bearer <token>
```

The admin token (the env value itself) authenticates unscoped — it can
poll or resolve any task type, and it is the only credential accepted
by the [token-management REST routes](../../reference/rest-api#tokens)
(`POST/GET /v1/tokens`, `DELETE /v1/tokens/{id}`).

Use the admin token to mint scoped, revocable **worker tokens** and hand
those to individual machines instead of distributing the admin
credential itself. A worker token (`Authorization: Bearer
dgn_{id}_{secret}`) is checked against the task-type prefixes it was
minted with — a poll naming a task type outside those prefixes gets
`403`. Revoking one worker token does not require rotating
`DAGNATS_BRIDGE_TOKEN` or bouncing every other worker. Worker tokens
are only meaningful once the env token is set: minting itself requires
the admin credential, so with it unset the bridge stays in dev mode
regardless of whether a worker-token store is wired in.

Revocation is not instant: each bridge process keeps an in-memory cache
kept current by a NATS KV watch, and during a reconnect it keeps
serving its last-known cache rather than failing every poll/resolve
outright. Revocation latency is therefore bounded by the watch's
reconnect window (capped at 30s), not zero.

## Setup

```go
b := bridge.NewBridge(nc, tel)
http.ListenAndServe(":8080", b.Handler())
```

The bridge binds optional KV buckets for **checkpoints** and **signals** at construction time. If these buckets are missing, the corresponding resolve actions return an error.

## Limitations

### Grouped task types cannot be polled over the bridge

A native Go worker registered with `worker.WithGroups(...)` consumes the
subject `task.<type>.<group>.>`. The bridge polls `task.<type>.>`, which
covers that subject and every other group for the same task type.

`TASK_QUEUES` uses JetStream's `WorkQueuePolicy` retention, which permits
exactly one consumer per filter subject and enforces that on filter
**overlap**, not equality. The bridge's filter always overlaps a grouped
worker's, so the two cannot coexist for the same task type: whichever
consumer is created second is rejected by the server.

A poll that hits this returns an error naming both filters, rather than an
empty task array. If you see `another consumer already covers an
overlapping filter on the TASK_QUEUES work-queue stream`, this is why —
the task type is being served by a grouped consumer.

**Use a native Go worker for grouped task types.** The bridge is for
non-Go workers on ungrouped types. If you need grouped work served over
HTTP, that is not currently supported and wants an issue describing the
use case — the protocol would need a `group` field on the poll request so
the bridge could target the grouped consumer exactly instead of
overlapping it.

## Examples

Working examples of non-Go workers using the HTTP bridge:

- **[Python worker](https://github.com/Craft-Design-Group/dagnats/tree/main/examples/http-worker-python)** -- complete Python worker with connect, poll, resolve, and reconnection logic
- **[curl walkthrough](https://github.com/Craft-Design-Group/dagnats/tree/main/examples/http-worker-curl)** -- step-by-step protocol walkthrough using only curl commands

## Related

- [Worker Configuration](/docs/workers/worker-configuration) -- Go native worker setup
- [Checkpoints](/docs/coordination/checkpoints) -- durable state persistence
- [Signals](/docs/coordination/signals) -- cross-step coordination
