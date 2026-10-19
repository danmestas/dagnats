---
title: HTTP Bridge
weight: 4
---

The HTTP bridge is an HTTP-to-NATS gateway that lets non-Go workers (Python, TypeScript, any language with an HTTP client) interact with DagNats over four REST endpoints.

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
    "max_tasks": 3,
    "worker_group": "gpu"
}
```

`max_tasks` defaults to 1. `worker_group` is optional. When it is omitted, the registration records the groups the worker's token is scoped to.

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
    "worker_group": "gpu",
    "max_tasks": 1,
    "timeout_ms": 30000
}
```

`worker_group` is optional. Leave it out to poll the ungrouped queue; set it to poll that group's queue (see [Worker groups](#worker-groups)). `max_tasks` defaults to 1 and `timeout_ms` to 5000.

Response:

```json
[
    {
        "task_id": "abc123.step-1",
        "run_id": "abc123",
        "step_id": "step-1",
        "input": {"url": "https://example.com/doc.pdf"},
        "metadata": {"team": "docs"},
        "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    }
]
```

`iteration` and `attempt` appear only when they are non-zero. `input`, `metadata`, `traceparent` and `tracestate` are omitted when empty.

The `timeout_ms` field controls how long the bridge waits for a task before returning empty. Maximum is 60 seconds.

`metadata` is the step's own static config (`StepDef.Metadata`), the same
map a NATS worker reads through `TaskContext.Metadata()`. It is omitted
when the step declares none. A `jobs:` workflow's job arrives here, as JSON
under `metadata["ci.job"]` (see the CI module reference).

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
| `complete` | Publishes step.completed with `output`, acks the NATS message |
| `fail` | Publishes step.failed with `error` and a `failure_type` |
| `continue` | Requests another iteration of an agent loop, with `output` as the next input |
| `pause` | Saves `checkpoint`, NAKs for `duration_ms` (1 to 3,600,000) so the task comes back |
| `checkpoint` | Saves `data` to the checkpoints KV and extends the claim |
| `heartbeat` | Extends the claim with nothing else |
| `stream` | Publishes `data` to `stream.{run_id}.{step_id}` for live consumers and extends the claim |
| `send_signal` | Writes `data` to the signal `name` of run `run_id` |
| `wait_signal` | Blocks until signal `name` arrives, or answers `408` after `timeout_ms` (1 to 3,600,000) |

`complete`, `fail`, `continue` and `pause` end the claim. The other actions leave the task in flight and restart its claim window.

Complete example:

```json
{
    "action": "complete",
    "output": {"extracted_text": "Hello world"}
}
```

`failure_type` is `retriable` (the default), `non_retriable` or `retry_after`. Fail with retry-after:

```json
{
    "action": "fail",
    "error": "rate limited by upstream API",
    "failure_type": "retry_after",
    "retry_after_ms": 5000
}
```

### Cancelled runs

Cancelling a run withdraws its tasks from HTTP workers:

- **Queued tasks are never handed out.** A poll that draws a task whose
  run has been cancelled drops it from the queue and returns nothing for
  it. It does not report the step as started.
- **Claimed tasks answer `409 Conflict`** with the body `run cancelled`
  on every resolve action, `heartbeat` included. The task is withdrawn
  at that point, so a later resolve for it gets `404`, and nothing the
  worker sends is recorded against the run. Treat the 409 as the signal
  to stop the work, and do not retry it.

A worker that never heartbeats learns about the cancel when it resolves.
To stop long work promptly, heartbeat while it runs.

Go workers on `sdk/httpclient` get the 409 as `httpclient.ErrRunCancelled`:

```go
if err := client.Complete(ctx, taskID, output); errors.Is(err, httpclient.ErrRunCancelled) {
    // the run was cancelled: discard the result, nothing was recorded
}
```

A resolve answers `200` on success, `404` if the task is unknown or already resolved, `403` if a different token claimed it, `409` if its run was cancelled, and `400` for a bad request.

### POST /v1/tasks/{id}/logs

Publishes stdout and stderr chunks for a claimed task to the run's log stream, the counterpart of `LogOut()` and `LogErr()` in the Go SDK:

```json
{
    "chunks": [
        {"stream": "stdout", "data": "Zm9vCg=="},
        {"stream": "stderr", "data": "YmFyCg=="}
    ]
}
```

Each chunk's `data` is base64. A request may carry up to 256 chunks and 1 MiB of body (larger answers `413`). The bridge assigns sequence numbers and enforces a per-step byte budget, so a worker cannot choose which attempt its chunks belong to.

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

`dagnats serve` mounts the bridge on its HTTP listener under `/v1/`, with no extra configuration. Point workers at that address (default `http://127.0.0.1:8080`). `bridge.NewBridge` takes an internal publisher type, so the package is not meant to be constructed outside the dagnats module.

The bridge binds the **checkpoints** and **signals** KV buckets when it is created. If a bucket is missing, the corresponding resolve actions return an error.

A claimed task is kept in the bridge's in-memory ack map. Every action that extends the claim (`heartbeat`, `stream`, `checkpoint`, `send_signal`, `wait_signal`) restarts that window, so a worker that keeps heartbeating keeps its task. A worker that goes silent loses it, and NATS redelivers it.

## Worker groups

A task whose step sets `worker_group` is published to a group-specific subject. To drain it over HTTP, name the group in `worker_group` on both connect and poll. A token minted with a group scope can poll only the groups it was minted for; any other group answers `403`.

A bridge poll uses the same durable consumer a native Go worker would create for that task type and group, so bridge workers and native workers for the same group share one queue. A poll with no `worker_group` takes only the ungrouped queue.

If creating the consumer fails because another consumer already covers an overlapping filter on the `TASK_QUEUES` work-queue stream, the poll returns an error naming both filters instead of an empty array. The error text says grouped task types are not pollable over the bridge; that wording is out of date, because polling a group works through `worker_group`.

## Examples

Working examples of non-Go workers using the HTTP bridge:

- **[Python worker](https://github.com/danmestas/dagnats/tree/main/examples/http-worker-python)** -- complete Python worker with connect, poll, resolve, and reconnection logic
- **[curl walkthrough](https://github.com/danmestas/dagnats/tree/main/examples/http-worker-curl)** -- step-by-step protocol walkthrough using only curl commands

## Related

- [Worker Configuration](/docs/workers/worker-configuration) -- Go native worker setup
- [Checkpoints](/docs/coordination/checkpoints) -- durable state persistence
- [Signals](/docs/coordination/signals) -- cross-step coordination
