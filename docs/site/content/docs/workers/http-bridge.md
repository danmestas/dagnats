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
        "input": {"url": "https://example.com/doc.pdf"}
    }
]
```

The `timeout_ms` field controls how long the bridge waits for a task before returning empty. Maximum is 60 seconds.

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

Set the `DAGNATS_BRIDGE_TOKEN` environment variable to enable bearer token authentication. When set, all requests must include:

```
Authorization: Bearer <token>
```

When unset, all requests are allowed (development mode).

## Setup

```go
b := bridge.NewBridge(nc, tel)
http.ListenAndServe(":8080", b.Handler())
```

The bridge binds optional KV buckets for **checkpoints** and **signals** at construction time. If these buckets are missing, the corresponding resolve actions return an error.

## Examples

Working examples of non-Go workers using the HTTP bridge:

- **[Python worker](https://github.com/Craft-Design-Group/dagnats/tree/main/examples/http-worker-python)** -- complete Python worker with connect, poll, resolve, and reconnection logic
- **[curl walkthrough](https://github.com/Craft-Design-Group/dagnats/tree/main/examples/http-worker-curl)** -- step-by-step protocol walkthrough using only curl commands

## Related

- [Worker Configuration](/docs/workers/worker-configuration) -- Go native worker setup
- [Checkpoints](/docs/coordination/checkpoints) -- durable state persistence
- [Signals](/docs/coordination/signals) -- cross-step coordination
