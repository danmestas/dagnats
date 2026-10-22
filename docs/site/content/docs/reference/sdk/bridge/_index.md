---
title: bridge
weight: 7
---

```
import "github.com/danmestas/dagnats/bridge"
```

HTTP-to-NATS gateway that lets non-Go workers interact with DagNats over HTTP. The bridge exposes four endpoints implementing the worker lifecycle: connect, poll, resolve, and log upload.

## Key Types

| Type | Description |
|------|-------------|
| `Bridge` | The HTTP handler implementing the bridge endpoints |

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/v1/workers/connect` | Register worker and maintain SSE heartbeat |
| `POST` | `/v1/tasks/poll` | Long-poll for available tasks |
| `POST` | `/v1/tasks/{id}/resolve` | `complete`, `fail`, `continue`, `pause`, `checkpoint`, `heartbeat`, `stream`, `send_signal`, `wait_signal`, `journal.append`, `journal.read` or `load_checkpoint` |
| `POST` | `/v1/tasks/{id}/logs` | Upload stdout and stderr chunks for a task |

See [HTTP Bridge](/docs/workers/http-bridge) for request and response details.

## Authentication

When the `DAGNATS_BRIDGE_TOKEN` environment variable is set, all requests must include an `Authorization: Bearer {token}` header. When unset, all requests are allowed (development mode).

## Architecture

The bridge translates HTTP requests into NATS operations:

- **Connect**: Registers the worker in the `workers` KV bucket and sends SSE heartbeats every 25 seconds to keep the HTTP connection alive
- **Poll**: Uses the same durable pull consumer a native Go worker would use for the task type (and `worker_group`), fetches messages, and stores them in an in-memory ack map keyed by task ID
- **Resolve**: Looks up the original NATS message in the ack map, publishes the matching event to the history stream, and acks or NAKs the message

## Usage

`dagnats serve` and `server.Run()` mount the bridge under `/v1/` on the HTTP listener; there is nothing to start by hand. `bridge.NewBridge` takes the internal tracing publisher type, so it cannot be constructed from outside the module.
