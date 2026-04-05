---
title: bridge
weight: 7
---

```
import "github.com/danmestas/dagnats/bridge"
```

HTTP-to-NATS gateway that lets non-Go workers interact with DagNats over HTTP. The bridge exposes three endpoints implementing the full worker lifecycle: connect, poll, and resolve.

## Key Types

| Type | Description |
|------|-------------|
| `Bridge` | The HTTP handler implementing the bridge endpoints |

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/v1/workers/connect` | Register worker and maintain SSE heartbeat |
| `POST` | `/v1/tasks/poll` | Long-poll for available tasks |
| `POST` | `/v1/tasks/{id}/resolve` | Complete, fail, pause, or checkpoint a task |

See [Wire Protocol](../../wire-protocol) for full request/response schemas.

## Authentication

When the `DAGNATS_BRIDGE_TOKEN` environment variable is set, all requests must include an `Authorization: Bearer {token}` header. When unset, all requests are allowed (development mode).

## Architecture

The bridge translates HTTP requests into NATS operations:

- **Connect**: Registers the worker in the `workers` KV bucket and sends SSE heartbeats every 25 seconds to maintain the HTTP connection and refresh the KV TTL
- **Poll**: Creates ephemeral pull subscriptions on the TASK_QUEUES stream for each requested task type, fetches messages, and stores them in an in-memory ack map keyed by task ID
- **Resolve**: Looks up the original NATS message in the ack map, publishes the appropriate event to the history stream, and ACKs or NAKs the message

## Usage

The bridge is typically started as part of `server.Run()` and does not need to be used directly. For custom setups:

```go
nc, _ := nats.Connect("nats://localhost:4222")
js, _ := jetstream.New(nc)
tel := observe.NewNoopTelemetry()

b := bridge.New(nc, js, tel)
http.ListenAndServe(":9090", b)
```
