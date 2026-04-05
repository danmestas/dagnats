---
title: httpclient
weight: 8
---

```
import "github.com/danmestas/dagnats/sdk/httpclient"
```

Go HTTP reference client for the DagNats bridge. Implements the worker protocol over HTTP and serves as a template for building SDKs in other languages.

## Key Types

| Type | Description |
|------|-------------|
| `Client` | HTTP client that implements the bridge wire protocol |

## Key Functions

| Function | Description |
|----------|-------------|
| `Connect(baseURL, token, workerID, taskTypes, maxTasks)` | Creates a client and registers the worker via SSE connect |

## Client Methods

| Method | Description |
|--------|-------------|
| `Poll(ctx, taskTypes, maxTasks, timeoutMs)` | Long-polls for available tasks |
| `Resolve(ctx, taskID, resolution)` | Resolves a task with complete/fail/pause/checkpoint |
| `Close()` | Disconnects the worker |

## Usage

```go
client, err := httpclient.Connect(
    "http://localhost:8080",
    "my-bridge-token",
    "worker-1",
    []string{"llm", "http"},
    2,
)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// Poll for tasks
tasks, err := client.Poll(ctx, []string{"llm"}, 1, 30000)
if err != nil {
    log.Fatal(err)
}

// Resolve a task
err = client.Resolve(ctx, tasks[0].TaskID, protocol.TaskResolution{
    Action: "complete",
    Output: json.RawMessage(`{"result": "ok"}`),
})
```

## SDK Template

This package demonstrates the three HTTP calls needed for any language SDK:

1. `POST /v1/workers/connect` -- register and maintain heartbeat
2. `POST /v1/tasks/poll` -- long-poll for tasks
3. `POST /v1/tasks/{id}/resolve` -- resolve tasks

See [Wire Protocol](../../wire-protocol) for the complete JSON schemas.
