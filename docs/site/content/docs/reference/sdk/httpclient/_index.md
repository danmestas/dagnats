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
| `New(baseURL, opts...)` | Creates a client. `WithToken(token)` sets the bearer token. |

## Client Methods

| Method | Description |
|--------|-------------|
| `Connect(ctx, workerID, taskTypes, maxTasks)` | Registers the worker and opens the SSE heartbeat stream |
| `Disconnect()` | Closes the heartbeat stream |
| `Poll(ctx, taskTypes, maxTasks, timeout)` | Long-polls for tasks and returns `[]protocol.TaskPayload` |
| `Complete(ctx, taskID, output)` | Finishes a task with output |
| `Fail(ctx, taskID, errMsg)` | Fails a task |
| `Pause(ctx, taskID, name, duration, checkpoint)` | Saves a checkpoint and has the task redelivered after `duration` |
| `Checkpoint(ctx, taskID, data)` | Saves state and extends the claim |

Resolving a task whose run was cancelled returns `ErrRunCancelled`.

## Usage

```go
client := httpclient.New("http://localhost:8080", httpclient.WithToken("my-bridge-token"))
if err := client.Connect(ctx, "worker-1", []string{"llm", "http"}, 2); err != nil {
    log.Fatal(err)
}
defer client.Disconnect()

// Poll for tasks
tasks, err := client.Poll(ctx, []string{"llm"}, 1, 30*time.Second)
if err != nil {
    log.Fatal(err)
}

// Resolve a task
for _, task := range tasks {
    err = client.Complete(ctx, task.TaskID, json.RawMessage(`{"result": "ok"}`))
    if errors.Is(err, httpclient.ErrRunCancelled) {
        continue // the run was cancelled; nothing was recorded
    }
    if err != nil {
        log.Fatal(err)
    }
}
```

## SDK Template

This package demonstrates the three HTTP calls needed for any language SDK:

1. `POST /v1/workers/connect` -- register and maintain heartbeat
2. `POST /v1/tasks/poll` -- long-poll for tasks
3. `POST /v1/tasks/{id}/resolve` -- resolve tasks

See [Wire Protocol](../../wire-protocol) for the complete JSON schemas.
