---
title: actor
weight: 5
---

```
import "github.com/danmestas/dagnats/actor"
```

Lightweight actor runtime for DagNats. Actors are goroutines with channel mailboxes, supervised by parent actors. Pure Go with no NATS imports -- NATS integration lives in the engine package.

## Key Types

| Type | Description |
|------|-------------|
| `Runtime` | Top-level actor system: spawns, stops, and monitors actors |
| `Actor` | Interface that user-defined actors implement: `Receive(msg)` |
| `Address` | Unique actor identifier with `Type` and `ID` fields |
| `Message` | Envelope carrying data to an actor's mailbox |

## Supervision

| Type | Description |
|------|-------------|
| `OneForOne` | Supervision strategy: restart only the failed child, leave siblings running |

When an actor panics, the supervisor catches the panic and applies the configured restart strategy. This prevents cascading failures across the actor hierarchy.

## Address Format

Addresses are formatted as `type.id` for logging and map keys:

```go
addr := actor.Address{Type: "workflow", ID: "run-abc"}
fmt.Println(addr.String()) // "workflow.run-abc"
```

## Usage

```go
rt := actor.NewRuntime()

// Spawn a supervised actor
rt.Spawn(actor.Address{Type: "worker", ID: "w1"}, myActor)

// Send a message
rt.Send(actor.Address{Type: "worker", ID: "w1"}, payload)

// Stop all actors
rt.Stop()
```

The engine uses the actor runtime internally to manage per-run orchestration goroutines with supervision.
