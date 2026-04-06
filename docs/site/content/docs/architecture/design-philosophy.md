---
title: Design Philosophy
weight: 1
---

DagNats is built on three complementary design philosophies that shape every decision in the codebase.

## Ousterhout: A Philosophy of Software Design

John Ousterhout's book argues that complexity is the root cause of software difficulty. DagNats applies three of its core principles.

### Deep Modules, Small Interfaces

A **deep module** hides significant implementation behind a simple interface. The caller gets rich behavior without understanding the internals.

The `worker` package is the clearest example. A handler receives a `TaskContext` with methods like `Complete(output)`, `Fail(err)`, and `Checkpoint(state)`. Behind that interface, the worker SDK manages NATS consumer creation, message acknowledgment, heartbeats, retry NAKs, KV persistence, and pub/sub streaming. The handler author sees none of this.

```go
w.Handle("summarize", func(ctx worker.TaskContext) error {
    result := summarize(ctx.Input())
    return ctx.Complete(result)
})
```

The engine's `ActorOrchestrator` follows the same pattern. It exposes `Start()` and `Stop()`. Internally it manages a JetStream consumer, per-run actor spawning, supervision, event routing, and KV snapshots.

### Pull Complexity Downward

When a feature is hard, push the difficulty into the library -- not into every caller. DagNats centralizes NATS setup in `natsutil.SetupAll()`, retry logic in the engine, and timer management in the `SLEEP_TIMERS` stream. Workers never create streams. Handlers never manage consumers. Users never configure dedup windows.

### Define Errors Out of Existence

Where possible, DagNats eliminates error conditions rather than handling them. Running `natsutil.SetupAll()` uses `CreateOrUpdateStream`, which succeeds whether the stream already exists or not. The worker directory bucket is optional -- if it is missing, workers function normally. Unknown config file keys produce warnings, not errors.

## TigerStyle: Safety-First Engineering

TigerBeetle's coding discipline prioritizes correctness through constraints and contracts.

### Safety > Performance > Developer Experience

Every design tradeoff follows this ordering. The engine uses assertions (panics) for programmer errors -- a nil connection, an empty address, an impossible state. These are not recoverable errors; they indicate bugs that must be fixed immediately. The codebase contains at minimum two assertions per function.

```go
func NewActorOrchestrator(nc *nats.Conn, tel *observe.Telemetry) *ActorOrchestrator {
    if nc == nil {
        panic("NewActorOrchestrator: nc must not be nil")
    }
    if tel == nil {
        panic("NewActorOrchestrator: tel must not be nil")
    }
    // ...
}
```

This is intentional. A panic at startup is better than a nil pointer dereference at 3 AM under load.

### Bounded Everything

All loops, queues, retries, and collections have explicit upper bounds. Configuration files are limited to 300 lines. Leaf remotes are capped at 10. Worker configs are capped at 50. Map steps allow at most 10,000 items. Dynamic planners generate at most 100 steps. Actor mailboxes are buffered channels with fixed capacity. Restart trackers allow at most 5 restarts per minute.

Unbounded growth is a bug. Every bound is documented and enforced.

### Assertions as Contracts

Functions declare their preconditions as panicking assertions. This serves as executable documentation: if you call `Send()` with an empty address, the panic message tells you exactly what went wrong. Test coverage verifies these contracts, and the contracts protect production.

## HIPP: Simple, Fast, Reliable

The HIPP philosophy (named after SQLite's design principles) emphasizes self-containment and minimal dependencies.

### Zero External Dependencies Beyond NATS

DagNats requires only a NATS server. No PostgreSQL, no Redis, no external queue, no coordination service. The `dagnats serve` command embeds the NATS server itself, achieving true single-binary deployment with zero infrastructure prerequisites.

### Single-Binary Deployment

The `dagnats serve` binary contains everything: embedded NATS, JetStream storage, the workflow engine, API, triggers, and HTTP server. Download one binary, run one command. This is not a convenience wrapper -- it is the primary deployment model.

### Do Not Import What You Can Write

The codebase avoids external dependencies aggressively. The actor runtime, supervision strategies, restart tracking, KV-backed rate limiting, event correlation, and config file parsing are all implemented in-house. Each is under 300 lines. If you can write the 50 lines yourself, do it -- you own the behavior, the tests, and the upgrade path.

## How These Apply Together

The three philosophies reinforce each other:

- **Deep modules** (Ousterhout) + **bounded everything** (TigerStyle) = interfaces that are both simple and safe
- **Pull complexity downward** (Ousterhout) + **zero dependencies** (HIPP) = rich behavior with a small dependency tree
- **Assertions as contracts** (TigerStyle) + **define errors out of existence** (Ousterhout) = clear separation between programmer errors (panic) and operational errors (return)
- **Single-binary** (HIPP) + **safety-first** (TigerStyle) = a system that is easy to deploy and hard to misconfigure

The result is a workflow engine where the happy path is obvious, the error path is explicit, and the operational surface area is minimal.
