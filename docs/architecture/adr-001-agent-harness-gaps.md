# ADR-001: Agent Harness Interface Gaps

**Status:** Accepted  
**Date:** 2026-04-08  
**Context:** Building auto-claude agent harness on dagnats  

## Context

While building [auto-claude](https://github.com/dmestas/auto-claude) — a TDD GAN harness that orchestrates Claude agents via dagnats — we discovered nine interface gaps that forced workarounds or blocked functionality entirely. The harness ports [inngest/utah](https://github.com/inngest/utah)'s durable agent loop pattern (think/act/observe with per-iteration checkpointing) onto dagnats' native `StepTypeAgentLoop` + `Continue()` + `Checkpoint()` primitives.

The auto-claude architecture has three layers:
- **`agent/`** — Pure domain. `Loop.Step()` runs one think/act/observe cycle. Zero external deps.
- **`adapter/`** — Anthropic SDK, filesystem tools, workspace memory. Implements port interfaces.
- **`harness/`** — Thin glue. `NewLoopHandler()` returns `func(worker.LoopTask) error`, wiring `Step()` to dagnats' `Continue()`/`Checkpoint()` lifecycle.

Each dagnats iteration = one `Step()` call. The handler loads conversation from checkpoint, runs one LLM call + tool executions, saves updated conversation to checkpoint, then calls `Continue()` (more tools needed) or `Complete()` (final answer).

## Decisions

### 1. LoopTask must include PutStream + Heartbeat

**Problem:** `LoopTask` composed `CheckpointTask + Continue + FailRetryAfter`. Agent loop handlers need to stream incremental LLM output (`PutStream`) and extend NATS ack deadlines during long API calls (`Heartbeat`). These existed on `StreamTask` and the full `TaskContext`, but not on `LoopTask`.

**Decision:** Add `PutStream(data []byte) error` and `Heartbeat() error` to `LoopTask`. No new implementations needed — `taskContext` already has both methods. This widens the interface contract to match what agent loop handlers actually need.

**Alternative rejected:** Create a new `AgentLoopTask` composite interface. Rejected because it fragments the role hierarchy unnecessarily — every loop handler benefits from streaming and heartbeat, not just agent ones.

### 2. Bridge needs continue/heartbeat/stream actions

**Problem:** The HTTP bridge (`POST /v1/tasks/{id}/resolve`) supported 6 actions: `complete`, `fail`, `pause`, `checkpoint`, `send_signal`, `wait_signal`. Non-Go agent workers (Python, TypeScript) using the bridge could not:
- Implement agent loops (no `continue`)
- Keep messages alive during long LLM calls (no `heartbeat`)
- Stream incremental output (no `stream`)

**Decision:** Add three resolve actions:
- `continue` — Publishes `step.continue` event with nonce-based dedup, acks message, removes from ackMap. Mirrors `worker/context.go:Continue()`.
- `heartbeat` — Calls `msg.InProgress()`. Does NOT ack or remove from ackMap.
- `stream` — Publishes to `stream.{runID}.{stepID}` via core NATS, extends ack deadline. Does NOT remove from ackMap.

### 3. TaskContext must expose Context()

**Problem:** `taskContext.ctx` was private. Handlers used `context.Background()` — no cancellation propagation, no trace context for LLM calls.

**Decision:** Add `Context() context.Context` to `TaskContext` and `SimpleTask` (so all role interfaces inherit it). Returns the context created in `handleMessage` which carries the OTel span.

### 4. RateLimitError as framework-level sentinel

**Problem:** Both auto-claude harnesses had identical `isRateLimitError()` functions doing string matching (`"rate_limit"`, `"429"`). The framework's `handleTaskError` only knew about `NonRetryableError`, falling through to a generic 5s NAK for everything else.

**Decision:** Add `RateLimitError{Err, RetryAfter}` to `worker/errors.go`. Check it in `handleTaskError` before `NonRetryableError`. On match: call `FailRetryAfter(err, retryAfter)`, ack, return. Handlers return `worker.NewRateLimitError(err, 30*time.Second)` instead of hand-rolling retry logic.

### 5. WorkerShim needs role-based Handle methods

**Problem:** `WorkerShim.Handle()` only accepted `worker.HandlerFunc`. The `ganharness.RegisterHandlers()` function needed `worker.HandleLoop()` for the GAN handler but couldn't use it with embedded workers — forced to take `*worker.Worker` directly, bypassing the embedded worker pattern.

**Decision:** Add `HandleLoop`, `HandleStream`, `HandleSignal`, `HandleSingleton` to `WorkerShim`. Each wraps the typed handler as `HandlerFunc` (same as `worker/roles.go` registration functions). Materialization dispatches `roleSingleton` to `w.HandleSingleton()` (changes consumer config); all others use `w.Handle()`.

### 6. AgentLoopConfig budget thresholds

**Problem:** Budget warnings ("you have N iterations left") were hardcoded in application code. Different workflows need different thresholds.

**Decision:** Add `BudgetWarnAt` and `BudgetForceAt` to `AgentLoopConfig`. Both are advisory — the engine does not enforce them. Workers read them to inject warnings into LLM conversations. Zero values mean "no warning" (backward compatible via `omitempty`).

### 7. MockTaskContext in dagnatstest

**Problem:** Three nearly identical mock implementations across auto-claude test files. Every external consumer faces the same boilerplate.

**Decision:** Add `dagnatstest.MockTaskContext` satisfying all role interfaces. Thread-safe via mutex. Configurable inputs, recorded outputs. Compile-time verified via `var _ worker.TaskContext = (*MockTaskContext)(nil)`.

## Consequences

- Agent loop handlers (`LoopTask`) now have full capabilities without casting to `TaskContext`
- Non-Go agents can implement the complete worker lifecycle over HTTP
- Rate limit handling is consistent across all handlers
- External consumers can test handlers without 50+ lines of mock boilerplate
- `SimpleTask.Context()` is a breaking change for consumers with hand-rolled mocks (must add one method)
