# ADR-002: Durable Agent Loop via dagnats Primitives

**Status:** Accepted  
**Date:** 2026-04-08  
**Context:** Choosing the durability model for auto-claude's agent harness  

## Context

The auto-claude agent harness runs Claude LLM agents in a think/act/observe loop. Each iteration involves an LLM API call (2-30s latency) followed by tool executions (file reads, shell commands, grep). A full conversation may run 20+ iterations over 30 minutes.

Three durability models were evaluated:

| Model | Reference | How it works |
|-------|-----------|-------------|
| **Inngest steps** | utah | Every LLM call and tool execution is a durable `step.run()`. Crash after tool 5 replays from that checkpoint. |
| **In-process loop** | auto-claude v1 | `Loop.Run()` executes all iterations in memory. Crash = total loss. |
| **dagnats agent loop** | dagnats engine | Each iteration is one `Continue()` cycle. Engine tracks `StepState.Iterations`. Checkpoint saves conversation between iterations. |

## Decision

Use **dagnats' native agent loop** (`StepTypeAgentLoop` + `Continue()` + `Checkpoint()`).

### How it works

```
Handler call 1:  LoadCheckpoint() → [nil]     → Step() → Checkpoint(msgs) → Continue()
Handler call 2:  LoadCheckpoint() → [msgs]    → Step() → Checkpoint(msgs) → Continue()
Handler call 3:  LoadCheckpoint() → [msgs]    → Step() → no tools          → Complete()
```

The engine re-enqueues the task after each `Continue()` with `Iteration: N+1`. If the process crashes mid-iteration, NATS redelivers the unacked message. The handler loads the previous iteration's checkpoint and retries only that one iteration.

### Core domain stays pure

The `agent/` package defines `Loop.Step()` — one think/act/observe cycle that takes `StepState` and returns `StepResult`. It knows nothing about NATS, checkpoints, or durability:

```go
type StepResult struct {
    Done     bool       // true = final text response
    State    StepState  // updated conversation
    Response string     // final answer (if Done)
}
```

The `harness/handler.go` maps this to dagnats:
- `StepResult.Done == false` → `Checkpoint(state) + Continue(output)`
- `StepResult.Done == true` → `Complete(response)`

### GAN pattern: multi-phase loops

The TDD GAN harness uses the same mechanism for adversarial coder/tester loops. Each `Continue()` alternates phases:

```
Iteration 0: Coder phase   → writes tests + code     → Checkpoint(phase=tester)  → Continue()
Iteration 1: Tester phase  → runs tests, reviews     → REJECTED                  → Continue()
Iteration 2: Coder phase   → fixes based on feedback  → Checkpoint(phase=tester)  → Continue()
Iteration 3: Tester phase  → runs tests              → APPROVED                   → Complete()
```

Within each phase, the handler runs a mini-loop of `Step()` calls (up to `MaxStepsPerPhase`) so the agent can make multiple tool calls. The phase is stored in the checkpoint state.

## Alternatives Rejected

### Per-tool-call durability (Inngest model)

Making every tool execution a separate dagnats step or checkpoint would give finer-grained recovery but:
- Adds ~50ms overhead per tool call (NATS publish + ack round-trip)
- Agent loops execute 5-50 tool calls per iteration — latency compounds
- Tool calls are fast (file reads: <1ms, grep: <10ms, bash: <1s) — the LLM call dominates
- Conversation state is the only thing worth checkpointing (tools are idempotent or re-runnable)

### No durability (in-process loop only)

The CLI REPL (`cmd/auto/main.go`) still uses `Loop.Run()` — a simple `for { Step(); if done break }` wrapper with zero durability. This is fine for interactive use but not for unattended pipelines that run for 30+ minutes.

## Consequences

- Crash recovery granularity is per-iteration (not per-tool-call) — acceptable because LLM calls are the expensive/slow operations
- `Loop.Step()` is independently testable with fake completers — no NATS needed
- Same `Step()` function serves both CLI (no durability) and dagnats (full durability)
- GAN multi-phase loops work naturally — phase tracking is just checkpoint state
- `MaxIterations` and `MaxDuration` are enforced by the engine, not application code
- Budget warnings use `AgentLoopConfig.BudgetWarnAt`/`BudgetForceAt` from the workflow definition
