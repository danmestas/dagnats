---
title: Overview
weight: 1
---

DagNats provides the infrastructure primitives that LLM agent pipelines need but ad-hoc scripts lack: durability, bounded execution, checkpointing, and observability.

## The Problem with Raw API Calls

Most LLM integrations start as a script: call the API, parse the response, maybe loop a few times. This works until it does not:

- **No retry on failure.** A transient 429 or 500 kills the entire run. You write retry logic. Then backoff. Then jitter.
- **No state persistence.** If the process dies mid-conversation, you lose everything and start over. For a 20-iteration agent loop, that is 20 wasted API calls.
- **No execution bounds.** An agent loop with no iteration cap burns tokens indefinitely. You add a counter. Then a timeout. Then a cost tracker.
- **No observability.** When something goes wrong at 2 AM, you grep logs. No traces, no metrics, no structured events.

Each of these is solvable individually. Solving all of them together, reliably, across multiple agents running concurrently, is a workflow engine.

## What DagNats Provides

| Concern | Raw Script | DagNats |
|---------|-----------|---------|
| Retry on failure | Manual retry loops | [Retry policies](/docs/reliability/retry-policies) with configurable backoff |
| State persistence | None (or ad-hoc files) | [Checkpoints](/docs/coordination/checkpoints) in NATS KV |
| Execution bounds | Manual counters | [MaxIterations, MaxDuration](/docs/step-types/agent-loops) on agent loops |
| Human intervention | Not possible mid-run | [Signals](/docs/coordination/signals) and [approval gates](/docs/step-types/approval-gates) |
| Parallel execution | threading/async | [Map steps](/docs/step-types/map-steps) with bounded concurrency |
| Dynamic planning | Hardcoded pipelines | [Planner steps](/docs/step-types/planner-steps) generate DAG fragments |
| Observability | print statements | Distributed traces, structured events, metrics |
| Cost control | Hope | Iteration caps, timeouts, [rate limits](/docs/flow-control/rate-limiting) |

## Core Primitives for LLM Pipelines

DagNats was not designed exclusively for AI. But the primitives it provides map directly to LLM agent requirements:

**Agent Loops** solve the variable-iteration problem. An LLM agent reasons in cycles (prompt, tool call, observe, decide). The number of cycles is unknown at design time. `StepTypeAgentLoop` with `Continue()` handles this natively with `MaxIterations` and `MaxDuration` as hard bounds.

**Checkpoints** solve the state persistence problem. Conversation history, tool results, and intermediate reasoning are saved to KV after each iteration. A crash replays only the current iteration, not the entire conversation.

**Signals** solve the human-in-the-loop problem. A running agent can pause and wait for human input via `WaitForSignal()`. An external system (CLI, API, Slack bot) sends the signal when the human responds.

**Planner Steps** solve the dynamic workflow problem. An LLM can analyze a task, generate a plan as a JSON DAG fragment, and the engine executes it. No predefined step graph required.

**Streaming** solves the real-time feedback problem. `PutStream()` publishes tokens as they arrive from the model API. Clients subscribe for live output without waiting for the full response.

## Section Guide

The pages in this section show how to compose these primitives into practical LLM agent patterns:

| Page | Pattern |
|------|---------|
| [Agent Loop Pattern](/docs/ai-patterns/agent-loop-pattern) | Core reasoning cycle with checkpointed state |
| [Tool Use as Steps](/docs/ai-patterns/tool-use-as-steps) | Each LLM tool call as a typed DAG step |
| [Planner Pattern](/docs/ai-patterns/planner-pattern) | LLM generates DAG dynamically |
| [Human in the Loop](/docs/ai-patterns/human-in-the-loop) | Approval gates and signal-based review |
| [Context Management](/docs/ai-patterns/context-management) | Conversation state across iterations and retries |
| [Multi-Agent Orchestration](/docs/ai-patterns/multi-agent-orchestration) | Delegation, fan-out, inter-agent communication |
| [Cost and Safety Controls](/docs/ai-patterns/cost-and-safety-controls) | Bounded execution, rate limits, spend caps |
| [Prompt and Response Schemas](/docs/ai-patterns/prompt-and-response-schemas) | Typed I/O validation for LLM handlers |

Each page includes working Go code patterns that reference actual DagNats APIs.
