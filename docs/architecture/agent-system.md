# Agent System

## Orchestration Model (`engine/`)

**Status (2026-08-06):** Production runs through a single event-sourced
`engine.Orchestrator`. It consumes the `WORKFLOW_HISTORY` stream,
loads run state from KV per event, and serializes each run's events
behind a per-run mutex (`getRunLock`, a `sync.Map` of `*sync.Mutex`)
to prevent concurrent KV load-modify-save races. Supervision
(retry/timeout) is provided by JetStream's `AckWait`/`MaxDeliver` per
this repo's NATS-native patterns, not by an in-process actor system.

The experimental actor-based engine path (`WorkflowActor`,
`ActorOrchestrator`) was removed on 2026-05-02 — see
[ADR-009](adr-009-remove-experimental-actor-orchestrator.md). The
standalone `actor/` runtime package it depended on was later deleted
too (issue #598) after remaining unused since; recover either from
`git log` if a future actor-model exploration needs a starting point.

## Agent Step Type

- `StepTypeAgent` routes to agent SDK (not core workers)
- `StepDef.Metadata map[string]string` carries agent config (opaque to engine)
- `WithStepRoutes(map[StepType]string)` functional option for custom routing
- Engine never interprets agent-specific metadata keys

## Agent SDK Integration (dagnats-agents, separate repo)

**Boundary:** DagNats core provides primitives. Agent runtime lives in `github.com/Craft-Design-Group/dagnats-agents` (TypeScript + Claude Agent SDK).

**Core provides:**
- `StepTypeAgent` constant + routing
- Child workflow spawn/complete/fail lifecycle
- `Metadata` on StepDef for role references
- `WithStreams()`/`WithKVBuckets()` for agent-specific NATS resources

**Agent SDK owns:**
- LLM tool-use loop
- Tool execution (file, search, bash as NATS microservices)
- Agent configs/roles (KV: `roles`, `tool_registry`)
- Conversation state management
- Streaming: AGENT_TASKS stream, `agent.task.>` subjects

**Tool execution model:** Tools run inside agent loop (tight, latency-sensitive inner loop), NOT as separate DAG steps.

## Deferred Features

- MCP integration (external tool servers)
- Co-located handlers (define workflow + handler in one file)
- Standalone tasks (escape DAG ceremony for simple operations)
- Full condition system (WaitFor, SleepCond, UserEventCond, Or/And combinators)
- Worker labels/affinity (heterogeneous fleet routing)
- Rate limiting as first-class primitive
- Durable SleepFor (LoopDelay on AgentLoop covers current use case)

Each has an explicit "build when" trigger documented during competitive analysis. Not speculative — deferred until production adoption creates the need.
