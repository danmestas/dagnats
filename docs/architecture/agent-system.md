# Agent System

## Architecture Decision: NATS-Native Actors (No Framework)

DagNats was already ~70% actor-like (per-run state, message-driven, supervision via NATS). Rather than adopt an actor framework, we implemented pure Go actor primitives that integrate naturally with NATS.

## Actor Runtime (`actor/` package)

**Core types:**
- `Address` — type + ID pair for actor identification
- `Message` — envelope with From address and Payload
- `Actor` interface — single `Receive(ctx, msg)` method
- `Context` — `Self()`, `Send()`, `Spawn()` for actor operations
- `Directive` — Restart, Stop, Escalate, Resume

**Supervision strategies:**
- `OneForOne` — restart only failed child (default)
- `AllForOne` — restart all siblings on failure
- Each strategy has `Decide(err) Directive`

**Restart tracking:** Bounded time-window counter (default: 5 restarts/minute). Prevents infinite restart loops. Iterative pruning of expired restarts.

**Runtime mechanics:**
- Per-actor mailbox (buffered channel)
- Spawn creates goroutine running receive loop
- Sequential message processing (thread-safe per actor)
- Parent supervision on child error
- Recursive kill tree on parent failure

**Key constraint:** `actor/` is pure Go with zero NATS dependencies. NATS integration is in `engine/`.

## Per-Workflow Actors (`engine/`)

**WorkflowActor** (`engine/workflow_actor.go`):
- Implements `actor.Actor`
- Holds `WorkflowRun` + `WorkflowDef` in memory (no per-event KV loads)
- Handles: started, completed, failed, continue events
- Still snapshots to KV for durability

**ActorOrchestrator** (`engine/actor_orch.go`):
- Subscribes to `history.>` stream
- Routes events to per-run WorkflowActors
- Spawns actors on demand
- OneForOne supervision

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
