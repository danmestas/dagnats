# Agent SDK Integration Design

DAG-orchestrated Claude Agent SDK workloads with role-based tooling, child workflows, and NATS-native communication.

## Problem

DagNats orchestrates DAG workflows, but steps today are simple task handlers. To automate LLM coding pipelines (brainstorm → spec → plan → implement → review), each step needs to be a full Claude agent with skills, sub-agents, permissions, and custom tools. Agents need to spawn child agents through the platform as nested workflows, and tools need to be configurable per-role with live updates.

## Design Principles

- DagNats core stays general-purpose. All agent-specific code lives in a separate package (`dagnats-agents`).
- All inter-component communication flows through NATS. No HTTP, no stdio, no child process IPC.
- No MCP. Custom tools use NATS request/reply for execution dispatch.
- TypeScript Agent SDK for agent execution. Go for orchestration and tool implementation.
- Roles are the selection mechanism, the registry is the resolution mechanism.

## Step Types After This Work

The existing `StepType` enum in `dag/types.go` defines `StepTypeNormal`, `StepTypeAgentLoop`, and `StepTypeSubWorkflow`. This spec adds `StepTypeAgent` and implements `StepTypeSubWorkflow` (previously defined but unimplemented).

| StepType | Routing | Purpose |
|---|---|---|
| `StepTypeNormal` | `TASK_QUEUES` (unchanged) | Simple Go task handler |
| `StepTypeAgentLoop` | `TASK_QUEUES` (unchanged) | Iterative Go handler with Continue() |
| `StepTypeAgent` | Configurable via routing map | Claude Agent SDK step (new) |
| `StepTypeSubWorkflow` | Engine-internal | Spawn child workflow (now implemented) |

`StepTypeAgent` is the new addition. `StepTypeSubWorkflow` is the existing type, now implemented as part of the child workflow support described below. Agent steps spawn child workflows at runtime via the `spawn_workflow` tool, which uses the `StepTypeSubWorkflow` machinery under the hood.

## Architecture

Three components, one bus.

### Go Engine (DagNats core — mostly unchanged)

The existing orchestrator dispatches tasks via event sourcing. Generic additions:

- **Configurable step type routing.** `NewOrchestrator` accepts a `StepRoutes map[dag.StepType]string` mapping step types to stream names. Unregistered types default to `TASK_QUEUES`. The agents package passes `{dag.StepTypeAgent: "AGENT_TASKS"}`. This changes the dispatch path in `buildTaskMsg` from a hardcoded subject to a map lookup — a single conditional.
- **Child workflow support (implements StepTypeSubWorkflow).** Any step can spawn a nested workflow. `WorkflowRun` gains `ParentRunID` and `ParentStepID` fields. The engine handles `workflow.spawn` events (create child run), orchestrates child workflows independently, and publishes `workflow.child.completed` when the child finishes.
- **Extensible NATS setup.** `natsutil.SetupAll` accepts optional `[]natsutil.StreamConfig` and `[]natsutil.KVConfig` parameters for additional streams and KV buckets. Downstream packages pass their resource definitions without forking.

### TypeScript Agent Worker (dagnats-agents package)

A standalone TypeScript service that subscribes to the `AGENT_TASKS` JetStream stream. For each task:

1. Resolves the step's role configuration from NATS KV.
2. Expands tool bundles into individual tool definitions.
3. Builds Agent SDK `tool()` handlers — thin shims that forward calls to Go services via NATS request/reply.
4. Runs Agent SDK `query()` with the resolved config (model, system prompt, skills, tools, permissions).
5. Publishes `step.completed` or `step.failed` to `WORKFLOW_HISTORY`.

The agent worker uses the full Agent SDK runtime: skills, tool picker, sub-agents, permissions, adaptive thinking.

### Go Tool Services (dagnats-agents package)

Tools are Go functions registered as NATS micro services. Each listens on `tool.exec.{name}`, receives JSON input, returns JSON output.

Tool categories:

- **File ops** — read, write, edit, glob, grep.
- **Git ops** — status, diff, commit, branch, log.
- **Shell** — bounded command execution with timeout and output limits.
- **Search** — codebase search.
- **Platform tools** — spawn_workflow, wait_for_workflow, get_step_output. Unlike the other categories, these are implemented in TypeScript within the agent worker process. They do not follow the NATS request/reply pattern because they need direct access to the agent worker's NATS connection and run context to publish events and watch KV. They are registered as `tool()` handlers alongside the NATS-backed tools but execute locally.

## Roles

A role is a reusable agent profile stored in NATS KV (`roles` bucket). It defines everything needed to configure an Agent SDK `query()` call.

```typescript
defineRole({
  name: "coder",
  model: "opus",
  systemPrompt: "You are an expert software engineer...",
  skills: ["superpowers:test-driven-development"],
  tools: ["file-ops", "git-ops", "shell", "spawn-workflow"],
  permissions: { allowWrite: true, allowShell: true },
  maxTurns: 50,
  effort: "high",
})
```

Role fields:

| Field | Type | Purpose |
|---|---|---|
| `name` | string | Unique identifier |
| `model` | `"opus" \| "sonnet" \| "haiku"` | Claude model for this role |
| `systemPrompt` | string | Agent system prompt |
| `skills` | string[] | Agent SDK skills to load |
| `tools` | string[] | Tool names or bundle names |
| `permissions` | object | Agent SDK permission config |
| `maxTurns` | number | Bound on agent conversation turns |
| `effort` | `"low" \| "medium" \| "high" \| "max"` | Thinking effort level |

Roles are stored in NATS KV and resolved at runtime. Updating a role in KV takes effect on the next agent run without redeploying anything.

## Tool Registry

Tools and bundles are stored in NATS KV (`tool_registry` bucket).

### Tool entry

```json
{
  "name": "file_read",
  "description": "Read file contents",
  "inputSchema": {
    "type": "object",
    "properties": {
      "path": { "type": "string" },
      "offset": { "type": "number" },
      "limit": { "type": "number" }
    },
    "required": ["path"]
  },
  "subject": "tool.exec.file_read",
  "bundle": "file-ops"
}
```

### Bundle entry

```json
{
  "name": "file-ops",
  "tools": ["file_read", "file_write", "file_edit", "glob", "grep"]
}
```

### Resolution flow

When the agent worker picks up a task:

1. Read role config from KV (`roles` bucket).
2. Expand tool bundles into individual tool names (`tool_registry` bucket).
3. Fetch each tool's schema and NATS subject from KV.
4. Build Agent SDK `tool()` handlers — each handler does `nats.request(tool.subject, input)` and returns the response.
5. Start `query()` with the fully resolved configuration.

Updates to tool schemas, bundle membership, or role config in KV are picked up by the next agent run. No redeploys needed.

### Tool execution error handling

Tool calls flow through NATS request/reply with a bounded timeout (default 30s, configurable per tool in the registry). Error cases:

- **Go service returns an error payload** — the tool handler translates it to a tool error result (`is_error: true` with the error message). The agent sees the error and can retry or try a different approach.
- **NATS request times out** — the tool handler returns a timeout error result to the agent. The agent decides whether to retry.
- **Go service is unavailable** — NATS returns no responders. The tool handler returns an availability error result.

The agent worker validates tool call inputs against the registry schema before forwarding to NATS. Invalid inputs are rejected immediately as tool error results without making a NATS request.

## TypeScript Workflow Definition SDK

Workflow authors define pipelines in TypeScript. Both the DAG shape and agent configuration are defined together:

```typescript
import { workflow } from "dagnats-agents";

const pipeline = workflow("code-review-pipeline", async (wf) => {
  const plan = wf.agent("plan", {
    role: "planner",
    input: wf.input(),
  });

  const implement = wf.agent("implement", {
    role: "coder",
    after: [plan],
  });

  const test = wf.agent("test", {
    role: "tester",
    after: [implement],
  });

  const review = wf.agent("review", {
    role: "reviewer",
    after: [test],
    skipIf: { step: test, output: "line_count", op: "<", value: 10 },
  });
});

await pipeline.register();
```

- `wf.agent()` creates a step with `StepTypeAgent` that routes to the TypeScript agent worker.
- Role binding is declared per step. The role's tools/prompt/skills resolve at runtime from KV.
- `after` and `skipIf` map to the same DAG primitives as the Go builder. The TypeScript `skipIf` serializes to the existing `ParentCond` JSON schema in `dag/condition.go`.
- `register()` serializes the workflow definition to JSON conforming to the existing `dag.WorkflowDef` schema and publishes it to NATS KV (`workflow_defs`). Agent steps include a `Role` field on `StepDef` (new field, ignored by existing Go workers).

The Go builder DSL continues to work for non-agent workflows.

## Child Workflows & Sub-Agent Spawning

When a running agent needs to delegate work to another agent, it calls the `spawn_workflow` tool.

### Agent perspective

The agent calls `spawn_workflow({ name, role, input })`. The agent pauses and waits. A child workflow runs through the full orchestration lifecycle. The result flows back to the parent agent as a tool result.

### Under the hood

1. The `spawn_workflow` tool handler publishes a `workflow.spawn` event to `WORKFLOW_HISTORY` with the parent's run ID and step ID.
2. The Go engine creates a child `WorkflowRun` linked to the parent (`ParentRunID`, `ParentStepID`).
3. The child workflow runs through normal orchestration — its own steps, events, and snapshots.
4. On child completion, the engine publishes `workflow.child.completed`.
5. The parent agent worker watches the child's run state via NATS KV watch.
6. The tool handler returns the child's output to the agent.

### Supporting tools

| Tool | Purpose |
|---|---|
| `spawn_workflow` | Create and wait for a child workflow |
| `wait_for_workflow` | Wait for a previously spawned child (for parallel children) |
| `get_step_output` | Read output from a sibling step in the same DAG |

### Bounds

Nesting depth is bounded (configurable, default 3). The engine enforces this when processing `workflow.spawn` events. Exceeding the limit fails the spawn with a non-retryable error.

### Parent waiting strategy

For v1, the parent Agent SDK session stays alive while waiting for child workflows. Agent steps are long-running by nature (LLM calls are slow), so holding a worker slot during child execution is not materially different. Session checkpointing and resumption is a future optimization.

### Resource bounds

With a max nesting depth of 3, a single workflow tree can hold up to 3 concurrent active Agent SDK sessions (parent + child + grandchild). Each session maintains an active Claude API connection and consumes a worker slot. The agent worker should enforce a configurable `maxConcurrentSessions` limit (default 10) across all workflow trees to prevent resource exhaustion. Sessions beyond the limit queue in NATS until a slot opens.

## NATS Resources

### Streams (added by agents package)

| Stream | Purpose | Policy |
|---|---|---|
| `AGENT_TASKS` | Agent step task queue | WorkQueue, same semantics as `TASK_QUEUES` |

### KV Buckets (added by agents package)

| Bucket | Purpose |
|---|---|
| `roles` | Role definitions (keyed by `role.{name}`) |
| `tool_registry` | Tool and bundle definitions (keyed by `tool.{name}` or `bundle.{name}`) |

### Subjects (added by agents package)

| Pattern | Purpose |
|---|---|
| `tool.exec.{name}` | NATS request/reply for tool execution |

Existing DagNats streams (`WORKFLOW_HISTORY`, `TASK_QUEUES`, `EVENTS`) and KV buckets (`workflow_defs`, `workflow_runs`) are unchanged.

## DagNats Core Changes

All changes are additive. Existing behavior is unchanged — existing step types still route to `TASK_QUEUES`, existing workflows still run identically. The child workflow mechanism is new functionality, not a modification of existing event handling.

### protocol package

Add new event types to `protocol.go`:

| Event Type | Purpose |
|---|---|
| `workflow.spawn` | Request to create a child workflow (carries parent run/step ID, child workflow name, input) |
| `workflow.child.completed` | Child workflow finished (carries child run ID, output, parent run/step ID) |
| `workflow.child.failed` | Child workflow failed (carries child run ID, error, parent run/step ID) |

These are new entries in the `EventType` constants. The orchestrator's `dispatchEvent` switch gains three new cases.

### dag/types.go

- Add `StepTypeAgent` constant to `StepType` enum.
- Add `Role` field to `StepDef` (string, optional — empty for non-agent steps).
- Add `ParentRunID` and `ParentStepID` fields to `WorkflowRun` (string, optional — empty for top-level runs).

### engine/orchestrator.go

- **`NewOrchestrator` accepts `StepRoutes map[dag.StepType]string`.** The dispatch function looks up the step type in this map. Missing entries default to `TASK_QUEUES`. This replaces the hardcoded subject construction with a map lookup — a single conditional change in `buildTaskMsg`.
- **Child workflow event handlers.** Three new cases in `dispatchEvent`:
  - `workflow.spawn` → validate nesting depth, create child `WorkflowRun`, publish `workflow.started` for child.
  - `workflow.child.completed` → look up parent run/step, publish result to parent step's waiting mechanism.
  - `workflow.child.failed` → same as completed but propagates error.

### natsutil package

- `SetupAll` accepts optional `[]StreamConfig` and `[]KVConfig` parameters. Downstream packages pass additional resource definitions. Existing callers with no extra resources are unchanged.

## Observability

- Agent tool calls are NATS requests, automatically visible in NATS monitoring.
- Go tool services publish metrics through existing `observe` interfaces (call count, latency, errors).
- The TypeScript agent worker publishes agent progress events (turn count, tool calls, token usage) to a NATS subject for real-time monitoring.
- Child workflows have their own trace context linked to the parent span via the existing propagation mechanism.

## Package Boundary

| DagNats core | dagnats-agents package |
|---|---|
| DAG logic, validation, resolution | Agent step type definition |
| Engine orchestration | TypeScript agent worker |
| Event sourcing, snapshots | Role and tool registry (KV schemas, resolution) |
| Child workflow lifecycle | Tool bridge (NATS → Agent SDK) |
| Extensible step routing | Go tool services (file, git, shell, search) |
| Extensible NATS setup | TypeScript workflow definition SDK |
| Worker framework | Platform tools (spawn, wait, get output) |
| Observability interfaces | Agent-specific observability events |

## End-to-End Flow

1. Author defines workflow and roles in TypeScript, registers via NATS.
2. User starts a run via CLI or API with input.
3. Go engine creates the run, resolves ready steps, dispatches agent tasks to `AGENT_TASKS`.
4. TypeScript agent worker picks up the task, resolves role from KV, builds tool handlers, runs Agent SDK `query()`.
5. Agent executes — calls Go tools via NATS, uses skills, spawns child workflows for subtasks.
6. Step completes — output published to `WORKFLOW_HISTORY`, engine advances the DAG.
7. Next steps fire based on dependencies. Repeat until DAG completes.
8. Run completes with the final output — a PR, a spec, a reviewed codebase.

## Open Questions

- **Agent SDK session persistence.** Can we checkpoint and resume Agent SDK sessions across child workflow waits? Deferred to v2.
- **Tool versioning.** Should tool registry entries support explicit versions, or is latest-always sufficient?
- **Workflow templates.** Should common pipelines (brainstorm → spec → plan → implement → review) be shipped as pre-built templates in the agents package?
- **Cost tracking.** Should the agent worker track and report token usage and API costs per step/run?
