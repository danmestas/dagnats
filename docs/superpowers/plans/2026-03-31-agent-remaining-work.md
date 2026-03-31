# Agent System: Remaining Work

Tracks what has been built, what remains, and the sequencing for completing
the agent system on DagNats.

## What's Built (Phases 1-4, 6)

### agent/ — Core Runtime (14 source files, 7 test files, 67+ tests passing)

| Package | Files | Status | Description |
|---------|-------|--------|-------------|
| `agent/` | config.go, conversation.go, runner.go, handler.go | Done | AgentConfig, SandboxConfig, ConversationState, Runner (LLM tool-use loop), Handler (worker bridge) |
| `agent/llm/` | client.go, anthropic.go, openai.go | Done | Multi-provider LLM interface, ProviderRegistry, Claude API client with SSE streaming, OpenAI-compatible client |
| `agent/tools/` | registry.go, sandbox.go, file.go, search.go, bash.go | Done | Tool interface, Registry, path validation sandbox, 7 built-in tools (read_file, write_file, edit_file, list_dir, glob, grep, bash) |
| `agent/skills/` | skill.go, builtins.go | Done | Skill type, Registry, ValidateSkill, 2 pre-built skills (explore-codebase, code-review) |

### Design Spec
| Doc | Status |
|-----|--------|
| `docs/superpowers/specs/2026-03-30-agent-system-design.md` | Done |

---

## What Remains

### Phase 5: Child Workflows (Subagents) — HIGH PRIORITY

The SubWorkflow step type exists in `dag/types.go` but the runtime support
(`SpawnWorkflow`, `WaitForChild`, orchestrator KV watch) is not implemented.
This blocks the subagent pattern where a parent agent spawns specialized child
agents.

**Work items:**

#### 5.1 Add StepStatusWaiting to dag/types.go
- Add `StepStatusWaiting` to the `StepStatus` enum (between Running and Completed)
- Update `stepStatusStrings`, `MarshalJSON`, `UnmarshalJSON`
- Update `completedSet` in `engine/orchestrator.go` — Waiting is NOT completed
- ~20 LOC, affects: `dag/types.go`

#### 5.2 Add AgentConfigRef to StepDef
- Add `AgentConfigRef string` field to `dag.StepDef`
- Validate: if Type is AgentLoop and Task is "agent-run", AgentConfigRef should
  be non-empty (warning, not error — allows runtime config injection)
- ~10 LOC, affects: `dag/types.go`, `dag/validate.go`

#### 5.3 Implement SpawnWorkflow on TaskContext
- Add `SpawnWorkflow(name string, input []byte) (string, error)` to
  `worker.TaskContext` interface
- Implementation: publishes a `workflow.started` event via NATS request/reply
  to the API service, returns the child run ID
- The parent step transitions to `StepStatusWaiting`
- ~60 LOC, affects: `worker/context.go`, `worker/worker.go`

#### 5.4 Orchestrator child workflow watcher
- When a step publishes `step.waiting` event (new event type), the orchestrator
  starts a KV watch on `run.{childRunID}` in the `workflow_runs` bucket
- When the child run reaches a terminal state (Completed/Failed), the
  orchestrator publishes `step.completed` or `step.failed` for the parent step
- Must handle: orchestrator restart (re-watch on snapshot load), child already
  complete by the time watch starts, watch timeout
- ~100 LOC, affects: `engine/orchestrator.go`, `protocol/protocol.go`

#### 5.5 Tests
- Unit test: StepStatusWaiting serialization round-trip
- Integration test: spawn child workflow, child completes, parent step resumes
- Integration test: spawn child, child fails, parent step fails
- Integration test: orchestrator restarts mid-wait, resumes watching
- ~150 LOC, new file: `engine/child_workflow_test.go`

**Total estimate: ~340 LOC**
**Depends on: nothing (can start immediately)**
**Blocks: subagent spawning from within agent loops**

---

### Phase 7: NATS Integration Wiring — HIGH PRIORITY

The agent handler (`agent/handler.go`) currently defines a `WorkerContext`
interface that mirrors `worker.TaskContext` but doesn't import it. This needs
to be wired to the actual DagNats worker binary.

**Work items:**

#### 7.1 Agent worker binary
- New binary `cmd/dagnats-agent/main.go` that:
  - Connects to NATS
  - Calls `natsutil.SetupAll()`
  - Creates `worker.Worker`
  - Creates `llm.ProviderRegistry` with Anthropic + OpenAI factories
  - Creates `tools.Registry` with all built-in tools
  - Creates `agent.Handler` and registers it as `"agent-run"` task
  - Starts the worker
- API keys from env vars: `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`
- SandboxConfig from env or config file
- ~80 LOC, new file: `cmd/dagnats-agent/main.go`

#### 7.2 KV-backed ConfigLoader
- Implement `agent.ConfigLoader` that reads from `agent_configs` KV bucket
- Cache configs in memory with KV watch for updates
- ~50 LOC, new file: `agent/kvconfig.go`

#### 7.3 New NATS resources in natsutil/
- Add `agent_configs` KV bucket to `SetupKVBuckets()`
- Add `skills` KV bucket to `SetupKVBuckets()`
- Add `conversation_blobs` Object Store to a new `SetupObjectStores()`
- Update `SetupAll()` to call the new functions
- ~30 LOC, affects: `natsutil/conn.go`

#### 7.4 Large payload handling
- When ConversationState exceeds `PayloadSizeThreshold` (768KB), store in
  `conversation_blobs` Object Store and pass a `PayloadRef` key instead
- Handler deserializes PayloadRef, fetches from Object Store if not inline
- ~60 LOC, affects: `agent/handler.go`, new: `agent/payload.go`

#### 7.5 TaskPayload extension
- Add `AgentConfigRef string` field to `protocol.TaskPayload`
- Orchestrator populates from `StepDef.AgentConfigRef` when publishing tasks
- ~15 LOC, affects: `protocol/protocol.go`, `engine/orchestrator.go`

**Total estimate: ~235 LOC**
**Depends on: Phase 5.2 (AgentConfigRef on StepDef)**

---

### Phase 8: MCP Integration — MEDIUM PRIORITY

MCP (Model Context Protocol) enables the agent to connect to external tool
servers. Deferred until built-in tools are proven in production.

**Work items:**

#### 8.1 MCP JSON-RPC client
- Implement JSON-RPC 2.0 over stdio and SSE transports
- Methods: `initialize`, `tools/list`, `tools/call`
- Connection lifecycle management (start, health check, shutdown)
- ~200 LOC, new file: `agent/tools/mcp.go`

#### 8.2 MCP transport layer
- Stdio transport: spawn subprocess, communicate via stdin/stdout
- SSE transport: HTTP client with Server-Sent Events
- ~150 LOC, new file: `agent/tools/mcp_transport.go`

#### 8.3 MCP tool adapter
- Adapt MCP tool schemas to `tools.Tool` interface
- Proxy `Execute()` calls to MCP server via JSON-RPC
- Handle MCP errors as tool errors (not NonRetryableError)
- ~80 LOC, included in `agent/tools/mcp.go`

#### 8.4 AgentConfig MCP fields
- Re-add `MCPServers []MCPServerConfig` to AgentConfig
- Handler connects to configured MCP servers at iteration start
- Connection pooling across iterations (MCP servers are long-lived)
- ~40 LOC, affects: `agent/config.go`, `agent/handler.go`

#### 8.5 Tests
- Unit test: JSON-RPC message encoding/decoding
- Integration test: stdio MCP server (mock subprocess)
- Integration test: tool list → registry, tool call → result
- ~120 LOC, new file: `agent/tools/mcp_test.go`

**Total estimate: ~590 LOC**
**Depends on: Phase 7 (NATS wiring working end-to-end)**
**Build when: built-in tools are proven and external tool servers needed**

---

### Phase 9: Hardened Sandbox — MEDIUM PRIORITY

The current sandbox validates file paths and sets bash timeouts. Production
use needs stronger isolation.

**Work items:**

#### 9.1 Container-based bash execution
- Replace `exec.CommandContext("bash", ...)` with container execution
- Options: `nsjail` (lightweight), `bubblewrap`, or Linux namespaces directly
- Mount workspace dir read-write, everything else read-only or unmounted
- Network access controlled by SandboxConfig.NetworkAccess
- ~150 LOC, affects: `agent/tools/bash.go`

#### 9.2 Resource limits
- CPU time limit per command (cgroup or ulimit)
- Memory limit per command
- Disk write limit (prevent filling disk)
- Process count limit (prevent fork bombs)
- ~60 LOC, affects: `agent/tools/bash.go`, `agent/tools/sandbox.go`

#### 9.3 Symlink resolution
- ValidatePath must resolve symlinks before checking workspace bounds
- Prevent symlink-based sandbox escapes
- ~20 LOC, affects: `agent/tools/sandbox.go`

**Total estimate: ~230 LOC**
**Depends on: nothing (can harden incrementally)**
**Build when: before any untrusted input reaches the agent**

---

### Phase 10: Observability & Metrics — LOW PRIORITY

The agent system needs its own telemetry layer on top of DagNats observe/.

**Work items:**

#### 10.1 Agent-specific metrics
- `agent.llm.calls` (counter, per provider/model)
- `agent.llm.tokens.input` / `agent.llm.tokens.output` (counter)
- `agent.llm.latency_ms` (histogram, per provider)
- `agent.tool.calls` (counter, per tool name)
- `agent.tool.errors` (counter, per tool name)
- `agent.tool.latency_ms` (histogram)
- `agent.iterations` (histogram, per agent config)
- ~50 LOC, affects: `agent/runner.go`

#### 10.2 Cost tracking
- Track token usage per run (input + output, per model)
- Store in run metadata or KV
- Enable per-workflow cost reporting
- ~40 LOC, new file: `agent/cost.go`

#### 10.3 Conversation logging
- Publish full conversation state to TELEMETRY stream after each iteration
- Enables debugging and replay of agent decisions
- Opt-in via AgentConfig flag (conversations may contain sensitive data)
- ~30 LOC, affects: `agent/runner.go`

**Total estimate: ~120 LOC**

---

### Phase 11: Rate Limiting — LOW PRIORITY

LLM API calls need rate limiting to avoid hitting provider quotas.

**Work items:**

#### 11.1 KV-backed token bucket
- Implement the rate limiting design from `docs/specs/2026-03-30-deferred-features.md`
- Rate limit key per provider (e.g. `ratelimit.anthropic`)
- Lazy refill on Acquire — no background goroutine
- ~80 LOC, new file: `agent/ratelimit.go`

#### 11.2 Wire into Runner
- Runner checks rate limit before each LLM call
- If rate limited, the iteration returns Continue with current state
  (lets the orchestrator re-enqueue with LoopDelay)
- ~20 LOC, affects: `agent/runner.go`

**Total estimate: ~100 LOC**
**Depends on: Phase 7 (NATS KV access)**
**Build when: workers hit LLM API rate limits**

---

### Phase 12: CLI Commands — LOW PRIORITY

Wire up the existing CLI stubs to support agent workflows.

**Work items:**

#### 12.1 Agent-specific CLI commands
```
dagnats agent run <skill-name> --input '{"prompt": "..."}'
dagnats agent configs list
dagnats agent configs register ./config.json
dagnats agent skills list
dagnats agent stream <run-id> <step-id>  # live token stream
```
- ~200 LOC, affects: `cli/root.go`, new files in `cli/`

#### 12.2 Wire existing stubs
- Implement `run start`, `run status`, `run history` using HTTP client
- ~100 LOC, affects: `cli/run.go`

**Total estimate: ~300 LOC**

---

## Sequencing

```
Now (done)          Phase 1-4, 6: Core agent runtime, all tests passing
                         │
Next                     ├── Phase 5: Child Workflows (subagents)
                         │        │
                         │        ▼
                         ├── Phase 7: NATS Integration Wiring
                         │        │
                         │        ▼
                         │   Phase 8: MCP Integration (when needed)
                         │
Parallel             Phase 9: Hardened Sandbox (independent)
                         │
Later                Phase 10: Observability
                     Phase 11: Rate Limiting
                     Phase 12: CLI Commands
```

**Critical path to first E2E run:**
Phase 5 (child workflows) + Phase 7 (NATS wiring) → first end-to-end agent
workflow running on real NATS with real LLM calls.

**Estimated total remaining: ~1,915 LOC** (across 12 work items)

---

## Open Questions

1. **API key management:** Where do LLM API keys live? Options: env vars (simple),
   NATS KV bucket `secrets` (consistent), external vault. Recommend env vars for
   now, vault adapter later.

2. **Context window management:** When conversation state grows beyond the LLM's
   context window, how do we handle it? Options: truncate oldest messages,
   summarize with a cheaper model, sliding window. This is critical for long-running
   agent loops. Recommend sliding window with summarization as Phase 13.

3. **Parallel tool execution:** The Runner currently executes tool calls
   sequentially. Claude Code executes independent tools in parallel. Adding
   `goroutine` parallelism within `executeToolCalls` is ~30 LOC but needs
   careful sandbox concurrency handling. Recommend: add after Phase 9.

4. **Agent-to-agent communication:** Can agents within the same workflow
   communicate directly (not just via step output)? NATS subjects like
   `agent.{runID}.{stepID}.message` could enable this. Defer until a
   use case demands it.

5. **Human-in-the-loop gates:** The deferred WaitFor feature
   (`docs/specs/2026-03-30-deferred-features.md`) enables approval gates.
   When a workflow reaches a WaitFor step, it publishes to
   `event.{runID}.approval_needed` and waits for a KV update. Build when
   the first workflow requires human approval.
