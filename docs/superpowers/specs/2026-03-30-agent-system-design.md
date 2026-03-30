# Agent System Design: Claude Code on DagNats

## Context

DagNats is a DAG workflow engine on NATS JetStream with event-sourced orchestration,
durable agent loops, and a deep worker interface. The goal is to build a system where
Claude Code-like agentic workflows (phased engineering with explore, plan, execute,
verify) run durably on DagNats — all LLM calls, tool execution, subagent spawning,
and communication flow through NATS with full resumability.

**What exists today:**
- DAG definition DSL with Normal, AgentLoop, SubWorkflow step types
- Event-sourced orchestrator with KV snapshots
- Worker framework with Continue/Complete/Fail/PutStream
- Trace propagation, observability, retry/timeout via NATS primitives

**What we need:** An LLM agent layer that uses these primitives to run autonomous
coding workflows without user intervention.

## Design Decisions

1. **Tool safety: Sandboxed** — Tools run in restricted environments. Bash in
   chroot/container, file ops scoped to a workspace directory. Security boundary
   enforced per-agent via `SandboxConfig` on AgentConfig.
2. **LLM provider: Multi-provider** — Interface supports multiple providers from
   day one. Claude (Anthropic API) + OpenAI-compatible client built initially.
3. **State management: Object Store** — Large conversation states (>1MB) stored in
   NATS Object Store (`conversation_blobs`), with reference key in step output.
4. **MCP: Deferred** — Start with built-in tools only (file, search, bash). MCP
   server support added in a follow-up phase once the core loop is proven.

---

## Architecture Overview

```
                    ┌─────────────────────────────────┐
                    │         Workflow DAG             │
                    │  explore → plan → execute → test │
                    └──────────┬──────────────────────┘
                               │
                    ┌──────────▼──────────────────────┐
                    │      DagNats Orchestrator        │
                    │  (existing engine/ package)      │
                    └──────────┬──────────────────────┘
                               │ task.agent-run.{runID}
                    ┌──────────▼──────────────────────┐
                    │      Agent Worker               │
                    │  (new agent/ package)            │
                    │                                  │
                    │  ┌─────────────────────────┐    │
                    │  │   LLM Tool-Use Loop     │    │
                    │  │  call LLM → tools →     │    │
                    │  │  feed results → repeat  │    │
                    │  └─────────────────────────┘    │
                    │         │          │             │
                    │    ┌────▼───┐ ┌───▼────┐        │
                    │    │Built-in│ │  MCP   │        │
                    │    │ Tools  │ │Servers │        │
                    │    └────────┘ └────────┘        │
                    └─────────────────────────────────┘
```

### Key Architectural Decision: Tool Execution Lives Inside the Agent Loop

LLM tool use is a tight inner loop: call LLM → parse tool calls → execute tools →
feed results back → call LLM again. This maps directly to **one AgentLoop step** in
DagNats where each iteration is one LLM round-trip. Tool execution happens in-process
within the worker — NOT as separate DAG steps.

This is the right call because:
1. Tool calls are latency-sensitive (the LLM needs results immediately)
2. A single LLM turn may invoke 3-10 tools — making each a DAG step would explode graph complexity
3. The agent loop already provides durability (iteration state in KV), bounded execution (MaxIterations/MaxDuration), and observability (agent.loop.iteration events)
4. DagNats retries handle worker crashes — the LLM call replays from the last checkpoint

**Subagents** are the exception: when an agent spawns a subagent, that becomes a
**child workflow** (SubWorkflow step type) with its own DAG, agent config, and
independent lifecycle. The parent waits via KV watch (already designed in the spec).

---

## New Packages

### 1. `agent/` — Agent Runtime (core new package)

The deep module that owns the LLM tool-use loop. A single `AgentRunner` handles:
- Calling the LLM API with conversation history
- Parsing tool-use responses
- Dispatching tool calls to registered tool providers
- Accumulating conversation state
- Deciding when to Continue vs Complete

```go
// agent/runner.go

// AgentRunner executes one agent loop iteration: send messages to LLM,
// execute any tool calls, return the result. Stateless between calls —
// all conversation state arrives via input and leaves via output.
type AgentRunner struct {
    llm       LLMClient
    tools     *ToolRegistry
    config    AgentConfig
    telemetry *observe.Telemetry
}

// Run executes a single iteration of the agent loop.
// Input is the serialized ConversationState from the previous iteration.
// Returns updated ConversationState and a decision (Continue or Complete).
func (r *AgentRunner) Run(ctx context.Context, input []byte) (*Result, error)

type Result struct {
    Output       []byte  // serialized ConversationState for next iteration
    Done         bool    // true = Complete, false = Continue
    FinalOutput  []byte  // non-nil only when Done=true (the step's output)
}
```

```go
// agent/config.go

// AgentConfig defines the identity and capabilities of an agent.
// Stored in KV bucket "agent_configs", referenced by name from StepDef.
type AgentConfig struct {
    Name         string         `json:"name"`
    SystemPrompt string         `json:"system_prompt"`
    Model        string         `json:"model"`        // e.g. "claude-sonnet-4-6"
    Provider     string         `json:"provider"`     // "anthropic", "openai"
    MaxTokens    int            `json:"max_tokens"`
    Temperature  float64        `json:"temperature"`
    Tools        []string       `json:"tools"`        // tool names from registry
    MaxTurns     int            `json:"max_turns"`    // safety bound per iteration
    Sandbox      *SandboxConfig `json:"sandbox,omitempty"`
}

// SandboxConfig defines the security boundary for tool execution.
// All tools run within these constraints. WorkspaceDir is the root
// for all file operations; no reads or writes escape it.
type SandboxConfig struct {
    WorkspaceDir  string   `json:"workspace_dir"`           // chroot for file ops
    AllowedPaths  []string `json:"allowed_paths,omitempty"` // additional read paths
    BashTimeout   time.Duration `json:"bash_timeout"`       // per-command timeout
    BashMaxOutput int      `json:"bash_max_output"`         // stdout/stderr cap in bytes
    NetworkAccess bool     `json:"network_access"`          // allow outbound network
}
```

```go
// agent/conversation.go

// ConversationState is the serializable state passed between agent loop
// iterations. Stored as step output in NATS KV (or Object Store if large).
type ConversationState struct {
    Messages     []Message `json:"messages"`
    TurnCount    int       `json:"turn_count"`
    Artifacts    []string  `json:"artifacts"`    // references to produced files, etc.
    AgentConfig  string    `json:"agent_config"` // name of the AgentConfig
}

type Message struct {
    Role    string          `json:"role"`    // "user", "assistant", "tool"
    Content json.RawMessage `json:"content"` // text or tool_use/tool_result blocks
}
```

### 2. `agent/llm/` — Multi-Provider LLM Client

```go
// agent/llm/client.go

// Client is the interface for calling LLM APIs. Implementations exist
// for Claude (Anthropic API) and OpenAI-compatible APIs.
type Client interface {
    // SendMessage sends a conversation to the LLM and returns the response.
    // Streams tokens to the optional StreamFunc for real-time output.
    SendMessage(ctx context.Context, req *Request) (*Response, error)
}

type Request struct {
    Model        string
    SystemPrompt string
    Messages     []Message
    Tools        []ToolDef    // tool schemas available to the LLM
    MaxTokens    int
    Temperature  float64
    StreamFunc   func(token string)  // called per token for PutStream
}

type Response struct {
    Content    []ContentBlock  // text and/or tool_use blocks
    StopReason string          // "end_turn", "tool_use", "max_tokens"
    Usage      Usage
}

type ContentBlock struct {
    Type  string          // "text" or "tool_use"
    Text  string          // non-empty when Type="text"
    ID    string          // tool call ID when Type="tool_use"
    Name  string          // tool name when Type="tool_use"
    Input json.RawMessage // tool arguments when Type="tool_use"
}

// ProviderRegistry maps provider names to Client constructors.
// Populated at startup. AgentConfig.Provider selects which client to use.
type ProviderRegistry struct {
    providers map[string]func(apiKey string) Client
}
```

**Implementations:**
- `agent/llm/anthropic.go` — Claude API (Messages API with streaming, tool_use blocks)
- `agent/llm/openai.go` — OpenAI-compatible (chat completions, function calling).
  Covers OpenAI, Groq, Together, local vLLM, etc.
```

### 3. `agent/tools/` — Tool Registry and Built-in Tools

```go
// agent/tools/registry.go

// Registry holds all available tools. Tools register themselves at
// worker startup. The registry resolves tool names from AgentConfig
// to executable implementations.
type Registry struct {
    tools map[string]Tool
}

// Tool is a single callable tool. Schema() returns the JSON schema
// the LLM sees. Execute() runs the tool and returns the result.
type Tool interface {
    Name() string
    Description() string
    Schema() json.RawMessage           // JSON Schema for input parameters
    Execute(ctx context.Context, input json.RawMessage) (json.RawMessage, error)
}
```

**Built-in tools** (same capabilities as Claude Code):

| Tool | File | Description |
|------|------|-------------|
| `read_file` | `agent/tools/file.go` | Read file contents |
| `write_file` | `agent/tools/file.go` | Write/create files |
| `edit_file` | `agent/tools/file.go` | String replacement edits |
| `glob` | `agent/tools/search.go` | Find files by pattern |
| `grep` | `agent/tools/search.go` | Search file contents |
| `bash` | `agent/tools/bash.go` | Execute shell commands (sandboxed) |
| `list_dir` | `agent/tools/file.go` | List directory contents |

All built-in tools respect `SandboxConfig` from the agent's config:
- File tools validate all paths are under `WorkspaceDir` or in `AllowedPaths`
- Bash tool runs commands in a chroot with `BashTimeout` and output capped at `BashMaxOutput`
- Path traversal attempts (../etc) are rejected with a non-retryable error

**MCP tools** (Phase 8, deferred):
Will be loaded dynamically from MCP servers at agent startup once built-in tools
are proven. MCPProvider connects via stdio/SSE, calls tools/list, and registers
proxy Tools that forward Execute() calls to the server.

### 4. `agent/worker.go` — The Agent Worker Handler

This is the bridge between DagNats worker framework and the agent runtime.
Registered as a handler for task type `"agent-run"`.

```go
// agent/worker.go

// RegisterAgentHandler registers the "agent-run" task handler on the worker.
// The handler deserializes AgentConfig + ConversationState from input,
// runs one agent loop iteration, and calls Continue or Complete.
func RegisterAgentHandler(w *worker.Worker, llm llm.Client, registry *tools.Registry) {
    w.Handle("agent-run", func(ctx worker.TaskContext) error {
        // 1. Deserialize input → ConversationState (or initial prompt)
        // 2. Load AgentConfig from state or KV
        // 3. Build AgentRunner with config's tools + MCP servers
        // 4. Wire PutStream for token streaming
        // 5. Run one iteration
        // 6. If result.Done → ctx.Complete(result.FinalOutput)
        //    Else → ctx.Continue(result.Output)  // serialized ConversationState
    })
}
```

### 5. `agent/skills/` — Reusable Workflow Templates (Skills)

Skills are pre-defined workflow DAGs + agent configs. Like Claude Code's skills
but durable and composable.

```go
// agent/skills/skill.go

// Skill is a reusable workflow template with pre-configured agent configs.
// Stored in KV bucket "skills".
type Skill struct {
    Name        string       `json:"name"`
    Description string       `json:"description"`
    Workflow    dag.WorkflowDef `json:"workflow"`
    Configs     map[string]AgentConfig `json:"configs"` // step→config mapping
}

// Registry holds available skills. Skills are loaded from KV at startup
// and can be invoked by name to create workflow runs.
type Registry struct {
    kv nats.KeyValue  // "skills" bucket
}

func (r *Registry) Get(name string) (Skill, error)
func (r *Registry) Register(skill Skill) error
func (r *Registry) Invoke(name string, input []byte) (string, error) // returns runID
```

---

## How Claude Code Concepts Map to DagNats

| Claude Code Concept | DagNats Mapping |
|---------------------|-----------------|
| **Main agent loop** | AgentLoop step with task="agent-run" |
| **Subagent** | SubWorkflow step → child workflow with own AgentConfig |
| **Tool use** | In-process tool execution within agent loop iteration |
| **MCP servers** | MCPProvider loaded at worker startup per AgentConfig |
| **Skills** | Skill = WorkflowDef + AgentConfigs stored in KV |
| **Token streaming** | PutStream() → `stream.{runID}.{stepID}` subject |
| **Conversation history** | ConversationState serialized between Continue() calls |
| **Phase transitions** | DAG step dependencies (explore.Complete → plan starts) |
| **Parallel subagents** | Fan-out in DAG (multiple SubWorkflow steps with same dep) |
| **Context passing** | Fan-in input resolution (outputs merge into JSON map) |

---

## Example: Code Review Workflow

```go
wf := dag.NewWorkflow("code-review")

explore := wf.AgentLoop("explore", "agent-run").
    WithMaxIterations(20).
    WithMaxDuration(5 * time.Minute)

plan := wf.AgentLoop("plan", "agent-run").
    After(explore).
    WithMaxIterations(10).
    WithMaxDuration(3 * time.Minute)

implement := wf.AgentLoop("implement", "agent-run").
    After(plan).
    WithMaxIterations(50).
    WithMaxDuration(15 * time.Minute)

test := wf.AgentLoop("test", "agent-run").
    After(implement).
    WithMaxIterations(30).
    WithMaxDuration(10 * time.Minute)

def, err := wf.Build()
```

Each step uses a different AgentConfig:
- **explore**: System prompt for codebase exploration, tools=[glob, grep, read_file], model=haiku (fast)
- **plan**: System prompt for architecture planning, tools=[read_file, glob, grep], model=opus (deep)
- **implement**: System prompt for code writing, tools=[read_file, write_file, edit_file, bash, glob, grep], model=sonnet
- **test**: System prompt for testing, tools=[bash, read_file, grep], model=sonnet

The input to each step is the previous step's final output (the completed
ConversationState summary/artifacts), resolved automatically by DagNats fan-in.

---

## NATS Resources (new)

| Resource | Name | Purpose |
|----------|------|---------|
| KV Bucket | `agent_configs` | AgentConfig definitions (JSON) |
| KV Bucket | `skills` | Skill templates (WorkflowDef + configs) |
| Object Store | `conversation_blobs` | Large conversation states (>1MB) |
| Subject | `stream.{runID}.{stepID}` | Real-time token streaming (existing) |

---

## What Needs to Change in Existing Code

### 1. Implement Child Workflows (engine/)
The SubWorkflow step type exists in `dag/types.go` but `SpawnWorkflow` is not
implemented. Needed for subagents.

- Add `SpawnWorkflow(name, input)` to TaskContext interface
- Add `StepStatusWaiting` to StepStatus enum
- Orchestrator watches child run KV entry, resumes parent on completion
- Uses existing KV watch pattern from design spec

### 2. Extend StepDef with Agent Config Reference (dag/)
```go
type StepDef struct {
    // ... existing fields ...
    AgentConfigRef string `json:"agent_config_ref,omitempty"` // name in agent_configs KV
}
```
This lets the orchestrator pass the config name to the worker in TaskPayload.

### 3. Large Payload Support (protocol/)
Conversation state can exceed NATS message size limits. Use Object Store for
payloads >1MB, store reference in event payload.

```go
// In protocol/payload.go or similar
type PayloadRef struct {
    Inline bool   `json:"inline"`
    Data   []byte `json:"data,omitempty"`    // when inline=true
    Bucket string `json:"bucket,omitempty"`  // when inline=false
    Key    string `json:"key,omitempty"`     // when inline=false
}
```

### 4. Setup New KV Buckets (natsutil/)
Add `agent_configs`, `skills` buckets and `conversation_blobs` object store
to `SetupAll()`.

---

## Implementation Order

### Phase 1: Agent Runtime Core
**Files:** `agent/config.go`, `agent/conversation.go`, `agent/runner.go`
- AgentConfig and ConversationState types
- AgentRunner with the LLM tool-use loop (call LLM → execute tools → accumulate)
- Pure logic, testable without NATS

### Phase 2: LLM Client (Multi-Provider)
**Files:** `agent/llm/client.go`, `agent/llm/anthropic.go`, `agent/llm/openai.go`
- LLM Client interface + ProviderRegistry
- Claude/Anthropic API implementation with streaming support
- OpenAI-compatible implementation (covers OpenAI, Groq, local vLLM)
- Token streaming wired to a callback (PutStream in Phase 4)

### Phase 3: Tool System (Sandboxed)
**Files:** `agent/tools/registry.go`, `agent/tools/file.go`, `agent/tools/search.go`,
`agent/tools/bash.go`, `agent/tools/sandbox.go`
- Tool interface and Registry
- SandboxConfig enforcement: path validation, chroot, timeouts
- Built-in tools: read_file, write_file, edit_file, glob, grep, bash, list_dir
- No MCP yet — deferred to Phase 8

### Phase 4: Worker Integration
**Files:** `agent/worker.go`, `natsutil/conn.go` (add KV buckets)
- RegisterAgentHandler bridging DagNats worker framework to AgentRunner
- Wire PutStream for token streaming
- ConversationState serialization between Continue() calls
- Large payload handling via Object Store

### Phase 5: Child Workflows (Subagents)
**Files:** `worker/context.go`, `engine/orchestrator.go`, `dag/types.go`
- Implement SpawnWorkflow on TaskContext
- Add StepStatusWaiting
- KV watch in orchestrator for child completion
- This enables subagent spawning from within agent loops

### Phase 6: Skills System
**Files:** `agent/skills/skill.go`, `agent/skills/registry.go`
- Skill definition and KV storage
- Invoke skill by name → creates workflow run
- Pre-built skills: explore-codebase, plan-implementation, code-review

### Phase 7: Example Workflows
**Files:** `examples/code-review/`, `examples/explore-plan-execute/`
- End-to-end example workflows demonstrating the full system
- Integration tests with real embedded NATS + mock LLM

### Phase 8: MCP Integration (deferred)
**Files:** `agent/tools/mcp.go`, `agent/tools/mcp_transport.go`
- JSON-RPC client for MCP protocol (stdio + SSE transports)
- MCPProvider: connect to server, list tools, register proxy Tools
- MCP tools get same sandbox constraints as built-in tools
- Build when: built-in tools are proven and we need external tool servers

---

## Verification Plan

1. **Unit tests (agent/)**: Test AgentRunner with mock LLM client, verify tool dispatch, conversation accumulation, Continue/Complete decisions
2. **Unit tests (agent/tools/)**: Test each built-in tool in isolation, test Registry lookup
3. **Integration tests (agent/worker.go)**: Real NATS, verify agent-run handler calls Continue/Complete correctly, verify PutStream token delivery
4. **Integration tests (engine/)**: Verify child workflow spawn + wait + resume
5. **E2E test**: Register a multi-phase workflow, start a run with mock LLM, verify all phases execute in order, subagents spawn and complete, final output is correct
6. **Streaming test**: Subscribe to `stream.{runID}.{stepID}`, verify tokens arrive in real-time during agent execution

---

## Critical Files to Modify

- `dag/types.go` — Add AgentConfigRef to StepDef
- `worker/context.go` — Implement SpawnWorkflow
- `engine/orchestrator.go` — Handle SubWorkflow waiting + child completion
- `natsutil/conn.go` — Add new KV buckets and object store

## Critical Files to Create

- `agent/config.go` — AgentConfig + SandboxConfig types
- `agent/conversation.go` — ConversationState + large payload handling
- `agent/runner.go` — AgentRunner (LLM tool-use loop)
- `agent/worker.go` — DagNats worker handler bridge
- `agent/llm/client.go` — LLM interface + ProviderRegistry
- `agent/llm/anthropic.go` — Claude API implementation
- `agent/llm/openai.go` — OpenAI-compatible implementation
- `agent/tools/registry.go` — Tool registry
- `agent/tools/sandbox.go` — Sandbox enforcement (path validation, chroot)
- `agent/tools/file.go` — File tools (read, write, edit, list)
- `agent/tools/search.go` — Search tools (glob, grep)
- `agent/tools/bash.go` — Shell execution tool (sandboxed)
- `agent/skills/skill.go` — Skill types and registry

## Existing Code to Reuse

- `dag.AgentLoopConfig` — Already has MaxIterations, MaxDuration, LoopDelay
- `dag.StepTypeAgentLoop` / `dag.StepTypeSubWorkflow` — Step types for agent + subagent
- `worker.TaskContext.Continue()` — Agent loop iteration handoff (`worker/context.go:104`)
- `worker.TaskContext.PutStream()` — Token streaming (`worker/context.go:143`)
- `worker.Typed[I,O]()` — Type-safe handler wrapper (`worker/typed.go`)
- `worker.NonRetryableError` — Permanent failure signal (`worker/errors.go`)
- `protocol.TaskPayload` — Already has Iteration field (`protocol/protocol.go`)
- `observe.Telemetry` — Full tracing/metrics/logging (`observe/observe.go`)
- `natsutil.SetupAll()` — Resource bootstrapping pattern (`natsutil/conn.go`)
- `dag.SkipIfOutput()` — Conditional skipping for optional phases (`dag/condition.go`)
- `engine.Orchestrator` — Already handles AgentLoop Continue/bounds (`engine/orchestrator.go:276`)
