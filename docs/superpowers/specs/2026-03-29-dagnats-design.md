# DagNats Design Spec

A DAG-based workflow engine built on NATS, combining Hatchet-style DAG orchestration with Temporal-style durable execution for autonomous LLM coding pipelines.

## Decisions

- **Language:** Go (Rust deferred)
- **Scale:** 3-node NATS cluster, tens of concurrent workflows
- **Workflow definition:** Graph DSL/Builder + Agent Loop step type
- **Auth:** NATS native JWT/nkey (operator → account → user)
- **State:** Event sourcing + KV snapshots
- **Control plane:** REST API (external) + NATS request/reply (internal)
- **UI:** CLI only, dashboard deferred
- **Design philosophy:** Ousterhout (minimize complexity, deep modules) + TigerStyle (safety > performance > DX)

## Use Case

Autonomous LLM coding pipelines: planning, coding, reviewing, and fixing code. Workflows trigger from NATS messages, cron jobs, webhooks, and other workflows.

## Core Data Model

### Workflow Definition (static DAG template)

```go
type WorkflowDef struct {
    Name    string
    Version string
    Steps   []StepDef
}

type StepDef struct {
    ID        string
    Task      string        // maps to a registered worker handler
    DependsOn []string      // step IDs that must complete first
    Retries   int
    Timeout   time.Duration
    Type      StepType      // Normal, AgentLoop, SubWorkflow
    Loop      *AgentLoopConfig // non-nil only when Type == StepTypeAgentLoop
}

// AgentLoopConfig holds bounds for agent loop steps.
// Only valid on StepTypeAgentLoop — Validate rejects it on other types.
type AgentLoopConfig struct {
    MaxIterations int
    MaxDuration   time.Duration
}
```

### Workflow Run (live execution)

```go
type WorkflowRun struct {
    RunID      string
    WorkflowID string
    Status     RunStatus  // Pending, Running, Completed, Failed, Cancelled
    Steps      map[string]StepState
    CreatedAt  time.Time
}

type StepState struct {
    Status   StepStatus // Pending, Queued, Running, Completed, Failed, Skipped
    Attempts int
    Output   []byte
    Error    string
}

// NewWorkflowRun creates the initial run state from a definition.
// Single constructor — no duplicate initialization logic.
func NewWorkflowRun(def WorkflowDef, runID string) WorkflowRun { ... }
```

### Event Types (immutable history)

```
workflow.started
step.queued
step.started
step.completed          // includes output
step.failed             // includes error + attempt count
step.continue           // agent loop: re-enqueue with new input
agent.loop.iteration    // agent loop: per-cycle trace
workflow.completed
workflow.failed
```

Each event carries `run_id`, `step_id`, timestamp, and payload.

## Component Architecture

### 1. Graph DSL (`dag/` package)

User-facing API for defining workflows. Compiles to DAG JSON stored in KV.

```go
wf := dagnats.NewWorkflow("code-review").
    Task("plan", "llm-planner").
    Task("code", "llm-coder").DependsOn("plan").
    Task("test", "test-runner").DependsOn("code").
    Task("review", "llm-reviewer").DependsOn("test").
    AgentLoop("fix", "llm-fixer").DependsOn("review")

wf.Register(client)
```

`AgentLoop` is a step that re-enqueues itself. The worker returns `Continue(nextInput)` or `Complete(output)`. Safety bounds configured per step: `MaxIterations`, `MaxDuration`.

Zero dependencies on NATS -- pure data structures and algorithms.

### 2. Orchestrator (`engine/` package)

Thin, stateless event processor. The orchestrator is an I/O shell around a pure decision core.

**Core loop:**

1. Consume events from `WORKFLOW_HISTORY` stream (partitioned by `run_id`)
2. Load KV snapshot if available, otherwise replay full history
3. Call `dag.Advance(def, run, event)` -- pure function that returns a list of `Action` values
4. Execute actions: publish task messages, update snapshot, emit workflow events
5. For agent loop steps returning `Continue`: re-enqueue the same step with new input

**`dag.Advance` (pure, no I/O):**

```go
// Advance processes an event and returns actions for the orchestrator to execute.
// Pure function — all decision logic lives here, testable without NATS.
func Advance(def WorkflowDef, run *WorkflowRun, evt Event) []Action
```

Action types: `EnqueueTask`, `CompleteWorkflow`, `FailWorkflow`, `ReEnqueueAgentLoop`.

Multiple instances coordinate via JetStream consumer groups. Each `run_id` processed by exactly one instance at a time.

**Fan-in input resolution:** When a step has multiple dependencies, all dependency outputs are collected into a JSON map keyed by step ID and passed as the input. Workers always receive a single input — for single deps it's the output directly, for fan-in it's the collected map. No silent nil inputs.

### 3. Workers (`worker/` package)

Standalone processes that register task handlers. Deep interface -- workers never see retries, timeouts, or DAG logic:

```go
type TaskContext interface {
    Input() []byte
    RunID() string
    StepID() string
    Complete(output []byte) error     // step done
    Fail(err error) error             // step failed
    Continue(output []byte) error     // agent loop: re-run with this input
    SpawnWorkflow(name string, input []byte) (string, error)
    WaitForAll(childIDs ...string) error
}
```

Workers ack only after success. On failure, NATS redelivers based on retry policy. Workers publish completion/failure events to the history stream.

### 4. Control Plane (`api/` package)

Two interfaces to the same logic:

- **REST API:** `POST /workflows`, `POST /runs`, `GET /runs/{id}`, `POST /runs/{id}/cancel`. Handles webhooks and cron triggers.
- **NATS request/reply:** Same operations on `api.workflows.*` and `api.runs.*` subjects. Used by internal services and child workflow spawning.

### 5. CLI (`cli/` package)

```
dagnats workflow list
dagnats workflow register ./my-workflow.go
dagnats run start code-review --input '{"repo": "..."}'
dagnats run status <run_id>
dagnats run history <run_id>
dagnats run retry <run_id> --step review
```

## NATS Primitives

| Primitive | Name | Purpose |
|-----------|------|---------|
| Stream | `WORKFLOW_HISTORY` | Immutable event log, subject `history.{run_id}` |
| Stream | `TASK_QUEUES` | Task distribution, subject `task.{worker_type}.{task_name}` |
| Stream | `EVENTS` | External triggers, subject `event.{name}` |
| KV Bucket | `workflow_defs` | DAG definitions (JSON) |
| KV Bucket | `workflow_runs` | Run state snapshots |
| Object Store | `workflow_blobs` | Large payloads (referenced by events) |

### NATS-Native Patterns (no custom infrastructure)

| Need | NATS Primitive | Rationale |
|------|---------------|-----------|
| Task distribution | JetStream pull consumers + `MaxAckPending` | Built-in concurrency control |
| Retry with backoff | `NakWithDelay` | No timer service needed |
| Exactly-once delivery | Message dedup via `Nats-Msg-Id` (`{run_id}.{step_id}.{attempt}`) | No custom dedup |
| Run state snapshots | KV with `Revision` for optimistic locking | No external DB |
| Cross-workflow signals | KV watches on `workflow_runs` entries | No bridge service |
| Worker health | NATS consumer idle heartbeats | No custom health checker |
| Internal API | NATS `micro` service framework | Built-in discovery + load balancing |
| Workflow def versioning | KV revision history | No custom version table |
| Large payloads | Object Store + references in events | Events stay small |
| Step timeouts | JetStream `AckWait` + `MaxDeliver` | Orchestrator only handles exhaustion |

## Error Handling & Retries

Workers never see retry logic. The interface is `Complete(output)` or `Fail(err)`. The orchestrator owns all retry policy:

1. Worker calls `Fail(err)` -> worker framework publishes `step.failed` event and calls `msg.NakWithDelay(backoff)`
2. JetStream redelivers the task message to a worker after the backoff period
3. Orchestrator tracks attempts via `step.failed` event count in history
4. If `MaxDeliver` is reached (configured per stream from DAG def retries): orchestrator receives `step.failed` with final attempt, marks step failed, evaluates DAG (fail workflow or skip downstream)

Timeouts use JetStream's `AckWait`. No completion event within deadline triggers automatic redelivery. Orchestrator intervenes only when `MaxDeliver` is exhausted.

## Agent Loop

An agent loop is a normal step that returns `Continue(nextInput)` instead of `Complete(output)`. The orchestrator re-enqueues it using the same DAG advancement logic -- no special code path.

```go
worker.Handle("llm-fixer", func(ctx dagnats.TaskContext) error {
    result := callLLM(ctx.Input())
    if result.NeedsMoreWork {
        return ctx.Continue(result.NextPrompt)
    }
    return ctx.Complete(result.FinalOutput)
})
```

Safety bounds (stored in `StepDef.Loop`, enforced by orchestrator):
- `MaxIterations` -- hard cap on loop cycles
- `MaxDuration` -- total wall time across all iterations

Validation rejects `AgentLoopConfig` on non-AgentLoop steps and requires it on AgentLoop steps.

Each iteration appends `agent.loop.iteration` event for observability.

## Child Workflows & Signals

A step spawns a child via `ctx.SpawnWorkflow(name, input)`. This publishes a `run.start` request via NATS request/reply and puts the step in `waiting` state.

The parent watches the child's KV entry via NATS KV watch:

```go
watcher, _ := kvRuns.Watch("run." + childRunID)
for entry := range watcher.Updates() {
    if entry != nil && isTerminal(entry.Value()) {
        // child done, parent step completes
    }
}
```

Same mechanism supports arbitrary cross-workflow signals. Any system can update a KV entry to unblock a waiting step -- human-in-the-loop approvals, webhook callbacks, external events.

## Project Structure

```
dagnats/
├── dag/          # DAG definition, Graph DSL, topological sort
│                 # Zero dependencies on NATS
├── engine/       # Orchestrator core loop
│                 # Depends on: dag, natsutil, observe
├── worker/       # Worker framework -- TaskContext, handler registration
│                 # Depends on: natsutil, observe
├── api/          # REST + NATS request/reply control plane
│                 # Depends on: engine, natsutil, observe
├── cli/          # CLI client
│                 # Depends on: api (as HTTP client)
├── observe/      # Observability interfaces + noop defaults
├── natsutil/     # Thin wrapper over nats.go -- only package importing nats.go
└── cmd/
    ├── dagnats-engine/   # orchestrator binary
    ├── dagnats-api/      # control plane binary
    └── dagnats/          # CLI binary
```

Key boundaries:
- `dag/` has zero external dependencies -- pure unit testable
- `natsutil/` owns NATS resource setup (streams, KV, consumers). Other packages may import `nats.go` for runtime operations (publish, subscribe, ack).
- `worker/` and `engine/` never import each other -- communicate only through NATS subjects

## Testing Strategy

Layered approach: pure logic gets pure tests, anything touching NATS uses a real embedded server.

### Layer 1: Pure unit tests (`dag/`)

No NATS, no I/O. Test DAG resolution exhaustively:
- Topological sort correctness
- Cycle detection
- Fan-out / fan-in resolution
- Agent loop step identification

### Layer 2: Integration tests (`engine/`, `worker/`)

Real embedded NATS server per test (~50ms startup):
- Orchestrator consumes events and enqueues correct tasks
- Workers receive, execute, publish completions
- Retry/backoff via `NakWithDelay`
- KV snapshot write and recovery
- Agent loop `Continue`/`Complete` flow
- Child workflow spawn and KV watch signaling

### Layer 3: End-to-end tests

Full workflow execution with real worker handlers:
- Complete lifecycle: start -> all steps -> complete
- Failure: retries exhaust -> workflow fails with correct state
- Agent loop: N iterations then complete
- Child workflows: spawn, complete, parent resumes
- Concurrent workflows: no interference

Each E2E test asserts on final KV state AND event history (paired assertions).

### Layer 4: Chaos/fault tests (stretch goal)

- Kill orchestrator mid-run -> restart -> resume from snapshot
- Slow worker exceeds timeout -> redelivery
- Duplicate delivery -> dedup via `Nats-Msg-Id`

### Testing Methodology: Red-Green TDD

All implementation follows red-green TDD:
1. **Red:** Write a failing test that describes the desired behavior
2. **Green:** Write the minimal code to make it pass
3. **Refactor:** Clean up while keeping tests green

### Testing Rules (TigerStyle)

- Minimum 2 assertions per test (positive result + invalid state absence)
- Bounded timeouts on all waits (no hanging tests)
- All errors handled (no `_ = err` in test helpers)
- Each test file opens with methodology comment
- No shared NATS servers between tests
- 70-line limit on test functions; split into named helpers

## Observability

All observability is provider-agnostic. Define interfaces in DagNats, implement adapters separately. No vendor imports outside adapter packages.

### Interfaces

```go
// Error reporting -- Sentry adapter planned, but never imported directly
type ErrorReporter interface {
    CaptureError(ctx context.Context, err error, tags map[string]string)
    CaptureMessage(ctx context.Context, msg string, level Level)
}

// Structured logging
type Logger interface {
    Info(msg string, fields ...Field)
    Error(msg string, err error, fields ...Field)
    With(fields ...Field) Logger
}

// Metrics
type Metrics interface {
    Counter(name string, tags map[string]string) Counter
    Histogram(name string, tags map[string]string) Histogram
    Gauge(name string, tags map[string]string) Gauge
}
```

### Key Metrics

- `workflow.runs.active` (gauge) -- concurrent running workflows
- `workflow.runs.completed` / `workflow.runs.failed` (counter)
- `step.duration_ms` (histogram) -- per task type
- `step.retries` (counter) -- per task type
- `agent.loop.iterations` (histogram) -- per workflow
- `nats.consumer.pending` (gauge) -- task queue depth

### Adapter Structure

```
dagnats/
├── observe/          # Interfaces: Logger, Metrics, ErrorReporter
├── observe/noop/     # No-op implementations (default)
├── observe/sentry/   # Sentry adapter (future)
├── observe/otel/     # OpenTelemetry adapter (future)
```
