# DagNats

DAG-based workflow engine built on NATS for autonomous LLM coding pipelines.

DagNats combines Hatchet-style DAG orchestration with Temporal-style durable execution, using NATS JetStream as the sole infrastructure dependency. Define workflows as directed acyclic graphs, register worker handlers, and let the engine handle task distribution, retries, event sourcing, and state recovery.

## Quick Start

```go
// Define a workflow
wf, _ := dag.NewWorkflow("code-review").
    Task("plan", "llm-planner").
    Task("code", "llm-coder").DependsOn("plan").
    Task("test", "test-runner").DependsOn("code").
    Task("review", "llm-reviewer").DependsOn("test").
    AgentLoop("fix", "llm-fixer").DependsOn("review").
        WithMaxIterations(10).
    Build()

// Register a worker
w := worker.NewWorker(nc, logger)
w.Handle("llm-coder", func(ctx worker.TaskContext) error {
    plan := ctx.Input()
    code := generateCode(plan)
    return ctx.Complete(code)
})
w.Start()
```

## Architecture

```
Triggers (REST, NATS, cron, webhooks, child workflows)
          |
    Control Plane  -->  Publish workflow.started event
          |
    Orchestrator   -->  Consume events, resolve DAG, enqueue ready tasks
          |
    Workers        -->  Pull tasks, execute handlers, publish results
          |
    Orchestrator   -->  Advance DAG, repeat until complete
```

All state lives in NATS. No external database, no Redis, no Postgres.

### Packages

| Package | Purpose | Dependencies |
|---------|---------|-------------|
| `dag/` | Pure DAG logic -- types, builder DSL, validation, resolution | stdlib only |
| `protocol/` | Wire-format types -- Event, EventType, TaskPayload | stdlib only |
| `engine/` | Orchestrator -- event consumption, DAG advancement, state snapshots | dag, protocol, observe, nats.go |
| `worker/` | Worker framework -- TaskContext interface, handler registration | protocol, observe, nats.go |
| `api/` | Control plane -- REST API + NATS request/reply | engine, protocol, observe, nats.go |
| `cli/` | CLI client | dag |
| `observe/` | Provider-agnostic observability interfaces + noop defaults | stdlib only |
| `natsutil/` | NATS resource setup + embedded test server | nats.go, nats-server |

### Key Design Decisions

**Thin Orchestrator.** The orchestrator is a stateless event processor. It consumes history events, resolves DAG dependencies via pure functions (`dag.ResolveReady`, `dag.ResolveInput`), publishes task messages, and updates KV snapshots. All delivery, retry, and timeout semantics are delegated to NATS primitives.

**Deep Worker Interface.** Workers get 5 methods: `Input()`, `Complete()`, `Fail()`, `Continue()`, and `RunID()`/`StepID()`. They never see retries, timeouts, or DAG logic. Transient failures (handler returns error) are retried automatically via JetStream `NakWithDelay`. Permanent failures (`ctx.Fail(err)`) terminate the workflow.

**Agent Loop.** A step type for LLM-driven iteration. The worker returns `Continue(nextInput)` to loop or `Complete(output)` to finish. Safety bounds (`MaxIterations`, `MaxDuration`) are enforced by the orchestrator. Each iteration is traced in the event history.

**Event Sourcing + KV Snapshots.** Full history in JetStream stream (`WORKFLOW_HISTORY`) for replay and audit. KV snapshots (`workflow_runs`) for fast state recovery without replaying the full history.

**NATS-Native Patterns.** No custom infrastructure:

| Need | NATS Primitive |
|------|---------------|
| Task distribution | JetStream pull consumers + MaxAckPending |
| Transient retries | NakWithDelay (worker) |
| Exactly-once delivery | Nats-Msg-Id dedup |
| Run state snapshots | KV with per-run mutex serialization |
| Step timeouts | AckWait + MaxDeliver |
| Internal API | NATS request/reply |

## NATS Resources

Created by `natsutil.SetupAll(nc)`:

| Resource | Name | Subjects |
|----------|------|----------|
| Stream | `WORKFLOW_HISTORY` | `history.>` |
| Stream | `TASK_QUEUES` | `task.>` |
| Stream | `EVENTS` | `event.>` |
| KV Bucket | `workflow_defs` | -- |
| KV Bucket | `workflow_runs` | -- |

## API

### REST

```
POST /workflows          Register a workflow definition
POST /runs               Start a new run: {"workflow": "name", "input": ...}
GET  /runs/{id}          Get run status and step states
```

### NATS Request/Reply

```
api.workflows.register   Register workflow (JSON body = WorkflowDef)
api.runs.start           Start run (JSON body = {"workflow": "name"})
api.runs.get             Get run (body = runID string)
```

### CLI

```
dagnats workflow list
dagnats workflow register
dagnats run start <workflow> --input '{...}'
dagnats run status <run_id>
dagnats run history <run_id>
```

## Running

```bash
# Start NATS with JetStream
nats-server -js

# Start the engine (orchestrator + resource setup)
NATS_URL=nats://localhost:4222 go run ./cmd/dagnats-engine

# Start the API server
NATS_URL=nats://localhost:4222 go run ./cmd/dagnats-api
```

## Testing

```bash
go test ./... -timeout 60s
```

Tests use real embedded NATS servers (no mocks). Each test gets its own server via `natsutil.StartTestServer(t)`. 65 tests across 10 packages including 2 end-to-end lifecycle tests.

## Design Philosophy

- **Ousterhout:** Deep modules with small interfaces. Pull complexity downward. Define errors out of existence.
- **TigerStyle:** Safety > Performance > DX. Assertions as contracts. Bounded everything. 70-line function limit.
- **Hipp:** Small. Fast. Reliable. Zero-config where possible. Minimal dependencies.

## Project Status

Core engine is functional with:
- Linear and fan-out/fan-in DAG execution
- Agent loop with bounded iterations and duration
- Event sourcing + KV snapshots
- REST and NATS control plane APIs
- Provider-agnostic observability interfaces

Not yet implemented:
- Child workflows (SpawnWorkflow, WaitForAll)
- NATS auth setup (operator/account/user JWTs)
- Dashboard / web UI
- Sentry / OpenTelemetry adapters

## License

Private. See repository settings.
