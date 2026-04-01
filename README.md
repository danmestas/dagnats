# DagNats

DAG-based workflow engine built on NATS for autonomous LLM coding pipelines.

Combines Hatchet-style DAG orchestration with Temporal-style durable execution, using NATS JetStream as the sole infrastructure dependency. Define workflows as directed acyclic graphs, register worker handlers, and let the engine handle task distribution, retries, event sourcing, and state recovery.

## Quick Start

```bash
# One command — embedded NATS, engine, API, triggers
dagnats serve
```

Zero config. Embedded NATS with JetStream, actor-based orchestrator, API server, and trigger system all running in one process.

See `examples/hello-world/` for a runnable two-step workflow.

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
w := worker.NewWorker(nc, tel)
w.Handle("llm-coder", func(ctx worker.TaskContext) error {
    plan := ctx.Input()
    code := generateCode(plan)
    return ctx.Complete(code)
})
w.Start()
```

## Architecture

```
Triggers (cron, NATS subjects, webhooks)
          |
    Control Plane  -->  REST API + NATS micro
          |
    Orchestrator   -->  Actor-based, per-run in-memory state
          |
    Workers        -->  Pull tasks, execute handlers, publish results
          |
    Orchestrator   -->  Advance DAG, repeat until complete
```

All state lives in NATS. No external database, no Redis, no Postgres.

### Packages

| Package | Purpose |
|---------|---------|
| `dag/` | Pure DAG logic — types, builder, validation, resolution, retry policies, schema validation |
| `engine/` | Actor-based orchestrator — event consumption, DAG advancement, concurrency, cancel |
| `worker/` | Worker framework — TaskContext, heartbeat, checkpoint, signals |
| `api/` | Control plane — REST + NATS request/reply |
| `trigger/` | Cron, NATS subject, and webhook triggers with live reload |
| `actor/` | Pure Go actor runtime — supervision, restart tracking, mailboxes |
| `server/` | Embedded NATS server, full lifecycle, single-binary deployment |
| `cli/` | CLI client — workflow, run, trigger, dlq, serve, status commands |
| `observe/` | Provider-agnostic observability interfaces |
| `natsutil/` | NATS resource setup + embedded test server |
| `protocol/` | Wire-format types — Event, EventType, TaskPayload |

### Key Design Decisions

**Actor-Based Orchestrator.** Per-workflow actors hold run state in memory. Events route to the correct actor via the actor runtime. Snapshots save to KV for durability. OneForOne supervision with bounded restart tracking.

**Deep Worker Interface.** `TaskContext` provides: `Input()`, `Complete()`, `Fail()`, `Continue()`, `PutStream()`, `Heartbeat()`, `Checkpoint()`/`LoadCheckpoint()`, `WaitForSignal()`/`SendSignal()`. Workers never see retries, timeouts, or DAG logic.

**Configurable Retry Policies.** Fixed, linear, or exponential backoff. Per-step override or workflow default. Resolution: step → workflow → legacy Retries field → no retry.

**Concurrency Limits.** KV-based counters with optimistic locking. Excess runs queued as pending, auto-started when slots open.

**Trigger System.** Cron (in-house parser, 30s tick), NATS subject subscriptions, HTTP webhooks with HMAC-SHA256. Live reload via KV watcher.

**Always-Embedded NATS.** `dagnats serve` starts an embedded NATS server. Standalone for single-machine, leaf node mode for connecting to a hub cluster. Components always connect to localhost.

**Event Sourcing + KV Snapshots.** Immutable history stream for replay and audit. KV snapshots for fast recovery.

**NATS-Native Patterns.** No custom infrastructure:

| Need | NATS Primitive |
|------|---------------|
| Task distribution | JetStream pull consumers + MaxAckPending |
| Retry with backoff | NakWithDelay |
| Exactly-once delivery | Nats-Msg-Id dedup |
| Run state snapshots | KV with optimistic locking |
| Step timeouts | AckWait + MaxDeliver |
| Cross-workflow signals | KV watches |
| Dead-letter queue | Dedicated stream |

## Running

### Single Binary (recommended)

```bash
# Zero config — everything in one process
dagnats serve

# With config file
cat > dagnats.yaml << EOF
data_dir: /var/lib/dagnats
http_addr: :8080
nats_port: 4222
EOF
dagnats serve

# Leaf node mode — connect to hub cluster
DAGNATS_LEAF_REMOTES=nats://hub1:7422,nats://hub2:7422 dagnats serve
```

### Distributed (scaling out)

```bash
# Separate processes for independent scaling
nats-server -js
NATS_URL=nats://cluster:4222 dagnats-engine
NATS_URL=nats://cluster:4222 dagnats-api
```

Workers always run as separate processes connecting to NATS.

## CLI

```bash
dagnats serve                                       # Start embedded server
dagnats status                                      # Check system health
dagnats workflow list                               # List registered workflows
dagnats workflow register workflow.json             # Register a workflow
dagnats run start <workflow> [input]                # Start a run
dagnats run status <run-id>                         # Check run status
dagnats run list [--workflow=X] [--status=running]  # List runs with filters
dagnats run events <id> [--full] [--type=T] [--step=S]  # View event history
dagnats run cancel <run-id>                         # Cancel a running workflow
dagnats run signal <run-id> <name> <data>           # Send signal to a run
dagnats trigger create <wf> --cron="..."            # Create a trigger
dagnats trigger list                                # List triggers
dagnats trigger enable <id>                         # Enable a trigger
dagnats trigger disable <id>                        # Disable a trigger
dagnats trigger delete <id>                         # Delete a trigger
dagnats dlq list [--run=<id>] [--limit=N]           # List dead-letter messages
dagnats dlq replay <seq>                            # Replay a failed message
```

## API

### REST

```
POST   /workflows              Register a workflow definition
GET    /workflows               List all workflows
POST   /runs                   Start a new run
GET    /runs/{id}              Get run status
POST   /runs/{id}/cancel       Cancel a run
POST   /runs/{id}/signal/{name}  Send signal
GET    /health                 NATS + JetStream connectivity check
GET    /health/telemetry       Telemetry stream usage stats
GET    /ready                  Server readiness
POST   /hooks/{path}           Webhook trigger endpoint
```

## NATS Resources

Created by `natsutil.SetupAll(nc)`:

| Type | Name | Purpose |
|------|------|---------|
| Stream | WORKFLOW_HISTORY | Immutable event log |
| Stream | TASK_QUEUES | Task distribution (work queue) |
| Stream | EVENTS | External triggers |
| Stream | DEAD_LETTERS | Permanent failures (30-day retention) |
| Stream | TELEMETRY | Observability signals (7-day, 1GB cap) |
| KV | workflow_defs | Workflow definitions |
| KV | workflow_runs | Run state snapshots |
| KV | triggers | Trigger definitions |
| KV | trigger_state | Cron timestamps |
| KV | signals | Cross-workflow signals |
| KV | checkpoints | Worker step state |
| KV | concurrency_runs | Per-workflow counters |

## Testing

```bash
go test ./... -timeout 120s
```

Tests use real embedded NATS servers (no mocks). Each test gets its own server via `natsutil.StartTestServer(t)`. 17 packages, all passing.

## Design Philosophy

- **Ousterhout:** Deep modules with small interfaces. Pull complexity downward.
- **TigerStyle:** Safety > Performance > DX. Assertions as contracts. Bounded everything.
- **HIPP:** Small. Fast. Reliable. Zero-config where possible. Minimal dependencies.

## License

Private. See repository settings.
