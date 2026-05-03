# DagNats

DAG-based workflow engine built on NATS for autonomous LLM coding pipelines.

Combines Hatchet-style DAG orchestration with Temporal-style durable execution, using NATS JetStream as the sole infrastructure dependency. Define workflows as directed acyclic graphs, register worker handlers, and let the engine handle task distribution, retries, event sourcing, and state recovery.

## Quick Start

### Install

```bash
# From source
go install github.com/danmestas/dagnats/cmd/dagnats@latest

# From a release tarball (linux/darwin, amd64/arm64)
# https://github.com/danmestas/dagnats/releases/latest
```

Or run the official Docker image:

```bash
docker build -t dagnats:latest .  # build locally; published image pending
docker run --rm -p 4222:4222 -p 8080:8080 dagnats:latest
```

### Run

```bash
# One command — embedded NATS, engine, API, triggers
dagnats serve
```

Zero config. Embedded NATS with JetStream, event-sourced orchestrator, API server, and trigger system all running in one process.

Scaffold a new workflow project with `dagnats init my-workflow`, or browse `examples/` (try `cd examples/hello-world && go run .` after starting `dagnats serve`).

**Full walkthrough:** see [docs/getting-started.md](docs/getting-started.md) for a zero-to-running-workflow guide in eight steps.

### Defining workflows

Workflows are JSON files (the primary, registry-friendly format). The Go builder shown below is for programmatic construction — useful for tests and in-process scenarios.

```go
// Define a workflow with the Go builder
wf, _ := dag.NewWorkflow("code-review").
    Task("plan", "llm-planner").
    Task("code", "llm-coder").DependsOn("plan").
    Task("test", "test-runner").DependsOn("code").
    Task("review", "llm-reviewer").DependsOn("test").
    AgentLoop("fix", "llm-fixer").DependsOn("review").
        WithMaxIterations(10).
    Build()

// Register a worker (variadic options: worker.WithGroups, worker.WithPartitions)
w := worker.NewWorker(nc)
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
    Orchestrator   -->  Event-sourced; reads run state from KV per event
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
| `internal/engine/` | Orchestrator — event consumption, DAG advancement, retry-backoff scheduler, step-timeout watchdog, concurrency, cancel |
| `worker/` | Worker framework — TaskContext, heartbeat, checkpoint, signals |
| `internal/api/` | Control plane — REST + NATS request/reply |
| `internal/trigger/` | Cron, NATS subject, and webhook triggers with live reload |
| `actor/` | General-purpose Go actor runtime — supervision, restart tracking, mailboxes (no longer used by the engine; see ADR-009) |
| `server/` | Embedded NATS server, full lifecycle, single-binary deployment |
| `cli/` | CLI client — workflow, run, trigger, dlq, serve, status commands |
| `observe/` | Provider-agnostic observability interfaces |
| `natsutil/` | NATS resource setup + embedded test server |
| `protocol/` | Wire-format types — Event, EventType, TaskPayload |

### Key Design Decisions

**Event-Sourced Orchestrator.** A single orchestrator subscribes to the workflow history stream. On each event, it loads the run snapshot from KV, advances the DAG, and saves. No long-lived in-memory state per run. (ADR-009 records the removal of an earlier actor-per-run prototype.)

**Engine as Sole Retry Authority.** Workers report failures via `step.failed`; the engine schedules the next attempt via a durable `SLEEP_TIMERS` consumer using the policy's backoff curve. Step-level `Timeout` arms a watchdog that emits a synthetic `step.failed` if the attempt is still running when it fires. (ADR-011.)

**Deep Worker Interface.** `TaskContext` provides: `Input()`, `Complete()`, `Fail()`, `FailPermanent()`, `FailRetryAfter()`, `Continue()`, `PutStream()`, `Heartbeat()`, `Checkpoint()`/`LoadCheckpoint()`, `WaitForSignal()`/`SendSignal()`. Workers never see retries, timeouts, or DAG logic.

**Configurable Retry Policies.** Fixed, linear, or exponential backoff. Per-step override or workflow default. Resolution: step → workflow → legacy Retries field → no retry.

**Concurrency Limits.** KV-based counters with optimistic locking. Excess runs queued as pending, auto-started when slots open.

**Trigger System.** Cron (in-house parser, 30s tick), NATS subject subscriptions, HTTP webhooks with HMAC-SHA256. Live reload via KV watcher.

**Always-Embedded NATS.** `dagnats serve` starts an embedded NATS server. Standalone for single-machine, leaf node mode for connecting to a hub cluster. Components always connect to localhost.

**Event Sourcing + KV Snapshots.** Immutable history stream for replay and audit. KV snapshots for fast recovery.

**NATS-Native Patterns.** No custom infrastructure:

| Need | NATS Primitive |
|------|---------------|
| Task distribution | JetStream pull consumers (durable per task type) |
| Retry with backoff | Engine-scheduled `SLEEP_TIMERS` entry per attempt (per-policy delay) |
| Step timeouts | Engine watchdog timer; emits synthetic `step.failed` on fire |
| Exactly-once delivery | `Nats-Msg-Id` dedup (attempt-suffixed for retries) |
| Run state snapshots | KV with optimistic locking |
| Cross-workflow signals | KV watches |
| Dead-letter queue | Dedicated stream |

## Running

`dagnats serve` covers single-machine deployments. For leaf-node and distributed topologies (and the trade-off matrix), see [Production guide → Deployment Topologies](docs/production.md#deployment-topologies).

## CLI

```bash
dagnats init <name>                                 # Scaffold new workflow project
dagnats serve                                       # Start embedded server
dagnats status                                      # Check system health
dagnats workflow list                               # List registered workflows
dagnats workflow register workflow.json             # Register a workflow
dagnats run start <workflow> [input] [--watch]       # Start a run (--watch tails events)
dagnats run status <run-id>                         # Check run status
dagnats run inspect <run-id>                        # Status + errors + DLQ in one view
dagnats run list [--workflow=X] [--status=running]  # List runs with filters
dagnats run events <id> [--full] [--type=T] [--step=S]  # View event history
dagnats run output <run-id>                         # Print final output of completed run
dagnats run cancel <run-id>                         # Cancel a running workflow
dagnats run signal <run-id> <name> <data>           # Send signal to a run
dagnats trigger create <wf> --cron="..."            # Create a trigger
dagnats trigger list                                # List triggers
dagnats trigger enable <id>                         # Enable a trigger
dagnats trigger disable <id>                        # Disable a trigger
dagnats trigger test <cron-expr> [--tz=TZ] [--count=N]  # Validate cron and show fire times
dagnats trigger delete <id>                         # Delete a trigger
dagnats dlq list [--run=<id>] [--limit=N]           # List dead-letter messages
dagnats dlq replay <seq>                            # Replay a failed message
dagnats dlq replay --run=<id>                       # Replay all failures for a run
```

All commands support `--json` for machine-readable output. Workflow JSON files support
editor autocomplete via `docs/workflow-schema.json` (add `"$schema"` reference).

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
make test
# or directly:
go test ./... -timeout 600s
```

Tests use real embedded NATS servers (no mocks). Each test gets its own server via `natsutil.StartTestServer(t)`. The full suite takes ~5 minutes locally (worker package ~75s, engine ~45s, e2e harness ~80s).

## Documentation

| Guide | Description |
|-------|-------------|
| [Getting Started](docs/getting-started.md) | Workflows, workers, and first run |
| [Configuration](docs/configuration.md) | Config keys, env vars, file format |
| [Production](docs/production.md) | Deployment, security, tuning, observability |
| [Workflow Schema](docs/workflow-schema.md) | JSON schema reference |
| [AGENTS.md](AGENTS.md) | Conventions for coding agents (Codex, Cursor, Claude Code) |

For coding agents and LLM tools: a curated [`llms.txt`](https://dagnats-docs.daniel-mestas.workers.dev/llms.txt) and a full-content [`llms-full.txt`](https://dagnats-docs.daniel-mestas.workers.dev/llms-full.txt) are regenerated on every commit.

## Design Philosophy

- **Ousterhout:** Deep modules with small interfaces. Pull complexity downward.
- **TigerStyle:** Safety > Performance > DX. Assertions as contracts. Bounded everything.
- **HIPP:** Small. Fast. Reliable. Zero-config where possible. Minimal dependencies.

## License

Apache License 2.0. See [LICENSE](LICENSE) for the full text.
