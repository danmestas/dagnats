# DagNats

DAG-based workflow engine built on NATS for autonomous LLM coding pipelines.

Combines Hatchet-style DAG orchestration with Temporal-style durable execution, using NATS JetStream as the sole infrastructure dependency. Define workflows as directed acyclic graphs, register worker handlers, and let the engine handle task distribution, retries, event sourcing, and state recovery. Gated tasks can also **author and launch workflows at runtime** (agent runtimes), and the internal control plane is discoverable via nats-micro.

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

## How it works

Workflows are DAGs. The orchestrator subscribes to a NATS history stream and advances the DAG one event at a time, reading and writing run state to a KV bucket. Workers pull tasks from a durable JetStream consumer, execute handlers, and publish results back as events. Retries, step timeouts, and concurrency limits are scheduled by the engine through durable NATS primitives — no timer service, no external database, no Redis.

Agent runtimes allow granted task handlers to access a `ControlPlane` capability, register new workflow definitions at runtime, and spawn child workflows. Capability grants are deny-by-default and enforced via policy configuration. The internal control plane endpoints run as nats-micro services, making them discoverable and observable via standard NATS tooling (`nats micro ls`).

`dagnats serve` covers single-machine deployments. For leaf-node and distributed topologies, see the [Production guide](docs/production.md#deployment-topologies). Architecture decisions are recorded in [docs/architecture/](docs/architecture/) (ADR-006 onwards).

### Trigger kinds

Workflows can fire on four event sources:

- **Cron** — time-based; `dagnats trigger create <wf> --cron="..."`.
- **Webhook** — fire-and-forget at `POST /hooks/{path}`; 202 with no result.
- **NATS subject** — subscribe to a JetStream subject filter; one message → one run.
- **HTTP request/response** — synchronous endpoint at `/api/{path}` whose response comes from a `respond` step in the DAG. Use this when the workflow *is* an HTTP API. See [ADR-013](docs/architecture/adr-013-http-trigger-respond-step.md) and [`examples/http-respond/`](examples/http-respond/).

Cron triggers are CLI-created. The other three are declared inline in workflow JSON under the workflow's `triggers` array.

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
POST   /hooks/{path}           Webhook trigger endpoint (fire-and-forget, 202)
*      /api/{user-path}        HTTP trigger endpoints (synchronous; method + path
                               defined per workflow; response comes from the
                               workflow's respond step — see ADR-013)
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
| KV | console_audit | Console audit log (operator + control-plane actions) |
| KV | http_idempotency | HTTP trigger idempotency-key → run_id and stored response payloads (1h TTL) |

## Testing

```bash
make test
# or directly:
go test ./... -timeout 600s
```

Tests use real embedded NATS servers (no mocks). Each test gets its own server via `natsutil.StartTestServer(t)`. The full suite takes ~5 minutes locally (worker package ~75s, engine ~45s, e2e harness ~80s).

## Console

`dagnats serve` mounts a server-rendered operator UI at
`http://127.0.0.1:8080/console/` (loopback by default). The console
is a live window onto NATS state — workflows, runs, triggers,
dead-letter queue, audit log, and a metrics dashboard with anomaly
markers. Mutations (retry, discard, toggle) flow through the same
API the CLI uses, and SSE pushes row-level updates without polling.
For deployment, auth modes, env vars, and the production checklist
see [docs/console.md](docs/console.md).

## Documentation

| Guide | Description |
|-------|-------------|
| [Getting Started](docs/getting-started.md) | Workflows, workers, and first run |
| [Console](docs/console.md) | Operator UI: deployment, auth, env vars, key workflows |
| [Console Contributing](docs/console-contributing.md) | DX guide for changing the console |
| [Configuration](docs/configuration.md) | Config keys, env vars, file format |
| [Production](docs/production.md) | Deployment, security, tuning, observability |
| [Workflow Schema](docs/workflow-schema.md) | JSON schema reference |
| [AGENTS.md](AGENTS.md) | Conventions for coding agents (Codex, Cursor, Claude Code) |
| [Agent Runtimes](docs/site/content/docs/ai-patterns/runtime-generated-workflows.md) | Runtime-generated workflows and agent runtime patterns |
| [Service Discovery](docs/site/content/docs/operations/service-discovery.md) | nats-micro control plane endpoints |

For coding agents and LLM tools: a curated [`llms.txt`](https://dagnats-docs.daniel-mestas.workers.dev/llms.txt) and a full-content [`llms-full.txt`](https://dagnats-docs.daniel-mestas.workers.dev/llms-full.txt) are regenerated on every commit.

## Acknowledgements

dagnats's design draws on three bodies of work:

- **John Ousterhout** — *A Philosophy of Software Design.* Deep modules with small interfaces; information hiding; pulling complexity downward.
- **Dr. Richard Hipp** — the design discipline behind SQLite. Small, fast, reliable; zero-config where possible; minimal dependencies; long-term maintainability over feature breadth.
- **TigerBeetle's TigerStyle** — Safety > Performance > Developer Experience; assertions as contracts; bounded everything; zero technical debt.

We are grateful for the public writing and code these projects have shared.

## License

Apache License 2.0. See [LICENSE](LICENSE) for the full text.

The console bundles [IBM Plex](https://github.com/IBM/plex) fonts under the SIL Open Font License v1.1; see [`internal/console/assets/fonts/OFL.txt`](internal/console/assets/fonts/OFL.txt).
