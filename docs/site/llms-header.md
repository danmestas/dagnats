# DagNats

> DAG-based workflow engine built on NATS JetStream, for durable workflows and autonomous LLM coding pipelines. All state lives in NATS (JetStream streams and KV); there is no external database. Go module `github.com/danmestas/dagnats`, single binary `dagnats`.

This file is generated from `docs/site/llms-header.md` plus the docs site. Every link below is a page on the docs site; [llms-full.txt](https://dagnats-docs.daniel-mestas.workers.dev/llms-full.txt) holds all pages in one file.

## Key concepts

- **Workflow**: a DAG of steps with dependencies, retries, timeouts, rate limits and concurrency limits, defined as JSON or built with the Go `dag` builder.
- **Step**: a unit of work. There are 10 step types: `normal`, `agent_loop`, `sub_workflow`, `agent`, `map`, `sleep`, `wait_for_event`, `approval`, `planner`, `respond`.
- **Worker**: a process that registers task handlers and pulls work from NATS. Go workers use the `worker` package; other languages use the HTTP bridge.
- **Run**: one execution of a workflow, with input, status and an event history.
- **Event**: an immutable record in the `WORKFLOW_HISTORY` stream; the engine resolves the DAG from these events.
- **Trigger**: starts runs from cron schedules, NATS events, webhooks, HTTP requests, run completion, the CLI or the REST API.
- **Coordination lease**: a TTL-based lease held in a NATS KV bucket, used by service replicas to elect a leader or own shards (ADR-024).

## Quick start

1. `dagnats serve` starts the embedded NATS server, engine, API and triggers.
2. Write a workflow as JSON (steps plus dependencies) and register it: `dagnats workflow register workflow.json`.
3. Write a Go worker: `worker.NewWorker(nc, ...)` then `worker.HandleTyped[I, O](w, "task-type", fn)`.
4. Run it: `dagnats run start <workflow> '<json input>' --watch`.

CLI command groups (`dagnats --help`): `workflow`, `run`, `trigger`, `dlq`, `workers`, `serve`, `init`, `config`, `status`, `logs`, `dev`, `trace`, `metrics`, `singleton`, `service`, `trigger-type`, `clean`, `demo`, `observe`, `sidecar`, `completion`. Add `--json` for machine-readable output.

## Deployment

- Single node: `dagnats serve` runs everything in one process.
- Cluster mode: N copies of `dagnats serve` form their own NATS cluster (config `nats_cluster_name`, `nats_cluster_routes`, `nats_cluster_auth_token`, `nats_jetstream_replicas`; ADR-005).
- Distributed: run `dagnats-engine` and `dagnats-api` as separate binaries against an external NATS cluster, or connect the embedded server as a leaf node.

## Go packages

- `dag`: workflow definitions, builder API, validation, the pure `Advance()` state machine. No NATS imports.
- `worker`: worker framework, `TaskContext`, typed handlers.
- `protocol`: wire types (`Event`, task payloads, resolutions).
- `observe`: provider-agnostic Logger, Tracer, Metrics and ErrorReporter interfaces.
- `server`: embedded NATS server and `dagnats serve` lifecycle.
- `bridge`: HTTP-to-NATS bridge for remote and non-Go workers.
- `sdk/httpclient`: reference Go client for the HTTP bridge worker protocol.
- `dagnatstest`: one-call test setup with an embedded NATS server.
- `ci`: parser and compiler for `.dagnats/ci.yml` CI specs.
