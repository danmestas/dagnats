# Getting Started with DagNats

This guide walks you from zero to a running workflow in eight steps.

## Concepts

A **workflow** is a DAG (directed acyclic graph) of **steps**. Each step
references a **task** name. **Workers** subscribe to task names and
execute the actual logic. The **engine** watches for events and advances
the DAG as steps complete.

```
Workflow: fetch-data --> transform --> publish
              \                         /
               +--- validate ----------+
```

Steps run in parallel when their dependencies allow it. In this example,
`transform` and `validate` both depend on `fetch-data` and run
concurrently. `publish` waits for both.

---

## 1. Install

From source (requires Go 1.21+):

```bash
go install github.com/danmestas/dagnats/cmd/dagnats@latest
```

Or build from a local clone:

```bash
git clone https://github.com/danmestas/dagnats.git
cd dagnats
make build
export PATH=$PWD/bin:$PATH
```

Verify the install:

```bash
dagnats --version
```

## 2. Start the Server

```bash
dagnats serve
```

This starts an embedded NATS server, the orchestration engine, the API,
and the trigger system. Default ports: NATS on 4222, HTTP API on 8080.

## 3. Scaffold a Project

In a new terminal:

```bash
dagnats init hello-world
cd hello-world
```

This creates a `hello-world/` directory containing:

- `workflow.json` -- a single-step workflow definition
- `main.go` -- a Go worker with a handler stub

To add a workflow to an existing project instead:

```bash
dagnats init workflow etl-pipeline \
  --steps=fetch,transform,load
```

## 4. Explore the Generated Files

### Understanding the workflow definition

Open `workflow.json`. It defines a minimal pipeline with one step:

```json
{
  "$schema": "https://raw.githubusercontent.com/danmestas/dagnats/main/docs/workflow-schema.json",
  "name": "hello-world",
  "version": "1.0",
  "steps": [
    {
      "id": "process",
      "task": "process"
    }
  ]
}
```

Key fields:

- **`name`** -- unique workflow identifier
- **`steps[].id`** -- unique within the workflow
- **`steps[].task`** -- the task name workers subscribe to
- **`steps[].depends_on`** -- step IDs that must complete first
  (omitted when a step has no dependencies)

A more interesting workflow chains steps together. Here is a two-step
pipeline where `uppercase` depends on `greet`:

```json
{
  "name": "hello-world",
  "version": "1.0",
  "steps": [
    {
      "id": "greet",
      "task": "greet",
      "depends_on": []
    },
    {
      "id": "uppercase",
      "task": "uppercase",
      "depends_on": ["greet"]
    }
  ]
}
```

### Understanding the worker

Open `main.go`. The scaffold registers a single handler for the
`"process"` task:

```go
w.Handle("process", func(ctx worker.TaskContext) error {
    input := ctx.Input()
    fmt.Printf("[process] input: %s\n", string(input))
    return ctx.Complete(input)
})
```

`ctx.Input()` returns the raw JSON input. `ctx.Complete(output)` marks
the step as done and passes output to any downstream steps.

For typed deserialization, use `worker.HandleTyped`:

```go
worker.HandleTyped(w, "greet",
    func(ctx worker.TaskContext, name string) (string, error) {
        return fmt.Sprintf("Hello, %s!", name), nil
    },
)
```

`HandleTyped` JSON-unmarshals input into your function's second
parameter and JSON-marshals the return value as step output.

If a step has multiple dependencies, the input is a JSON object keyed
by parent step ID:

```go
// Step depends on ["fetch", "validate"]
// Input: {"fetch": <output>, "validate": <output>}
worker.HandleTyped(w, "merge",
    func(
        ctx worker.TaskContext,
        input map[string]json.RawMessage,
    ) (Result, error) {
        // Parse each parent's output individually
    },
)
```

### Raw handlers

For full control, use the untyped `Handle` method:

```go
w.Handle("my-task", func(ctx worker.TaskContext) error {
    raw := ctx.Input()           // []byte of JSON input
    runID := ctx.RunID()         // workflow run ID
    stepID := ctx.StepID()       // current step ID
    attempt := ctx.RetryCount()  // 0-based retry attempt

    // Do work...

    return ctx.Complete([]byte(`{"result": "done"}`))
})
```

Call exactly one of these per execution:

- `ctx.Complete(output)` -- success, advance the DAG
- `ctx.Fail(err)` -- permanent failure
- `ctx.Continue(output)` -- loop again (agent-loop steps only)

## 5. Register the Workflow

```bash
dagnats workflow register workflow.json
```

This stores the definition in NATS KV. You can validate first without
registering:

```bash
dagnats workflow validate workflow.json
```

## 6. Run the Worker

In a separate terminal (with `dagnats serve` still running):

```bash
cd hello-world
go run .
```

The worker connects to NATS and waits for tasks.

## 7. Start a Workflow Run

```bash
dagnats run start hello-world '{"name":"world"}' --watch
```

The second argument is the JSON input passed to the first step(s). The
`--watch` flag streams status updates until the run completes.

## 8. Inspect the Result

```bash
dagnats run status --last
dagnats run output --last
```

Or query a specific run by ID:

```bash
dagnats run status <run-id>
dagnats run output <run-id>
```

---

## What Happens Under the Hood

1. The API publishes a `WorkflowStarted` event to the history stream
2. The engine picks it up, creates a run snapshot in KV, and enqueues
   entry-point steps (those with no dependencies)
3. Workers receive task messages on `task.<taskType>.>` via JetStream
4. On completion, a `StepCompleted` event is published
5. The engine resolves the next ready steps and enqueues them
6. When all steps complete, the run is marked complete

---

## Adding More Complexity

### Timeouts

```json
{
  "id": "slow-step",
  "task": "slow-task",
  "timeout": "5m",
  "depends_on": ["previous"]
}
```

Set a workflow-level deadline with the top-level `"timeout"` field.

### Retries

```json
{
  "id": "flaky-step",
  "task": "http-call",
  "depends_on": [],
  "retry": {
    "max_attempts": 3,
    "strategy": "exponential",
    "initial_delay": "2s",
    "max_delay": "30s",
    "multiplier": 2.0
  }
}
```

Strategies: `fixed`, `linear`, `exponential`.

In your worker, return a normal error to trigger a retry. Use
`worker.NewNonRetryableError(err)` to skip retries and fail
immediately.

### Conditional Skipping

Skip a step based on a parent step's output:

```json
{
  "id": "notify",
  "task": "send-notification",
  "depends_on": ["check"],
  "skip_if": {
    "step_id": "check",
    "field": "needs_notification",
    "op": "==",
    "value": false
  }
}
```

### Agent Loops

For iterative tasks (polling, LLM agent loops, convergence
algorithms):

```json
{
  "id": "review-loop",
  "task": "agent-review",
  "type": "agent_loop",
  "depends_on": ["fetch"],
  "loop": {
    "max_iterations": 10,
    "max_duration": "10m",
    "loop_delay": "2s"
  }
}
```

The worker calls `ctx.Continue(output)` to loop again or
`ctx.Complete(output)` to finish. Use checkpoints to persist state
across iterations:

```go
w.Handle("agent-review", func(ctx worker.TaskContext) error {
    // Load state from previous iteration
    raw, _ := ctx.LoadCheckpoint()
    var state MyState
    json.Unmarshal(raw, &state)

    // Do work, update state...
    state.Iterations++

    data, _ := json.Marshal(state)
    ctx.Checkpoint(data)

    if state.Done {
        return ctx.Complete(data)
    }
    return ctx.Continue(data)
})
```

### Failure Handlers

Route to a cleanup step when something fails:

```json
{
  "id": "deploy",
  "task": "deploy",
  "depends_on": ["build"],
  "on_failure": "rollback"
},
{
  "id": "rollback",
  "task": "rollback",
  "depends_on": ["build"]
}
```

### Cross-Step Signals

Pause a step until an external signal arrives (human approval,
webhook, etc.):

```go
// In your worker -- wait for a signal
w.Handle("wait-approval", func(ctx worker.TaskContext) error {
    data, err := ctx.WaitForSignal(
        "approval", 5*time.Minute,
    )
    if err != nil {
        return ctx.Fail(
            fmt.Errorf("approval timed out: %w", err),
        )
    }
    return ctx.Complete(data)
})
```

Send the signal via CLI:

```bash
dagnats run signal <run-id> approval '{"approved": true}'
```

### Worker Groups

Route specific steps to dedicated worker pools:

```json
{
  "id": "gpu-step",
  "task": "inference",
  "worker_group": "gpu-workers",
  "depends_on": ["preprocess"]
}
```

```go
w := worker.NewWorker(nc, worker.WithGroups("gpu-workers"))
w.Handle("inference", func(ctx worker.TaskContext) error {
    // Runs only on workers in the "gpu-workers" group
})
```

### Concurrency Limits

Limit how many runs or steps execute in parallel:

```json
{
  "name": "expensive-workflow",
  "version": "1.0",
  "concurrency": {
    "max_runs": 3,
    "max_steps": 2
  },
  "steps": [...]
}
```

### Cron Triggers

Schedule recurring workflow runs:

```bash
dagnats trigger create my-workflow \
  --cron="*/30 * * * *" \
  --input='{"source":"cron"}'
```

### Heartbeats for Long Tasks

For tasks that take longer than the JetStream AckWait window, send
heartbeats to prevent redelivery:

```go
w.Handle("long-task", func(ctx worker.TaskContext) error {
    for chunk := range chunks {
        process(chunk)
        ctx.Heartbeat()
    }
    return ctx.Complete(result)
})
```

---

## Using the REST API

All CLI operations are also available via HTTP:

```bash
# Register a workflow
curl -X POST http://localhost:8080/workflows \
  -H 'Content-Type: application/json' \
  -d @workflow.json

# Start a run
curl -X POST http://localhost:8080/runs \
  -H 'Content-Type: application/json' \
  -d '{"workflow": "hello-world", "input": "Dan"}'

# Check status
curl http://localhost:8080/runs/<run-id>

# Cancel a run
curl -X POST http://localhost:8080/runs/<run-id>/cancel

# Send a signal
curl -X POST \
  http://localhost:8080/runs/<run-id>/signal/approval \
  -d '{"approved": true}'
```

---

## Observability

Export traces to any OTLP/HTTP-compatible backend (SigNoz,
Grafana Tempo, Jaeger):

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 dagnats serve
```

All workflow events, step transitions, and worker executions are
traced. Spans are always written to the internal NATS `TELEMETRY`
stream regardless of whether an external exporter is configured.

View traces without an external collector:

```bash
dagnats trace <run-id>
dagnats run inspect <run-id> --trace
```

---

## What Next?

- **More examples:** browse `examples/` for agent loops, retries,
  cron triggers, signals, sub-workflows, and more
- **Add workflows to existing projects:**
  `dagnats init workflow <name> --steps=a,b,c`
- **Schedule runs:** `dagnats trigger create <wf> --cron="..."`
- **Retries and timeouts:** see the sections above, or the
  [workflow schema reference](workflow-schema.md)
- **Configuration:** see [configuration.md](configuration.md)
- **Going to production:** see the
  [Production Deployment Guide](production.md)

---

## Quick Reference

| Command | Description |
|---------|-------------|
| `dagnats serve` | Start server (NATS + engine + API) |
| `dagnats init <name>` | Scaffold a new project |
| `dagnats init workflow <name>` | Add a workflow to existing project |
| `dagnats workflow register <file>` | Register a workflow definition |
| `dagnats workflow validate <file>` | Validate without registering |
| `dagnats run start <name> [input]` | Start a workflow run |
| `dagnats run status <id>` | Check run status |
| `dagnats run output <id>` | Get final output |
| `dagnats run list` | List all runs |
| `dagnats run events <id>` | View event history |
| `dagnats run cancel <id>` | Cancel a running workflow |
| `dagnats run signal <id> <name> <data>` | Send a signal |
| `dagnats trigger create <wf> --cron=...` | Schedule recurring runs |
| `dagnats config show` | Show resolved configuration |
