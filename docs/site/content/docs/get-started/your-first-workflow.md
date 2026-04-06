---
title: Your First Workflow
weight: 3
---

Build a three-step workflow with typed handlers, retries, and timeouts, then inspect the run using CLI tools.

This tutorial assumes you completed the [Quickstart](/docs/get-started/quickstart) and have `dagnats serve` running.

## Understanding the Workflow Definition

A **workflow definition** is a JSON document with a name, version, and a list of steps. Each step declares a unique `id`, a `task` name that maps to a worker handler, and an optional `depends_on` array listing step IDs that must complete before this step runs.

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

The engine uses **Kahn's algorithm** to validate that the dependency graph is acyclic and to determine execution order. Steps with no dependencies (or whose dependencies have all completed) are dispatched in parallel.

## Adding a Third Step

Extend the workflow with a `reverse` step that depends on `uppercase`:

```json
{
  "name": "hello-pipeline",
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
    },
    {
      "id": "reverse",
      "task": "reverse",
      "depends_on": ["uppercase"]
    }
  ]
}
```

This creates a linear chain: `greet` -> `uppercase` -> `reverse`. Save this as `pipeline.json` and register it:

```bash
dagnats workflow register pipeline.json
```

## Writing Typed Handlers

`worker.HandleTyped[I, O]` is a generic function that wraps your handler with automatic JSON marshaling. The type parameters `I` and `O` define the input and output types. The worker framework deserializes the task input into `I` and serializes the return value as `O`.

```go
worker.HandleTyped(w, "greet",
	func(ctx worker.TaskContext, name string) (string, error) {
		if name == "" {
			name = "World"
		}
		return fmt.Sprintf("Hello, %s!", name), nil
	},
)
```

For structured data, use Go structs:

```go
type ReviewInput struct {
	Repo   string `json:"repo"`
	Branch string `json:"branch"`
}

type ReviewOutput struct {
	Issues int    `json:"issues"`
	Report string `json:"report"`
}

worker.HandleTyped(w, "review",
	func(ctx worker.TaskContext, in ReviewInput) (ReviewOutput, error) {
		// ... perform review ...
		return ReviewOutput{Issues: 3, Report: "Found 3 issues"}, nil
	},
)
```

If JSON deserialization fails (e.g., the input does not match the expected type), the handler returns a **non-retryable error** automatically. Retrying with the same malformed input would not help.

Add the `reverse` handler to your worker:

```go
worker.HandleTyped(w, "reverse",
	func(ctx worker.TaskContext, input string) (string, error) {
		runes := []rune(input)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return string(runes), nil
	},
)
```

## Running With --watch

Start the worker, then kick off a run:

```bash
dagnats run start hello-pipeline '"World"' --watch
```

The `--watch` flag subscribes to the run's event stream and prints each event as it arrives:

```
Run def456 started
  step.greet: completed
  step.uppercase: completed
  step.reverse: completed
Run def456 completed
```

Each line corresponds to an **event** written to the `WORKFLOW_HISTORY` JetStream stream. The engine processes these events to advance the DAG.

## Inspecting Runs

After a run completes, use the CLI to inspect its state:

```bash
dagnats run status <run-id>
```

This shows the current state of the run and each step, including outputs:

```
Run:    def456
Status: completed
Steps:
  greet:     completed  "Hello, World!"
  uppercase: completed  "HELLO, WORLD!"
  reverse:   completed  "!DLROW ,OLLEH"
```

To see the full event history:

```bash
dagnats run events <run-id>
```

This prints every event in chronological order -- useful for debugging why a step failed or understanding execution timing.

## Adding Timeout and Retry Policy

Steps can declare a **timeout** (maximum execution duration) and a **retry policy** (what to do when a step fails).

```json
{
  "id": "greet",
  "task": "greet",
  "depends_on": [],
  "timeout": "30s",
  "type": "normal",
  "retry": {
    "max_attempts": 3,
    "strategy": "exponential",
    "initial_delay": "1s",
    "max_delay": "30s",
    "multiplier": 2.0
  }
}
```

- **timeout** sets the per-step deadline. If the worker does not complete within this duration, the task is redelivered (up to `MaxDeliver`). Under the hood, this maps to NATS `AckWait`.
- **retry.max_attempts** is the total number of retry attempts after the first failure.
- **retry.strategy** controls backoff: `fixed` (constant delay), `linear` (delay * attempt), or `exponential` (delay * multiplier^attempt).
- **retry.initial_delay** is the base delay between attempts.
- **retry.max_delay** caps the delay to prevent unbounded waits.

You can also set a **default retry policy** at the workflow level that applies to all steps unless overridden:

```json
{
  "name": "hello-pipeline",
  "version": "1.1",
  "default_retry": {
    "max_attempts": 2,
    "strategy": "fixed",
    "initial_delay": "5s",
    "max_delay": "5s"
  },
  "steps": [...]
}
```

## What Happens Under the Hood

When you call `dagnats run start`, here is what the system does:

1. **API** receives the request and publishes a `workflow.started` event to the `WORKFLOW_HISTORY` JetStream stream at subject `history.<run-id>`.

2. **Engine** consumes the event. It calls `dag.Advance(def, run, event)` -- a pure function that takes the workflow definition, current run state, and new event, then returns a list of **actions** (e.g., `EnqueueTask`, `CompleteWorkflow`).

3. For each `EnqueueTask` action, the engine publishes a task message to the `TASK_QUEUES` stream at subject `task.<task-name>`.

4. **Worker** pulls the task via a JetStream pull consumer. It executes the registered handler and publishes a `step.completed` or `step.failed` event back to `WORKFLOW_HISTORY`.

5. The engine consumes the completion event, calls `dag.Advance` again, and dispatches the next ready steps. This loop continues until all steps complete or a terminal failure occurs.

6. When the final step completes, `dag.Advance` returns a `CompleteWorkflow` action. The engine writes `workflow.completed` to the history stream.

The engine is **stateless** -- it reconstructs run state by replaying the event stream. KV snapshots in the `workflow_runs` bucket are an optimization for fast reads, not the source of truth. If the engine crashes and restarts, it replays from the stream and resumes exactly where it left off.

## Next Steps

- [Core Concepts](/docs/concepts) -- deep dive into workflows, events, and the DAG model
- [Step Types](/docs/step-types) -- agent loops, sub-workflows, map steps, and more
- [Reliability](/docs/reliability) -- retry policies, dead letter queues, compensation
- [Worker SDK](/docs/workers) -- full TaskContext API, checkpointing, signals
