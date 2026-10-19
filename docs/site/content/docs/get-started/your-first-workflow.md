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
Started: 3c1f0a9e2b7d4c58a6e1f9d02b34c7a1

  13:20:11  workflow.started -
  13:20:11  step.queued greet
  13:20:11  step.started greet
  13:20:11  step.completed greet
  13:20:11  step.queued uppercase
  ...
  13:20:11  step.completed reverse
  13:20:11  workflow.completed -

Run completed: 3c1f0a9e2b7d4c58a6e1f9d02b34c7a1
```

Each line corresponds to an **event** written to the `WORKFLOW_HISTORY` JetStream stream. The engine processes these events to advance the DAG.

## Inspecting Runs

After a run completes, use the CLI to inspect its state:

```bash
dagnats run status <run-id>
```

This shows the current state of the run and each step:

```
Run:      3c1f0a9e2b7d4c58a6e1f9d02b34c7a1
Workflow: hello-pipeline
Status:   completed
Created:  2026-10-19 13:20:11 UTC

Steps:
  greet                completed (attempts: 1)
  uppercase            completed (attempts: 1)
  reverse              completed (attempts: 1)
```

To print the final output, or the full event history:

```bash
dagnats run output <run-id>      # "!DLROW ,OLLEH"
dagnats run events <run-id>
```

`run events` prints every event in chronological order, with each step's output in the data column -- useful for debugging why a step failed or understanding execution timing. `dagnats run inspect <run-id>` combines status, failures and dead letters in one view.

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

- **timeout** sets the per-attempt deadline. If the attempt has not finished within this duration, the engine fails it as retriable and the step's retry policy decides what happens next. See [Timeouts](/docs/reliability/timeouts).
- **retry.max_attempts** is the total number of retry attempts after the first failure.
- **retry.strategy** controls backoff: `fixed` (constant delay), `linear` (delay * attempt), or `exponential` (delay * multiplier^(attempt-1)).
- **retry.initial_delay** is the base delay between attempts.
- **retry.max_delay** caps the delay to prevent unbounded waits.
- **retry.multiplier** applies to `exponential` and defaults to 2 when omitted.

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

1. **API** validates the input, picks a run ID, and publishes a `workflow.started` event to the `WORKFLOW_HISTORY` JetStream stream at subject `history.<run-id>`. The event carries the workflow definition the run will use.

2. **Engine** consumes the event through its durable `orchestrator` consumer and creates the run's snapshot in the `workflow_runs` KV bucket. It then calls `Advance(def, run, event)` -- a pure function that takes the workflow definition, the run state, and the event, and returns the updated run plus a list of **side effects** (for example `EnqueueTask`, `CompleteWorkflow`, `FailWorkflow`, `SkipStep`).

3. For each `EnqueueTask`, the engine publishes a task message to the `TASK_QUEUES` stream at subject `task.<task-type>.<run-id>`.

4. **Worker** pulls the task via a JetStream pull consumer. It executes the registered handler and publishes a `step.completed` or `step.failed` event back to `WORKFLOW_HISTORY`.

5. The engine consumes the completion event, loads the run snapshot, calls `Advance` again, and dispatches the next ready steps. This loop continues until all steps complete or a terminal failure occurs.

6. When the final step completes, `Advance` returns a `CompleteWorkflow` effect. The engine saves the terminal snapshot and writes `workflow.completed` to the history stream.

The engine keeps no per-run state in memory between events: each event loads the run's snapshot from the `workflow_runs` KV bucket and saves it back with a revision check, so two engines in a cluster cannot overwrite each other. The history stream records what happened and drives the engine; the snapshot is the current state. If the engine crashes, JetStream redelivers every event it had not acknowledged, and the engine ignores events it has already applied.

## Next Steps

- [Core Concepts](/docs/concepts) -- deep dive into workflows, events, and the DAG model
- [Step Types](/docs/step-types) -- agent loops, sub-workflows, map steps, and more
- [Reliability](/docs/reliability) -- retry policies, dead letter queues, compensation
- [Worker SDK](/docs/workers) -- full TaskContext API, checkpointing, signals
