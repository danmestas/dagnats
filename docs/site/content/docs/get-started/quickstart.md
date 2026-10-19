---
title: Quickstart
weight: 2
---

Get DagNats running and execute your first workflow in under five minutes.

## Prerequisites

- Go 1.27.1 or later (the version in `go.mod`)
- No other dependencies (NATS is embedded)

## 1. Install DagNats

```bash
go install github.com/danmestas/dagnats/cmd/dagnats@latest
```

Verify the installation:

```bash
dagnats --version
```

## 2. Start the Server

```bash
dagnats serve
```

This starts an embedded NATS server, the workflow engine, the API service, and an HTTP gateway -- all in a single process. Data is stored in a platform-default directory (`~/.local/share/dagnats` on Linux, `~/Library/Application Support/dagnats` on macOS).

Leave this terminal running.

## 3. Define a Workflow

Create a file called `workflow.json`:

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

This defines a two-step DAG: `greet` runs first, then `uppercase` runs after `greet` completes. The `task` field maps each step to a handler registered in your worker.

## 4. Register the Workflow

In a new terminal:

```bash
dagnats workflow register workflow.json
```

The server validates the JSON, checks for cycles in the dependency graph, and stores the definition in a NATS KV bucket.

## 5. Write a Worker

Create `main.go`:

```go
package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer nc.Close()

	w := worker.NewWorker(nc)

	worker.HandleTyped(w, "greet",
		func(ctx worker.TaskContext, name string) (string, error) {
			if name == "" {
				name = "World"
			}
			return fmt.Sprintf("Hello, %s!", name), nil
		},
	)

	worker.HandleTyped(w, "uppercase",
		func(ctx worker.TaskContext, input string) (string, error) {
			return strings.ToUpper(input), nil
		},
	)

	w.Start()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	w.Stop()
}
```

`worker.HandleTyped` registers a typed handler that automatically marshals and unmarshals JSON. The `greet` handler receives a string input and returns a greeting. The `uppercase` handler receives the output of `greet` and returns it uppercased.

## 6. Run the Worker

In a new terminal:

```bash
go run main.go
```

The worker connects to NATS, creates a pull consumer for each of the `greet` and `uppercase` task types, and waits for work. It connects to `nats://127.0.0.1:4222`, the port `dagnats serve` uses by default.

## 7. Start a Workflow Run

In a fourth terminal:

```bash
dagnats run start hello-world '"World"' --watch
```

The `--watch` flag streams events as they happen. You should see output like:

```
Started: 855c8b1dcca2a2d3ec13824d7e6f5953

  13:09:34  workflow.started -
  13:09:34  step.queued greet
  13:09:34  step.started greet
  13:09:34  step.completed greet
  13:09:34  step.queued uppercase
  13:09:34  step.started uppercase
  13:09:34  step.completed uppercase
  13:09:34  workflow.completed -

Run completed: 855c8b1dcca2a2d3ec13824d7e6f5953
```

Add `--output` to print the terminal step's output when the run completes.

## 8. Check the Result

```bash
dagnats run status --last
dagnats run output --last
```

`run status` shows the state of each step and its attempt count. `run output` prints the output of the terminal step, which should be `"HELLO, WORLD!"`. `run events --last` shows each step's output in the event data.

## What Just Happened

1. The CLI sent a `StartRun` request to the API over NATS
2. The API created the run and published a `workflow.started` event to the history stream
3. The engine consumed the event, resolved the DAG, and dispatched `greet` to the task queue
4. Your worker pulled the task, executed the handler, and published the result
5. The engine received `step.completed`, resolved the next ready step, and dispatched `uppercase`
6. Your worker executed `uppercase` and published the result
7. The engine saw all steps complete and wrote `workflow.completed`

The CLI, engine and worker talk only through NATS JetStream -- the CLI makes no HTTP calls and there is no separate database or polling loop.

## Next Steps

- [Your First Workflow](/docs/get-started/your-first-workflow) -- deeper tutorial with typed handlers, retries, and timeouts
- [Core Concepts](/docs/concepts) -- understand the event sourcing model and DAG resolution
- [Worker SDK](/docs/workers) -- full worker API reference

{{< callout type="info" >}}
Building LLM agent pipelines? Check out [AI & LLM Patterns](/docs/ai-patterns/) for agent loops, checkpoints, and multi-agent orchestration.
{{< /callout >}}

{{< callout type="info" >}}
Want your workflow to *be* an HTTP API instead of a background job? See [Durable Endpoints](/docs/triggers/http) -- a synchronous HTTP trigger paired with a [`respond` step](/docs/step-types/respond-steps), with retries, timeouts, and event-sourced state included.
{{< /callout >}}
