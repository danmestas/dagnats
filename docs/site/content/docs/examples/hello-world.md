---
title: Hello World
weight: 1
---

A minimal two-step DAG that greets a user by name and uppercases the result, demonstrating typed handlers and JSON I/O.

## Workflow Definition

The workflow declares two steps in sequence: `greet` produces a greeting string, and `uppercase` transforms it. The `depends_on` field creates the edge in the DAG.

```json
{
  "name": "hello-world",
  "version": "1.0",
  "steps": [
    {
      // First step: no dependencies, runs immediately.
      "id": "greet",
      "task": "greet",        // matches the handler name registered in the worker
      "depends_on": []
    },
    {
      // Second step: waits for "greet" to complete.
      // Its input is the JSON output of the greet step.
      "id": "uppercase",
      "task": "uppercase",
      "depends_on": ["greet"]
    }
  ]
}
```

## Worker Implementation

The worker connects to NATS and registers two typed handlers. `HandleTyped` automatically deserializes the task input and serializes the return value as JSON, so handlers work with native Go types instead of raw bytes.

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
	// Connect to NATS. Falls back to localhost:4222.
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, err := nats.Connect(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer nc.Close()

	w := worker.NewWorker(nc, nil)

	// Step 1: produce a greeting from the input name.
	// HandleTyped deserializes the JSON input into a string
	// and serializes the returned string back to JSON.
	worker.HandleTyped(w, "greet",
		func(
			ctx worker.TaskContext, name string,
		) (string, error) {
			if name == "" {
				name = "World"
			}
			greeting := fmt.Sprintf("Hello, %s!", name)
			fmt.Printf("[greet] %s\n", greeting)
			return greeting, nil
		},
	)

	// Step 2: uppercase the greeting from step 1.
	// The input string is the JSON output of the greet handler.
	worker.HandleTyped(w, "uppercase",
		func(
			ctx worker.TaskContext, input string,
		) (string, error) {
			result := strings.ToUpper(input)
			fmt.Printf("[uppercase] %s\n", result)
			return result, nil
		},
	)

	fmt.Println("Worker ready. Waiting for tasks...")
	w.Start()

	// Block until Ctrl-C, then gracefully shut down.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	fmt.Println("\nShutting down...")
	w.Stop()
}
```

## Running the Example

1. Start the DagNats server:
   ```bash
   dagnats serve
   ```

2. In a second terminal, start the worker:
   ```bash
   go run ./examples/hello-world/
   ```

3. In a third terminal, register and run the workflow:
   ```bash
   dagnats workflow register examples/hello-world/workflow.json
   dagnats run start hello-world '"Alice"'
   ```

4. The worker output shows the execution:
   ```
   [greet] Hello, Alice!
   [uppercase] HELLO, ALICE!
   ```

## What's Happening

1. The engine parses the DAG and finds `greet` has no dependencies, so it dispatches it immediately.
2. The `greet` handler receives `"Alice"` as its typed input, produces `"Hello, Alice!"`, and returns it as the step output.
3. The engine sees `uppercase` depends on `greet`. Now that `greet` is complete, it dispatches `uppercase` with the greet output as input.
4. The `uppercase` handler transforms the string and completes. With all steps done, the workflow run finishes.

Key concepts demonstrated:
- **`HandleTyped`** removes all JSON boilerplate. You define Go function signatures and DagNats handles serialization.
- **`depends_on`** creates edges in the DAG. Step outputs flow automatically to downstream step inputs.
- **`worker.NewWorker`** + `w.Start()` subscribes to NATS subjects for each registered task. The worker pulls tasks from a durable JetStream consumer.

## Related

- [Workflow Definitions](/docs/getting-started/workflow-definitions) -- how to write workflow JSON
- [Writing Workers](/docs/getting-started/writing-workers) -- worker patterns and `HandleTyped`
