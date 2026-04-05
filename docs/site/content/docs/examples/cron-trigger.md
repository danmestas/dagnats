---
title: Cron Trigger
weight: 6
---

A minimal single-step workflow designed to run on a cron schedule, demonstrating how to set up time-based workflow triggers.

## Workflow Definition

The workflow itself is simple -- a single `tick` step that logs the current time. The cron schedule is configured separately when registering the workflow, not in the workflow JSON.

```json
{
  "name": "cron-trigger",
  "version": "1.0",
  "steps": [
    {
      "id": "tick",
      "task": "tick",
      "type": "normal",
      "depends_on": []
    }
  ]
}
```

## Worker Implementation

The handler prints the current UTC timestamp and completes. Each cron trigger creates a new workflow run, so the handler runs fresh each time.

```go
package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func main() {
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
	w.Handle("tick", handleTick)

	fmt.Println("Worker ready. Waiting for tasks...")
	w.Start()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	fmt.Println("\nShutting down...")
	w.Stop()
}

// handleTick prints the current time and completes.
// Each cron invocation creates a fresh workflow run.
func handleTick(ctx worker.TaskContext) error {
	now := time.Now().UTC().Format(time.RFC3339)
	fmt.Printf("[tick] %s\n", now)
	return ctx.Complete([]byte(now))
}
```

## Running the Example

1. Start the DagNats server:
   ```bash
   dagnats serve
   ```

2. In a second terminal, start the worker:
   ```bash
   go run ./examples/cron-trigger/
   ```

3. In a third terminal, register the workflow and add a cron schedule:
   ```bash
   dagnats workflow register examples/cron-trigger/workflow.json
   dagnats cron add cron-trigger "*/1 * * * *" '{}'
   ```
   This schedules the workflow to run every minute with an empty JSON input.

4. Watch the worker print timestamps every minute:
   ```
   [tick] 2025-01-15T10:00:00Z
   [tick] 2025-01-15T10:01:00Z
   [tick] 2025-01-15T10:02:00Z
   ```

5. Remove the schedule when done:
   ```bash
   dagnats cron remove cron-trigger
   ```

## What's Happening

1. The `dagnats cron add` command registers a cron expression with the DagNats server.
2. The server's cron scheduler evaluates the expression and starts a new workflow run at each matching time.
3. Each run is independent -- the engine creates a fresh run, dispatches the `tick` step, and the handler executes.
4. The workflow definition stays simple. The scheduling concern is separate from the workflow logic.

This separation means any workflow can be cron-triggered without modification. You can add a cron schedule to the hello-world example, the retry-errors example, or any other workflow.

Key concepts demonstrated:
- **Cron schedules** are an operational concern, configured outside the workflow definition.
- **Each trigger creates a new run** -- there is no shared state between cron invocations.
- **Standard cron expressions** -- uses the familiar `* * * * *` format (minute, hour, day, month, weekday).

## Related

- [Cron Schedules](/docs/triggers/cron-schedules) -- cron configuration and expression reference
