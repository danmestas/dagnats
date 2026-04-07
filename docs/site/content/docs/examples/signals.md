---
title: Signals
weight: 4
---

A three-step workflow where a preparation step runs in parallel with a signal-waiting step, demonstrating cross-step coordination with `WaitForSignal` and `SendSignal`.

## Workflow Definition

The `prepare` and `wait-for-approval` steps both have no dependencies, so they run in parallel. The `finalize` step depends on both and only runs after both complete.

```json
{
  "name": "signals",
  "version": "1.0",
  "steps": [
    {
      // Runs immediately: prepares resources.
      "id": "prepare",
      "task": "prepare",
      "type": "normal",
      "depends_on": []
    },
    {
      // Also runs immediately: blocks waiting for an external signal.
      "id": "wait-for-approval",
      "task": "wait-for-approval",
      "type": "normal",
      "depends_on": []
    },
    {
      // Runs only after BOTH prepare and wait-for-approval complete.
      "id": "finalize",
      "task": "finalize",
      "type": "normal",
      "depends_on": ["prepare", "wait-for-approval"]
    }
  ]
}
```

## Worker Implementation

The `wait-for-approval` handler calls `ctx.WaitForSignal` to block until an external process sends the named signal or the timeout expires.

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

	w.Handle("prepare", handlePrepare)
	w.Handle("wait-for-approval", handleWaitForApproval)
	w.Handle("finalize", handleFinalize)

	fmt.Println("Worker ready. Waiting for tasks...")
	w.Start()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	fmt.Println("\nShutting down...")
	w.Stop()
}

// handlePrepare simulates preparation work and completes.
func handlePrepare(ctx worker.TaskContext) error {
	fmt.Println("[prepare] preparing resources...")
	result := []byte(`{"prepared":true}`)
	fmt.Printf("[prepare] done: %s\n", result)
	return ctx.Complete(result)
}

// handleWaitForApproval blocks until an external signal arrives
// or times out after 5 minutes.
func handleWaitForApproval(ctx worker.TaskContext) error {
	fmt.Println("[wait-for-approval] waiting for signal...")

	// The signal name "approval" is application-defined.
	data, err := ctx.WaitForSignal(
		"approval", 5*time.Minute,
	)
	if err != nil {
		return fmt.Errorf("signal wait failed: %w", err)
	}

	fmt.Printf("[wait-for-approval] received: %s\n", data)
	return ctx.Complete(data)
}

// handleFinalize combines outputs from prepare and approval.
func handleFinalize(ctx worker.TaskContext) error {
	input := string(ctx.Input())
	result := fmt.Sprintf(
		"finalized with input: %s", input,
	)
	fmt.Printf("[finalize] %s\n", result)
	return ctx.Complete([]byte(result))
}
```

## Running the Example

1. Start the DagNats server:
   ```bash
   dagnats serve
   ```

2. In a second terminal, start the worker:
   ```bash
   go run ./examples/signals/
   ```

3. In a third terminal, register and start the workflow:
   ```bash
   dagnats workflow register examples/signals/workflow.json
   dagnats run start signals '{}'
   ```

4. The `prepare` step completes immediately. The `wait-for-approval` step blocks, waiting for a signal. Send it:
   ```bash
   dagnats signal send <run-id> approval '{"approved":true}'
   ```

5. Watch the workflow complete:
   ```
   [prepare] preparing resources...
   [prepare] done: {"prepared":true}
   [wait-for-approval] waiting for signal...
   [wait-for-approval] received: {"approved":true}
   [finalize] finalized with input: ...
   ```

## What's Happening

1. The engine dispatches `prepare` and `wait-for-approval` in parallel (neither has dependencies on the other).
2. `prepare` completes immediately with `{"prepared":true}`.
3. `wait-for-approval` calls `WaitForSignal("approval", 5*time.Minute)`, which watches a NATS KV key for the named signal. The handler blocks.
4. An external process (the CLI, another workflow, or an API call) sends the `approval` signal with a JSON payload.
5. `WaitForSignal` returns the signal data. The handler completes with that data as output.
6. Now both dependencies of `finalize` are satisfied. The engine dispatches it with the combined inputs.
7. `finalize` produces the final result and the workflow completes.

Key concepts demonstrated:
- **`WaitForSignal`** -- blocks a handler until a named signal arrives via NATS KV watch. Includes a timeout for safety.
- **Parallel execution** -- steps without mutual dependencies run concurrently.
- **External coordination** -- signals allow human approval, webhook callbacks, or cross-workflow communication.
- **Bounded waits** -- the 5-minute timeout ensures the handler never blocks forever.

## Related

- [Signals](/docs/coordination/signals) -- signal concepts and API reference
