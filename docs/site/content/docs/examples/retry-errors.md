---
title: Retry and Error Handling
weight: 5
---

A workflow with a flaky fetch step that demonstrates retry policies with exponential backoff and `on_failure` routing to an error reporter.

## Workflow Definition

The `fetch` step configures an exponential retry policy and an `on_failure` fallback. If all retries exhaust, the engine routes to the `report-error` step instead of failing the entire workflow.

```json
{
  "name": "retry-errors",
  "version": "1.0",
  "steps": [
    {
      "id": "fetch",
      "task": "fetch",
      "type": "normal",
      "depends_on": [],
      "retry": {
        "max_attempts": 3,
        "strategy": "exponential",
        "initial_delay": 1000000000,
        "max_delay": 10000000000,
        "multiplier": 2.0
      },
      "on_failure": "report-error"
    },
    {
      "id": "process",
      "task": "process",
      "type": "normal",
      "depends_on": ["fetch"]
    },
    {
      "id": "report-error",
      "task": "report-error",
      "type": "normal",
      "depends_on": []
    }
  ]
}
```

## Worker Implementation

The `fetch` handler uses `ctx.RetryCount()` to know which attempt it is on. It deliberately fails the first two attempts to demonstrate the retry mechanism.

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

	w := worker.NewWorker(nc)

	w.Handle("fetch", handleFetch)
	w.Handle("process", handleProcess)
	w.Handle("report-error", handleReportError)

	fmt.Println("Worker ready. Waiting for tasks...")
	w.Start()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	fmt.Println("\nShutting down...")
	w.Stop()
}

// handleFetch simulates a flaky HTTP fetch that fails twice
// before succeeding. RetryCount is 0 on the first dispatch
// and 2, 3, ... on engine retries, so 0 and 2 fail and 3 succeeds.
func handleFetch(ctx worker.TaskContext) error {
	attempt := ctx.RetryCount()
	fmt.Printf("[fetch] attempt %d\n", attempt)

	// Simulate transient failures on the first two attempts.
	if attempt < 3 {
		return fmt.Errorf(
			"fetch failed (attempt %d)", attempt,
		)
	}

	// Third attempt succeeds.
	data := []byte(`{"status":"ok","items":["a","b","c"]}`)
	fmt.Printf("[fetch] success: %s\n", data)
	return ctx.Complete(data)
}

// handleProcess uppercases the fetched data.
func handleProcess(ctx worker.TaskContext) error {
	input := string(ctx.Input())
	result := strings.ToUpper(input)
	fmt.Printf("[process] %s\n", result)
	return ctx.Complete([]byte(result))
}

// handleReportError logs the error from a failed step.
// Only called if fetch exhausts all retries.
func handleReportError(ctx worker.TaskContext) error {
	input := string(ctx.Input())
	fmt.Printf("[report-error] failure reported: %s\n", input)
	return ctx.Complete([]byte("error reported"))
}
```

## Running the Example

1. Start the DagNats server:
   ```bash
   dagnats serve
   ```

2. In a second terminal, start the worker:
   ```bash
   go run ./examples/retry-errors/
   ```

3. In a third terminal, register and run:
   ```bash
   dagnats workflow register examples/retry-errors/workflow.json
   dagnats run start retry-errors '{}'
   ```

4. Watch the retries and eventual success:
   ```
   [fetch] attempt 0
   [fetch] attempt 2
   [fetch] attempt 3
   [fetch] success: {"status":"ok","items":["a","b","c"]}
   [process] {"STATUS":"OK","ITEMS":["A","B","C"]}
   ```

## What's Happening

1. The engine dispatches `fetch`. The handler sees `RetryCount()` 0 and returns an error.
2. The engine applies the retry policy: waits 1 second (initial delay), then redispatches. `RetryCount()` is now 2, and the handler fails again.
3. The engine waits 2 seconds (1s * 2.0 multiplier), then redispatches. `RetryCount()` is 3 and the handler succeeds.
4. With `fetch` complete, the engine dispatches `process`, which transforms the data.

If you change `counterTarget` so that all 3 attempts fail, the engine would route to `report-error` instead of `process`, because of the `on_failure` configuration.

The engine schedules each retry as a durable timer on the `SLEEP_TIMERS` stream (a `NakWithDelay` under the hood) and dispatches a fresh task when it fires -- there is no separate timer service.

Key concepts demonstrated:
- **`RetryCount()`** -- lets the handler know which attempt it is on (0 on the first dispatch, then 2, 3, ... on engine retries; a redelivery of the same attempt keeps its number).
- **Exponential backoff** -- delays grow as `initial_delay * multiplier^attempt`, capped at `max_delay`.
- **`on_failure` routing** -- graceful degradation instead of workflow failure.
- **NATS-native retries** -- retry delays are durable JetStream timers, no external timer needed.

## Related

- [Retry Policies](/docs/reliability/retry-policies) -- full retry configuration reference
- [Error Handling](/docs/reliability/error-handling) -- error routing and failure strategies
