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
        "max_attempts": 3,             // try up to 3 times total
        "strategy": "exponential",     // exponential backoff
        "initial_delay": 1000000000,   // 1 second initial delay (nanoseconds)
        "max_delay": 10000000000,      // 10 second cap on delay
        "multiplier": 2.0              // double the delay each retry
      },
      "on_failure": "report-error"     // route here if all retries fail
    },
    {
      // Happy path: runs after a successful fetch.
      "id": "process",
      "task": "process",
      "type": "normal",
      "depends_on": ["fetch"]
    },
    {
      // Error path: runs only if fetch exhausts all retries.
      // Receives the error details as input.
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

	w := worker.NewWorker(nc, nil)

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
// before succeeding. RetryCount is 0-based: 0 and 1 fail,
// 2 succeeds.
func handleFetch(ctx worker.TaskContext) error {
	attempt := ctx.RetryCount()
	fmt.Printf("[fetch] attempt %d\n", attempt)

	// Simulate transient failures on first two attempts.
	if attempt < 2 {
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
   [fetch] attempt 1
   [fetch] attempt 2
   [fetch] success: {"status":"ok","items":["a","b","c"]}
   [process] {"STATUS":"OK","ITEMS":["A","B","C"]}
   ```

## What's Happening

1. The engine dispatches `fetch`. The handler returns an error on attempt 0.
2. The engine applies the retry policy: waits 1 second (initial delay), then redispatches. Attempt 1 also fails.
3. The engine waits 2 seconds (1s * 2.0 multiplier), then redispatches. Attempt 2 succeeds.
4. With `fetch` complete, the engine dispatches `process`, which transforms the data.

If you change `counterTarget` so that all 3 attempts fail, the engine would route to `report-error` instead of `process`, because of the `on_failure` configuration.

The retry mechanism uses NATS `NakWithDelay` under the hood -- there is no separate timer service. The message stays in the JetStream consumer and is redelivered after the computed delay.

Key concepts demonstrated:
- **`RetryCount()`** -- lets the handler know which attempt it is on (0-based).
- **Exponential backoff** -- delays grow as `initial_delay * multiplier^attempt`, capped at `max_delay`.
- **`on_failure` routing** -- graceful degradation instead of workflow failure.
- **NATS-native retries** -- uses `NakWithDelay` for retry scheduling, no external timer needed.

## Related

- [Retry Policies](/docs/reliability/retry-policies) -- full retry configuration reference
- [Error Handling](/docs/reliability/error-handling) -- error routing and failure strategies
