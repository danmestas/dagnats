---
title: Agent Loop
weight: 2
---

A counter that increments through checkpointed iterations, demonstrating the agent loop pattern with `Continue()` and `Complete()` semantics.

## Workflow Definition

The workflow has a single step with `type: "agent_loop"`. The `loop` configuration sets the maximum number of iterations and the delay between each iteration (in nanoseconds -- 1 second here).

```json
{
  "name": "agent-loop",
  "version": "1.0",
  "steps": [
    {
      "id": "counter",
      "task": "counter",
      "type": "agent_loop",    // enables iterative execution
      "depends_on": [],
      "loop": {
        "max_iterations": 10,  // safety bound: never run more than 10 times
        "loop_delay": 1000000000  // 1 second between iterations (nanoseconds)
      }
    }
  ]
}
```

## Worker Implementation

The handler loads its checkpoint (previous state) on each iteration, increments the counter, saves the checkpoint, and decides whether to continue or complete. This pattern is the foundation for LLM agent loops that iterate until a goal is met.

```go
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"

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
	w.Handle("counter", handleCounter)

	fmt.Println("Worker ready. Waiting for tasks...")
	w.Start()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	fmt.Println("\nShutting down...")
	w.Stop()
}

// counterState is the checkpoint payload for the counter loop.
type counterState struct {
	Count int `json:"count"`
}

const counterTarget = 5

// handleCounter loads the checkpoint, increments the counter,
// saves the checkpoint, and either continues or completes.
func handleCounter(ctx worker.TaskContext) error {
	// Cast to Checkpointable to access checkpoint methods.
	// Agent loop steps always implement this interface.
	cp := ctx.(worker.Checkpointable)
	state := loadCounter(cp)
	state.Count++

	fmt.Printf(
		"[counter] iteration %d / %d\n",
		state.Count, counterTarget,
	)

	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	// Persist state to NATS KV so it survives restarts.
	if err := cp.Checkpoint(data); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	// Decision point: are we done?
	if state.Count >= counterTarget {
		fmt.Println("[counter] target reached, completing")
		return ctx.Complete(data)  // finish the step
	}

	return ctx.Continue(data)  // request another iteration
}

// loadCounter reads the checkpoint from KV. Returns a zero-value
// counterState if no checkpoint exists yet.
func loadCounter(cp worker.Checkpointable) counterState {
	raw, err := cp.LoadCheckpoint()
	if err != nil || raw == nil {
		return counterState{}
	}

	var state counterState
	if err := json.Unmarshal(raw, &state); err != nil {
		return counterState{}
	}
	return state
}
```

## Running the Example

1. Start the DagNats server:
   ```bash
   dagnats serve
   ```

2. In a second terminal, start the worker:
   ```bash
   go run ./examples/agent-loop/
   ```

3. In a third terminal, register and run:
   ```bash
   dagnats workflow register examples/agent-loop/workflow.json
   dagnats run start agent-loop '{}'
   ```

4. Watch the worker iterate:
   ```
   [counter] iteration 1 / 5
   [counter] iteration 2 / 5
   [counter] iteration 3 / 5
   [counter] iteration 4 / 5
   [counter] iteration 5 / 5
   [counter] target reached, completing
   ```

## What's Happening

1. The engine dispatches the `counter` task for the first time. No checkpoint exists, so the counter starts at 0.
2. The handler increments to 1, saves a checkpoint `{"count":1}` to NATS KV, and calls `ctx.Continue(data)`.
3. `Continue()` tells the engine to re-dispatch the same step after the configured `loop_delay` (1 second).
4. On each subsequent iteration, `LoadCheckpoint()` restores the previous state. The handler increments, saves, and continues.
5. When the counter reaches 5, the handler calls `ctx.Complete(data)` instead, which marks the step as finished.
6. The `max_iterations` bound (10) acts as a safety net -- if the handler never calls `Complete()`, the engine stops it after 10 iterations.

Key concepts demonstrated:
- **`Continue()` vs `Complete()`** -- the handler controls the loop by choosing which to call.
- **Checkpoints** persist state across iterations in NATS KV. If the worker crashes mid-loop, it resumes from the last checkpoint.
- **`max_iterations`** provides a hard upper bound, preventing runaway loops.
- **`loop_delay`** adds backoff between iterations, useful for rate-limiting API calls in LLM agent patterns.

## Related

- [Agent Loops](/docs/step-types/agent-loops) -- step type reference
- [Agent Loop Pattern](/docs/ai-patterns/agent-loop-pattern) -- design pattern for LLM agents
