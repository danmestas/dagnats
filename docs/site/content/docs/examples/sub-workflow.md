---
title: Sub-Workflow
weight: 7
---

A parent order pipeline that delegates payment processing to a child workflow, demonstrating sub-workflow steps with input/output mapping.

## Workflow Definitions

This example uses two workflow files. The parent workflow validates an order, spawns a child workflow for payment, and sends confirmation when the child completes.

### Parent: order-pipeline

```json
{
  "name": "order-pipeline",
  "version": "1.0",
  "steps": [
    {
      // Step 1: validate the incoming order.
      "id": "validate",
      "task": "validate-order",
      "type": "normal"
    },
    {
      // Step 2: spawn the "payment-flow" child workflow.
      // type: "sub_workflow" tells the engine to start a new
      // workflow run and wait for it to complete.
      "id": "process-payment",
      "type": "sub_workflow",
      "config": {
        "workflow": "payment-flow"   // name of the child workflow
      },
      "depends_on": ["validate"]
    },
    {
      // Step 3: runs after the child workflow completes.
      // Input is the child workflow's final output.
      "id": "confirm",
      "task": "send-confirmation",
      "type": "normal",
      "depends_on": ["process-payment"]
    }
  ]
}
```

### Child: payment-flow

```json
{
  "name": "payment-flow",
  "version": "1.0",
  "steps": [
    {
      "id": "charge",
      "task": "charge",
      "type": "normal"
    }
  ]
}
```

The child workflow is a standalone workflow -- it can be run independently or as a sub-workflow. When used as a sub-workflow, it receives the parent step's input and its output flows back to the parent.

## Worker Implementation

A single worker handles tasks for both the parent and child workflows. The `charge` handler runs inside the child workflow context but is registered the same way as any other handler.

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

// order represents the input payload.
type order struct {
	Item   string `json:"item"`
	Amount int    `json:"amount"`
}

// paymentResult is the child workflow's output.
type paymentResult struct {
	TransactionID string `json:"transaction_id"`
	Status        string `json:"status"`
}

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

	// Parent workflow step 1: validate the order.
	worker.HandleTyped(w, "validate-order",
		func(
			ctx worker.TaskContext, o order,
		) (order, error) {
			fmt.Printf("[validate] order: %s ($%d)\n",
				o.Item, o.Amount)
			if o.Amount <= 0 {
				return order{}, fmt.Errorf(
					"invalid amount: %d", o.Amount,
				)
			}
			return o, nil
		},
	)

	// Child workflow step: charge payment.
	// This runs inside the "payment-flow" sub-workflow.
	// The handler has no knowledge of the parent workflow.
	worker.HandleTyped(w, "charge",
		func(
			ctx worker.TaskContext, o order,
		) (paymentResult, error) {
			fmt.Printf("[charge] processing $%d for %s\n",
				o.Amount, o.Item)
			return paymentResult{
				TransactionID: "txn-001",
				Status:        "charged",
			}, nil
		},
	)

	// Parent workflow step 3: send confirmation.
	// Input is the child workflow's output (paymentResult JSON).
	worker.HandleTyped(w, "send-confirmation",
		func(
			ctx worker.TaskContext, result json.RawMessage,
		) (string, error) {
			fmt.Printf("[confirm] payment complete: %s\n",
				string(result))
			return "confirmation sent", nil
		},
	)

	fmt.Println("Sub-workflow worker ready. Waiting for tasks...")
	w.Start()

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
   go run ./examples/sub-workflow/
   ```

3. In a third terminal, register both workflows and start the parent:
   ```bash
   dagnats workflow register examples/sub-workflow/workflow.json
   dagnats workflow register examples/sub-workflow/payment-flow.json
   dagnats run start order-pipeline '{"item":"widget","amount":42}'
   ```

4. Watch the full execution:
   ```
   [validate] order: widget ($42)
   [charge] processing $42 for widget
   [confirm] payment complete: {"transaction_id":"txn-001","status":"charged"}
   ```

## What's Happening

1. The engine starts the `order-pipeline` workflow and dispatches `validate`. The handler checks that the amount is positive and passes the order through.
2. The engine sees `process-payment` is a `sub_workflow` step. It starts a new `payment-flow` workflow run, passing the validate output as the child's input.
3. Inside the child workflow, the `charge` step runs. It produces a `paymentResult` and completes.
4. The child workflow finishes. The engine passes the child's output back to the parent as the output of the `process-payment` step.
5. The engine dispatches `confirm` with the payment result. The handler logs the confirmation and the parent workflow completes.

Key concepts demonstrated:
- **`sub_workflow` step type** -- spawns a child workflow and waits for it to complete.
- **Input/output mapping** -- the parent's step output becomes the child's input; the child's final output becomes the parent step's output.
- **Composability** -- the child workflow (`payment-flow`) is a standalone workflow that can also run independently.
- **Single worker** -- one worker process can handle tasks from both parent and child workflows.

## Related

- [Sub-Workflows](/docs/step-types/sub-workflows) -- step type reference and configuration
