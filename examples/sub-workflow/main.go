// examples/sub-workflow/main.go
// Demonstrates sub-workflow steps. The parent workflow validates an
// order, then spawns a "payment-flow" child workflow to process
// payment. When the child completes, the parent sends confirmation.
//
// Run alongside `dagnats serve`:
//
//	Terminal 1: dagnats serve
//	Terminal 2: go run ./examples/sub-workflow/
//	Terminal 3: dagnats workflow register examples/sub-workflow/workflow.json
//	            dagnats workflow register examples/sub-workflow/payment-flow.json
//	            dagnats run start order-pipeline '{"item":"widget","amount":42}'
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"

	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

type order struct {
	Item   string `json:"item"`
	Amount int    `json:"amount"`
}

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

	w := worker.NewWorker(nc)

	// Parent workflow step 1: validate the order
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

	// Child workflow step: charge payment
	// This runs inside the "payment-flow" sub-workflow.
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

	// Parent workflow step 3: send confirmation
	// Input is the child workflow's output (paymentResult).
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
