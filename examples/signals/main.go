// examples/signals/main.go
// Demonstrates cross-step signal coordination. The prepare step
// runs in parallel with wait-for-approval. The finalize step
// depends on both and combines their outputs.
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
