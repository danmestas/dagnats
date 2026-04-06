// examples/cron-trigger/main.go
// Demonstrates cron trigger lifecycle. A simple tick handler
// prints the current timestamp and completes.
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

	w := worker.NewWorker(nc)

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
func handleTick(ctx worker.TaskContext) error {
	now := time.Now().UTC().Format(time.RFC3339)
	fmt.Printf("[tick] %s\n", now)
	return ctx.Complete([]byte(now))
}
