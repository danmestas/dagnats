// examples/hello-world/main.go
// Minimal DagNats worker demonstrating a two-step workflow.
// Run alongside `dagnats serve` to see the full lifecycle.
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

	// Step 1: produce a greeting from the input name.
	// HandleTyped handles JSON marshal/unmarshal automatically.
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

	// Step 2: uppercase the greeting from step 1
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

	// Block until interrupted.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	fmt.Println("\nShutting down...")
	w.Stop()
}
