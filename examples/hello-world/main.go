// examples/hello-world/main.go
// Minimal DagNats worker demonstrating a two-step workflow.
// Run alongside `dagnats serve` to see the full lifecycle.
package main

import (
	"fmt"
	"os"
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

	// Step 1: produce a greeting from the input name
	w.Handle("greet", func(ctx worker.TaskContext) error {
		name := string(ctx.Input())
		if name == "" {
			name = "World"
		}
		greeting := fmt.Sprintf("Hello, %s!", name)
		fmt.Printf("[greet] %s\n", greeting)
		return ctx.Complete([]byte(greeting))
	})

	// Step 2: uppercase the greeting from step 1
	w.Handle("uppercase", func(ctx worker.TaskContext) error {
		input := string(ctx.Input())
		result := strings.ToUpper(input)
		fmt.Printf("[uppercase] %s\n", result)
		return ctx.Complete([]byte(result))
	})

	fmt.Println("Worker ready. Waiting for tasks...")
	w.Start()
}
