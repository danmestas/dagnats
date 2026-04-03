// examples/retry-errors/main.go
// Demonstrates retry policies and error handling. The fetch handler
// fails the first two attempts, succeeds on the third. If all
// retries exhaust, the on_failure step logs the error.
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
// before succeeding. RetryCount is 0-based: 0 and 1 fail, 2
// succeeds.
func handleFetch(ctx worker.TaskContext) error {
	attempt := ctx.RetryCount()
	fmt.Printf("[fetch] attempt %d\n", attempt)

	if attempt < 2 {
		return fmt.Errorf(
			"fetch failed (attempt %d)", attempt,
		)
	}

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
func handleReportError(ctx worker.TaskContext) error {
	input := string(ctx.Input())
	fmt.Printf("[report-error] failure reported: %s\n", input)
	return ctx.Complete([]byte("error reported"))
}
