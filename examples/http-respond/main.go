// examples/http-respond/main.go
// Minimal DagNats worker for the http-respond example. The `echo` task
// receives the HTTP request envelope from the trigger and returns a
// shaped JSON response that the `respond` step then ships back as the
// HTTP body.
//
// Pair with `dagnats serve` and curl per the README.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"

	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

// echoInput mirrors the HTTP request envelope the trigger passes in.
// Fields are documented at internal/httpenvelope/envelope.go.
type echoInput struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers"`
	Body    json.RawMessage   `json:"body"`
}

// echoOutput is the JSON shape the respond step ships back to the
// HTTP client. Keep simple — workflow authors choose the schema.
type echoOutput struct {
	You    string          `json:"you_sent"`
	Method string          `json:"method"`
	Path   string          `json:"path"`
	Body   json.RawMessage `json:"body,omitempty"`
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

	worker.HandleTyped(w, "echo",
		func(
			ctx worker.TaskContext, in echoInput,
		) (echoOutput, error) {
			fmt.Printf("[echo] %s %s\n", in.Method, in.Path)
			return echoOutput{
				You:    "the dagnats http trigger",
				Method: in.Method,
				Path:   in.Path,
				Body:   in.Body,
			}, nil
		},
	)

	fmt.Println("Worker ready. Waiting for /api/echo requests...")
	w.Start()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	fmt.Println("\nShutting down...")
	w.Stop()
}
