// examples/http-respond/main.go
// Minimal DagNats worker for the http-respond example. The `echo`
// task uses worker.UnwrapTrigger() so the typed handler receives the
// HTTP request fields directly via worker.HTTPEnvelope — see #229
// for the path to first-class trigger metadata (kind, source,
// workflow_id, timestamp) on the TaskContext, which this example
// does not need.
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
			ctx worker.TaskContext, in worker.HTTPEnvelope,
		) (echoOutput, error) {
			fmt.Printf("[echo] %s %s\n", in.Method, in.Path)
			var body json.RawMessage
			if len(in.Body) > 0 {
				body = json.RawMessage(in.Body)
			}
			return echoOutput{
				You:    "the dagnats http trigger",
				Method: in.Method,
				Path:   in.Path,
				Body:   body,
			}, nil
		},
		worker.UnwrapTrigger(),
	)

	fmt.Println("Worker ready. Waiting for /api/echo requests...")
	w.Start()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	fmt.Println("\nShutting down...")
	w.Stop()
}
