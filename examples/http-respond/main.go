// examples/http-respond/main.go
// Minimal DagNats worker for the http-respond example. The `echo`
// task uses worker.UnwrapTrigger() so the typed handler receives the
// HTTP request fields directly — the trigger envelope's outer
// metadata (trigger kind, source, workflow_id, timestamp) is hidden
// from the worker, since this example does not need it.
//
// Workers that DO need the metadata fields should drop the option
// and unmarshal the full envelope themselves; see issue #229 for the
// path to first-class metadata access on the TaskContext.
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

// httpRequestData mirrors internal/httpenvelope.Envelope — the
// fields the HTTP trigger lifts from the inbound request. Body is
// the raw bytes; JSON marshalling renders []byte as base64, so
// unmarshalling back into []byte yields the original bytes.
type httpRequestData struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    []byte            `json:"body,omitempty"`
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
			ctx worker.TaskContext, in httpRequestData,
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
