// examples/http-respond/main.go
// Minimal DagNats worker for the http-respond example. The `echo`
// task receives the trigger envelope (which wraps the HTTP request
// envelope under .data) and returns a shaped JSON response that the
// `respond` step then ships back as the HTTP body.
//
// Trigger envelopes are wrapped: the worker's input JSON has the
// trigger metadata at the top level (`trigger`, `source`,
// `workflow_id`, `timestamp`) and the request envelope nested under
// `data`. Same convention as webhook and subject triggers. See
// docs/site/content/docs/triggers/http.md for the full shape.
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

// triggerEnvelope mirrors internal/trigger.TriggerEnvelope on the
// wire. Every trigger kind (cron, webhook, subject, http) lands
// here; the kind-specific payload lives under Data.
type triggerEnvelope struct {
	Trigger    string          `json:"trigger"`
	Source     string          `json:"source"`
	WorkflowID string          `json:"workflow_id"`
	Timestamp  string          `json:"timestamp"`
	Data       httpRequestData `json:"data"`
}

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
			ctx worker.TaskContext, in triggerEnvelope,
		) (echoOutput, error) {
			fmt.Printf("[echo] %s %s\n", in.Data.Method, in.Data.Path)
			var body json.RawMessage
			if len(in.Data.Body) > 0 {
				body = json.RawMessage(in.Data.Body)
			}
			return echoOutput{
				You:    "the dagnats http trigger",
				Method: in.Data.Method,
				Path:   in.Data.Path,
				Body:   body,
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
