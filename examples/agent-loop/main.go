// examples/agent-loop/main.go
// Demonstrates the iterative agent loop pattern. A counter step
// loads its checkpoint, increments, saves, and continues until
// the counter reaches 5, then completes.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"

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

	w.Handle("counter", handleCounter)

	fmt.Println("Worker ready. Waiting for tasks...")
	w.Start()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	fmt.Println("\nShutting down...")
	w.Stop()
}

// counterState is the checkpoint payload for the counter loop.
type counterState struct {
	Count int `json:"count"`
}

const counterTarget = 5

// handleCounter loads the checkpoint, increments the counter,
// saves the checkpoint, and either continues or completes.
func handleCounter(ctx worker.TaskContext) error {
	cp := ctx.(worker.Checkpointable)
	state := loadCounter(cp)
	state.Count++

	fmt.Printf(
		"[counter] iteration %d / %d\n",
		state.Count, counterTarget,
	)

	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	if err := cp.Checkpoint(data); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	if state.Count >= counterTarget {
		fmt.Println("[counter] target reached, completing")
		return ctx.Complete(data)
	}

	return ctx.Continue(data)
}

// loadCounter reads the checkpoint from KV. Returns a zero-value
// counterState if no checkpoint exists yet.
func loadCounter(cp worker.Checkpointable) counterState {
	raw, err := cp.LoadCheckpoint()
	if err != nil || raw == nil {
		return counterState{}
	}

	var state counterState
	if err := json.Unmarshal(raw, &state); err != nil {
		return counterState{}
	}
	return state
}
