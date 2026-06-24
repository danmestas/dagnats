// examples/planner/main.go
// Demonstrates the agent-runtime control plane (ADR-021 Phase A, #376).
// The "planner" workflow has a single gated "plan" step: it declares the
// "control-plane" capability, so when the worker is built WITH a granted
// control plane, the step's handler receives a ControlPlane handle. The
// handler authors an ephemeral child workflow def AT RUNTIME and launches
// a child run of it; the child's "child-work" task then completes the run.
//
// The grant is explicit and deny-by-default: drop WithControlPlane below
// and ctx.ControlPlane() returns nil, so the handler must always nil-check.
//
// Run alongside `dagnats serve`:
//
//	Terminal 1: dagnats serve
//	Terminal 2: go run ./examples/planner/
//	Terminal 3: dagnats workflow register examples/planner/planner.json
//	            dagnats run start planner '{}'
//
// The child def is NOT pre-registered — the planner authors it at runtime
// under a server-scoped name (agent.<run>.do-step), so there is no child
// JSON file to register.
package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/danmestas/dagnats/dag"
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

	// The grant: WithControlPlane wires a runtime control plane. Without
	// it, gated steps still run but ctx.ControlPlane() returns nil.
	w := worker.NewWorker(nc,
		worker.WithControlPlane(worker.NewControlPlane(nc)),
	)

	// Gated step: authors a child def at runtime and launches it.
	w.Handle("plan-task", planHandler)

	// Child task: the runtime-authored workflow's only step.
	w.Handle("child-work", func(ctx worker.TaskContext) error {
		fmt.Println("[child-work] doing the planned work")
		return ctx.Complete([]byte(`{"done":true}`))
	})

	fmt.Println("Planner worker ready. Waiting for tasks...")
	w.Start()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	fmt.Println("\nShutting down...")
	w.Stop()
}

// planHandler is the gated handler. It registers an ephemeral workflow
// and starts a child run of it, surfacing any typed control-plane error
// as a step failure rather than crashing.
func planHandler(ctx worker.TaskContext) error {
	cp := ctx.ControlPlane()
	if cp == nil {
		// Deny-by-default: this only happens if the deployment did not
		// grant a control plane (no WithControlPlane).
		return ctx.Fail(fmt.Errorf("control plane not granted"))
	}

	childDef := dag.WorkflowDef{
		Name:    "do-step",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "work", Task: "child-work", Type: dag.StepTypeNormal},
		},
	}
	scopedName, err := cp.RegisterWorkflow(
		ctx.Context(), childDef, worker.RegisterOpts{},
	)
	if err != nil {
		return ctx.Fail(fmt.Errorf("register child: %w", err))
	}
	fmt.Printf("[plan] registered ephemeral workflow %q\n", scopedName)

	runID, err := cp.StartRun(ctx.Context(), scopedName, nil)
	if err != nil {
		return ctx.Fail(fmt.Errorf("start child run: %w", err))
	}
	fmt.Printf("[plan] launched child run %s\n", runID)

	return ctx.Complete([]byte(`{"planned":true}`))
}
