# planner — agent-runtime control plane (ADR-021 Phase A, #376)

A worker whose gated `plan` step authors an ephemeral workflow **at
runtime** and launches a child run of it, using the worker-side
`ControlPlane` handle.

## What it shows

- **Gated capability.** The `plan` step declares
  `required_capabilities: ["control-plane"]`. The worker is built with
  `worker.WithControlPlane(worker.NewControlPlane(nc))`, so the step's
  handler receives a non-nil `ctx.ControlPlane()`. Drop the option and
  the handle is nil — deny-by-default.
- **Runtime authoring.** `RegisterWorkflow` validates the def and
  persists it under a **server-computed scoped name**
  (`agent.<owner-run>.do-step`). The worker never names the KV key.
- **Child run with lineage.** `StartRun` routes through the engine's
  existing spawn-event path, so the child run inherits `ParentRunID`,
  the nesting-depth cap, and parent-step linkage for free.
- **Typed errors, no panics.** Every boundary failure comes back as a
  `*worker.ControlPlaneError` the handler can branch on; the
  orchestrator is never crashed by agent-supplied data.

## Run it

```sh
# Terminal 1
dagnats serve

# Terminal 2
go run ./examples/planner/

# Terminal 3
dagnats workflow register examples/planner/planner.json
dagnats run start planner '{}'
```

The child workflow (`do-step`) is authored at runtime, so there is no
child JSON file to register — the planner creates it for you.
