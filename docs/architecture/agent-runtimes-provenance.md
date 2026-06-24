# Agent-runtimes provenance: reuse, don't mint (ADR-021 Phase A)

Status: accepted (design note, not a numbered ADR)
Scope: `#379` — the provenance + agent-runtimes console view, the final
slice of ADR-021 Phase A.

## Decision

Phase A reused the lineage state that earlier slices already persist. It
minted **zero new event types**. No `workflow.generated`, no
`run.started{by:runtime}`. The acceptance criteria for "show the spawn
tree, its budget, and which nodes were runtime-spawned" are satisfied by
data that is already on the wire:

- **`EventWorkflowSpawn`** (`#376`) — published on `history.<parentRunID>`,
  carrying `ChildRunID` / `ChildWorkflow` / `ParentStepID`. The orchestrator
  consumes it to link the child run.
- **Run snapshots** (`#377`) — every `dag.WorkflowRun` carries `RootRunID`,
  `ParentRunID`, and `ParentStepID`. `engine.RootRunIDOf` is the SINGLE
  definition of a run's tree-root (a run with `RootRunID==""` self-roots).
  The tree is fully reconstructable from a snapshot scan.
- **`runtime.spawn` audit rows** (`#380`) — each runtime spawn writes an
  audit row whose `Target` is the child `RunID`. The console reads these to
  tag runtime-origin nodes. When the audit KV is empty/unwired the tag is
  simply absent (honest), never fabricated.
- **`worker.RuntimeBudget`** (`#378`) — `api.Service.Budget(ctx, root)`
  returns real, scan-backed active/max run + def counts. Ceilings come from
  the control plane's configured limits, not a guess.

## Why not a new event

A new event type would be a second, drift-prone source of truth for a fact
the snapshots already encode. The lineage tree and the run snapshots would
then have to be kept in sync, and any divergence would surface as a console
that lies. Reusing `RootRunIDOf` + the existing audit rows keeps one source
of truth and one definition of "root".

## Console shape (the only net-new work)

`internal/console/agents.go` adds the `/console/agents` page:

- `assembleTree(runs, root)` — ONE deep helper, called by both the list
  path (`ListAgentRuntimes`) and the single-root SSE path (`AgentRuntime`).
  It groups by `engine.RootRunIDOf`, filters to actual runtimes (a lone
  top-level run is NOT a runtime and is omitted), and walks the tree with a
  bounded, cycle-defended **iterative BFS** (no recursion). Depth is capped
  at `engine.MaxNestingDepth` — the real enforced spawn ceiling, not a
  console-invented constant — and node count at `maxNodesPerTree`.
- The SSE pump reuses `WatchRuns` (no new watcher). Each tree-member
  `RunUpdate` re-projects ONLY that root's tree via `AgentRuntime` and
  patches `#agent-tree-<root>` inner. A lone non-tree run produces no patch.

## Honesty rules

- Budget unbacked (`BudgetOK==false`) → the budget block is OMITTED
  entirely. Never a fabricated `0/0` or a dash for the whole block.
- The runtime-origin tag renders only when a `runtime.spawn` audit row
  backs the node.
- No fabricated columns (no token/cost/duration unless a real run field).
- Empty page → an honest empty-state card.
