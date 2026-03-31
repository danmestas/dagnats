# Sample App & Terminal Dashboard Design

A sample code-review pipeline and a live terminal dashboard for visualizing DagNats agent workflows.

## Problem

Users need a working example showing how to define workflows, register roles/tools, and run agent pipelines with dagnats-agents. They also need a way to see what's happening — which steps are running, what tools are being called, and how the DAG progresses.

## Components

### examples/code-review/

A self-contained TypeScript example that runs a 3-step agent workflow: analyze → implement → review.

**main.ts** — Entry point:
- Connects to NATS
- Registers roles (analyzer, coder, reviewer) with tool bundles
- Defines the workflow via the `workflow()` SDK
- Registers the workflow definition in KV
- Starts the agent worker
- Kicks off a run with CLI-provided input
- Waits for completion, prints result

**roles.ts** — Role and tool configuration:
- Analyzer role: read-only tools (file_read, grep), model: sonnet, low effort
- Coder role: full tools (file_read, file_write, shell_exec), model: opus, high effort
- Reviewer role: read-only tools, model: sonnet, high effort

Each role has a focused system prompt describing its job in the pipeline.

### cmd/dagnats-watch/

A Go binary that subscribes to NATS events and renders a live-updating terminal view.

**Display:**
- Top section: run metadata (ID, workflow name, status, elapsed time)
- Middle section: DAG steps with status indicators, timing, and role
- Active tool calls shown indented under the running step
- Bottom section: scrolling event log (last N events)

**Status indicators:**
- `[ ]` pending
- `[▶]` running (with elapsed time)
- `[✓]` completed (with duration)
- `[✗]` failed (with error snippet)
- `[⊘]` skipped

**NATS subscriptions:**
- `history.>` for workflow lifecycle events (step queued/started/completed/failed)
- NATS micro service stats for tool call visibility

**Interface:**
```
dagnats-watch                    # watch all runs
dagnats-watch --run <id>         # watch specific run
dagnats-watch --workflow <name>  # filter by workflow name
```

**Implementation approach:**
- Raw ANSI escape codes for rendering — no TUI library
- Single-file main.go (under 300 lines, bounded)
- Subscribe to NATS, maintain in-memory state per run, re-render on each event
- Graceful degradation: if terminal is too narrow, show compact view

## NATS Subjects Used

| Subject | Source | Dashboard Use |
|---|---|---|
| `history.{runID}` | Engine + Agent Worker | Step lifecycle, workflow status |
| `tool.exec.{name}` | Go tool services | Tool call activity (optional) |

The dashboard is read-only — it never publishes to NATS.

## Dependencies

- Sample app depends on: dagnats-agents TypeScript package, running NATS server, running Go tool services
- Dashboard depends on: NATS connection only (no other services needed)

## Not Included

- Web UI — terminal dashboard is sufficient for v1
- Persistent history — dashboard is live-only, stateless
- Multi-run comparison — one run at a time (or all runs interleaved)
