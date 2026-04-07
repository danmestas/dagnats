# Developer Experience Tooling

## dagnatstest Package

`dagnatstest.Server(t)` starts an embedded NATS server with all streams and KV buckets provisioned. Returns a connected client, cleaned up on test end.

`dagnatstest.RunAndWait(t, svc, workflow, input, timeout)` starts a run and polls every 25ms until terminal status. `WaitForStatus(t, svc, runID, timeout, statuses...)` polls for specific statuses. Both use `t.Helper()` and `t.Context()` for clean failure reporting. No defaults on timeout — forces test authors to think about bounds.

## dagnats dev (Watch Mode)

`dagnats dev [--dir=.] [--delay=500ms]` watches Go files, rebuilds, and restarts the worker process on change. Polling-based watcher (no fsnotify dependency) with 200ms debounce. Verifies NATS connectivity on startup. On build failure, keeps the old process running. Output prefixed with `[dev]` in cyan; child process stdout/stderr passed through unmodified.

**Files:** `cli/dev.go` (entry), `cli/dev_watch.go` (watcher), `cli/dev_runner.go` (build/restart).

## dagnatstest.NewHarness(t)

`NewHarness(t)` returns a `Harness` struct with `NC`, `Engine`, `Svc`, `Worker` — all wired to an embedded NATS server with streams/KV provisioned. Eliminates ~15 lines of boilerplate per integration test. Worker is created but NOT started — register handlers first, then call `h.Start(t)`. `h.RegisterAndRun(t, def, input, timeout)` registers a workflow, starts a run, and blocks until terminal status.

`HandleTypedOn[I,O](h, t, taskType, fn)` is a package-level generic function (Go generics can't be methods) matching the `worker.HandleTyped` pattern.

## dagnatstest Fixtures

Pre-built workflow definitions for common DAG topologies:

- `LinearDef(t, n)` — n steps in sequence, task names `task-0` through `task-(n-1)`
- `FanOutDef(t, n)` — 1 root → n parallel branches
- `FanInDef(t, n)` — root → n branches → join
- `DiamondDef(t)` — a → {b, c} → d

`PassHandler()` completes with input as output. `FailHandler(msg)` returns a `NonRetryableError`. All Def functions generate unique names via `t.Name()` + atomic counter to avoid KV collisions in parallel tests.

## Shell Completions

`dagnats completion bash` and `dagnats completion zsh` generate shell scripts that delegate all logic to a hidden `dagnats __complete` command.

**Static completions:** top-level commands, subcommands, flags — all hardcoded in sorted `[]string` vars. **Dynamic completions:** workflow names and run IDs fetched from NATS KV with 500ms timeout. Silent failure if NATS unreachable (shell completion convention).

## dagnats run inspect

Unified debug view combining status, failure events, DLQ entries, and optional trace spans. One command replaces `run status` + `run events --type` + `dlq list` + `trace`.

**Cross-references:** Failure events and DLQ entries displayed inline under their corresponding failed step with `replay:` and `view:` copy-paste hints. `--trace` flag fetches spans from TELEMETRY stream and renders the span tree after steps. `--json` includes all sections with `omitempty`.

**Architecture:** `gatherInspectData` collects all data into a single `inspectData` struct. Two renderers (`renderInspectHuman`, `renderInspectJSON`) consume the same data — no divergence possible.

## dagnats clean

Purges run data for a clean slate between test runs. Purges 5 streams (WORKFLOW_HISTORY, TASK_QUEUES, EVENTS, DEAD_LETTERS, SLEEP_TIMERS) and clears 10 runtime KV buckets. Preserves workflow definitions and telemetry by default. `--all` clears everything. `--force` skips confirmation prompt.

## dagnats status --detail

Extends the status command with three sections for operational health:

- **Queue Health:** Per-task-type pending, in-flight, redelivered counts from TASK_QUEUES consumer info. Partitioned consumers grouped by task name.
- **DLQ Summary:** Total count, oldest/newest timestamps, per-task breakdown from DEAD_LETTERS stream info with subject filter.
- **Engine Lag:** WORKFLOW_HISTORY stream sequence vs orchestrator consumer delivered sequence. Scheduled timer count from SLEEP_TIMERS stream.

All calls read JetStream metadata only — no message bodies scanned. ~N+4 API calls where N = task types.

## dagnats trigger history

`dagnats trigger history <trigger-id> [--limit=N] [--json]` shows fire events for a trigger.

**Data source:** `TRIGGER_HISTORY` stream (`trigger.fire.{triggerID}`, 30-day retention). `TriggerFire` records published by trigger scheduler on every fire, including skipped fires (singleton/dedup/concurrency). Run status enriched at query time from `workflow_runs` KV with bounded parallel lookups (16 concurrent).

**Columns:** time, status (started/completed/failed/cancelled/skipped), run ID, duration.
