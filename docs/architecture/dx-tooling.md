# Developer Experience Tooling

## dagnatstest Package

`dagnatstest.Server(t)` starts an embedded NATS server with all streams and KV buckets provisioned. Returns a connected client, cleaned up on test end.

`dagnatstest.RunAndWait(t, svc, workflow, input, timeout)` starts a run and polls every 25ms until terminal status. `WaitForStatus(t, svc, runID, timeout, statuses...)` polls for specific statuses. Both use `t.Helper()` and `t.Context()` for clean failure reporting. No defaults on timeout — forces test authors to think about bounds.

## dagnats dev (Watch Mode)

`dagnats dev [--dir=.] [--delay=500ms]` watches Go files, rebuilds, and restarts the worker process on change. Polling-based watcher (no fsnotify dependency) with 200ms debounce. Verifies NATS connectivity on startup. On build failure, keeps the old process running. Output prefixed with `[dev]` in cyan; child process stdout/stderr passed through unmodified.

**Files:** `cli/dev.go` (entry), `cli/dev_watch.go` (watcher), `cli/dev_runner.go` (build/restart).

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
