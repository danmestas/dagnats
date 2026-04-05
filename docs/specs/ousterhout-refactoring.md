# Ousterhout Refactoring Backlog

Technical debt identified during a full Ousterhout-style module review (2026-04-05).
Organized by priority. All items are incremental — no architectural changes required.

## High Priority (70-line rule violations in hot paths)

### engine: 6 functions over 60 lines

All in `internal/engine/orchestrator.go`. These are linear event handlers
that read top-to-bottom but exceed the project's 70-line function limit.

| Function | Lines | Extraction target |
|----------|-------|-------------------|
| `enqueueReady` | 79 | Extract per-step-type dispatch into helpers |
| `handleStepContinue` | 69 | Extract loop-bound check + delay scheduling |
| `handleStepFailed` | 67 | Extract retry-after vs permanent failure branches |
| `handleStepCompleted` | 62 | Extract sticky binding + ready-step dispatch |
| `handleWorkflowCancelled` | 62 | Extract cascade-cancel-children logic |
| `publishTask` | 61 | Extract rate-limit check + sticky routing |

### api: 2 functions over 70 lines + instrumentation boilerplate

| Function | Lines | File | Extraction target |
|----------|-------|------|-------------------|
| `startRunInner` | 91 | `internal/api/service.go` | Extract idempotency check, event construction |
| `scheduleRunInner` | 82 | `internal/api/scheduled.go` | Extract validation, timer scheduling |

The outer/inner instrumentation pattern (span + metrics + error recording)
is repeated ~20 times across Service methods (~200 lines of boilerplate).
Extract an `instrument()` helper:

```go
func (s *Service) instrument(
    ctx context.Context, name string, fn func() error,
) error
```

### server: startComponents at 112 lines

`server/server.go` `startComponents()` does 8 sequential setup steps
with per-step error cleanup. Extract into phases:
- `startNATSAndStreams()` — NATS server + client + JetStream setup
- `startServices()` — orchestrator, API, triggers, bridge
- `startWorkers()` — materialize embedded worker shims

## Medium Priority (API surface cleanup)

### cli: unexport internal formatting functions

23 exported functions; should be ~5. Unexport:
- `ColorRed`, `ColorGreen`, `ColorYellow`, `ColorGray`, `ColorBold`,
  `ColorStatus` (6 functions — internal styling)
- `FormatDLQWatchAction`, `FormatDLQWatchActionSkipped`,
  `FormatDLQWatchSummary`, `FormatDLQWatchActionJSON`,
  `FormatDLQWatchSummaryJSON` (5 functions — DLQ formatting)
- `FormatRunOutput`, `FormatRunStatus`, `FormatRunStatusWithDef`
  (3 functions — run formatting)
- `FormatCronTest`, `FormatCronTestJSON` (2 functions — cron testing)
- `ResolveRunID`, `HasLastFlag`, `StripLastFlag` (3 functions — internals)

Keep exported: `Run`, `FormatJSON`, `GetEnvWithFallback`, `HasJSONFlag`,
`StripJSONFlag`, `HasHelpFlag`.

### cli: split run.go (857 lines)

Largest file in the codebase. Split by subcommand:
- `run_start.go` — start, start --output
- `run_list.go` — list, events
- `run_cancel.go` — cancel, cancel-all
- `run_retry.go` — retry (already exists)
- `run_signal.go` — signal
- `run_output.go` — output (already exists)
- `run_format.go` — shared formatting helpers + JSON types

## Low Priority (minor style / consistency)

### natsutil: inconsistent dedup window representation

`WORKFLOW_HISTORY` uses raw nanoseconds (`5_000_000_000`) while
`TELEMETRY` uses `5 * time.Second`. Both mean the same thing.
Standardize on `time.Duration` form.

### bridge: AckMap TTL cleanup

Polled tasks that never get resolved leave entries in the ack map.
NATS AckWait handles redelivery, but map entries persist (tiny memory
leak). Add a periodic sweep or TTL-based eviction. Not urgent — bridge
is typically short-lived and entries are small.

### actor: AllForOne supervision not implemented

`RestartScope` is declared but `handleFailure()` always restarts only
the failed actor. Either implement `RestartAll` or document as reserved.
Restart budget (5/minute) is also hardcoded — consider making it
configurable via `SpawnOption`.
