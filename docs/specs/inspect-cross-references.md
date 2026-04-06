# Cross-Reference Debug Output in Inspect

**Status:** Design
**Date:** 2026-04-06
**Depends on:** Nothing (extends existing inspect command)

## Problem

`dagnats run inspect` already unifies status + failure events + DLQ
entries, but the three sections are disconnected. When debugging:

1. A failure event shows a trace ID, but you have to manually run
   `dagnats trace <trace-id>` to see the span tree.
2. DLQ entries show step ID and error but don't link back to the
   failure event timestamp or trace.
3. There's no "why did this step fail?" summary — you must read raw
   event payloads and mentally correlate them.

The inspect command is close to being a one-stop debug view but falls
short of actually replacing the multi-command workflow.

## Design

### 1. Inline DLQ Entries Under Failed Steps

Currently, steps and DLQ entries are in separate sections:

```
Steps:
  deploy               failed (attempts: 3/3) error: connection refused

Dead Letters:
  SEQ  TASK      STEP     ERROR
  42   deploy    deploy   connection refused
```

Change to inline DLQ entries under their corresponding step:

```
Steps:
  fetch                completed (attempts: 1)
  deploy               failed (attempts: 3/3) error: connection refused
    DLQ #42: connection refused
    replay: dagnats dlq replay 42
  notify               skipped (attempts: 0)
```

The `replay:` hint tells the user exactly what to run next.

### 2. Inline Failure Events Under Failed Steps

Currently, failure events are in a separate "Failures:" section.
Merge them into the step display:

```
Steps:
  fetch                completed (attempts: 1)
  deploy               failed (attempts: 3/3)
    15:04:02  step.failed  connection refused
    15:04:05  step.failed  connection refused (retry 2)
    15:04:11  step.failed  connection refused (retry 3)
    trace: abc123def456
    DLQ #42: connection refused
    replay: dagnats dlq replay 42
  notify               skipped (attempts: 0)
```

This gives a complete per-step debug story: what happened, when, the
trace ID for deeper investigation, and the DLQ entry with replay
command.

### 3. Add Trace Command Hint

When a trace ID is present, add a copy-pasteable command:

```
    trace: abc123def456
    view:  dagnats trace abc123def456
```

### 4. Keep Separate Sections for JSON Output

The `--json` output keeps the current structure (`run`, `failures`,
`dead_letters` as separate top-level keys) for machine consumption.
The inline view is only for human-readable output.

### 5. Implementation

Changes to `cli/inspect.go`:

1. Remove `printFailureEvents()` and `printRunDeadLetters()` as
   separate passes.
2. Add a new `printStepsWithContext()` function that:
   - Iterates steps in dependency order (topological sort from the
     workflow def, falling back to alphabetical if def unavailable)
   - For each failed step, collects matching failure events and DLQ
     entries
   - Prints them indented under the step line
3. The main `runInspectCmd` calls `printStepsWithContext()` instead of
   three separate print functions.

New helper:

```go
// stepDebugContext collects failure events and DLQ entries for a step.
type stepDebugContext struct {
    Failures    []api.RunEvent
    DeadLetters []api.DeadLetter
    TraceID     string
}

// collectStepContexts groups failures and DLQ entries by step ID.
func collectStepContexts(
    failures []api.RunEvent,
    deadLetters []api.DeadLetter,
) map[string]stepDebugContext
```

### 6. Files Changed

| File | Change |
|------|--------|
| `cli/inspect.go` | Replace three-section output with per-step inline context |

### 7. Example: Full Inspect Output (After)

```
Run:      a1b2c3d4
Workflow: deploy-pipeline
Status:   failed
Created:  2026-04-06 15:03:58 UTC

Steps:
  fetch                completed (attempts: 1)
  build                completed (attempts: 1)
  deploy               failed (attempts: 3/3)
    15:04:02  step.failed  connection refused
    15:04:05  step.failed  connection refused
    15:04:11  step.failed  connection refused
    trace: abc123def456
    view:  dagnats trace abc123def456
    DLQ #42: connection refused
    replay: dagnats dlq replay 42
  notify               skipped (attempts: 0)
```
