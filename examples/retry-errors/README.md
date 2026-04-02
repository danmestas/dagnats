# Retry Errors Example

Demonstrates retry policies and error handling. The `fetch` step fails twice
before succeeding on the third attempt using exponential backoff. If all retries
exhaust, `report-error` runs via `on_failure`.

## Workflow

- `fetch` - Flaky HTTP fetch (retries 3x with exponential backoff)
- `process` - Uppercases the fetched data (depends on fetch)
- `report-error` - Logs errors (triggered by on_failure)

## Run It

Terminal 1 -- start the server:

```bash
dagnats serve
```

Terminal 2 -- start the worker:

```bash
go run ./examples/retry-errors/
```

Terminal 3 -- register and run:

```bash
dagnats workflow register examples/retry-errors/workflow.json
dagnats run start retry-errors '""'
# Started: abc123...
```

## Expected Behavior

```
[fetch] attempt 0           # fails
[fetch] attempt 1           # fails
[fetch] attempt 2           # succeeds
[process] {"STATUS":"OK","ITEMS":["A","B","C"]}
```

## Inspect the Run

```bash
dagnats run inspect abc123
dagnats run events abc123
```

The events stream shows step.failed events for the first two attempts,
followed by step.completed on the third.
