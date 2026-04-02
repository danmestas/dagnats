# Signals Example

Demonstrates cross-step signal coordination. The `prepare` and
`wait-for-approval` steps run in parallel. The `finalize` step depends on both
and combines their outputs.

## Workflow

- `prepare` - Does preparation work, completes immediately
- `wait-for-approval` - Blocks until an external signal arrives (5 min timeout)
- `finalize` - Runs after both steps complete

## Run It

Terminal 1 -- start the server:

```bash
dagnats serve
```

Terminal 2 -- start the worker:

```bash
go run ./examples/signals/
```

Terminal 3 -- register, run, and send the signal:

```bash
dagnats workflow register examples/signals/workflow.json
dagnats run start signals '""'
# Started: abc123...

# The worker will print:
# [prepare] preparing resources...
# [wait-for-approval] waiting for signal...

# Send the approval signal:
dagnats run signal abc123 approval '{"approved": true}'

# The worker will print:
# [wait-for-approval] received: {"approved": true}
# [finalize] finalized with input: ...
```

## Inspect the Run

```bash
dagnats run status abc123
dagnats run events abc123
```
