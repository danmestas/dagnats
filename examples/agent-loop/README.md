# Agent Loop Example

Demonstrates the iterative agent loop pattern. A single `counter` step
increments a checkpointed counter on each iteration and completes when it
reaches 5.

## Workflow

- `counter` - Agent loop step (max 10 iterations, 1s delay between iterations)

## Run It

Terminal 1 -- start the server:

```bash
dagnats serve
```

Terminal 2 -- start the worker:

```bash
go run ./examples/agent-loop/
```

Terminal 3 -- register and run:

```bash
dagnats workflow register examples/agent-loop/workflow.json
dagnats run start agent-loop '""'
# Started: abc123...
```

## Expected Output

```
[counter] iteration 1 / 5
[counter] iteration 2 / 5
[counter] iteration 3 / 5
[counter] iteration 4 / 5
[counter] iteration 5 / 5
[counter] target reached, completing
```

Each iteration saves a checkpoint to KV. If the worker crashes mid-loop, it
resumes from the last saved counter value.

## Inspect the Run

```bash
dagnats run inspect abc123
dagnats run events abc123
```

The events stream shows step.continue events for iterations 1-4 and a
step.completed event for iteration 5.
