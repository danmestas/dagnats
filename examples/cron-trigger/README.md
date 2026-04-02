# Cron Trigger Example

Demonstrates the cron trigger lifecycle. A simple `tick` handler prints the
current timestamp each time the workflow runs.

## Workflow

- `tick` - Prints the current UTC time and completes

## Run It

Terminal 1 -- start the server:

```bash
dagnats serve
```

Terminal 2 -- start the worker:

```bash
go run ./examples/cron-trigger/
```

Terminal 3 -- register the workflow and manage triggers:

```bash
dagnats workflow register examples/cron-trigger/workflow.json
```

### Test a cron expression

```bash
dagnats trigger test "*/5 * * * *"
# Shows the next 5 fire times for every-5-minutes
```

### Create a trigger

```bash
dagnats trigger create cron-trigger "*/1 * * * *" --input '""'
# Triggers the workflow every minute
```

### List triggers

```bash
dagnats trigger list
```

### Disable a trigger

```bash
dagnats trigger disable <trigger-id>
```

## Expected Output

Each minute the worker prints:

```
[tick] 2025-01-15T12:00:00Z
[tick] 2025-01-15T12:01:00Z
...
```
