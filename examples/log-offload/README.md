# Log Offload Example

Reference implementation for #634: dagnats owns the BUILD_LOGS hot lane for
the duration of a run (#624); long-term log storage is deliberately NOT
dagnats' job. This example is the workflow + worker an operator points a
`run_terminal` trigger at to copy a finished run's logs somewhere durable —
S3, GCS, a database, whatever you already run. dagnats never learns what
storage you chose.

Storage-agnostic by construction: everything in `offload.go` except
`writeChunk` is generic (find the consumer, drain it, hand each chunk to a
writer). `writeChunk` is the ONLY function this example expects you to
replace. The shipped `writeChunk` writes one newline-delimited JSON file per
`(step, attempt, iteration)` to a local directory — a working reference
target with zero external dependencies, not a production recommendation.

## Workflow

- `offload` - task `logs.offload`. Drains every BUILD_LOGS chunk for the
  triggering run and writes it out. 3 retries, 10 minute timeout.

## Trigger

`trigger.json` registers a `run_terminal` trigger: `log-offload` (this
workflow) fires whenever `your-build-workflow` (a placeholder — replace with
your actual build/deploy workflow's name) reaches `completed`, `failed`, or
`cancelled`. `log-offload` and the workflow it watches MUST be different
names — a `run_terminal` trigger whose watched workflow equals its own target
is rejected at registration (it would re-trigger itself forever every time it
finishes).

## Run It

Terminal 1 -- start the server:

```bash
dagnats serve
```

Terminal 2 -- start the offload worker:

```bash
export LOG_OFFLOAD_DIR=/tmp/dagnats-logs
go run ./examples/log-offload/
```

Terminal 3 -- register the workflow and trigger:

```bash
dagnats workflow register examples/log-offload/workflow.json

# Edit trigger.json's "workflow" field to name your real build workflow
# first, then register it directly against the triggers KV bucket — there
# is no file-based `trigger register` CLI command yet, only the
# flag-driven `dagnats trigger create` for the simpler trigger types.
nats kv put triggers log-offload-on-your-build-workflow \
  "$(cat examples/log-offload/trigger.json)"
```

Now every time `your-build-workflow` finishes, `log-offload` starts
automatically with input `{"run_id", "workflow_id", "status", "labels"}`
describing the run that just finished, and the worker writes
`$LOG_OFFLOAD_DIR/{step}.{attempt}.{iteration}.ndjson` for every
step/attempt/iteration that wrote to BUILD_LOGS during that run — iteration
is 0 for a normal step and increments per agent-loop `Continue` without
touching attempt, so a Continue'd step's iterations land in separate files
instead of one overwriting another.

## The TTL constraint

BUILD_LOGS' hot retention must exceed this workflow's own retry horizon. With
`retries: 3` and a 10 minute step timeout, a chronically failing offload has
roughly 30-40 minutes of headroom before it gives up for good. A dagnats
deployment's default hot TTL (7 days) is comfortable; a deployment that
shortens BUILD_LOGS retention below that horizon will silently lose logs for
any run whose offload step keeps failing.

## Loop guards

`run_terminal` triggers carry two independent guards (#634):

- **Self-trigger** (register-time): `log-offload`'s trigger cannot watch
  `log-offload` itself — checked when the trigger is registered.
- **Cross-workflow cycles** (runtime): every run started by a `run_terminal`
  trigger carries `TriggerDepth = source run's depth + 1`. Past a depth of 8,
  the engine refuses to start the chained run, logs a warning naming both run
  IDs, and increments `trigger.run_terminal.depth_refusals`. A one-hop offload
  trigger like this one never gets close to that cap; it only matters if you
  chain several `run_terminal` triggers together (A→B→C→...).
