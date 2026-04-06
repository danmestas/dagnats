---
title: CLI Reference
weight: 1
---

Complete reference for the `dagnats` command-line interface.

## Global Flags

| Flag | Description |
|------|-------------|
| `--json` | Output in JSON format (available on all commands) |
| `--help`, `-h` | Show usage information |
| `--version`, `-v` | Print version |

## Usage

```
dagnats <command> [args]
```

---

## workflow

Manage workflow definitions.

### workflow list

List all registered workflows.

```
dagnats workflow list [--json]
```

**Output columns:** NAME, STEPS, TIMEOUT

**Example:**
```bash
dagnats workflow list
# NAME              STEPS  TIMEOUT
# code-review       5      30m0s
# deploy-pipeline   3      none

dagnats workflow list --json
# [{"name":"code-review","steps":[...],"timeout":"30m"}]
```

### workflow register

Register a workflow from a JSON file. Creates a new workflow or updates an existing one. Warns if no workers are active for the workflow's task types.

```
dagnats workflow register <file> [--json]
```

| Argument | Required | Description |
|----------|----------|-------------|
| `file` | Yes | Path to workflow definition JSON file |

**Example:**
```bash
dagnats workflow register workflow.json
# Workflow created: code-review (5 steps)

dagnats workflow register workflow.json --json
# {"name":"code-review","action":"created","steps":5}
```

### workflow show

Show detailed information about a registered workflow, including its step dependency table.

```
dagnats workflow show <name> [--json]
```

| Argument | Required | Description |
|----------|----------|-------------|
| `name` | Yes | Workflow name |

**Example:**
```bash
dagnats workflow show code-review
# Name:        code-review
# Version:     1.0.0
# Steps:       5
# Timeout:     30m0s
#
#   ID            TASK              DEPENDS ON
#   fetch-diff    git.fetch-diff    -
#   lint          lint.run          fetch-diff
#   review-loop   agent.code-review fetch-diff
```

### workflow validate

Validate a workflow JSON file offline (no NATS connection required). Runs structural validation including cycle detection.

```
dagnats workflow validate <file> [--json]
```

| Argument | Required | Description |
|----------|----------|-------------|
| `file` | Yes | Path to workflow definition JSON file |

**Example:**
```bash
dagnats workflow validate workflow.json
# Valid: code-review (5 steps)

dagnats workflow validate bad.json
# invalid: step "x" depends on non-existent step "y"

dagnats workflow validate workflow.json --json
# {"valid":true,"name":"code-review","steps":5}
```

---

## run

Manage workflow runs.

### run start

Start a new workflow run. Optionally provide JSON input, schedule for later, watch until completion, or print output.

```
dagnats run start <workflow> [input] [flags] [--json]
```

| Argument | Required | Description |
|----------|----------|-------------|
| `workflow` | Yes | Workflow name to run |
| `input` | No | JSON input payload |

| Flag | Description |
|------|-------------|
| `--watch` | Watch run events until completion |
| `--output` | Print terminal step output on completion (implies `--watch`) |
| `--at=TIME` | Schedule run at RFC3339 time instead of running immediately |

**Example:**
```bash
dagnats run start code-review '{"pr": 42}'
# Started: a1b2c3d4e5f6...

dagnats run start deploy --at=2025-01-15T09:00:00Z
# Scheduled a1b2c3d4 (run at 2025-01-15T09:00:00Z)

dagnats run start code-review '{"pr": 42}' --watch --output
# Started: a1b2c3d4...
# [events stream...]
# Output:
# {"issues_found": 3, "summary": "..."}
```

### run status

Show the current status of a workflow run, including per-step breakdown.

```
dagnats run status <run-id> [--last] [--json]
```

| Argument | Required | Description |
|----------|----------|-------------|
| `run-id` | Yes* | Run ID (accepts 8+ character prefixes) |

| Flag | Description |
|------|-------------|
| `--last` | Use the most recent run instead of specifying an ID |

**Example:**
```bash
dagnats run status a1b2c3d4
# Run:      a1b2c3d4e5f6...
# Workflow: code-review
# Status:   running
# Created:  2025-01-15 09:00:00 UTC
#
# Steps:
#   fetch-diff           completed (attempts: 1)
#   lint                 running (attempts: 1)

dagnats run status --last --json
```

### run inspect

Unified debug view combining run status, failure events, and dead-letter entries for a single run. Replaces the three-command workflow of `run status` + `run events` + `dlq list`.

```
dagnats run inspect <run-id> [--last] [--json]
```

| Flag | Description |
|------|-------------|
| `--last` | Use the most recent run |

**Example:**
```bash
dagnats run inspect a1b2c3d4
# Run:      a1b2c3d4...
# Workflow: code-review
# Status:   failed
# ...
# Failures:
#   09:15:30  step.failed              lint
#           trace: abc123...
#           {"error": "lint timeout"}
# Dead Letters:
#   SEQ  TASK      STEP  ERROR
#   42   lint.run  lint  timeout exceeded
```

### run cancel

Cancel a running or scheduled workflow.

```
dagnats run cancel <run-id> [--last] [--json]
```

**Example:**
```bash
dagnats run cancel a1b2c3d4
# Cancelled: a1b2c3d4...

dagnats run cancel --last --json
# {"run_id":"a1b2c3d4...","cancelled":true}
```

### run cancel-all

Bulk cancel runs matching a workflow and optional filters.

```
dagnats run cancel-all [flags]
```

| Flag | Required | Description |
|------|----------|-------------|
| `--workflow=ID` | Yes | Workflow ID to filter |
| `--status=STATUS` | No | `running`, `pending`, or `all` (default: `all`) |
| `--after=TIME` | No | Only runs created after this RFC3339 time |
| `--before=TIME` | No | Only runs created before this RFC3339 time |
| `--dry-run` | No | Preview matching runs without cancelling |
| `--json` | No | JSON output |

**Example:**
```bash
dagnats run cancel-all --workflow=deploy --status=pending --dry-run
# [dry-run] Would cancel 3 runs
#   a1b2c3d4...
#   e5f6a7b8...
#   c9d0e1f2...

dagnats run cancel-all --workflow=deploy
# Cancelled 3 runs
```

### run bulk

Start multiple runs of a workflow with different inputs.

```
dagnats run bulk [flags] [inputs...]
```

| Flag | Required | Description |
|------|----------|-------------|
| `--workflow=ID` | Yes | Workflow ID |
| `--from-file=PATH` | No | JSONL file with one input per line |
| `--json` | No | JSON output |

Inputs can be provided as positional arguments, from a JSONL file, or both.

**Example:**
```bash
dagnats run bulk --workflow=deploy '{"env":"staging"}' '{"env":"prod"}'
# Started 2 runs:
#   a1b2c3d4...
#   e5f6a7b8...

dagnats run bulk --workflow=deploy --from-file=inputs.jsonl --json
# {"run_ids":["a1b2...","e5f6..."],"total":2}
```

### run retry-all

Bulk retry failed runs of a workflow.

```
dagnats run retry-all [flags]
```

| Flag | Required | Description |
|------|----------|-------------|
| `--workflow=ID` | Yes | Workflow ID to filter |
| `--mode=MODE` | Yes | `rerun` (fresh start with original input) or `replay` (re-publish DLQ messages) |
| `--after=TIME` | No | Only runs created after this RFC3339 time |
| `--before=TIME` | No | Only runs created before this RFC3339 time |
| `--dry-run` | No | Preview matching runs without retrying |
| `--json` | No | JSON output |

**Example:**
```bash
dagnats run retry-all --workflow=deploy --mode=rerun --dry-run
# [dry-run] Would retry 2 runs
#   a1b2c3d4...
#   e5f6a7b8...

dagnats run retry-all --workflow=deploy --mode=replay
# Retried 2 runs
#   a1b2c3d4 (replayed)
#   e5f6a7b8 (replayed)
```

### run signal

Send a named signal with a JSON payload to a running workflow.

```
dagnats run signal <run-id> <name> <payload> [--last] [--json]
```

| Argument | Required | Description |
|----------|----------|-------------|
| `run-id` | Yes* | Run ID (or use `--last`) |
| `name` | Yes | Signal name |
| `payload` | Yes | JSON payload |

**Example:**
```bash
dagnats run signal a1b2c3d4 approval '{"approved": true}'
# Signal sent: approval

dagnats run signal --last done '{}' --json
# {"run_id":"a1b2...","signal":"done","sent":true}
```

### run list

List workflow runs with optional filtering.

```
dagnats run list [flags] [--json]
```

| Flag | Description |
|------|-------------|
| `--workflow=ID` | Filter by workflow ID |
| `--status=STATUS` | Filter by status (client-side) |
| `--scheduled` | List scheduled (pending) runs instead |

**Output columns:** RUN_ID, WORKFLOW, STATUS, CREATED, STEPS

**Example:**
```bash
dagnats run list --workflow=deploy --status=failed
# RUN_ID     WORKFLOW  STATUS  CREATED              STEPS
# a1b2c3d4   deploy    failed  2025-01-15 09:00:00  3

dagnats run list --scheduled
# RUN ID    WORKFLOW  RUN AT                    STATUS
# a1b2c3d4  deploy    2025-01-16T09:00:00Z      scheduled
```

### run events

Show the event history for a run with optional filtering.

```
dagnats run events <run-id> [flags] [--json]
```

| Flag | Description |
|------|-------------|
| `--last` | Use the most recent run |
| `--full` | Show full event data (default truncates to 200 chars) |
| `--type=TYPE` | Filter by event type (e.g., `step.completed`) |
| `--step=STEP` | Filter by step ID |

**Output columns:** TIMESTAMP, TYPE, STEP, TRACE, DATA

**Example:**
```bash
dagnats run events a1b2c3d4 --type=step.failed --full
# TIMESTAMP            TYPE           STEP    TRACE       DATA
# 2025-01-15 09:05:00  step.failed    lint    abc123...   {"error":"..."}
```

### run watch

Attach to an existing run and tail its events in real time until completion.

```
dagnats run watch <run-id> [--last]
```

**Example:**
```bash
dagnats run watch a1b2c3d4
# [events stream until run completes or fails]
```

### run output

Print the final output of a completed run's terminal steps (steps with no dependents).

```
dagnats run output <run-id> [--last] [--json]
```

**Example:**
```bash
dagnats run output a1b2c3d4
# {"issues_found": 3, "summary": "All checks passed"}

dagnats run output --last --json
# {"run_id":"a1b2...","status":"completed","outputs":{"post-results":"{...}"}}
```

### run retry

Re-run a workflow using an existing run's workflow ID and input.

```
dagnats run retry <run-id> [input] [--last] [--json]
```

| Argument | Required | Description |
|----------|----------|-------------|
| `run-id` | Yes* | Original run ID (or use `--last`) |
| `input` | No | Override input (uses original run's input if omitted) |

**Example:**
```bash
dagnats run retry a1b2c3d4
# Retrying workflow deploy: e5f6a7b8...

dagnats run retry --last --json
# {"original_run_id":"a1b2...","workflow":"deploy","new_run_id":"e5f6..."}
```

---

## trigger

Manage workflow triggers (cron, NATS subject, webhook).

### trigger create

Create a new trigger. Exactly one of `--cron`, `--subject`, or `--webhook` is required.

```
dagnats trigger create <workflow-id> [flags] [--json]
```

| Flag | Description |
|------|-------------|
| `--cron=EXPR` | Cron expression (5-field or extended) |
| `--subject=SUB` | NATS subject pattern |
| `--webhook=PATH` | Webhook URL path |
| `--tz=TZ` | Timezone for cron (default: `UTC`) |
| `--backfill` | Enable backfill for cron triggers |
| `--secret=SEC` | Webhook HMAC secret (or set `DAGNATS_WEBHOOK_SECRET`) |

**Example:**
```bash
dagnats trigger create deploy --cron="0 9 * * 1-5" --tz=America/Denver
# Trigger created: trig-a1b2c3d4e5f6a7b8

dagnats trigger create deploy --webhook=/hooks/deploy --secret=mysecret --json
# {"trigger_id":"trig-a1b2c3d4e5f6a7b8"}
```

### trigger list

List all registered triggers.

```
dagnats trigger list [--json]
```

**Output columns:** ID, WORKFLOW, TYPE, CONFIG, ENABLED

**Example:**
```bash
dagnats trigger list
# ID                     WORKFLOW  TYPE     CONFIG              ENABLED
# trig-a1b2c3d4e5f6a7b8  deploy    cron     0 9 * * 1-5        yes
# trig-c9d0e1f2a3b4c5d6  ingest    subject  events.incoming.>   yes
```

### trigger update

Update an existing trigger's configuration in-place.

```
dagnats trigger update <trigger-id> [flags] [--json]
```

| Flag | Description |
|------|-------------|
| `--cron=EXPR` | New cron expression |
| `--tz=TZ` | New timezone |
| `--backfill` | Enable backfill |
| `--subject=SUB` | New NATS subject |
| `--webhook=PATH` | New webhook path |
| `--secret=SEC` | New webhook secret |

**Example:**
```bash
dagnats trigger update trig-a1b2c3d4 --cron="0 8 * * 1-5"
# Trigger updated: trig-a1b2c3d4
```

### trigger delete

Delete a trigger.

```
dagnats trigger delete <trigger-id> [--json]
```

**Example:**
```bash
dagnats trigger delete trig-a1b2c3d4e5f6a7b8
# Trigger deleted: trig-a1b2c3d4e5f6a7b8
```

### trigger enable

Enable a disabled trigger.

```
dagnats trigger enable <trigger-id> [--json]
```

### trigger disable

Disable a trigger without deleting it.

```
dagnats trigger disable <trigger-id> [--json]
```

### trigger history

View fire history for a trigger, including timestamps, statuses, run IDs, and durations.

```
dagnats trigger history <trigger-id> [--limit=N] [--json]
```

| Argument | Required | Description |
|----------|----------|-------------|
| `trigger-id` | Yes | Trigger ID |

| Flag | Description |
|------|-------------|
| `--limit=N` | Max fire records to display (default: 10, max: 500) |

**Example:**
```bash
dagnats trigger history trig-a1b2c3d4e5f6a7b8
# TIME                 STATUS     RUN ID              DURATION
# 2025-01-15 09:00:00  completed  a1b2c3d4e5f6a7b8    2.5s
# 2025-01-14 09:00:00  failed     c9d0e1f2a3b4c5d6    1.2s

dagnats trigger history trig-a1b2c3d4e5f6a7b8 --limit=5 --json
# [{"fired_at":"...","status":"completed","run_id":"...","duration":"2.5s"},...]
```

### trigger test

Validate a cron expression and show the next N fire times. Offline operation (no NATS required).

```
dagnats trigger test <cron-expr> [flags] [--json]
```

| Flag | Description |
|------|-------------|
| `--tz=TZ` | Timezone (default: `UTC`) |
| `--count=N` | Number of fire times to show (default: 5, max: 100) |

**Example:**
```bash
dagnats trigger test "0 9 * * 1-5" --tz=America/Denver --count=3
# Valid: 0 9 * * 1-5
#
# Next 3 fire times (America/Denver):
#   1. 2025-01-20 09:00 MST
#   2. 2025-01-21 09:00 MST
#   3. 2025-01-22 09:00 MST

dagnats trigger test "invalid" --json
# {"expression":"invalid","valid":false,"error":"..."}
```

---

## dlq

Manage the dead-letter queue.

### dlq list

List dead-letter messages with optional filtering.

```
dagnats dlq list [flags] [--json]
```

| Flag | Description |
|------|-------------|
| `--run=RUN_ID` | Filter by run ID |
| `--limit=N` | Max messages to fetch (default: 50) |

**Output columns:** SEQ, SUBJECT, RUN_ID, STEP_ID, TASK, ERROR, TIMESTAMP

**Example:**
```bash
dagnats dlq list --limit=10
# SEQ  SUBJECT        RUN_ID    STEP_ID  TASK      ERROR           TIMESTAMP
# 42   dead.lint.run  a1b2c3d4  lint     lint.run  timeout exceeded 2025-01-15 09:05:00
```

### dlq replay

Replay a dead-letter message by sequence number or replay all messages for a run.

```
dagnats dlq replay <sequence-number> [--json]
dagnats dlq replay --run=<run-id> [--json]
```

**Example:**
```bash
dagnats dlq replay 42
# Replayed dead letter 42

dagnats dlq replay --run=a1b2c3d4
# Replayed dead letter 42 (lint.run)
# Replayed 1 dead letters for run a1b2c3d4
```

### dlq watch

Continuously poll the DLQ and auto-replay messages on an interval. Runs until interrupted (Ctrl+C).

```
dagnats dlq watch [flags]
```

| Flag | Description |
|------|-------------|
| `--interval=DURATION` | Poll interval (default: `30s`) |
| `--max-replays=N` | Max replay attempts per message (default: 3) |
| `--run=RUN_ID` | Filter by run ID |
| `--json` | JSON output (one line per action) |

**Example:**
```bash
dagnats dlq watch --interval=10s --max-replays=5
# Watching DLQ every 10s (max 5 replays per message)
# [replay 1/5] seq=42 task=lint.run run=a1b2c3d4 err=timeout
# [skip exhausted 5/5] seq=42 task=lint.run run=a1b2c3d4
# ^C
# Watch summary: 3 replayed, 1 exhausted
```

---

## workers

Observe worker status.

### workers list

List currently registered workers from the `workers` KV bucket.

```
dagnats workers list [--json]
```

**Output columns:** WORKER_ID, TASK_TYPES, LANGUAGE, MAX_TASKS

**Example:**
```bash
dagnats workers list
# WORKER_ID   TASK_TYPES       LANGUAGE  MAX_TASKS
# worker-1    llm,http         go        4
# worker-2    lint.run         python    2
```

---

## serve

Start the embedded NATS server with DagNats engine and API.

```
dagnats serve
```

Reads configuration from `dagnats.yaml` (optional, in current directory) and environment variables.

| Environment Variable | Description |
|---------------------|-------------|
| `DAGNATS_DATA_DIR` | JetStream data directory |
| `DAGNATS_HTTP_ADDR` | HTTP API listen address |
| `DAGNATS_NATS_PORT` | NATS client port |

Run `dagnats config show` to see the effective configuration.

**Example:**
```bash
DAGNATS_HTTP_ADDR=:8080 dagnats serve
```

---

## init

Scaffold a new workflow project directory with boilerplate files.

```
dagnats init <name> [--json]
```

| Argument | Required | Description |
|----------|----------|-------------|
| `name` | Yes | Project name (alphanumeric and hyphens only) |

Creates a directory with `workflow.json` and `main.go` (worker boilerplate).

**Example:**
```bash
dagnats init my-pipeline
# Created my-pipeline/
#   my-pipeline/workflow.json
#   my-pipeline/main.go

dagnats init my-pipeline --json
# {"name":"my-pipeline","directory":"my-pipeline","files":["workflow.json","main.go"]}
```

### init workflow

Scaffold a workflow JSON definition with linearly chained steps and print handler registration code snippets.

```
dagnats init workflow <name> [--steps=a,b,c]
```

| Argument | Required | Description |
|----------|----------|-------------|
| `name` | Yes | Workflow name (alphanumeric and hyphens, 2-256 chars) |

| Flag | Description |
|------|-------------|
| `--steps=a,b,c` | Comma-separated step names (default: single `process` step, max: 20) |

Steps are chained linearly -- each step depends on the previous one. The generated JSON file includes a `$schema` reference for editor validation.

**Example:**
```bash
dagnats init workflow image-pipeline --steps=fetch,resize,upload
# Created image-pipeline.json
#
# Register handlers in your worker:
#
# w.Handle("image-pipeline-fetch", handleFetch)
# w.Handle("image-pipeline-resize", handleResize)
# w.Handle("image-pipeline-upload", handleUpload)

dagnats init workflow simple-job
# Created simple-job.json
#
# Register handlers in your worker:
#
# w.Handle("simple-job-process", func(ctx worker.TaskContext) error {
#     input := ctx.Input()
#     // TODO: implement process logic
#     return ctx.Complete(input)
# })
```

---

## config

View effective server configuration.

### config show

Print the resolved configuration from `dagnats.yaml` and environment variables.

```
dagnats config show [--json]
```

**Example:**
```bash
dagnats config show
# data_dir:         /tmp/dagnats
# http_addr:        :8080
# nats_port:        4222
# monitor_port:     (disabled)
# leaf_remotes:     (none)
# leaf_credentials: (none)
# max_store_bytes:  1073741824

dagnats config show --json
# {"data_dir":"/tmp/dagnats","http_addr":":8080",...}
```

Running `dagnats config` without a subcommand defaults to `show`.

---

## status

Show system health: NATS connection, JetStream availability, active run count, stream details, and per-workflow metrics.

```
dagnats status [--detail] [--json]
```

| Flag | Description |
|------|-------------|
| `--detail` | Include queue health, dead-letter summary, and engine lag |

**Example:**
```bash
dagnats status
# NATS:        connected
# JetStream:   available (5 streams)
# Active runs: 2

dagnats status --detail
# NATS:        connected
# JetStream:   available (5 streams)
# Active runs: 2
#
# Task Queues:
#   TASK        PENDING  IN-FLIGHT  REDELIVERED  ACK WAIT
#   lint.run    3        1          0            30000ms
#   llm.chat    0        2          1            60000ms
#
# Dead Letters: 1 total
#   Oldest: 2025-01-15 09:05:00
#   Newest: 2025-01-15 09:05:00
#   TASK      COUNT
#   lint.run  1
#
# Engine:
#   History lag:     0 messages (0.0s)
#   Scheduled timers: 1

dagnats status --json
# {"nats":"connected","jetstream":"available","stream_count":5,"active_runs":2,...}
```

---

## logs

Tail the NATS telemetry log stream in real time. Subscribes to the `TELEMETRY` JetStream stream and prints formatted log records. Blocks until interrupted (Ctrl+C).

```
dagnats logs [flags]
```

| Flag | Description |
|------|-------------|
| `--level=LEVEL` | Filter by log level (`ERROR`, `WARN`, `INFO`, `DEBUG`) |
| `--service=NAME` | Filter by service name |
| `--tail=N` | Show last N historical messages and exit (max: 10000) |

**Example:**
```bash
dagnats logs --level=ERROR
# Tailing logs on telemetry.logs.*.ERROR ...
# 09:15:30 ERROR   engine  step failed [run_id=a1b2 step=lint]

dagnats logs --tail=20 --service=api
# 09:14:00 INFO    api  started run [run_id=a1b2 workflow=deploy]
# 09:14:01 INFO    api  started run [run_id=c3d4 workflow=review]
```

---

## dev

Watch mode for development. Builds and restarts a Go project automatically when `.go` files change. Verifies NATS is reachable before entering the watch loop. Adds the dev binary (`.dagnats-dev`) to `.gitignore` automatically.

```
dagnats dev [--dir=DIR] [--delay=MS]
```

| Flag | Description |
|------|-------------|
| `--dir` | Project directory to watch (default: `.`) |
| `--delay` | Poll interval in milliseconds (default: 500, minimum: 100) |

The watcher polls for changes to non-test `.go` files, skipping hidden directories, `vendor/`, and `.git/`. On change detection, it rebuilds the project and restarts the binary. If a rebuild fails, the previous process keeps running. The child process runs with `DAGNATS_DEV_MODE=true` set in the environment.

Shutdown is clean: `Ctrl+C` sends SIGTERM to the child process (with a 5-second SIGKILL fallback) and removes the dev binary.

**Example:**
```bash
dagnats dev
# [dev] watching 42 files in .
# [dev] building...
# [dev] started
# [dev] change detected, rebuilding...
# [dev] restarted
# ^C
# [dev] shutting down...

dagnats dev --dir=./cmd/worker --delay=1000
# [dev] watching 12 files in ./cmd/worker
# [dev] building...
# [dev] started
```

---

## Run ID Prefix Resolution

All commands that accept a run ID support 8+ character prefix matching. The CLI resolves the prefix to the full run ID by scanning existing runs. If the prefix is ambiguous, the command reports an error.

```bash
# These are equivalent if a1b2c3d4 uniquely matches:
dagnats run status a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6
dagnats run status a1b2c3d4
```
