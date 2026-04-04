# Embedded Workers

## Design Decision: Shim-Then-Materialize

Embed worker handlers directly in `dagnats serve` via a Go API and config-driven handlers, eliminating the need for a separate worker process. Workers register through a `WorkerShim` that records handler registrations before `Run()`. During `startComponents()`, shims materialize into real `*worker.Worker` instances.

## Go API

```go
cfg := server.ConfigFromEnv()
srv := server.New(cfg)

w := server.EmbeddedWorker(srv)  // returns *WorkerShim
w.Handle("upper", func(ctx worker.TaskContext) error {
    return ctx.Complete([]byte(strings.ToUpper(string(ctx.Input()))))
})
w.WithGroups("gpu")  // optional worker group routing

srv.Run()  // materializes shim -> real worker, starts everything
```

**Panic guards:** `EmbeddedWorker` panics if called after `Run()`, if srv is nil, or if max embedded workers (50) exceeded. `Handle` panics on empty taskType, nil handler, or after `Run()`.

## Server Lifecycle Integration

**Startup** (end of `startComponents`, after streams/KV exist):
1. For each shim: create `worker.Worker` with groups, register all handlers, start
2. Set `shim.started = true`, nil out `workerShims` (no stale state)

**Shutdown** (between triggers and orchestrator):
1. Stop embedded workers so in-flight tasks can publish completion events
2. Then stop orchestrator

## Config-Driven Handlers

Two built-in handler types configured via `dagnats.yaml`:

```yaml
worker.run-tests.exec: go test ./...
worker.notify.http: https://example.com/hook
worker.check.http: https://example.com/check
worker.check.http_method: PUT
```

**Exec handler:** Splits command on spaces, runs via `os/exec`. Stdin receives task input. Stdout becomes output on success. Stderr included in error. Environment injected: `DAGNATS_RUN_ID`, `DAGNATS_STEP_ID`, `DAGNATS_RETRY_COUNT`. 5-minute default timeout. 10MB output cap.

**HTTP handler:** Sends task input as request body to URL. Response body becomes output on 2xx, error on non-2xx. Default method POST, configurable. 60-second timeout. 10MB response cap.

## Config Parsing

Keys follow `worker.{task}.{field}` pattern. Fields: `exec`, `http`, `http_method`. Parsed in `applyConfigValue` when key starts with `worker.`.

**Validation rules:**
- No duplicate task names
- Exactly one of exec or http (not both, not neither)
- Max 50 worker configs

**Env var override:** `DAGNATS_WORKER_{TASK}_EXEC`, `DAGNATS_WORKER_{TASK}_HTTP`, `DAGNATS_WORKER_{TASK}_HTTP_METHOD` (task name uppercased, hyphens to underscores).

## CLI Wiring

`cli/serve.go` creates a single `EmbeddedWorker`, iterates `cfg.Workers`, calls `buildHandler(wc)` for each. `buildHandler` dispatches to `execHandler` or `httpHandler` based on config.

## What This Does NOT Cover

- **Worker registration discovery**: workers are explicitly configured, no auto-discovery
- **Auth/TLS for HTTP handlers**: future concern
- **Shell expansion in exec**: commands split on whitespace only, no shell features
