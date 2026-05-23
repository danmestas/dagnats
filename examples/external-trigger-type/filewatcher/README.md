# Filewatcher External Trigger Type

Demonstrates the [External trigger SDK](../../../docs/architecture/)
(parent #273, Phase 2.4 / #337) end-to-end. A single in-process worker
contributes a new trigger kind, `filewatcher`, that fires a workflow
whenever a configured filesystem path changes.

This example is the canonical answer to "how do I add a custom trigger
type to dagnats without modifying the engine?" Workers own the
behaviour; the engine just holds the registration and bridges
activate/deactivate events.

## What it shows

- `worker.RegisterTriggerType` — publishes a `TriggerTypeDef` into the
  `trigger_types` KV bucket and asks the engine for an
  `externalRegistrar`.
- `worker.WatchTriggers` — subscribes to
  `_TRIGGER.filewatcher.{activate,deactivate}` so the worker is
  notified whenever a trigger of this kind is enabled or disabled.
- `fsnotify` integration — on activate, the worker installs a kernel
  watch on the configured path; on filesystem events, it publishes
  `workflow.started` directly to JetStream.

## Audit-locked constants

```go
const filewatcherKind     = "filewatcher"
const filewatcherWorkerID = "filewatcher-example" // stable, NOT random
const filewatcherVersion  = "1"                   // never bumps
```

The Phase 2.7 change in [#351](https://github.com/danmestas/dagnats/issues/351)
turns version mismatch into a hard error when live triggers exist for
a kind. If this example used a random worker ID per boot, or
auto-bumped the version, every rebuild would conflict with itself.
Pinning both makes restart a no-op at the engine's ack layer.

## Trigger config schema

```json
{
  "path": "/abs/or/relative/path/to/watch",
  "events": ["create", "write", "remove", "rename", "chmod"]
}
```

`path` is required. `events` is optional — empty means "fire on any
filesystem operation".

## Payload schema (what the workflow sees)

```json
{
  "path":  "/absolute/path/to/the/file/that/changed",
  "event": "create"
}
```

The worker fills these into `TriggerEnvelope.Data` on every
`workflow.started`. Your workflow's first step receives the envelope
as its input.

## Run it

Terminal 1 — start the dagnats server:

```bash
dagnats serve
```

Terminal 2 — start the filewatcher worker:

```bash
go run ./examples/external-trigger-type/filewatcher
```

The worker prints `filewatcher worker ready. Ctrl+C to stop.` once
registration completes. Verify the kind landed in the registry:

```bash
dagnats trigger-type list
# Should show: filewatcher  owner=filewatcher-example  version=1
dagnats trigger-type describe filewatcher
# Prints the full TriggerTypeDef including ConfigSchema / PayloadSchema.
```

Terminal 3 — register a workflow that consumes the envelope, then
create a `filewatcher` trigger pointing at it.

The CLI does not yet have a `trigger create --filewatcher=...` flag
for External kinds (that lives behind a future Phase). For now the
trigger goes in via direct KV write (or a future API call). The
integration test in `filewatcher_test.go` shows the exact wire shape:

```go
trigger.TriggerDef{
    ID: "fw-1", WorkflowID: "my-workflow", Enabled: true,
    External: &trigger.ExternalTriggerConfig{
        Kind:   "filewatcher",
        Config: json.RawMessage(`{"path": "/tmp/inbox"}`),
    },
}
```

Touch a file under the watched path:

```bash
touch /tmp/inbox/hello.txt
```

You'll see the worker log `filewatcher: fired workflow` and the engine
will dispatch your workflow with the envelope above.

## Restart safety

Stopping and restarting the worker is a no-op at the engine level:
same `Name + OwnerWorkerID + ConfigSchema` re-registration is
explicitly idempotent (see
`internal/trigger/ack_micro.go installExternalRegistrar`). The Phase
2.7 #351 work tightens this with a version check; because this
example pins `Version: "1"` forever, that check passes too.

## Tests

```bash
go test ./examples/external-trigger-type/...
```

Two integration tests cover:

- `TestFilewatcher_FiresOnFileCreate` — end-to-end fire path. A
  trigger lands in KV, the worker's catch-up scan activates it, and
  touching a temp dir produces a `workflow.started` event.
- `TestFilewatcher_RestartIsIdempotent` — start, stop, start again
  with the same stable Version + OwnerWorkerID. Asserts the second
  start returns no error and the `trigger_types` KV record still
  carries the audit-locked constants.
