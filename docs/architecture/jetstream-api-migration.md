# JetStream API Migration

## Design Decision: New `jetstream.JetStream` API, Zero Legacy

Migrated the entire project from legacy `nats.JetStreamContext` to the new
`github.com/nats-io/nats.go/jetstream` API. Zero legacy references remain
in source files. The legacy API is maintenance-only; new features
(atomic publish, consumer groups, stream config options) only exist on the
new API.

## Why

The Orbit extensions (`jetstreamext`, `pcgroups`) require the new API. Rather
than maintaining dual interfaces, we migrated everything. This also picks up
the new `jetstream.KeyValue` (context-aware), consumer-based subscriptions
(replacing `js.Subscribe`), and typed publish options (`jetstream.WithMsgID`).

## Key Differences

| Legacy | New |
|--------|-----|
| `nc.JetStream()` → `nats.JetStreamContext` | `jetstream.New(nc)` → `jetstream.JetStream` |
| `js.KeyValue("bucket")` | `js.KeyValueStore(ctx, "bucket")` |
| `kv.Get(key)` | `kv.Get(ctx, key)` |
| `js.Subscribe(subject, handler, opts...)` | `stream.CreateOrUpdateConsumer(ctx, cfg)` + `cons.Consume(handler)` |
| `js.Publish(subject, data, nats.MsgId(id))` | `js.Publish(ctx, subject, data, jetstream.WithMsgID(id))` |
| `nats.ErrKeyNotFound` | `jetstream.ErrKeyNotFound` |
| Handler: `func(*nats.Msg)` | Handler: `func(jetstream.Msg)` |

## Migration Strategy

Dual-interface period: both `jsLegacy` and `js` fields coexisted during
migration. Converted bottom-up (leaf functions first, aggregates last).
Removed legacy field once all callers migrated.

## Scope

116 files changed across all packages: `internal/engine/`, `worker/`,
`bridge/`, `internal/trigger/`, `internal/observe/`, `internal/api/`,
`internal/natsutil/`, `cli/`, `server/`.

## Context Threading

Contexts are threaded from entry points through all JetStream/KV operations:

| Entry Point | Context Source |
|-------------|---------------|
| Orchestrator event handler | Trace context from message headers |
| Worker message handler | Trace context from message headers |
| HTTP bridge handlers | `r.Context()` from net/http |
| API Service methods | `ctx` parameter (caller provides) |
| CLI commands | `context.Background()` (no parent — correct) |

**Where `context.Background()` remains (justified):**
- Constructors and startup (no parent context exists)
- SleepTimer/Correlator fire methods (`WithTimeout(5s)` — react to
  timers/watchers, no parent event context)
- Observe telemetry publishes (`WithTimeout(2s)` — must not be cancelled
  by the request they're recording)
- Setup functions (`WithTimeout(30s)` — bounded startup)
- WorkflowActor (runs in actor system, no parent request context)
- Trace context origins (`extractTraceCtxJS` fallback)

**Worker `TaskContext`:** Uses stored `tc.ctx` (from message trace context)
for all internal JetStream operations. No public API change — `Complete`,
`Fail`, etc. signatures unchanged.

## Other Constraints

- `*nats.Msg` is still used for constructing outbound messages (publish).
  `jetstream.Msg` is for received messages only.
- `nats.Header` is still used for header construction.
- `*nats.Conn` is still the connection type.
