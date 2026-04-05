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

## Constraints

- All new `ctx` parameters use `context.Background()`. Threading real
  contexts through the call chain is a follow-up.
- `*nats.Msg` is still used for constructing outbound messages (publish).
  `jetstream.Msg` is for received messages only.
- `nats.Header` is still used for header construction.
- `*nats.Conn` is still the connection type.
