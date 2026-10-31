---
title: program
weight: 12
---

```
import "github.com/danmestas/dagnats/program"
```

The contract between dagnats and a program: a separately released binary that runs tasks, workflows and services on a dagnats it shares with other programs. A program declares a `Manifest`; its `Name` is the prefix root of everything it owns. dagnats binds the program's own NKey to a NATS user whose permissions `PermissionsFor` derives from the manifest, and the program dials with a reply inbox confined to `_INBOX.<name>`.

## Key Types and Functions

| Name | Description |
|------|-------------|
| `Manifest`, `StreamSpec`, `BucketSpec`, `Link` | What a program declares: task types, workflows, streams, buckets, protocols |
| `Validate(m)` | Pure check of names, prefixes and sizes |
| `PermissionsFor(m)` | The NATS permission set for the program's user, as a plain `Permissions` struct |
| `InboxPrefix(name)` | The inbox prefix to dial with: `nats.CustomInboxPrefix(program.InboxPrefix(name))` |
| `LoadOrCreateKey(path)` | The program's NKey; the seed is written once, mode 0600, and never leaves the process |
| `Provision(ctx, js, m, grant)` | The host creates the manifest's streams and buckets; programs may not |
| `Attach(ctx, nc, prog, grant)` | Calls `Start` with a `Host`, then starts the worker serving its handlers; returns a stop func |
| `Program`, `Host`, `Grant` | The three-method program interface and what `Start` receives |

See `examples/program-min` for a program serving one task over a restricted user.
