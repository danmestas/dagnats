# dagnats serve — Unified Single-Binary Server

## Problem

Running DagNats requires starting 5 separate processes (NATS, engine, API,
triggers, workers) across 5 terminals with no coordination, no health checks,
and a race condition on first boot when engine and API both call SetupAll.

## Solution

New `server/` package that owns the full lifecycle: embedded NATS server,
resource setup, engine, API, triggers, HTTP, and graceful shutdown. The
`dagnats serve` CLI command is a thin wrapper calling `server.New(cfg).Run()`.

Separate binaries (`dagnats-engine`, `dagnats-api`) remain for distributed
deployments where components run on different machines.

## Architecture Decision: Always-Embedded NATS

The embedded NATS server always runs. The only question is topology:

- **Standalone** (default): self-contained, zero config, single machine
- **Leaf node**: embedded server connects to a hub cluster, NATS handles
  message routing transparently

All internal components connect to `localhost:{port}` regardless of mode.
Workers and CLI clients connect via NATS — local or remote, same code path.

Internal calls use NATS request/reply (micro). External callers use HTTP.

## Config

```go
type Config struct {
    DataDir     string   // JetStream storage
    HTTPAddr    string   // HTTP listen address
    NATSPort    int      // Embedded NATS port
    LeafRemotes []string // Hub URLs for leaf node mode
}
```

### Resolution Order (highest wins)

1. Env vars: `DAGNATS_DATA_DIR`, `DAGNATS_HTTP_ADDR`, `DAGNATS_NATS_PORT`,
   `DAGNATS_LEAF_REMOTES` (comma-separated)
2. Config file: `./dagnats.yaml` in working directory (loaded if present)
3. Platform defaults

### Config File

```yaml
data_dir: /var/lib/dagnats
http_addr: ":8080"
nats_port: 4222
leaf_remotes:
  - nats://hub1.prod:7422
  - nats://hub2.prod:7422
```

- No file required — zero-config starts everything on defaults
- Fixed path `./dagnats.yaml` — no `--config` flag
- Invalid YAML → fatal error with line number
- Unknown keys → ignored (forward compatibility)

### Platform Defaults

| Field | Default |
|-------|---------|
| DataDir | macOS: `~/Library/Application Support/dagnats/`, Linux: `~/.local/share/dagnats/` |
| HTTPAddr | `:8080` |
| NATSPort | `4222` |
| LeafRemotes | empty (standalone mode) |

## Server Struct

```go
type Server struct {
    cfg    Config
    ns     *natsserver.Server
    nc     *nats.Conn
    orch   *engine.Orchestrator
    api    *api.Service
    trig   *trigger.TriggerService
    http   *http.Server
    tel    *observe.Telemetry
}
```

## Startup Order (in Run())

1. Resolve data dir (platform default if not set, create if missing)
2. Start embedded NATS server (standalone or leaf node)
3. Connect client to `nats://localhost:{port}`
4. `natsutil.SetupAll(nc)` — streams, KV buckets, telemetry stream
5. `simple.SetupTelemetry(nc)` — tracing, metrics, logging
6. `api.NewService(nc, tel)` — control plane
7. `engine.NewOrchestrator(nc, tel)` — workflow execution
8. `trigger.NewTriggerService(nc)` — cron/subject/webhook
9. `orch.Start()` + `trig.Start()` — subscribe to streams
10. Start HTTP server (non-blocking)
11. Block on SIGINT/SIGTERM

## Shutdown Order (reverse of startup)

1. HTTP server graceful shutdown (5s timeout)
2. `trig.Stop()` — unsubscribe triggers, stop scheduler
3. `orch.Stop()` — unsubscribe from history stream
4. Telemetry flush
5. `nc.Drain()` — flush pending NATS messages
6. `ns.Shutdown()` — stop embedded NATS

Sequential, deterministic, bounded. No goroutine leaks.

## Embedded NATS Setup

```go
opts := &natsserver.Options{
    Host:      "0.0.0.0",
    Port:      cfg.NATSPort,
    JetStream: true,
    StoreDir:  filepath.Join(cfg.DataDir, "jetstream"),
}

if len(cfg.LeafRemotes) > 0 {
    opts.LeafNode = natsserver.LeafNodeOpts{
        Remotes: remotes, // parsed from cfg.LeafRemotes
    }
}
```

`ReadyForConnections(5s)` blocks until the server is accepting clients.

## CLI Integration

`cli/serve.go` is thin:

```go
func runServeCmd(args []string) {
    cfg := server.ConfigFromEnv()
    srv := server.New(cfg)
    if err := srv.Run(); err != nil {
        fmt.Fprintf(os.Stderr, "error: %v\n", err)
        os.Exit(1)
    }
}
```

Added to the root dispatcher alongside `workflow`, `run`, `trigger`, `dlq`.

## Package Structure

```
server/
  server.go  — Server struct, New(), Run(), Stop()
  config.go  — Config struct, ConfigFromEnv(), YAML loading, platform defaults
  nats.go    — Embedded NATS server setup (standalone vs leaf)
```

## What This Does NOT Cover

- **Worker registration in-process**: workers are user code, always separate
  binaries connecting via NATS
- **Auth/TLS**: future concern, not blocking the single-binary story
- **Clustering between serve instances**: leaf node mode + hub handles this
