---
title: server
weight: 6
---

```
import "github.com/danmestas/dagnats/server"
```

Embedded NATS server with the full DagNats stack: engine, API, bridge, and trigger runtime. Provides programmatic startup for single-binary deployment.

## Key Types

| Type | Description |
|------|-------------|
| `Config` | Server configuration: data directory, ports, leaf node remotes, store limits |
| `Server` | The assembled server: embedded NATS, engine, HTTP API, bridge |

## Key Functions

| Function | Description |
|----------|-------------|
| `New(cfg)` | Creates a new server from configuration |
| `ConfigFromEnv()` | Loads configuration from `dagnats.yaml` and environment variables |
| `EmbeddedWorker(srv)` | Returns a `*worker.Worker` connected to the embedded server's NATS |

## Configuration Sources

Configuration is resolved in order (later sources override):

1. Built-in defaults
2. `dagnats.yaml` in the current directory
3. Environment variables

| Environment Variable | Config Field | Default |
|---------------------|-------------|---------|
| `DAGNATS_DATA_DIR` | `DataDir` | `/tmp/dagnats` |
| `DAGNATS_HTTP_ADDR` | `HTTPAddr` | `:8080` |
| `DAGNATS_NATS_PORT` | `NATSPort` | `4222` |
| `DAGNATS_LEAF_REMOTES` | `LeafRemotes` | (none) |
| `DAGNATS_LEAF_CREDENTIALS` | `LeafCredentials` | (none) |
| `DAGNATS_MAX_STORE_BYTES` | `MaxStoreBytes` | `1073741824` (1 GiB) |
| `DAGNATS_MONITOR_PORT` | `MonitorPort` | (disabled) |

## Server Lifecycle

1. `New(cfg)` creates the server and starts the embedded NATS instance
2. Optionally attach embedded workers with `EmbeddedWorker(srv)`
3. `Run()` starts the engine, API, bridge, and blocks until SIGINT/SIGTERM
4. Graceful shutdown stops all components in reverse order

## Usage

```go
cfg := server.ConfigFromEnv()
srv := server.New(cfg)

// Optional: register embedded workers
w := server.EmbeddedWorker(srv)
w.Handle("process", myHandler)

// Blocks until signal
if err := srv.Run(); err != nil {
    log.Fatal(err)
}
```
