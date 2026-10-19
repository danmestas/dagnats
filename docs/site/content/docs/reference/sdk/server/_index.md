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
| `New(cfg)` | Creates a new server from configuration. It does not start anything. |
| `DefaultConfig()` | Built-in defaults |
| `ConfigFromEnv()` | Loads configuration from `dagnats.yaml` and environment variables |
| `EmbeddedWorker(srv)` | Returns a `*WorkerShim` to register handlers on before `Run()` |
| `(*Server).Run()` | Starts NATS, the engine, API, bridge and triggers, and blocks until shutdown |
| `(*Server).HTTPAddr()` | The bound HTTP address once `Run` has finished starting |

## Configuration Sources

Configuration is resolved in order (later sources override):

1. Built-in defaults
2. `dagnats.yaml` in the current directory
3. Environment variables

| Environment Variable | Config Field | Default |
|---------------------|-------------|---------|
| `DAGNATS_DATA_DIR` | `DataDir` | `~/Library/Application Support/dagnats` on macOS, `$XDG_DATA_HOME/dagnats` or `~/.local/share/dagnats` on Linux |
| `DAGNATS_HTTP_ADDR` | `HTTPAddr` | `127.0.0.1:8080` |
| `DAGNATS_NATS_PORT` | `NATSPort` | `4222` |
| `DAGNATS_LEAF_REMOTES` | `LeafRemotes` | (none) |
| `DAGNATS_LEAF_CREDENTIALS` | `LeafCredentials` | (none) |
| `DAGNATS_MAX_STORE_BYTES` | `MaxStoreBytes` | derived from free disk space |
| `DAGNATS_MAX_MEMORY_BYTES` | `MaxMemoryBytes` | 1 GiB |
| `DAGNATS_MONITOR_PORT` | `MonitorPort` | (disabled) |

The server has more settings (cluster, WebSocket, limits). See the configuration reference for the full list.

## Server Lifecycle

1. `New(cfg)` builds the server object
2. Optionally register embedded workers with `EmbeddedWorker(srv)`
3. `Run()` starts embedded NATS, the engine, API, bridge, triggers and your workers, then blocks until SIGINT/SIGTERM
4. Graceful shutdown stops all components

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
