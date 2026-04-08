# Sidecar DX Improvements

Date: 2026-04-06

## Summary

Eight improvements to the sidecar OTel collector, prioritized by the DX audit
scoring the sidecar at 5.3/10. Split into two phases: trivial fixes (phase 1)
and structural work (phase 2).

## Phase 1: Trivial Fixes

### 1. Print env vars in startup banner

Add an export hint to `printStartBanner` after the collector address line:

```
  Export:      export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
```

Replace `0.0.0.0` with `localhost` in the hint since apps connect locally.

**Files:** `cli/sidecar.go`

### 2. Error on unknown subcommand

Change the `default:` case in `runSidecarCmd` from falling through to
`runSidecarStartCmd(args)` to printing an error, showing usage, and exiting 1.
Matches the pattern in `runObserveCmd`.

**Files:** `cli/sidecar.go`

### 3. Reject unknown YAML keys

In `LoadConfig`, replace `yaml.Unmarshal` with `yaml.NewDecoder` using
`KnownFields(true)`. Unknown keys become a parse error with a clear message
naming the offending field.

**Files:** `sidecar/config.go`

### 4. Show backend forwarding in banner

Add a conditional line in `printStartBanner` when `cfg.Backend != nil`:

```
  Backend:     https://otel.example.com (forwarding)
```

**Files:** `cli/sidecar.go`

## Phase 2: Structural Work

### 5. Supervisor health endpoint

The supervisor gets a small HTTP server exposing internal state.

**Config:** New `SupervisorConfig` struct and field in `SidecarConfig`:

```go
type SupervisorConfig struct {
    Listen string `yaml:"listen"`
}
```

Added to `SidecarConfig`:

```go
Supervisor SupervisorConfig `yaml:"supervisor"`
```

`DefaultConfig()` sets `Supervisor.Listen` to `"localhost:4320"`.
`Validate()` checks that `Supervisor.Listen` is not empty.

YAML representation:

```yaml
supervisor:
  listen: "localhost:4320"
```

**Endpoint:** `GET /healthz` returns JSON:

```json
{
  "status": "ok",
  "uptime_seconds": 3621,
  "processes": [
    {
      "name": "otlp2parquet",
      "status": "running",
      "pid": 12345,
      "restarts": 0,
      "uptime_seconds": 3621
    },
    {
      "name": "otelcol",
      "status": "running",
      "pid": 12346,
      "restarts": 1,
      "uptime_seconds": 842
    },
    {
      "name": "dagnats-mcp-duckdb",
      "status": "running",
      "pid": 12347,
      "restarts": 0,
      "uptime_seconds": 3620
    }
  ],
  "storage": {
    "path": "./telemetry-data",
    "type": "local"
  }
}
```

**Supervisor changes:**
- Add `startedAt time.Time` to `Supervisor`, set in `Start()`
- Add `startedAt time.Time` to `Process`, set in `Start()`
- Add `restarts int` to `Process`, incremented in `RestartWithBackoff()`
- `Supervisor.Run()` starts the health HTTP server before the signal wait loop,
  shuts it down on stop with a `5 * time.Second` context deadline on
  `http.Server.Shutdown()`
- Handler reads `s.cfg.Storage` for storage info and iterates `s.processes`
  for per-process state (already has `IsRunning()`, PID via `cmd.Process.Pid`)
- Bounded: `ReadTimeout: 5s`, `WriteTimeout: 5s`, `MaxHeaderBytes: 1 << 16`
  on `http.Server`

**Banner update:** `printStartBanner` adds a health endpoint line:

```
  Health:      http://localhost:4320/healthz
```

**Updated `sidecar status` command:**
- Probes `GET /healthz` on the configured supervisor address
- If reachable: prints per-process table:
  ```
  Sidecar running (uptime: 1h0m21s)
    NAME                STATUS   PID    RESTARTS  UPTIME
    otlp2parquet        running  12345  0         1h0m21s
    otelcol             running  12346  1         14m2s
    dagnats-mcp-duckdb  running  12347  0         1h0m20s
  Storage: ./telemetry-data (local)
  ```
- If unreachable: falls back to binary-exists check (current behavior) with
  "sidecar not running" message
- Supports `--json` flag (passes through the healthz JSON)
- Updates `printSidecarUsage()` to document `--json` flag

**Updated `observe status` sidecar section:**
- Switches from raw TCP probe to hitting `/healthz` for richer data
  ("running, 3 processes healthy" vs "detected")

**Files:** `sidecar/config.go`, `sidecar/supervisor.go`, `sidecar/process.go`,
`cli/sidecar.go`, `cli/observe_status.go`

### 6. `sidecar init` command

New subcommand that writes a minimal `dagnats.yaml` to the current directory.
All fields commented out with defaults shown:

```yaml
# Sidecar configuration — uncomment to override defaults.
# listen: 0.0.0.0:4318
# supervisor:
#   listen: localhost:4320
# storage:
#   type: local
#   local_path: ./telemetry-data
# backend:
#   endpoint: https://otel.example.com
#   headers:
#     Authorization: Bearer <token>
# mcp:
#   listen: ""  # empty = stdio
```

Refuses to overwrite if `dagnats.yaml` already exists (prints error, exits 1).
No flags.

**Files:** `cli/sidecar.go`

### 7. Include `dagnats-mcp-duckdb` in install and status

The MCP DuckDB binary is built from `cmd/dagnats-mcp-duckdb/` in this repo,
not downloaded externally. Different install strategy than the download-based
binaries.

`InstallAll()` currently iterates `knownBinaries` and calls `Install()` which
downloads tarballs. The go-build path is handled separately:

- Add a new `BuildLocal(name, pkg string)` function in `sidecar/install.go`
  that runs `go build -o ~/.dagnats/bin/<name> <pkg>`
- `InstallAll()` gets a second loop after the download loop that handles
  local builds. A `localBinaries` map (or slice of structs) lists
  `{"dagnats-mcp-duckdb", "./cmd/dagnats-mcp-duckdb/"}`
- `FindBinary` already searches `~/.dagnats/bin/`, so no changes needed there
- `sidecar status` includes `dagnats-mcp-duckdb` in the binary check list
- `checkBinariesAvailable()` includes it with the same hint

**Files:** `sidecar/install.go`, `cli/sidecar.go`

### 8. `sidecar start --dry-run`

Parses `--dry-run` flag in `runSidecarStartCmd`. Loads config, validates,
generates collector YAML (all existing code paths). Prints the generated YAML
to stdout, then exits without starting processes.

**Files:** `cli/sidecar.go`

## Testing Strategy

All items follow red-green TDD per project rules.

**Phase 1 tests:**
- Banner output contains `OTEL_EXPORTER_OTLP_ENDPOINT` string
- Unknown subcommand triggers exit code 1, not start
- Config with unknown key returns parse error
- Banner with backend config shows endpoint, without backend omits line

**Phase 2 tests:**
- Health endpoint returns valid JSON with expected schema
- Health endpoint reflects actual process state (running/stopped)
- `sidecar status` with health endpoint reachable shows process table
- `sidecar status` with health endpoint unreachable falls back to binary check
- `sidecar init` creates `dagnats.yaml` in current directory
- `sidecar init` refuses to overwrite existing file
- `sidecar install` builds `dagnats-mcp-duckdb` from source
- `sidecar status` includes `dagnats-mcp-duckdb` in output
- `--dry-run` prints collector YAML and does not start processes
- `SupervisorConfig` defaults to `localhost:4320`
- `Validate()` accepts new config fields

## Non-Goals

- Remote sidecar management (health endpoint is localhost only)
- Log persistence for child processes (future work)
- Version pinning in config (noted in audit, not included in this batch)
- Data flow metrics ("last span received at") — depends on otlp2parquet
  exposing stats, out of scope
