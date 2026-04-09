# ADR-003: Sidecar DX Improvements

**Status:** Implemented  
**Date:** 2026-04-07  
**PR:** #117  

## Context

DX audit scored the sidecar OTel collector at 5.3/10. Common pain points:
no feedback on startup, silent failures on typos, no health visibility,
no config scaffolding. Eight improvements split into two phases.

## Decisions

### Phase 1: Trivial Fixes

1. **Print OTEL export hint in startup banner.** Replace `0.0.0.0` with
   `localhost` so users can copy-paste `export OTEL_EXPORTER_OTLP_ENDPOINT`.
2. **Error on unknown subcommand.** `dagnats sidecar bogus` now prints
   error + usage and exits 1 instead of silently falling through to start.
3. **Reject unknown YAML keys.** `LoadConfig` uses `yaml.NewDecoder` with
   `KnownFields(true)` so typos in `dagnats.yaml` produce a clear error.
4. **Show backend forwarding in banner.** Conditional line when
   `cfg.Backend != nil`.

### Phase 2: Structural Work

5. **Supervisor health endpoint.** `GET /healthz` on `localhost:4320`
   returns JSON with uptime, per-process status (name, PID, restarts,
   uptime), and storage info. Bounded HTTP server (5s read/write timeout).
6. **`sidecar init` command.** Scaffolds a commented-out `dagnats.yaml`
   template. Refuses to overwrite existing file.
7. **Include dagnats-mcp-duckdb in install/status.** `sidecar install`
   builds from in-repo Go source via `BuildLocal`. Status command probes
   health endpoint with binary-detection fallback. `--json` flag for
   machine-readable output.
8. **`--dry-run` flag on sidecar start.** Prints generated collector
   YAML to stdout without starting processes.

## Consequences

- Sidecar DX target: ~8/10.
- Health endpoint enables monitoring without external tooling.
- `sidecar init` eliminates the "blank config" cold-start problem.
- `--dry-run` helps debug collector config without side effects.
