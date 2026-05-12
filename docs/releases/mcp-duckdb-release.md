# Releasing `dagnats-mcp-duckdb`

`dagnats-mcp-duckdb` is a sidecar MCP server backed by DuckDB. It is the only
dagnats binary that uses CGO (via `marcboeker/go-duckdb`); the main `dagnats`
binary stays pure-Go (CI-asserted). To keep both worlds happy, the MCP DuckDB
binary ships as its own per-platform release tarball alongside the main
`dagnats_<version>_<os>_<arch>.tar.gz` artifacts.

## What ships today

- `dagnats-mcp-duckdb-linux-amd64.tar.gz` — the production target.

Other platforms (linux/arm64, darwin/{amd64,arm64}) ship in a follow-up
release; the build pipeline is structured to make adding them mechanical.

## How the linux/amd64 build works

`make build-mcp-duckdb-linux-amd64` runs `go build` inside a
`golang:1.26-bookworm` container under `--platform linux/amd64`. CGO is
enabled and `go-duckdb` resolves to its default static-link build tag, so
`libduckdb.a` is folded into the binary. The resulting binary needs only
standard glibc, libstdc++, libm, libdl, and libgcc_s — present on every
Ubuntu/Debian/RHEL release.

Host requirements: Docker. No DuckDB toolchain, no Go, no CGO.

## When to bump the bundled DuckDB version

The DuckDB version is pinned via `cmd/dagnats-mcp-duckdb/go.mod`'s
`marcboeker/go-duckdb` dependency. To bump:

1. `cd cmd/dagnats-mcp-duckdb`
2. `go get github.com/marcboeker/go-duckdb@<new-version>`
3. `go mod tidy`
4. `make build-mcp-duckdb-linux-amd64`
5. Smoke-test on `ubuntu:24.04`:
   ```sh
   docker run --rm --platform linux/amd64 -v "$PWD/dist:/dist" \
     ubuntu:24.04 sh -c 'cd /tmp && \
       tar xzf /dist/dagnats-mcp-duckdb-linux-amd64.tar.gz && \
       cd dagnats-mcp-duckdb-linux-amd64 && \
       ldd ./dagnats-mcp-duckdb && \
       ./dagnats-mcp-duckdb --help'
   ```
6. Commit the `go.mod` / `go.sum` bump and a CHANGELOG entry.

## Version-tracking convention

The sidecar's `defaultMCPDuckDBVersion` constant in `sidecar/install.go`
tracks the dagnats release tag, not the underlying DuckDB version. The
tarball is published into the dagnats GitHub release for that tag and the
sidecar downloads it via the standard `knownBinaries` URL template. Bump
`defaultMCPDuckDBVersion` whenever the dagnats release tag bumps.

## Soft-optional safety net

If the binary fails to download (e.g., an asset hasn't been published for
the current dagnats version yet) and `go` is not on PATH for a local
build, the sidecar logs a notice and continues without MCP DuckDB queries.
See `sidecar/supervisor.go` and PR #187.
