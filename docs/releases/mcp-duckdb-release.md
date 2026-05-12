# Releasing `dagnats-mcp-duckdb`

`dagnats-mcp-duckdb` is a sidecar MCP server backed by DuckDB. It is the only
dagnats binary that uses CGO (via `marcboeker/go-duckdb`); the main `dagnats`
binary stays pure-Go (CI-asserted). To keep both worlds happy, the MCP DuckDB
binary ships as its own per-platform release tarball alongside the main
`dagnats_<version>_<os>_<arch>.tar.gz` artifacts.

## What ships today

- `dagnats-mcp-duckdb-linux-amd64.tar.gz` — the production target.
- `dagnats-mcp-duckdb-linux-arm64.tar.gz` — Linux servers on arm64.
- `dagnats-mcp-duckdb-darwin-amd64.tar.gz` — macOS Intel dev hosts.
- `dagnats-mcp-duckdb-darwin-arm64.tar.gz` — macOS Apple silicon dev hosts.

All four are statically linked against `libduckdb.a` shipped inside
`marcboeker/go-duckdb` v1.8.x's `deps/` tree. No bundled shared library,
no rpath/`@loader_path` gymnastics — the binary only links against
platform-standard runtime libs.

## How the builds work

### linux (amd64, arm64)

`make build-mcp-duckdb-linux-{amd64,arm64}` runs `go build` inside a
`golang:1.26-bookworm` container under `--platform linux/<arch>`. CGO is
enabled and `go-duckdb` resolves to its default static-link build tag, so
`libduckdb.a` is folded into the binary. The resulting binary needs only
standard glibc, libstdc++, libm, libdl, and libgcc_s — present on every
Ubuntu/Debian/RHEL release.

Host requirements: Docker (with qemu-user installed for cross-arch builds
when the host is amd64-only; Docker Desktop bundles it).

### darwin (amd64, arm64)

`make build-mcp-duckdb-darwin-{amd64,arm64}` runs natively on a macOS host
with `CGO_ENABLED=1`, `CC="clang -arch <arch>"`, and
`MACOSX_DEPLOYMENT_TARGET=11.0`. The CI release pipeline builds both
slices on a single `macos-latest` runner (arm64 native, amd64
cross-compile) to keep macOS minutes low.

Host requirements: macOS with Xcode CommandLineTools (clang + the macOS
SDK). No DuckDB toolchain, no Homebrew install.

The resulting binaries link against `/usr/lib/libc++`,
`/usr/lib/libSystem`, `CoreFoundation.framework`, and `Security.framework`
— all present on every macOS 11.0+ install.

## When to bump the bundled DuckDB version

The DuckDB version is pinned via `cmd/dagnats-mcp-duckdb/go.mod`'s
`marcboeker/go-duckdb` dependency. To bump:

1. `cd cmd/dagnats-mcp-duckdb`
2. `go get github.com/marcboeker/go-duckdb@<new-version>`
3. `go mod tidy`
4. Build all four artifacts:
   ```sh
   make build-mcp-duckdb-linux-amd64
   make build-mcp-duckdb-linux-arm64
   make build-mcp-duckdb-darwin-amd64   # macOS host only
   make build-mcp-duckdb-darwin-arm64   # macOS host only
   # or, on a macOS dev host with Docker running:
   make build-mcp-duckdb-all
   ```
5. Smoke-test linux/{amd64,arm64} on `ubuntu:24.04`:
   ```sh
   for arch in amd64 arm64; do
     docker run --rm --platform linux/$arch -v "$PWD/dist:/dist" \
       ubuntu:24.04 sh -c "cd /tmp && \
         tar xzf /dist/dagnats-mcp-duckdb-linux-$arch.tar.gz && \
         cd dagnats-mcp-duckdb-linux-$arch && \
         ldd ./dagnats-mcp-duckdb && \
         ./dagnats-mcp-duckdb --help"
   done
   ```
6. Smoke-test darwin/{amd64,arm64} on the macOS dev host (the arm64
   binary runs natively; the amd64 one runs under Rosetta if installed):
   ```sh
   for arch in arm64 amd64; do
     tar xzf "dist/dagnats-mcp-duckdb-darwin-$arch.tar.gz" -C /tmp
     "/tmp/dagnats-mcp-duckdb-darwin-$arch/dagnats-mcp-duckdb" --help
   done
   ```
7. Commit the `go.mod` / `go.sum` bump and a CHANGELOG entry.

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
