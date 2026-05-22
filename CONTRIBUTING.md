# Contributing to dagnats

Thanks for your interest in `dagnats`, a workflow orchestration engine
combining DAG-based task graphs with NATS-backed coordination. This document
covers everything you need to get a working checkout, run the tests, and
submit changes.

## Development setup

Requires Go 1.26 or newer.

```
git clone https://github.com/danmestas/dagnats
cd dagnats
make build
```

`make build` compiles the CLI, server, and worker binaries. The repository
ships with an embedded NATS server, so no external broker is required for
local development.

## Running tests

```
make test       # full test suite
make vet        # go vet
make lint       # gofmt + vet + staticcheck (matches CI)
make fmt        # gofmt + goimports
make serve      # build and run the dagnats server
```

`make test` is the canonical pre-PR gate — the same checks run in CI.

## Code layout

- `cmd/`, `cli/` — CLI entry points.
- `dag/` — DAG validation and execution engine.
- `actor/` — actor runtime with checkpoint/heartbeat semantics.
- `worker/` — task execution worker.
- `server/`, `bridge/`, `sidecar/` — server and bridge components.
- `protocol/` — wire-protocol definitions.
- `sdk/` — client SDK.
- `observe/` — instrumentation hooks.
- `internal/` — non-exported implementation packages.
- `examples/` — usage examples.
- `e2e/`, `dagnatstest/`, `testutil/` — end-to-end tests and shared fixtures.

## Submitting changes

1. Open a feature branch off `main`. Direct commits to `main` are not accepted.
2. Run `make test` and `make lint` locally before pushing.
3. Open a PR; CI will re-run the full suite.

## Reporting issues

Open an issue at https://github.com/danmestas/dagnats/issues with
reproduction steps, expected vs. actual behavior, and a minimal repro
where possible.
