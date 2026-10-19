# DagNats

DAG-based workflow engine built on NATS for autonomous LLM coding pipelines.

## Design Philosophy

- **Ousterhout:** Minimize complexity. Deep modules with small interfaces hiding rich behavior. Pull complexity downward. Define errors out of existence.
- **TigerStyle:** Safety > Performance > DX. Zero technical debt. Assertions as contracts. Bounded everything. 70-line function limit.

## Language & Tools

- Go (module: github.com/danmestas/dagnats)
- NATS JetStream for streams, KV, object store
- `gofmt` for formatting
- `go vet` + `staticcheck` for linting

## Architecture

Five components: `dag/` (pure DAG logic), `internal/engine/` (orchestrator), `worker/` (task framework), `internal/api/` (control plane), `cli/` (CLI client). `internal/natsutil/` owns NATS resource setup; other packages may import `nats.go` for runtime operations.

`docs/architecture/` contains two kinds of files:

- **ADRs** (`adr-NNN-*.md`) — load-bearing decisions with context, alternatives, and consequences:
  - `adr-001-agent-harness-gaps.md` — interface gaps in the agent harness
  - `adr-002-durable-agent-loop.md` — durable agent loop via dagnats primitives
  - `adr-003-sidecar-dx-improvements.md` — sidecar DX improvements
  - `adr-004-lazy-orchestrator-subsystems.md` — lazy orchestrator subsystems
  - `adr-005` … `adr-024` — cluster mode, consumers, retry authority, HTTP triggers, console, trigger registrar, services, `dagnats.yaml` hot reload, WebSocket, agent runtimes, gated writes, `dagnatstest` coupling, coordination leases. `docs/architecture/README.md` indexes all of them.
- **Design notes** (everything else, e.g., `core-design.md`, `agent-system.md`) — background reading. May be superseded by later ADRs; check the file header for status.

When adding a new decision, write a numbered ADR. When taking notes that don't represent a decision, use a descriptive filename without the `adr-` prefix.

## Coding Rules

- All errors must be handled. No `_ = err`.
- Minimum 2 assertions per function for programmer errors (panic on invariant violations).
- No recursion. Iterative with explicit stack where needed.
- All loops and queues must have fixed upper bounds.
- Functions must not exceed 70 lines. Push `if`s up, `for`s down.
- Variables declared close to use. Smallest possible scope.
- Descriptive names, no abbreviations. Units/qualifiers last: `timeout_ms`, `retry_count_max`.
- Comments say WHY, not what. Code says what.
- No unnecessary dependencies. If you can write the 50 lines yourself, do it.
- Line length hard limit: 100 columns.

## Testing

- **Red-green TDD.** Write a failing test first, then the minimal code to pass it, then refactor.
- `dag/` package: pure unit tests, no NATS
- `internal/engine/`, `worker/`: integration tests with real embedded NATS server
- E2E: full workflow lifecycle with real workers
- Minimum 2 assertions per test (positive + negative space)
- Bounded timeouts on all test waits
- No shared NATS servers between tests
- Each test file opens with a methodology comment
- CI's `race` job runs `go test -race` over the concurrency-heavy packages (`bridge/`, `worker/`, `internal/engine/`, `internal/api/`, `internal/workertoken/`, `internal/natsutil/`); the fast `test` job in `ci.yml` does not run with `-race`.

## Observability

- **OpenTelemetry is the abstraction.** Packages instrument with the OTel API (`go.opentelemetry.io/otel` tracer and meter) and `log/slog`; no in-house wrapper interfaces.
- `observe/` owns the SDK setup (`InitTelemetry`): NATS-backed exporters by default (`observe/natsexporter`), OTLP when configured, and trace-context propagation through NATS headers and events.
- Vendors stay provider-agnostic by plugging in as OTel exporters or behind an OTLP collector (Sentry, Honeycomb, Grafana). No vendor SDK imports outside an exporter package.
- Observability is a first-class concern, not an afterthought.

## NATS-Native Patterns

Use NATS primitives instead of custom infrastructure:
- `NakWithDelay` for retries (no timer service)
- `AckWait` for worker claims; step timeouts are engine watchdog timers in `SLEEP_TIMERS`
- KV watches for cross-workflow signals (no bridge service)
- `Nats-Msg-Id` for dedup
- `micro` framework for internal API
