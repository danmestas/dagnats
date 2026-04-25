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

Five components: `dag/` (pure DAG logic), `engine/` (orchestrator), `worker/` (task framework), `api/` (control plane), `cli/` (CLI client). `natsutil/` owns NATS resource setup; other packages may import `nats.go` for runtime operations.

`docs/architecture/` contains two kinds of files:

- **ADRs** (`adr-NNN-*.md`) — load-bearing decisions with context, alternatives, and consequences:
  - `adr-001-agent-harness-gaps.md` — interface gaps in the agent harness
  - `adr-002-durable-agent-loop.md` — durable agent loop via dagnats primitives
  - `adr-003-sidecar-dx-improvements.md` — sidecar DX improvements
  - `adr-004-lazy-orchestrator-subsystems.md` — lazy orchestrator subsystems
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
- `engine/`, `worker/`: integration tests with real embedded NATS server
- E2E: full workflow lifecycle with real workers
- Minimum 2 assertions per test (positive + negative space)
- Bounded timeouts on all test waits
- No shared NATS servers between tests
- Each test file opens with a methodology comment

## Observability

- All observability interfaces must be **provider-agnostic**. Define interfaces in-house, implement adapters separately.
- Error reporting: interface now, Sentry adapter later. No direct Sentry imports outside the adapter.
- Structured logging, tracing, and metrics should follow the same pattern: interface + adapter.
- Observability is a first-class concern, not an afterthought.

## NATS-Native Patterns

Use NATS primitives instead of custom infrastructure:
- `NakWithDelay` for retries (no timer service)
- `AckWait` + `MaxDeliver` for timeouts
- KV watches for cross-workflow signals (no bridge service)
- `Nats-Msg-Id` for dedup
- `micro` framework for internal API
