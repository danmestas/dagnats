# AGENTS.md

DagNats is a DAG-based workflow engine on NATS JetStream. Go module: `github.com/danmestas/dagnats`.

## Commands

| Action | Command |
|---|---|
| Build | `make build` |
| Test | `make test` (real embedded NATS, no mocks; 120s timeout) |
| Lint | `make lint` (vet + staticcheck) |
| Format | `make fmt` |
| Run server | `make serve` |

Always run `make test` and `make lint` before opening a PR.

## Architecture

Five Go packages under the repo root:

- `dag/` — pure DAG logic (types, builder, validation, resolution). No NATS imports.
- `engine/` — actor-based orchestrator. Consumes events, advances DAGs, manages concurrency.
- `worker/` — worker SDK. `TaskContext` is the deep interface (Input/Complete/Fail/Continue/Heartbeat/Checkpoint/WaitForSignal).
- `api/` — REST + NATS micro control plane.
- `cli/` — command-line client.

Plus `natsutil/` (NATS resource setup, owns embedded test server), `protocol/` (wire types), `observe/` (provider-agnostic telemetry interfaces).

All persistent state lives in NATS JetStream and KV. There is no external database.

## Coding rules (TigerStyle, hard requirements)

- All errors handled. No `_ = err`.
- Functions ≤ 70 lines. Push `if`s up, `for`s down.
- Min 2 assertions per function for programmer errors. Panic on invariant violations.
- No recursion. Iterative with explicit stack.
- All loops and queues have fixed upper bounds.
- Line length ≤ 100 columns.
- Variables declared close to use, smallest scope.
- Descriptive names, units last: `timeout_ms`, `retry_count_max`.
- Comments say WHY, not what. Code says what.
- No new dependencies if a 50-line in-house implementation suffices.

## Testing rules (red-green TDD, hard)

- Failing test first → minimal code to pass → refactor.
- `dag/` is pure unit tests, no NATS.
- `engine/`, `worker/` use a real embedded NATS server per test (`natsutil.StartTestServer(t)`).
- Min 2 assertions per test (positive + negative space).
- Bounded timeouts on every wait. No `time.Sleep` without a deadline.
- Each test file opens with a methodology comment.

## Observability rules

- Interfaces in `observe/` are provider-agnostic.
- Vendor adapters (Sentry, OTel exporters) live in separate packages. No vendor imports outside the adapter.
- Logging, tracing, metrics, error reporting all follow the interface + adapter pattern.

## NATS-native patterns

Use NATS primitives instead of building infrastructure:

| Need | Primitive |
|---|---|
| Retry with backoff | `NakWithDelay` |
| Step timeout | `AckWait` + `MaxDeliver` |
| Cross-workflow signal | KV watch |
| Exactly-once delivery | `Nats-Msg-Id` dedup |
| Internal API | `micro` framework |
| DLQ | dedicated stream |

## Where things live

- ADRs: `docs/architecture/adr-NNN-*.md`. Numbered, load-bearing decisions.
- Design notes: `docs/architecture/*.md` (no `adr-` prefix). Background, may be superseded.
- User docs: `docs/{getting-started,configuration,production,observability,workflow-schema}.md`.
- Hugo published site: `docs/site/content/docs/`. Independent tree from `docs/*.md`.
- Examples: `examples/` (try `cd examples/hello-world && go run .`).

When making a load-bearing decision, write a numbered ADR. Otherwise drop a design note with a descriptive filename.

## Doc-tree gotcha

`docs/*.md` (engineering reference) and `docs/site/content/docs/` (Hugo published site) are **separate trees**. Editing one does not update the other. Decide which audience you're writing for.

## Pre-merge checklist

- [ ] `make test` passes
- [ ] `make lint` clean
- [ ] `gofmt -w .` applied
- [ ] New function ≤ 70 lines, ≥ 2 assertions
- [ ] New test ≥ 2 assertions, bounded timeout
- [ ] If load-bearing decision: ADR written
