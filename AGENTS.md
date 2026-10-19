# AGENTS.md

DagNats is a DAG-based workflow engine on NATS JetStream. Go module: `github.com/danmestas/dagnats`.

## Commands

| Action | Command |
|---|---|
| Build | `make build` |
| Test | `make test` (real embedded NATS, no mocks; `-p 4`, 600s timeout) |
| Lint | `make lint` (gofmt check + vet + staticcheck, matches CI) |
| Format | `make fmt` |
| Run server | `make serve` (builds, then `./bin/dagnats serve`) |
| Regenerate SDK API docs | `make docs-gen-sdk` (needs `gomarkdoc` v1.1.0) |
| Validate llms.txt generation | `make docs-check-llms` |

Always run `make test` and `make lint` before opening a PR. CI also runs `go test -race` over the
concurrency-heavy packages (`.github/workflows/race.yml`) and fails if `make docs-gen-sdk` changes
the tree.

## Architecture

Five core Go packages:

- `dag/` — pure DAG logic (types, builder, validation, resolution). No NATS imports.
- `internal/engine/` — orchestrator. Consumes events, advances DAGs, manages concurrency, retries, and timeouts.
- `worker/` — worker SDK. `TaskContext` is the deep interface (Input/Complete/Fail/Continue/Heartbeat/Checkpoint/LoadCheckpoint/WaitForSignal/SendSignal, plus log writers and a control-plane handle).
- `internal/api/` — REST + NATS micro control plane.
- `cli/` — command-line client.

Plus `internal/natsutil/` (NATS resource setup, owns the embedded test server), `server/` (embedded `dagnats serve`), `bridge/` (HTTP worker bridge), `protocol/` (wire types), `observe/` (provider-agnostic telemetry interfaces), `internal/lease/` (coordination leases, ADR-024), `internal/trigger/` (cron, event, webhook, HTTP triggers), `internal/console/` (control-plane UI), `ci/` (`.dagnats/ci.yml` compiler), `sdk/httpclient/` (reference HTTP worker client), `dagnatstest/` (test harness for consumers), `dagnatsext/` (public types shared with the worker SDK), `cmd/` (the `dagnats`, `dagnats-api`, `dagnats-engine` binaries).

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
- `internal/engine/`, `worker/` use a real embedded NATS server per test (`natsutil.StartTestServer(t)`).
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
| Step timeout | engine watchdog timer in `SLEEP_TIMERS` (`AckWait` is only the worker's claim) |
| Cross-workflow signal | KV watch |
| Exactly-once delivery | `Nats-Msg-Id` dedup |
| Internal API | `micro` framework |
| DLQ | dedicated `DEAD_LETTERS` stream |

## Where things live

- ADRs: `docs/architecture/adr-NNN-*.md`. Numbered, load-bearing decisions.
- Design notes: `docs/architecture/*.md` (no `adr-` prefix). Background, may be superseded.
- Engineering reference: `docs/*.md` (`getting-started`, `configuration`, `production`, `observability`, `workflow-schema`, `wire-protocol`, `console`).
- Hugo published site: `docs/site/content/docs/`. Independent tree from `docs/*.md`.
- LLM-facing docs: `docs/site/static/llms.txt` and `llms-full.txt` are generated (git-ignored) from the Hugo content by `scripts/gen-llms-txt.sh` during the Cloudflare build. The curated top of `llms.txt` is `docs/site/llms-header.md`; edit that, never the generated files. Every content page needs a frontmatter `title`; only the shortcodes `cards`/`card`, `callout`, `tabs`/`tab` and `ref` are supported. `scripts/check-llms-txt.sh` (CI) enforces this.
- Examples: `examples/` (try `cd examples/hello-world && go run .`).

When making a load-bearing decision, write a numbered ADR. Otherwise drop a design note with a descriptive filename.

## Doc-tree gotcha

`docs/*.md` (engineering reference) and `docs/site/content/docs/` (Hugo published site) are **separate trees**. Editing one does not update the other. Decide which audience you're writing for.

## Pre-merge checklist

- [ ] `make test` passes
- [ ] `make lint` clean
- [ ] `gofmt -w .` applied
- [ ] If you changed a public Go API: `make docs-gen-sdk` and commit the result
- [ ] If you touched `docs/site/content` or `llms-header.md`: `make docs-check-llms` passes
- [ ] New function ≤ 70 lines, ≥ 2 assertions
- [ ] New test ≥ 2 assertions, bounded timeout
- [ ] If load-bearing decision: ADR written
