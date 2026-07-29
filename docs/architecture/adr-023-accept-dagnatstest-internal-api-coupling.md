# ADR-023: Accept the `dagnatstest` → `internal/api` coupling (no split, for now)

**Status:** Accepted (2026-07-29, #601)
**Deciders:** Dan Mestas

## Context

`dagnatstest` (the canonical test-helper package, 12+ importers) transitively
depends on `internal/api` — `dlq_fixture.go`, `harness.go`, `helpers.go`, and
`setup.go` all import it directly. `internal/api/natsapi.go` in turn imports
`observe`.

This creates a real import cycle for any in-package test in `observe`
(`package observe`, not `package observe_test`) that tries to import
`dagnatstest`: `observe(test) → dagnatstest → internal/api → observe`.

Discovered while implementing #599's item 4 (folding the one-consumer
`testutil` package): `dagnatstest` was the natural target for the fold, but
importing it from `observe/collect_test.go` failed with `import cycle not
allowed in test`, reproduced directly by temporarily adding the import and
running `go vet`. The #599 fold worked around it by co-locating the moved
helper in `observe`'s own test files instead, rather than in `dagnatstest`.

## Decision

Do not split `dagnatstest` to break this coupling right now. No live consumer
is currently blocked by it — the one case that hit the constraint (#599 item
4) found a working alternative without needing the split. Breaking the
coupling (e.g. a leaner `dagnatstest` core plus a separate
`dagnatstest/apifixtures` sub-package for the `internal/api`-dependent
helpers) is real design work that locks in a shape across `dagnatstest`'s
12+ importers — exactly the kind of change that shouldn't happen
speculatively, without a concrete driver.

## Consequences

- `dagnatstest` remains unusable from any `observe` in-package test
  (`package observe`). A future case that hits this wall has two paths: split
  `dagnatstest` at that point (with a concrete need driving the design), or
  find another co-location workaround, per #599's precedent.
- The constraint is documented at the import site (`dagnatstest`'s package
  doc, see `cli_fixture.go`/`harness.go`) so a future contributor investigating
  the same failure doesn't have to rediscover it from scratch.

## Alternatives Considered

**Split `dagnatstest` now, preemptively.** Rejected. There is no second
consumer waiting on this; speculative architecture work with no concrete
driver tends to guess wrong about the actual shape needed once a real second
case shows up.
