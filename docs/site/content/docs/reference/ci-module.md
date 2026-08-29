---
title: CI Module Reference
weight: 6
---

Reference for the `ci` package and the mounted `/v1/ci/{compile,validate}`
control-plane endpoints (#633, superseding the API half of #631).

## What this is

`.dagnats/ci.yml` is a small, declarative CI spec: a set of named checks
(each backed by a Dagger function call), an optional deploy step, and an
optional durable human-approval gate before deploy. The `ci` package
(`github.com/danmestas/dagnats/ci`, module root) parses that YAML and
compiles it into a `dag.WorkflowDef` the DagNats engine can run.

`ci` lives at the module root rather than under `internal/` for one
concrete reason: the `dagnats-ci` add-on is a *separate* Go module
(`github.com/danmestas/dagnats-ci`, `replace github.com/danmestas/dagnats
=> ../`) so it can eventually be extracted into its own repository, and Go
does not let one module import another module's `internal/` tree. This is
the same shape as the `openapi/` package's promotion (#614) — a small,
stdlib-leaning public package at the root, used by both the core module and
a nested consumer.

**Core DagNats stays spec-agnostic.** `POST /workflows` (see
[REST API Reference](../rest-api)) only ever accepts a `dag.WorkflowDef` — it
has no idea `ci.yml` exists. The two `/v1/ci/*` endpoints below are the only
place `.dagnats/ci.yml` awareness enters the control plane; they compile a
spec into a `dag.WorkflowDef` and hand it to the exact same
`RegisterWorkflowWithWarnings` path `POST /workflows` uses. The worker set
that actually executes `dagger.call` and `ci.approval` steps (the Dagger
caller, the Dagster adapter) is out of scope for this package and endpoint
pair — see the tracking issue for that follow-up.

## The `ci.yml` spec

```yaml
on:
  pull_request: { branches: [main] }
  push:         { branches: [main] }
  schedule:     { cron: "0 6 * * *" }   # DagNats cron trigger, not GitHub Actions

defaults:
  module: "."          # Dagger module path in the repository

checks:
  test:  { call: "test" }
  lint:  { call: "lint" }
  build: { call: "build", needs: [test, lint], timeout: "20m" }

deploy:
  call: "publish"
  needs: [build]
  approval: required   # durable human gate before deploy
```

Each check compiles to a `dagger.call` step; `approval: required` inserts an
`approve-deploy` step (`ci.approval` task) ahead of the deploy step so the
engine waits for a human signal instead of deploying unattended. `branches:`
under `deploy:` is rejected — it isn't implemented yet and a silent no-op
would be worse than an explicit diagnostic.

## Diagnostics, not fail-fast errors

Unlike the pre-#633 compiler (which returned the first `error` it hit),
`ci.Parse` and `ci.Compile` accumulate every problem they find into a
`[]Diagnostic` and keep going, so an author fixes every mistake in one pass
instead of one `ci.yml` push per error:

```go
type Diagnostic struct {
	Line    int    `json:"line,omitempty"`
	Column  int    `json:"column,omitempty"`
	Field   string `json:"field,omitempty"`
	Message string `json:"message"`
}
```

- `Line` / `Column` are 1-based YAML source positions. `Parse` decodes via
  `yaml.Node` first specifically to keep these — they're 0 for a diagnostic
  raised later, inside `Compile`, once the original bytes are no longer
  available (an unknown `needs` target, an invalid timeout, a `dag.Validate`
  failure). That's the "best available" position, not a missing feature.
- `Field` names the offending top-level field (`Parse`) or check/deploy step
  (`Compile`) when known.
- Accumulation is bounded by `DiagnosticsMax` (100): once reached, one final
  `"too many errors"` diagnostic is appended and every further problem is
  silently dropped, so a pathological spec can't produce an unbounded
  response body.

Three entry points:

```go
func Parse(spec []byte) (Spec, []Diagnostic)
func Compile(name string, s Spec) (dag.WorkflowDef, []Diagnostic)
func CompileYAML(name string, spec []byte) (dag.WorkflowDef, []Diagnostic)
```

`CompileYAML` is `Parse` + `Compile` for callers holding raw bytes (the
`dagnats-ci compile` CLI and both endpoints below use it). The returned
`dag.WorkflowDef` is only valid — and only non-zero — when the diagnostics
slice is empty.

## `POST /v1/ci/compile`

Compiles a `ci.yml` spec and, optionally, registers the result.

**Request:**

```json
{
  "name": "ci:myrepo",
  "spec": "checks:\n  test: { call: \"test\" }\n",
  "register": false
}
```

`name` is required (`400` if empty). `spec` must be at most 256 KiB — the
body is read through a bounded `io.LimitReader` before the YAML decoder ever
sees it, so an oversized spec is rejected (`413`) rather than truncated.

**Response, `200 OK`** (no diagnostics):

```json
{
  "workflow": { "name": "ci:myrepo", "version": "1.0.0", "steps": [...] },
  "def_hash": "3f1a...c2",
  "registered": false,
  "warnings": []
}
```

With `"register": true`, the compiled definition is persisted through the
same `RegisterWorkflowWithWarnings` call `POST /workflows` uses —
`registered` is `true` and `warnings` carries that call's
respond-reachability warnings (see
[Workflow Re-registration and def_hash](../wire-protocol#workflow-re-registration-and-def_hash)
for `def_hash`'s meaning).

**Response, `422 Unprocessable Entity`** (one or more diagnostics):

```json
{
  "diagnostics": [
    { "field": "build", "message": "check \"build\": unknown needs target \"lint\"" }
  ]
}
```

Nothing is registered on a `422`, even with `"register": true` — diagnostics
short-circuit before registration is ever attempted.

## `POST /v1/ci/validate`

Same request body, minus `register` (it is accepted but ignored — validate
never registers, by design, regardless of what the body sends). Always
`200 OK`:

```json
{ "valid": true, "diagnostics": [] }
```

or

```json
{
  "valid": false,
  "diagnostics": [
    { "line": 2, "column": 12, "field": "defaults", "message": "..." }
  ]
}
```

Use this to let an author check a `ci.yml` (e.g. in a pre-commit hook or
editor integration) without any risk of a side effect.

## `dagnats-ci compile` CLI

The `dagnats-ci` module's CLI wraps `ci.CompileYAML` directly:

```
dagnats-ci compile .dagnats/ci.yml --name ci:myrepo
```

On success it prints the compiled `dag.WorkflowDef` JSON to stdout. On
diagnostics it prints one `file:line:col: field: message` line per
diagnostic to stderr and exits `1` — nothing is written to stdout.

## See also

- [REST API Reference](../rest-api) for the rest of the control-plane surface.
- [Wire Protocol Reference](../wire-protocol#annotations-in-taskresolutiondata)
  for the annotation convention a `dagger.call`/`ci.approval` worker should
  use in `TaskResolution.Data` when it wants to surface findings.
