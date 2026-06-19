# dagnats-ci

Durable GitHub CI orchestration on DagNats, with Dagger as the execution engine.

DagNats is the durable orchestrator — event-sourced runs, retries, approval
gates, cron scheduling, DLQ. Dagger is the execution substrate — BuildKit-backed,
content-addressed, reproducible containers. This add-on wires the two together:
GitHub event → DagNats run → Dagger execution → GitHub Check.

## What this module implements now

- **CI-spec compiler** (`internal/compile`): parses `.dagnats/ci.yml` and
  compiles it into a `dag.WorkflowDef` ready for the DagNats engine. Checks map
  to `dagger.call` steps; `approval: required` inserts a durable human-gate step
  before deployment.

- **Webhook signature verification** (`internal/githubapp`): constant-time
  HMAC-SHA256 verification of the `X-Hub-Signature-256` header.

- **GitHub event normalization** (`internal/githubapp`): decodes `push` and
  `pull_request` webhook payloads into a slim `Event` struct and converts them
  to `dagnatsext.TriggerEnvelope` for submission to the DagNats engine.

- **`compile` CLI** (`cmd/dagnats-ci`): reads a ci.yml file and prints the
  compiled `dag.WorkflowDef` JSON to stdout.

  ```
  dagnats-ci compile .dagnats/ci.yml --name ci:myrepo
  ```

## CI spec format

```yaml
on:
  pull_request: { branches: [main] }
  push:         { branches: [main] }
  schedule:     { cron: "0 6 * * *" }   # DagNats cron trigger — not GitHub Actions

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
  branches: [main]
```

## Module layout

```
dagnats-ci/
  go.mod                       # replace => ../ for in-repo development
  internal/
    compile/
      spec.go                  # parse .dagnats/ci.yml
      compile.go               # compile → dag.WorkflowDef
      compile_test.go
    githubapp/
      webhook.go               # HMAC-SHA256 verify
      event.go                 # event parsing + ToEnvelope
      githubapp_test.go
  cmd/dagnats-ci/main.go       # compile subcommand
```

This is a nested Go module using `replace github.com/danmestas/dagnats => ../`
for in-repo development against the local parent. It is intended to be
extractable to its own repository later once the integration is stable.

## Follow-ups that need external infrastructure

The following are **not** implemented in this PR and are intentional follow-ups:

- **Serve mode**: an HTTPS webhook receiver process that verifies GitHub
  signatures, exchanges GitHub App installation tokens, and submits runs to a
  live DagNats instance over NATS.
- **GitHub App token exchange** (`internal/githubapp/auth.go`): JWT-based
  installation access token negotiation with the GitHub Apps API. This requires
  a real App ID and private key, which are not available at build time.
- **Starting runs over NATS**: `worker.StartRun` / trigger envelope submission
  to a running dagnats engine. Requires a live NATS connection and a registered
  workflow.
- **Dagger workers**: the `dagger.call` and `ci.approval` task handlers that
  actually invoke the Dagger CLI and report back via `ctx.Complete` / `ctx.Fail`.
  Requires a running Dagger engine and a cloned repository workspace.
- **GitHub Checks reporter**: creating and updating GitHub Check runs via the
  Checks API as DagNats steps transition through their lifecycle.

- **Deploy branch-gating and `on.*.branches` trigger filtering**: declaring
  `branches:` under `deploy:` in ci.yml is currently **rejected by the compiler**
  with an explicit error rather than silently ignored (a silent skip-if would
  reference a `branch` step output that no Phase 1 runner emits, so the gate
  would not gate). Both deploy `branches:` filtering and `on.pull_request.branches`
  / `on.push.branches` trigger filtering are Phase 4 follow-ups, dependent on
  the runner emitting a branch step output for the workflow to test against.