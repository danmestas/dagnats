# Console

The dagnats console is a server-rendered operator UI that mounts at
`/console/` on the same HTTP listener as `/api` and `/metrics`. It
ships with `dagnats serve` — no separate process, no extra port.

## Overview

The console is a **live window onto the NATS state** the engine and
trigger system are already keeping. It does not maintain a separate
database. Every page reads from JetStream KV buckets, derives a view,
and renders HTML. Mutations (retry a DLQ entry, toggle a trigger)
publish through the same API the CLI uses. Server-Sent Events push
row-level updates so list pages refresh without polling.

Five top-level sections cover the operator's day:

- **Dashboard** — system overview, live tiles for runs / failures /
  latency, heartbeat indicator.
- **Workflows** — registered workflow definitions, drilldown to a
  per-workflow detail page with DAG visualisation.
- **Runs** — every run with status / workflow / range filters, live
  patch updates as runs complete, drilldown to a per-run detail page.
- **Triggers** — cron, webhook, NATS subject, and HTTP triggers with
  enable / disable toggles.
- **DLQ** — dead-letter entries with retry, soft-discard
  (5-second undo window), and hard-discard.
- **Ops** — operator sub-surface: workers, leases, audit log, KV
  inspector, metrics dashboard.

A first-run banner on the dashboard introduces these layers; it
dismisses to `localStorage` so repeat visits don't see it.

## Deployment

Enable the console by simply starting the server with the default
binding:

```bash
dagnats serve
```

The console mounts at `http://127.0.0.1:8080/console/` by default.
This loopback bind is intentional: with no authentication configured,
only processes on the host can reach the surface. The same listener
serves `/api` and `/metrics`.

### Listener address

Override the bind with `DAGNATS_HTTP_ADDR`:

```bash
DAGNATS_HTTP_ADDR=0.0.0.0:8080 dagnats serve
```

A non-loopback bind without auth refuses every console request with
a 503 + JSON body, and emits a loud startup log line. This is by
design — exposing a write-capable operator UI to the network without
a credential is the most common operational footgun.

### Auth modes

| Mode         | When to use                                | Env vars                                       |
|--------------|--------------------------------------------|------------------------------------------------|
| `loopback`   | Single-host dev / single-operator setups   | (default; nothing to set)                      |
| `basic`      | A small operator group; HTTP Basic Auth    | `DAGNATS_CONSOLE_PASSWORD`                     |
| `forward`    | Behind a reverse proxy doing SSO           | `DAGNATS_CONSOLE_TRUST_FORWARDED_AUTH=true`    |
| (disabled)   | Non-loopback bind without password / proxy | (the server refuses requests with 503)         |

Forward-auth trusts the `X-Forwarded-User` header set by an upstream
proxy. **Only enable forward-auth when the proxy strips this header
on incoming requests** — otherwise any caller can spoof an identity.

### Read-only mode

```bash
CONSOLE_READ_ONLY=true dagnats serve
```

Every mutation endpoint returns 405 with a canonical JSON body. The
layout shows a "Read-only mode" banner; mutation buttons render
visible-but-disabled with a tooltip explaining the env var. Use
this for staging environments where you want to give operators
read access without retry / discard capability.

### CSRF protection

Forward-auth and basic-auth modes apply HMAC-SHA256 CSRF tokens to
every mutation. The secret comes from `CONSOLE_CSRF_SECRET`:

```bash
CONSOLE_CSRF_SECRET=$(openssl rand -hex 32) dagnats serve
```

If the env var is unset, the server generates a random secret on
boot and prints a warning. Restarts rotate it, invalidating any
in-flight tokens — set the env var in production for stable tokens.
Loopback mode skips CSRF entirely (the OS access boundary already
prevents the attack the token defends against).

### Metrics endpoint

The `/metrics` exporter has its own gate, independent of the console:

| Mode       | When to use                                 | Env vars                       |
|------------|---------------------------------------------|--------------------------------|
| `loopback` | Prometheus scrapes from the same host       | (default)                      |
| `basic`    | Prometheus scrapes with a service token     | `METRICS_BASIC_USER` + `METRICS_BASIC_PASS` |
| `forward`  | Behind a reverse proxy injecting identity   | (proxy strips `X-Forwarded-User` on input) |
| `none`     | Public scrape (dev only; logs a warning)    | `METRICS_AUTH=none`            |

On startup the server emits one INFO log line announcing the
resolved mode. When the mode is `none` AND the bind is non-loopback,
the line escalates to WARN with operator-actionable text.

## Configuration reference

`dagnats config show` prints every resolved value, with secrets
masked. The console-relevant rows:

| Variable                                | Default              | What it does                                                       |
|-----------------------------------------|----------------------|--------------------------------------------------------------------|
| `DAGNATS_HTTP_ADDR`                     | `127.0.0.1:8080`     | TCP listener for `/console`, `/api`, `/metrics`.                   |
| `DAGNATS_CONSOLE_PASSWORD`              | (unset)              | Basic-auth password. Setting this enables `basic` console auth.    |
| `DAGNATS_CONSOLE_TRUST_FORWARDED_AUTH`  | `false`              | When `true`, trust `X-Forwarded-User` from upstream proxy.         |
| `CONSOLE_READ_ONLY`                     | `false`              | When `true`, every console mutation returns 405.                   |
| `CONSOLE_CSRF_SECRET`                   | (random per-restart) | HMAC secret for CSRF tokens; set for stable tokens across restarts.|
| `METRICS_AUTH`                          | `loopback`           | Gate mode for `/metrics`: `loopback`/`basic`/`forward`/`none`.     |
| `METRICS_BASIC_USER`                    | (unset)              | Basic-auth user for `METRICS_AUTH=basic`.                          |
| `METRICS_BASIC_PASS`                    | (unset)              | Basic-auth pass for `METRICS_AUTH=basic`.                          |

## Key workflows

### Finding a run

Open `/console/runs`. Filter by workflow / status / time range. Each
row links to a detail page with the DAG, step grid, event timeline,
input, and output.

If you have a run id from a CLI invocation or a webhook response,
paste it into the "Find run by id" lookup form — it redirects to the
detail page directly.

### Investigating a failure

From the runs list, filter `status=failed`. Open the run. The detail
page shows:

- the last-step error (alert at top),
- step status grid with timings,
- event timeline,
- the run's input + output JSON.

If the failure left a dead-letter entry, the audit log will show the
DLQ retry button is available from `/console/dlq`.

### Retrying a DLQ entry

`/console/dlq` lists every dead-letter sequence with reason class,
workflow, run id, and attempt count. Retry resubmits the task to the
worker queue; discard removes it from JetStream with a 5-second undo
toast (soft-discard mode).

Every retry / discard lands in the audit log within a second.

### Watching metrics

`/console/ops/metrics` shows system-health tiles + throughput + latency
charts. Anomaly markers (open circles, muted-rust colour) appear where
p99 latency exceeded the configured threshold of p50 — by default,
3×. Click a marker to land on a runs-list filtered to that anomaly's
time window.

### Anomaly threshold

The threshold lives in one Go constant
(`internal/console/metrics_anomaly.AnomalyP99OverP50Ratio`) and is
rendered into the metrics-page glossary so the operator sees the
same number the detector uses. A guard test asserts the two match.

## Production checklist

Before exposing the console to non-loopback traffic:

- [ ] Set `DAGNATS_HTTP_ADDR` to your intended bind.
- [ ] Choose an auth mode: `DAGNATS_CONSOLE_PASSWORD` (basic) or
      reverse proxy + `DAGNATS_CONSOLE_TRUST_FORWARDED_AUTH=true`
      (forward).
- [ ] Set `CONSOLE_CSRF_SECRET` to a 32-byte hex string so tokens
      survive restarts.
- [ ] Decide `CONSOLE_READ_ONLY`: `true` for staging, `false` (or
      unset) for live ops.
- [ ] Set `METRICS_AUTH` for `/metrics`: `basic` (with
      `METRICS_BASIC_USER` + `METRICS_BASIC_PASS`) or `forward`.
- [ ] Run `dagnats config show` and confirm every value is what you
      expect — sensitive vars print as `(set)`, never as their value.

## Failure modes

### Console renders blank

Symptoms: the page loads but the body is empty, or the dashboard
fails to load any tile.

Check:

1. Browser console for asset 404s — usually a CSP / proxy stripping
   `/console/assets/*` URLs.
2. The auth mode resolution — a misconfigured proxy might be
   stripping `X-Forwarded-User` so every request lands as
   "loopback" but with the wrong RemoteAddr.
3. `dagnats config show` to confirm `DAGNATS_HTTP_ADDR` matches
   what your proxy is forwarding to.

### `/metrics` returns 401

The metrics gate refused the request. Common causes:

- `METRICS_AUTH=loopback` (the default) and Prometheus is scraping
  from a non-loopback IP.
- `METRICS_AUTH=basic` but the scraper isn't sending Basic Auth, or
  the credentials don't match.
- `METRICS_AUTH=forward` but the upstream proxy isn't injecting
  `X-Forwarded-User`.

Check the startup log for the `metrics endpoint: auth_mode=…` line
to confirm the resolved mode.

### Audit bucket fills up

The console audit log is a JetStream KV bucket with a TTL. If the
bucket fills before TTL expires, mutations may slow down or fail.

Mitigations:

- Lower the bucket TTL (see the audit emitter wiring in
  `server/server.go`).
- Bump the bucket's `MaxBytes` if the operator team is
  high-throughput.
- Filter the audit log page (`/console/ops/audit`) by date range to
  reduce render cost.

### Onboarding banner doesn't dismiss

The first-run banner gates on `localStorage` key
`dagnats-console-onboarded`. If you're in Safari private mode, an
embedded WebView, or have storage disabled, the banner re-appears
every visit. The banner is editorial copy, not load-bearing UI —
ignore it or whitelist storage for the origin.

## Architecture

The console is documented in [ADR-014](architecture/adr-014-control-plane-ui.md).
The implementation arc spanned 8 PRs (#236–#244, merged 2026-05-08
through 2026-05-17). Internals live at `internal/console/`; the
deploy-time asset bundling pipeline is `internal/console/assets/README.md`.

For contributing, see [console-contributing.md](console-contributing.md).
