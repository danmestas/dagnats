# ADR-020: NATS WebSocket Listener for Browser Clients

Status: Accepted (2026-05-22)
Parent: ADR-014 (control-plane UI) + Phase 6 of #273
Implements: #359 (part 1; the in-tree server change)

## Context

Phase 6 of the control-plane work needs browser-side workers and
trigger watchers — code running in the operator's tab that talks
directly to the embedded NATS rather than going through a
serialising HTTP shim. The NATS client wire protocol is text-based
and works over WebSocket; the embedded `nats-server/v2` already
exposes a `WebsocketOpts` struct on `server.Options` and the
upstream `nats.go` client accepts `ws://` URLs natively. The work
is wiring, not a new transport.

The dagnats default posture so far is "TCP NATS bound to
localhost in standalone mode, 0.0.0.0 in leaf mode" — no TLS at
the server-options layer. There is no top-level
`server.Options.TLSConfig` populated anywhere in the codebase
today; operators terminate TLS at a sidecar or a fronting proxy
when they want it.

That last point matters: turning the WebSocket on naively would
ship cleartext bearer tokens (the upstream `WebsocketOpts`
comment is explicit about this). The minimum-viable wiring has
to refuse to start in the unsafe shape.

## Decision

Add two configuration knobs and wire `natsserver.WebsocketOpts`
into the embedded server boot path.

### Configuration surface

- `nats_ws_port` / `DAGNATS_NATS_WS_PORT` / `--nats-ws-port=N`
  — integer. `0` (default) disables the WebSocket listener.
  Non-zero binds it on that port using the same host posture as
  the TCP port (`127.0.0.1` standalone, `0.0.0.0` in leaf mode).
- `nats_ws_no_tls` / `DAGNATS_NATS_WS_NO_TLS` /
  `--nats-ws-no-tls` — boolean. `false` (default) means TLS
  required; until top-level NATS TLS is wired this means
  `nats_ws_port > 0` returns an error at startup. Setting it to
  `true` is the explicit opt-in to insecure dev mode and emits
  a `stderr` warning on every startup.

The default state is "off". The unsafe-but-functional state
requires the operator to write `--nats-ws-no-tls` themselves.
The safe production posture (TLS) is wired in a follow-up once
top-level NATS TLS exists; until then the WebSocket listener is
a development-only feature.

### Auth model

`WebsocketOpts.{Username,Password,Token}` are left zero. Upstream
behavior in that configuration is "fall back to
`Options.Users` / top-level auth", which is exactly the issue
contract ("TLS + auth pull from the same options the TCP NATS
port uses"). No new auth model, no second knob.

### Host binding

The WebSocket listener reuses the `host` variable already
computed for the TCP listener. Operators get one binding posture
to reason about, not two.

## Consequences

- **Browser SDK becomes possible.** The companion repo
  `dagnats-browser-sdk` (sibling repo per Q1 of the #273 Phase 6
  planner) can wrap `nats.ws` and ship the Worker / Handle /
  Typed surface to in-page code. That work is out of scope for
  this ADR.
- **TLS hardening is a known follow-up.** This ADR explicitly
  defers top-level server TLS to a separate change; the
  `--nats-ws-no-tls` flag remains a flag (not a hard-coded
  default) so the TLS path can be wired without breaking
  dev-mode operators.
- **Surface area stays small.** Two flags, ~25 lines of wiring,
  one ADR. No new packages, no new auth model, no separate
  endpoint surface.
- **The warning is annoying on purpose.** Per the audit comment
  and the dagnats stance ("safety > performance > DX"), the
  stderr line on every startup is cheap insurance against
  shipping cleartext to production.

## Alternatives considered

1. **HTTP fan-in shim instead of native WebSocket.** Rejected.
   The control-plane already speaks NATS; another protocol layer
   doubles the failure modes and pushes complexity outward. The
   browser SDK would still need to serialise NATS semantics over
   HTTP, which is exactly what `nats.ws` already does over
   WebSocket — and `nats.ws` has the actual NATS team's
   maintenance behind it.
2. **Always-on WebSocket with a separate auth.** Rejected. Two
   auth surfaces means two ways to forget to lock something
   down. The "WebSocket inherits the TCP port's auth" contract
   from the issue is the right shape.
3. **Hard-block WebSocket without TLS.** Rejected for now. There
   is no server-side TLS today; a hard block would be a
   "feature flag that is always off" until the TLS work lands.
   Better to ship the listener with the explicit insecure flag,
   document the dev-only posture, and remove the flag when the
   TLS work makes it redundant.
4. **Default the host to `0.0.0.0`.** Rejected. The TCP port
   defaults to `127.0.0.1` in standalone mode; the WebSocket
   listener does the same thing for the same reason. Operators
   who want remote access already know the leaf-mode pattern.

## References

- nats-server `WebsocketOpts`:
  `pkg/mod/github.com/nats-io/nats-server/v2@v2.12.6/server/opts.go:518`
- `nats.go` WebSocket client tests:
  `pkg/mod/github.com/nats-io/nats.go@v1.50.0/test/ws_test.go`
- Parent: #273 Phase 6, refined plan comment 4522116321
- Issue: #359
- Sibling repo follow-up: `dagnats-browser-sdk` (separate work)
