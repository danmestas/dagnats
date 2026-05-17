/*
 * dagnats console — connection-state indicator.
 *
 * Listens to Datastar's `datastar-fetch` CustomEvent stream and reflects
 * the current SSE health into the header pill (#console-connection).
 *
 * Datastar dispatches the following types on `datastar-fetch`:
 *   - "started"           → an SSE / fetch begins
 *   - "finished"          → it completes successfully (one-shot fetch)
 *   - "error"             → a request failed
 *   - "retrying"          → the engine is in its backoff loop
 *   - "retries-failed"    → backoff exhausted; we render "retries-failed"
 *
 * States:
 *   - "idle"           — no SSE active. Muted dot.
 *   - "live"           — at least one healthy SSE open. Green dot.
 *   - "reconnecting"   — actively retrying. Amber dot.
 *   - "offline"        — manually forced offline (network gone). Red dot.
 *   - "retries-failed" — backoff exhausted. Red dot, click to refresh.
 *
 * Norman's Error-recovery principle: in any degraded state the operator
 * must know what to DO, not just that something is degraded. HINTS
 * carries actionable copy. For offline / retries-failed we additionally
 * wire a click handler that reloads the page — the fix is a refresh,
 * and the pill is now the affordance for it.
 *
 * State transitions go through render() so the aria-live region only
 * announces *real* transitions; the same state repeating is silent.
 */

(function () {
  const PILL_ID = "console-connection";
  const VALID = [
    "idle",
    "live",
    "reconnecting",
    "offline",
    "retries-failed",
  ];
  const LABELS = {
    idle: "idle",
    live: "live",
    reconnecting: "reconnecting",
    offline: "offline",
    "retries-failed": "offline",
  };
  // HINTS is operator-facing recovery copy. The phrasing for the two
  // clickable states must include "click to refresh" — the test asserts
  // it and the click handler delivers on it.
  const HINTS = {
    live: "Live (SSE healthy)",
    idle: "No active stream for this page",
    reconnecting:
      "Reconnecting — the page will catch up automatically; " +
      "refresh if it persists",
    offline: "Connection lost — click to refresh",
    "retries-failed": "Failed to reconnect — click to refresh",
  };
  const CLICKABLE = { offline: true, "retries-failed": true };

  // openCount tracks live SSE connections so multiple pages with their
  // own SSE don't trample each other. retriesInFlight gates the
  // reconnecting state — when no retry is active and at least one
  // connection is open, we report "live".
  let openCount = 0;
  let retriesInFlight = 0;
  let currentState = "idle";

  function pill() {
    return document.getElementById(PILL_ID);
  }

  function applyClickability(el, state) {
    if (CLICKABLE[state]) {
      el.style.cursor = "pointer";
      el.setAttribute("role", "button");
      el.setAttribute("tabindex", "0");
      el.onclick = function () {
        window.location.reload();
      };
      return;
    }
    el.style.cursor = "default";
    el.removeAttribute("tabindex");
    el.setAttribute("role", "status");
    el.onclick = null;
  }

  function render(next) {
    if (VALID.indexOf(next) < 0) return;
    if (next === currentState) return;
    currentState = next;
    const el = pill();
    if (!el) return;
    el.setAttribute("data-state", next);
    el.setAttribute("title", HINTS[next] || next);
    const label = el.querySelector(".console-connection-label");
    if (label) label.textContent = LABELS[next];
    applyClickability(el, next);
  }

  function compute() {
    if (retriesInFlight > 0) return "reconnecting";
    if (openCount > 0) return "live";
    return "idle";
  }

  function onFetch(evt) {
    if (!evt || !evt.detail) return;
    const type = evt.detail.type;
    if (type === "started") {
      openCount++;
      render(compute());
      return;
    }
    if (type === "finished") {
      if (openCount > 0) openCount--;
      render(compute());
      return;
    }
    if (type === "error" || type === "retrying") {
      retriesInFlight++;
      render("reconnecting");
      return;
    }
    if (type === "retries-failed") {
      retriesInFlight = 0;
      openCount = 0;
      render("retries-failed");
    }
  }

  function initialRender() {
    // Initial state is idle; the pill markup already renders that,
    // but we still need to install the hint title + clear any
    // clickability so a server-side data-state="offline" hand-off
    // can't strand the click handler in a stale shape.
    const el = pill();
    if (!el) return;
    el.setAttribute("title", HINTS.idle);
    applyClickability(el, "idle");
  }

  function init() {
    document.addEventListener("datastar-fetch", onFetch);
    initialRender();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }

  // Expose for tests + the browser smoke harness.
  window.__dagnatsConnection = {
    get state() {
      return currentState;
    },
    _forceState: function (s) {
      render(s);
    },
    _reset: function () {
      openCount = 0;
      retriesInFlight = 0;
      currentState = "idle";
      const el = pill();
      if (el) {
        el.setAttribute("data-state", "idle");
        el.setAttribute("title", HINTS.idle);
        const label = el.querySelector(".console-connection-label");
        if (label) label.textContent = LABELS.idle;
        applyClickability(el, "idle");
      }
    },
    _dispatch: function (type) {
      onFetch(new CustomEvent("datastar-fetch", { detail: { type: type } }));
    },
  };
})();
