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
 *   - "retries-failed"    → backoff exhausted; we render "offline"
 *
 * States:
 *   - "idle"          — no SSE active. Muted dot.
 *   - "live"          — at least one healthy SSE open. Green dot.
 *   - "reconnecting"  — actively retrying. Amber dot.
 *   - "offline"       — retries exhausted. Red dot.
 *
 * State transitions go through render() so the aria-live region only
 * announces *real* transitions; the same state repeating is silent.
 */

(function () {
  const PILL_ID = "console-connection";
  const VALID = ["idle", "live", "reconnecting", "offline"];
  const LABELS = {
    idle: "idle",
    live: "live",
    reconnecting: "reconnecting",
    offline: "offline",
  };
  const TITLES = {
    idle: "Connection state: idle (no live stream)",
    live: "Connection state: live (SSE healthy)",
    reconnecting: "Connection state: reconnecting (retrying)",
    offline: "Connection state: offline (retries exhausted)",
  };

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

  function render(next) {
    if (VALID.indexOf(next) < 0) return;
    if (next === currentState) return;
    currentState = next;
    const el = pill();
    if (!el) return;
    el.setAttribute("data-state", next);
    el.setAttribute("title", TITLES[next]);
    const label = el.querySelector(".console-connection-label");
    if (label) label.textContent = LABELS[next];
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
      render("offline");
    }
  }

  function init() {
    document.addEventListener("datastar-fetch", onFetch);
    // Initial state is idle; the pill markup already renders that.
    render("idle");
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
      render("idle");
    },
    _dispatch: function (type) {
      onFetch(new CustomEvent("datastar-fetch", { detail: { type: type } }));
    },
  };
})();
