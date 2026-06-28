/*
 * dagnats console — entry point.
 *
 * Importing `datastar.js` registers every attribute / action / watcher
 * with the engine. Each `attribute()` call enqueues the plugin and
 * schedules a setTimeout(0) flush that, on first invocation, also
 * calls the engine's `apply()` to walk the DOM and wire every
 * `data-*` attribute that landed in the static HTML. That auto-init
 * is the path that wires `data-init`, `data-on:*`, etc.
 *
 * The vendored bundle is patched to also assign `window.datastar` so
 * the headless-Chrome smoke test can introspect bootstrap state. We
 * do NOT call `apply()` ourselves — the engine's deferred apply is
 * the one keyed on the `queuedAttributeNames` set, and an early
 * external `apply()` would no-op against the cleared queue.
 */

import "./datastar.js";
import "./basecoat.js";
import "./sparkline.js";
import "./command_palette.js";
import "./sheet.js";

// Theme toggle — three-state cycle: System (prefers-color-scheme) →
// Light → Dark → System. State lives in localStorage; absence of
// the key means "follow system." Vanilla JS instead of Datastar
// signals because the state is purely client-side and persistent.
(function () {
  const STORAGE_KEY = "dagnats-console-theme";
  const MODES = ["system", "light", "dark"];
  const LABELS = { system: "System", light: "Light", dark: "Dark" };

  function current() {
    try {
      const v = localStorage.getItem(STORAGE_KEY);
      return MODES.indexOf(v) >= 0 ? v : "system";
    } catch (_) {
      return "system";
    }
  }

  function apply(mode) {
    // Theme attribute lives on <html> so the html element paints the
    // chosen palette. Setting it on <body> only leaves the html
    // element's background resolving --bg from :root — which causes
    // a two-tone scroll-bounce in light mode when the OS prefers dark.
    if (mode === "system") {
      document.documentElement.removeAttribute("data-theme");
    } else {
      document.documentElement.setAttribute("data-theme", mode);
    }
  }

  function updateButton(btn, mode) {
    if (!btn) return;
    btn.setAttribute("data-theme-mode", mode);
    btn.setAttribute(
      "aria-label",
      "Theme: " + LABELS[mode] + " (click to cycle)",
    );
    const label = btn.querySelector(".theme-toggle-label");
    if (label) label.textContent = LABELS[mode];
  }

  function cycle() {
    const next = MODES[(MODES.indexOf(current()) + 1) % MODES.length];
    try {
      localStorage.setItem(STORAGE_KEY, next);
    } catch (_) {}
    apply(next);
    updateButton(document.getElementById("theme-toggle"), next);
  }

  function init() {
    const mode = current();
    apply(mode);
    const btn = document.getElementById("theme-toggle");
    updateButton(btn, mode);
    if (btn) btn.addEventListener("click", cycle);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();

// Delegated row-click drill-in. List tables render a per-row
// data-href on the <tr>; clicking anywhere in the row navigates to
// that detail page. A single document-level listener handles every
// list table (workflows, runs, triggers, streams, workers, functions)
// instead of scattering per-template handlers. The first column still
// carries a real <a> for keyboard users — this is a mouse convenience,
// so the <tr> deliberately gets no tabindex/role (no duplicate tab
// stop). Clicks that originate on an interactive descendant (the name
// link, a toggle switch, an inline action button, a form control) are
// left alone so the row-click never hijacks them.
(function () {
  const INTERACTIVE =
    'a[href], button, [role="switch"], [role="checkbox"], input, label, select';

  function onClick(event) {
    if (event.defaultPrevented || event.button !== 0) return;
    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
      return;
    }
    const target = event.target;
    if (!target || typeof target.closest !== "function") return;
    if (target.closest(INTERACTIVE)) return;
    const row = target.closest("tr[data-href]");
    if (!row) return;
    const href = row.getAttribute("data-href");
    if (!href) return;
    window.location.assign(href);
  }

  // Guard against double-binding if this bundle is evaluated twice.
  if (!window.__dagnatsRowClickBound) {
    window.__dagnatsRowClickBound = true;
    document.addEventListener("click", onClick);
  }
})();
