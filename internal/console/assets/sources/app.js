/*
 * dagnats console — entry point.
 *
 * Datastar's upstream bundle exports the engine but does NOT auto-walk
 * the DOM on import. The vendored `datastar.js` is patched to expose
 * `window.datastar` and to call `apply()` at DOMContentLoaded — so
 * importing the bundle as a side-effect is enough to wire every
 * `data-*` attribute that landed in the static HTML. Surfaced to the
 * window for the headless-Chrome smoke test which asserts on
 * `window.datastar`.
 */

import * as datastar from "./datastar.js";
import "./basecoat.js";

// Defensive fallback: if the bundle's auto-bootstrap missed (race on
// `import` vs DOMContentLoaded under exotic loaders), call apply()
// explicitly. The engine's apply() is idempotent for already-walked
// roots.
if (typeof window !== "undefined") {
  if (!window.datastar) {
    window.datastar = datastar;
  }
  if (typeof datastar.apply === "function") {
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", function () {
        try { datastar.apply(); } catch (_) {}
      });
    } else {
      try { datastar.apply(); } catch (_) {}
    }
  }
}

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
    if (mode === "system") {
      document.body.removeAttribute("data-theme");
    } else {
      document.body.setAttribute("data-theme", mode);
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
