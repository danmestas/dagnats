/*
 * dagnats console — entry point.
 *
 * Imports Datastar (auto-initializes `data-*` attributes on load),
 * pulls in Basecoat's interactive components (dialogs, dropdowns, etc.),
 * and exposes any global hooks the console templates rely on.
 *
 * The build pipeline (esbuild) bundles this with `datastar.js` and
 * `basecoat.js` into a single `console.js`, which is gzipped and
 * embedded into the dagnats binary via `//go:embed`.
 */

import "./datastar.js";
import "./basecoat.js";

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
