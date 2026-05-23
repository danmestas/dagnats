/*
 * sidebar-collapse.js — desktop rail collapse-to-icons toggle (R6, #341).
 *
 * The rail (≥1024px) is the `.console-header` element, which carries
 * `aria-expanded="true"` by default. Clicking
 * `#sidebar-collapse-toggle` flips it to `"false"`; CSS keys on the
 * attribute to shrink the rail to ~56px and hide label spans. State
 * persists across reloads via localStorage so an operator's
 * preference survives.
 *
 * At <1024px the rail CSS doesn't apply — the same DOM renders as the
 * horizontal topbar or hamburger drawer. The attribute is still on
 * the element but invisible; toggling it has no visual effect until
 * the viewport widens, which matches the audit-locked regime model
 * (don't change DOM per breakpoint, let CSS pick which layout fits).
 *
 * Plain vanilla JS — no Datastar signal, no framework. The state is
 * client-only and persistent; a Datastar signal would force a
 * server round-trip for a purely cosmetic toggle.
 */
(function () {
  const STORAGE_KEY = "dagnats-console-rail-collapsed";
  const TOGGLE_ID = "sidebar-collapse-toggle";
  const RAIL_SELECTOR = ".console-header";

  function isCollapsed() {
    try {
      return localStorage.getItem(STORAGE_KEY) === "true";
    } catch (_) {
      return false;
    }
  }

  function persist(collapsed) {
    try {
      localStorage.setItem(STORAGE_KEY, collapsed ? "true" : "false");
    } catch (_) {}
  }

  function apply(rail, btn, collapsed) {
    // aria-expanded reads "is the rail expanded?" — true when open,
    // false when collapsed to icons. That matches the ARIA pattern
    // for disclosure widgets exactly.
    rail.setAttribute("aria-expanded", collapsed ? "false" : "true");
    if (btn) {
      btn.setAttribute(
        "aria-label",
        collapsed ? "Expand sidebar" : "Collapse sidebar",
      );
      btn.setAttribute("aria-expanded", collapsed ? "false" : "true");
    }
  }

  function init() {
    const rail = document.querySelector(RAIL_SELECTOR);
    if (!rail) return;
    const btn = document.getElementById(TOGGLE_ID);
    const collapsed = isCollapsed();
    apply(rail, btn, collapsed);
    if (!btn) return;
    btn.addEventListener("click", function () {
      const next = rail.getAttribute("aria-expanded") !== "false";
      // next === true means rail WAS expanded → now collapsed.
      persist(next);
      apply(rail, btn, next);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
