/*
 * dagnats console — side sheet open/close glue (Phase 2 T12).
 *
 * Datastar patches the sheet markup into #sheet-outlet (inner mode);
 * we observe that mutation and flip the .sidesheet-open class one frame
 * later so the CSS transition runs (cannot transition on the first
 * paint of a freshly-inserted element). Esc, backdrop-click, and the X
 * button all close.
 *
 * No bundler glue, plain DOM API. Loaded via app.js import so the
 * minified console.js bundle owns the module.
 */
(function () {
  const OUTLET_ID = "sheet-outlet";
  const FOCUSABLE =
    'a[href], button:not([disabled]), input:not([disabled]), ' +
    'select:not([disabled]), textarea:not([disabled]), ' +
    '[tabindex]:not([tabindex="-1"])';

  // tabbables returns the in-order focusable descendants of the sheet.
  // Used to seed initial focus and to wrap the Tab focus trap.
  function tabbables(sheet) {
    return Array.prototype.slice
      .call(sheet.querySelectorAll(FOCUSABLE))
      .filter((el) => el.offsetParent !== null || el === document.activeElement);
  }

  function openSheet() {
    const sheet = document.querySelector("#" + OUTLET_ID + " .sidesheet");
    if (!sheet) return;
    // requestAnimationFrame is the canonical way to trigger a
    // transition on a just-inserted element. document.body.offsetHeight
    // would also force layout, but rAF is cheaper and clearer.
    requestAnimationFrame(() => {
      sheet.classList.add("sidesheet-open");
      const backdrop = document.querySelector("#" + OUTLET_ID + " .sidesheet-backdrop");
      if (backdrop) backdrop.classList.add("sidesheet-open");
      // Move focus into the dialog on open so keyboard + screen-reader
      // users land inside it. The close button is the conventional first
      // stop for an inspect-only sheet.
      const closeBtn = sheet.querySelector("[data-sidesheet-close]");
      if (closeBtn) closeBtn.focus();
      else sheet.focus();
    });
  }

  function closeSheet() {
    const outlet = document.getElementById(OUTLET_ID);
    if (!outlet) return;
    const sheet = outlet.querySelector(".sidesheet");
    const backdrop = outlet.querySelector(".sidesheet-backdrop");
    if (sheet) sheet.classList.remove("sidesheet-open");
    if (backdrop) backdrop.classList.remove("sidesheet-open");
    // After the slide-out transition, remove the markup so screen
    // readers stop seeing the dialog and the next open is a fresh DOM.
    setTimeout(() => {
      if (outlet) outlet.innerHTML = "";
    }, 260);
  }

  function init() {
    const outlet = document.getElementById(OUTLET_ID);
    if (!outlet) return;

    // Observe Datastar patches so the slide-in fires whenever a new
    // sheet markup lands in the outlet. The watcher is bounded to the
    // outlet subtree so unrelated DOM mutations don't trigger it.
    const observer = new MutationObserver((records) => {
      for (const r of records) {
        if (r.addedNodes && r.addedNodes.length > 0) {
          openSheet();
          return;
        }
      }
    });
    observer.observe(outlet, { childList: true, subtree: false });

    // Esc closes, Tab is trapped within the dialog. Both attach to
    // document so they survive outlet content replacement across patches.
    document.addEventListener("keydown", (e) => {
      const open = outlet.querySelector(".sidesheet.sidesheet-open");
      if (!open) return;
      if (e.key === "Escape") {
        e.preventDefault();
        closeSheet();
        return;
      }
      if (e.key !== "Tab") return;
      // Focus trap: Tab off either end wraps to the other end so focus
      // never escapes the modal dialog while it is open.
      const items = tabbables(open);
      if (items.length === 0) {
        e.preventDefault();
        return;
      }
      const first = items[0];
      const last = items[items.length - 1];
      const active = document.activeElement;
      if (e.shiftKey && (active === first || !open.contains(active))) {
        e.preventDefault();
        last.focus();
      } else if (!e.shiftKey && (active === last || !open.contains(active))) {
        e.preventDefault();
        first.focus();
      }
    });

    // Click on the X button OR backdrop both close. Delegation keeps
    // the binding stable across patch cycles.
    document.addEventListener("click", (e) => {
      const closer = e.target.closest("[data-sidesheet-close]");
      if (closer && outlet.contains(closer)) {
        e.preventDefault();
        closeSheet();
      }
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
