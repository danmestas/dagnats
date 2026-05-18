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

    // Esc closes whenever a sheet is open. We attach to document so the
    // handler keeps working after the outlet contents are replaced.
    document.addEventListener("keydown", (e) => {
      if (e.key !== "Escape") return;
      const open = outlet.querySelector(".sidesheet.sidesheet-open");
      if (open) {
        e.preventDefault();
        closeSheet();
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
