// onboarding.js — first-run banner gate. Renders the welcome aside
// on /console/ when localStorage shows the operator hasn't dismissed
// it yet. Single click-handler on the Got it button. No timers, no
// network — pure DOM + storage. Bounded by a one-shot init.
(function () {
  "use strict";

  // STORAGE_KEY is the single source of truth for the dismiss flag.
  // Pinned here so the test harness and the page logic agree.
  const STORAGE_KEY = "dagnats-console-onboarded";

  function storageAvailable() {
    try {
      const t = "__dagnats_probe__";
      window.localStorage.setItem(t, t);
      window.localStorage.removeItem(t);
      return true;
    } catch (_) {
      return false;
    }
  }

  function init() {
    const banner = document.getElementById("console-onboarding");
    if (banner === null) {
      return;
    }
    // No localStorage available (Safari private, file://, etc.) —
    // fail open. Show the banner; it's editorial copy, not
    // load-bearing UI.
    let dismissed = false;
    if (storageAvailable()) {
      try {
        dismissed = window.localStorage.getItem(STORAGE_KEY) === "1";
      } catch (_) {
        dismissed = false;
      }
    }
    if (dismissed) {
      banner.hidden = true;
      return;
    }
    banner.hidden = false;
    const btn = document.getElementById("console-onboarding-dismiss");
    if (btn === null) {
      return;
    }
    btn.addEventListener("click", function () {
      banner.hidden = true;
      if (storageAvailable()) {
        try {
          window.localStorage.setItem(STORAGE_KEY, "1");
        } catch (_) {
          // Fail open — the operator dismissed it for this session.
        }
      }
    }, { once: true });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init, { once: true });
  } else {
    init();
  }
})();
