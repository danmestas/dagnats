/*
 * command_palette.js — keyboard + focus management for the cmd+k palette.
 *
 * Why this lives outside Datastar:
 *   - The global cmd+k / ctrl+k listener needs to fire regardless of
 *     where focus currently sits in the document; Datastar attribute
 *     wiring is element-scoped.
 *   - Focus trapping while the palette is open keeps Tab cycles inside
 *     the dialog (Norman: constraints) — this is plain DOM work, not
 *     reactive state.
 *   - The arrow-key navigation between results is a small state machine
 *     that doesn't benefit from signals. Keep it local.
 *
 * The palette markup itself (template "command-palette") owns the
 * data-on:input attribute that fires the SSE search. This file only
 * concerns itself with opening/closing the overlay and shuttling focus.
 */
(function () {
  const PALETTE_ID = "command-palette";
  const RESULTS_ID = "command-results";
  const INPUT_ID = "command-input";

  function paletteEl() {
    return document.getElementById(PALETTE_ID);
  }

  function isOpen() {
    const el = paletteEl();
    return !!el && !el.hasAttribute("hidden");
  }

  function open() {
    const el = paletteEl();
    if (!el) return;
    el.removeAttribute("hidden");
    el.setAttribute("aria-hidden", "false");
    // Defer focus to next tick so the browser commits the visibility
    // change before the keyboard moves. Without it Safari occasionally
    // drops the focus event on the floor.
    setTimeout(function () {
      const input = document.getElementById(INPUT_ID);
      if (input) {
        input.focus();
        try {
          input.select();
        } catch (_) {}
      }
    }, 0);
  }

  function close() {
    const el = paletteEl();
    if (!el) return;
    el.setAttribute("hidden", "");
    el.setAttribute("aria-hidden", "true");
    const input = document.getElementById(INPUT_ID);
    if (input) input.value = "";
    const results = document.getElementById(RESULTS_ID);
    if (results) results.innerHTML = "";
  }

  function results() {
    const list = document.getElementById(RESULTS_ID);
    if (!list) return [];
    return Array.prototype.slice.call(
      list.querySelectorAll(".cmdk-result"),
    );
  }

  function focusedIndex() {
    const rows = results();
    for (let i = 0; i < rows.length; i++) {
      if (rows[i] === document.activeElement) return i;
    }
    return -1;
  }

  function focusResult(delta) {
    const rows = results();
    if (rows.length === 0) return;
    const current = focusedIndex();
    let next;
    if (current < 0) {
      next = delta > 0 ? 0 : rows.length - 1;
    } else {
      next = (current + delta + rows.length) % rows.length;
    }
    rows[next].focus();
  }

  function activateFirstHit() {
    const rows = results();
    if (rows.length === 0) return false;
    rows[0].click();
    return true;
  }

  document.addEventListener("keydown", function (e) {
    // Modifier+K opens the palette from anywhere. metaKey on macOS,
    // ctrlKey on Linux/Windows; both fire on this listener so the
    // shortcut feels native on each platform.
    const isMod = e.metaKey || e.ctrlKey;
    if (isMod && (e.key === "k" || e.key === "K")) {
      e.preventDefault();
      open();
      return;
    }
    if (!isOpen()) return;
    if (e.key === "Escape") {
      e.preventDefault();
      close();
      return;
    }
    if (e.key === "ArrowDown") {
      e.preventDefault();
      focusResult(1);
      return;
    }
    if (e.key === "ArrowUp") {
      e.preventDefault();
      focusResult(-1);
      return;
    }
    if (e.key === "Enter") {
      // When focus is in the input box, navigate to the first hit.
      // When focus is on a result row, the anchor handles its own click.
      const active = document.activeElement;
      if (active && active.id === INPUT_ID) {
        if (activateFirstHit()) e.preventDefault();
      }
    }
  });

  // Click anywhere on the backdrop (the area outside the dialog) closes
  // the palette. data-command-backdrop is on the backdrop element so
  // the listener doesn't fire on dialog clicks.
  document.addEventListener("click", function (e) {
    if (!isOpen()) return;
    const target = e.target;
    if (target && target.matches && target.matches("[data-command-backdrop]")) {
      close();
    }
  });
})();
