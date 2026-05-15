/*
 * dagnats console — total-count chip ticker.
 *
 * Server SSE handlers emit a tiny `<span id="<page>-count-tick" data-
 * last="<seq>" hidden>` replacement on every list-row update. This
 * script watches each registered tick element for attribute changes
 * and increments the visible `<span id="<page>-count">` accordingly.
 *
 * Pairs with streams_extra.go's emitCountChip helper. The server
 * stays stateless wrt the count — it only signals "an update happened
 * at seq=N"; the client maintains the displayed integer.
 *
 * Pages register their chip + tick by element-id pair on this list:
 */
(function () {
  const PAIRS = [
    { count: "triggers-count", tick: "triggers-count-tick" },
    { count: "dlq-count",      tick: "dlq-count-tick" },
  ];

  // Each chip's running count. Initialised from the data-count attr
  // server-rendered with the page. Server-side count is the truth at
  // page render; the client only nudges it as SSE updates arrive.
  const counts = {};
  const lastSeq = {};

  function read(id) {
    const el = document.getElementById(id);
    if (!el) return null;
    return el;
  }

  function init() {
    PAIRS.forEach(function (pair) {
      const chip = read(pair.count);
      const tick = read(pair.tick);
      if (!chip || !tick) return;
      counts[pair.count] = Number(chip.getAttribute("data-count") || "0");
      lastSeq[pair.tick] = Number(tick.getAttribute("data-last") || "0");
      watch(pair, chip, tick);
    });
  }

  function watch(pair, chip, tick) {
    // Datastar patches replace the tick element's outerHTML, so
    // re-resolve on every observed mutation. The MutationObserver
    // monitors the chip's parent because the tick span sits next to
    // the chip and gets replaced wholesale.
    const parent = tick.parentNode || chip.parentNode || document.body;
    const observer = new MutationObserver(function () {
      const t = read(pair.tick);
      const c = read(pair.count);
      if (!t || !c) return;
      const seq = Number(t.getAttribute("data-last") || "0");
      if (seq <= lastSeq[pair.tick]) return;
      lastSeq[pair.tick] = seq;
      counts[pair.count] = counts[pair.count] + 1;
      c.textContent = String(counts[pair.count]);
      c.setAttribute("data-count", String(counts[pair.count]));
      c.classList.add("is-incremented");
      setTimeout(function () {
        c.classList.remove("is-incremented");
      }, 600);
    });
    observer.observe(parent, {
      subtree: true,
      childList: true,
      attributes: true,
      attributeFilter: ["data-last"],
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
