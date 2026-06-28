/*
 * sparkline.js — initialise 60x16px uPlot sparkline canvases on list
 * pages. Lives in the console bundle so any page that pulls
 * console.js + the vendored uplot.min.js gets sparkline support for
 * free.
 *
 * Methodology:
 *   - On DOMContentLoaded, scan for canvas[data-sparkline-data].
 *   - Each canvas's data-sparkline-data attribute is a JSON array of
 *     hourly buckets (length 24 by convention). Parse, build the x
 *     axis as 0..N-1, init a fixed-size uPlot.
 *   - The template's responsibility: only render the canvas when the
 *     row has real activity data. An absent canvas is the honest
 *     empty-state path; the JS never invents a flat-line series.
 *   - Defensive: if window.uPlot is missing (vendored bundle didn't
 *     load) we leave the canvas blank rather than throw.
 *   - Re-init after Datastar SSE swaps: a PatchElements swap replaces
 *     `#workflows-tbody` (filter fragment) or prepends a live
 *     `#trigger-row-*`, landing fresh canvases the one-shot
 *     DOMContentLoaded scan never sees. Datastar dispatches no
 *     document event specifically for an element-only patch
 *     (`datastar-signal-patch` fires only when signals change), so we
 *     observe the DOM directly with a MutationObserver and re-scan.
 *     The per-canvas `__sparklineInit` guard makes every re-scan
 *     idempotent and cheap.
 */

(function () {
  const SELECTOR = "canvas[data-sparkline-data]";

  // initSparklines scans every un-initialised sparkline canvas and
  // renders it. Idempotent: the `__sparklineInit` guard on each canvas
  // means an already-rendered canvas is skipped, so calling this after
  // each DOM mutation is safe and cheap.
  function initSparklines() {
    if (typeof window === "undefined") return;
    const canvases = document.querySelectorAll(SELECTOR);
    if (canvases.length === 0) return;
    if (typeof window.uPlot !== "function") {
      // uPlot vendored bundle didn't load — leave canvases blank
      // rather than throw. The empty-state contract still holds:
      // operator sees no sparkline rather than a misleading flat line.
      return;
    }
    const MAX = 256; // bounded loop
    for (let i = 0; i < canvases.length && i < MAX; i++) {
      renderOne(canvases[i]);
    }
  }

  function renderOne(canvas) {
    if (!canvas) return;
    if (canvas.__sparklineInit) return;
    canvas.__sparklineInit = true;
    const raw = canvas.dataset.sparklineData;
    if (!raw) return;
    let data;
    try {
      data = JSON.parse(raw);
    } catch (_) {
      return;
    }
    if (!Array.isArray(data) || data.length === 0) return;
    // Honesty: when every bucket is zero, hide the canvas rather than
    // draw a flat-line at the bottom edge. The template should have
    // suppressed this case, but defend in depth in case a row reports
    // [0,0,0,...] via the metrics layer.
    let nonZero = 0;
    for (let i = 0; i < data.length; i++) {
      if (data[i] > 0) {
        nonZero++;
        break;
      }
    }
    if (nonZero === 0) {
      canvas.style.visibility = "hidden";
      return;
    }
    const x = new Array(data.length);
    for (let i = 0; i < data.length; i++) x[i] = i;
    const opts = {
      width: 60,
      height: 16,
      padding: [1, 1, 1, 1],
      legend: { show: false },
      cursor: { show: false },
      scales: {
        x: { time: false },
        y: { auto: true, range: (_u, dMin, dMax) => [0, Math.max(dMax, 1)] },
      },
      axes: [
        { show: false },
        { show: false },
      ],
      series: [
        {},
        {
          stroke: "var(--paper-ink-soft, currentColor)",
          width: 1.25,
          points: { show: false },
          fill: "color-mix(in srgb, currentColor 12%, transparent)",
        },
      ],
    };
    try {
      new window.uPlot(opts, [x, data], canvas);
    } catch (_) {
      // Init failure: leave the canvas blank rather than corrupting
      // the row. Sparkline is progressive enhancement, not load-bearing.
      canvas.style.visibility = "hidden";
    }
  }

  // addedSparkline reports whether a MutationRecord inserted a sparkline
  // canvas (itself or a descendant). Hot pages (metrics tile patches,
  // toasts, banners) mutate the body several times/sec; without this
  // pre-filter every batch would run a full-document querySelectorAll
  // even on pages with zero sparklines. ELEMENT_NODE === 1.
  function addedSparkline(records) {
    for (let r = 0; r < records.length; r++) {
      const added = records[r].addedNodes;
      for (let n = 0; n < added.length; n++) {
        const node = added[n];
        if (node.nodeType !== 1) continue;
        if (node.matches && node.matches(SELECTOR)) return true;
        if (node.querySelector && node.querySelector(SELECTOR)) return true;
      }
    }
    return false;
  }

  // observeSwaps re-runs initSparklines after Datastar SSE element
  // patches land new canvases. A subtree childList observer fires for
  // every DOM insertion; the `__sparklineInit` guard inside
  // initSparklines keeps each re-scan a no-op once a canvas is drawn.
  // Body-wide observation stays general (no per-tbody bookkeeping); the
  // addedSparkline pre-filter skips the scan entirely when a mutation
  // batch added no sparkline canvas, killing the scan-storm on
  // tile/toast/banner churn. Coalesce bursts via a microtask so a
  // single patch dropping many rows triggers one scan.
  function observeSwaps() {
    if (typeof MutationObserver !== "function" || !document.body) return;
    let scheduled = false;
    const observer = new MutationObserver(function (records) {
      if (scheduled) return;
      if (!addedSparkline(records)) return;
      scheduled = true;
      Promise.resolve().then(function () {
        scheduled = false;
        initSparklines();
      });
    });
    observer.observe(document.body, { childList: true, subtree: true });
  }

  // Expose initSparklines on window so other bundle modules (and the
  // shipped-bundle test) can reference the re-scan entry point by a
  // stable name that survives esbuild's minify pass.
  if (typeof window !== "undefined") {
    window.initSparklines = initSparklines;
  }

  function bootstrap() {
    initSparklines();
    observeSwaps();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bootstrap);
  } else {
    bootstrap();
  }
})();
