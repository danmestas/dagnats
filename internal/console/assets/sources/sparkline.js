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
 */

(function () {
  const SELECTOR = "canvas[data-sparkline-data]";

  function init() {
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

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
