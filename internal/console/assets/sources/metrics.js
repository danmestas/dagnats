// metrics.js — client-side bootstrap for /console/ops/metrics.
//
// Initialises one µPlot per server-rendered chart canvas. The server
// rendered the data into data-* attributes (data-chart-x,
// data-chart-series, data-chart-values); we parse it on
// DOMContentLoaded and pass it to µPlot.
//
// Live updates arrive via the metrics SSE stream (registered by the
// page's data-init) which patches tiles. Charts re-init on each
// significant change via a global window event MetricChartReplace
// that the SSE handler dispatches.
//
// The script is deliberately small. Bounded loops everywhere; no
// timers; works in browsers without modules.
(function () {
  "use strict";

  if (typeof window.uPlot !== "function") {
    // µPlot didn't load — fall back to plain-text values.
    console.warn("metrics.js: window.uPlot not defined, skipping chart init");
    return;
  }

  const STROKES = {
    "paper-indigo": "#4f63b4",
    "warm-clay": "#b76a4a",
    "muted-rust": "#8a3d2e",
    "paper-stripe": "#bcb8a9",
    "warm-near-black": "#2b261f",
    "warm-cream": "#f7f1e3",
  };

  function strokeFor(name) {
    if (typeof name !== "string" || name.length === 0) {
      return STROKES["warm-near-black"];
    }
    if (Object.prototype.hasOwnProperty.call(STROKES, name)) {
      return STROKES[name];
    }
    return STROKES["warm-near-black"];
  }

  function parseFloats(raw) {
    if (typeof raw !== "string" || raw.length === 0) {
      return [];
    }
    const trimmed = raw.replace(/^\[/, "").replace(/\]$/, "");
    if (trimmed.length === 0) {
      return [];
    }
    const parts = trimmed.split(/\s+|,/);
    const out = [];
    const MAX = 4096;
    for (let i = 0; i < parts.length && i < MAX; i++) {
      const v = parseFloat(parts[i]);
      if (Number.isFinite(v)) {
        out.push(v);
      }
    }
    return out;
  }

  function parseValuesArray(raw) {
    if (typeof raw !== "string" || raw.length === 0) {
      return [];
    }
    const out = [];
    const MAX = 16;
    const groups = raw.split(";");
    for (let i = 0; i < groups.length && i < MAX; i++) {
      const g = groups[i].trim();
      if (g.length === 0) {
        continue;
      }
      out.push(parseFloats(g));
    }
    return out;
  }

  function parseSeriesMeta(raw) {
    if (typeof raw !== "string" || raw.length === 0) {
      return [];
    }
    const out = [];
    const MAX = 16;
    const items = raw.split(";");
    for (let i = 0; i < items.length && i < MAX; i++) {
      const meta = items[i].trim();
      if (meta.length === 0) {
        continue;
      }
      const parts = meta.split("|");
      const label = parts[0] || "series " + (i + 1);
      const stroke = parts[1] || "paper-indigo";
      out.push({ label: label, strokeName: stroke });
    }
    return out;
  }

  function buildOptions(canvas, seriesMeta, unit) {
    const series = [
      { label: "Time" },
    ];
    const MAX = 16;
    for (let i = 0; i < seriesMeta.length && i < MAX; i++) {
      const sm = seriesMeta[i];
      series.push({
        label: sm.label,
        stroke: strokeFor(sm.strokeName),
        width: 1.5,
        points: { show: false },
      });
    }
    return {
      width: Math.max(canvas.clientWidth, 320),
      height: 220,
      cursor: { drag: { x: true, y: false } },
      legend: { live: true },
      axes: [
        { stroke: strokeFor("warm-near-black") },
        {
          stroke: strokeFor("warm-near-black"),
          label: unit || "",
          grid: { stroke: strokeFor("paper-stripe"), width: 0.5 },
        },
      ],
      series: series,
    };
  }

  function initChart(canvas) {
    if (canvas === null || typeof canvas.dataset !== "object") {
      return;
    }
    const xs = parseFloats(canvas.dataset.chartX || "");
    if (xs.length === 0) {
      return;
    }
    const series = parseSeriesMeta(canvas.dataset.chartSeries || "");
    const values = parseValuesArray(canvas.dataset.chartValues || "");
    if (series.length === 0 || values.length === 0) {
      return;
    }
    const data = [xs];
    const MAX = 16;
    for (let i = 0; i < series.length && i < MAX; i++) {
      data.push(values[i] || []);
    }
    const opts = buildOptions(
      canvas, series, canvas.dataset.chartUnit || "",
    );
    try {
      // eslint-disable-next-line no-new
      const u = new window.uPlot(opts, data, canvas);
      canvas.__uplot = u;
    } catch (err) {
      console.warn("metrics.js: uPlot init failed", err);
    }
  }

  function bootAll() {
    const canvases = document.querySelectorAll(
      ".console-chart-canvas[data-chart-id]",
    );
    if (!canvases || canvases.length === 0) {
      return;
    }
    const MAX = 64;
    for (let i = 0; i < canvases.length && i < MAX; i++) {
      initChart(canvases[i]);
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bootAll, { once: true });
  } else {
    bootAll();
  }
})();
