// metrics.js — client-side bootstrap for /console/ops/metrics.
//
// Responsibilities:
//   1. Initialise one uPlot per server-rendered chart canvas. Data
//      arrives in data-* attributes which we parse on DOMContentLoaded.
//   2. Render the anomaly overlay (muted-rust open circles) over the
//      latency chart as a points-only overlay drawn via uPlot's
//      draw hook so the markers track the chart axes.
//   3. Subscribe to SSE-driven tile patches (the server already pushes
//      patches for the headline tiles). When a patch lands, fetch the
//      matching chart's JSON via /console/api/metrics/chart/<id> and
//      call uPlot.setData() so the chart redraws without a page
//      refresh. Per-chart throttle mirrors the server-side 4Hz cap.
//   4. On each setData call, briefly highlight the latest data point
//      with a fading paper-indigo overlay (600ms). Respect
//      prefers-reduced-motion — that media query disables the fade.
//
// Bounded loops everywhere. No timers we can't cancel. Works in
// browsers without modules.
(function () {
  "use strict";

  // anomalyURLFor builds the runs-filter URL for one anomaly marker.
  // Exported on window so the test harness can verify the wire shape
  // even on pages where uPlot isn't loaded (the chart canvas may not
  // render, but the URL contract should still be inspectable).
  function anomalyURLFor(workflow, sinceSecs, untilSecs) {
    const params = [];
    if (typeof workflow === "string" && workflow.length > 0) {
      params.push("workflow=" + encodeURIComponent(workflow));
    }
    params.push("status=failed");
    if (Number.isFinite(sinceSecs)) {
      params.push("since=" + Math.floor(sinceSecs));
    }
    if (Number.isFinite(untilSecs)) {
      params.push("until=" + Math.floor(untilSecs));
    }
    return "/console/runs?" + params.join("&");
  }
  if (typeof window === "object") {
    window.__dagnatsAnomalyURLFor = anomalyURLFor;
  }

  if (typeof window.uPlot !== "function") {
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

  // Mapping from a tile's data-metric to the chart-id that depends on it.
  // The SSE emits PatchElements per tile; we use the metric name to
  // decide which chart needs a setData refresh.
  const METRIC_TO_CHART = {
    "workflow.runs.completed": "chart-throughput",
    "workflow.runs.failed": "chart-throughput",
    "snapshot.save.duration_ms": "chart-latency",
  };

  const SETDATA_HZ = 4;
  const SETDATA_INTERVAL_MS = Math.floor(1000 / SETDATA_HZ);
  const HIGHLIGHT_DURATION_MS = 600;

  const PREFERS_REDUCED_MOTION =
    typeof window.matchMedia === "function" &&
    window.matchMedia("(prefers-reduced-motion: reduce)").matches;

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
    const parts = trimmed.split(/[\s,]+/);
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

  function parseAnomalyReasons(raw) {
    if (typeof raw !== "string" || raw.length === 0) {
      return [];
    }
    const MAX = 1024;
    const out = raw.split("|");
    if (out.length > MAX) {
      return out.slice(0, MAX);
    }
    return out;
  }

  function buildAnomalyHook(canvas) {
    return function (u) {
      const xs = canvas.__anomalyXs || [];
      const ys = canvas.__anomalyYs || [];
      if (xs.length === 0 || ys.length === 0) {
        return;
      }
      const ctx = u.ctx;
      if (!ctx) {
        return;
      }
      ctx.save();
      ctx.strokeStyle = strokeFor("muted-rust");
      ctx.lineWidth = 1.5;
      const MAX = 1024;
      for (let i = 0; i < xs.length && i < ys.length && i < MAX; i++) {
        const x = u.valToPos(xs[i], "x", true);
        const y = u.valToPos(ys[i], "y", true);
        if (!Number.isFinite(x) || !Number.isFinite(y)) {
          continue;
        }
        ctx.beginPath();
        ctx.arc(x, y, 6, 0, Math.PI * 2);
        ctx.stroke();
      }
      ctx.restore();
    };
  }

  function buildHighlightHook(canvas) {
    return function (u) {
      const ts = canvas.__lastSetData || 0;
      if (ts === 0 || PREFERS_REDUCED_MOTION) {
        return;
      }
      const elapsed = Date.now() - ts;
      if (elapsed >= HIGHLIGHT_DURATION_MS) {
        return;
      }
      const opacity = 0.3 * (1 - elapsed / HIGHLIGHT_DURATION_MS);
      const ctx = u.ctx;
      if (!ctx) {
        return;
      }
      const xs = u.data[0];
      const last = xs.length - 1;
      if (last < 0) {
        return;
      }
      const px = u.valToPos(xs[last], "x", true);
      const MAX = 4;
      for (let s = 1; s < u.series.length && s < MAX + 1; s++) {
        const ys = u.data[s];
        if (!ys || ys.length === 0) {
          continue;
        }
        const py = u.valToPos(ys[last], "y", true);
        if (!Number.isFinite(px) || !Number.isFinite(py)) {
          continue;
        }
        ctx.save();
        ctx.fillStyle = "rgba(79, 99, 180, " + opacity.toFixed(3) + ")";
        ctx.beginPath();
        ctx.arc(px, py, 7, 0, Math.PI * 2);
        ctx.fill();
        ctx.restore();
      }
      if (typeof window.requestAnimationFrame === "function") {
        window.requestAnimationFrame(function () {
          if (canvas.__uplot && typeof canvas.__uplot.redraw === "function") {
            canvas.__uplot.redraw(false);
          }
        });
      }
    };
  }

  // buildZoomFlagHook returns a uPlot setScale hook that tracks whether
  // the operator has an active manual x-zoom. The hook fires on every
  // x-scale change — drag-zoom, double-click reset, and our own re-pin.
  // We set canvas.__userZoomed when the current x-domain differs from
  // the server-pinned full window, and clear it when it snaps back. The
  // SSE refresh path consults this flag and skips its forced re-pin
  // while a zoom is active, so a live chart stays zoomable.
  function buildZoomFlagHook(canvas) {
    return function (u, key) {
      if (key !== "x") {
        return;
      }
      const scale = u.scales.x;
      const pinMin = canvas.__chartXMin;
      const pinMax = canvas.__chartXMax;
      if (!Number.isFinite(pinMin) || !Number.isFinite(pinMax) ||
          !Number.isFinite(scale.min) || !Number.isFinite(scale.max)) {
        return;
      }
      // Tolerance of one second absorbs float jitter from valToPos.
      const atFullWindow =
        Math.abs(scale.min - pinMin) <= 1 &&
        Math.abs(scale.max - pinMax) <= 1;
      canvas.__userZoomed = !atFullWindow;
    };
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
    const opts = {
      width: Math.max(canvas.clientWidth, 320),
      height: 240,
      cursor: { drag: { x: true, y: false } },
      // The template renders its own accessible .console-chart-legend;
      // uPlot ships no legend CSS here, so its DOM legend would stack as
      // unstyled "Time / Completed / Failed" text over the plot. Suppress it.
      legend: { show: false },
      axes: [
        // size reserves vertical room for the time axis. uPlot renders
        // a two-row time axis (incremental ticks + an absolute date row);
        // the default ~50px band crams the date row flush against the
        // canvas bottom edge. 64px gives the date row clearance so it
        // reads as part of the chart, not spilling past it.
        { stroke: strokeFor("warm-near-black"), size: 64 },
        {
          stroke: strokeFor("warm-near-black"),
          label: unit || "",
          grid: { stroke: strokeFor("paper-stripe"), width: 0.5 },
        },
      ],
      series: series,
      hooks: {
        draw: [
          buildAnomalyHook(canvas),
          buildHighlightHook(canvas),
        ],
        setScale: [
          buildZoomFlagHook(canvas),
        ],
      },
    };
    // Pin the x-domain to the server-clamped recent window so µPlot
    // stops auto-extrapolating future ticks under sparse data.
    const xmin = canvas.__chartXMin;
    const xmax = canvas.__chartXMax;
    if (Number.isFinite(xmin) && Number.isFinite(xmax) && xmax > xmin) {
      opts.scales = { x: { range: [xmin, xmax] } };
    }
    return opts;
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
    canvas.__anomalyXs = parseFloats(canvas.dataset.chartAnomalyX || "");
    canvas.__anomalyYs = parseFloats(canvas.dataset.chartAnomalyY || "");
    canvas.__anomalyReasons = parseAnomalyReasons(
      canvas.dataset.chartAnomalyReasons || "",
    );
    canvas.__anomalySinceSecs = parseFloats(
      canvas.dataset.chartAnomalySince || "",
    );
    canvas.__anomalyUntilSecs = parseFloats(
      canvas.dataset.chartAnomalyUntil || "",
    );
    canvas.__chartWorkflow = canvas.dataset.chartWorkflow || "";
    canvas.__chartXMin = parseFloat(canvas.dataset.chartXmin);
    canvas.__chartXMax = parseFloat(canvas.dataset.chartXmax);
    canvas.__lastFetch = 0;
    canvas.__lastSetData = 0;
    canvas.__userZoomed = false;
    const data = [xs];
    const MAX = 16;
    for (let i = 0; i < series.length && i < MAX; i++) {
      data.push(values[i] || []);
    }
    const opts = buildOptions(
      canvas, series, canvas.dataset.chartUnit || "",
    );
    try {
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
    installSSEHook();
    installAnomalyTooltip();
    installAnomalyClick();
  }

  // installAnomalyClick navigates to /console/runs filtered to the
  // anomaly's time window when the operator clicks a marker. Reuses
  // the same hit-test the tooltip uses so the cursor and the click
  // agree on what's "near" a marker.
  function installAnomalyClick() {
    document.addEventListener("click", function (ev) {
      const canvases = document.querySelectorAll(
        ".console-chart-canvas[data-chart-id]",
      );
      const MAX = 8;
      for (let i = 0; i < canvases.length && i < MAX; i++) {
        const c = canvases[i];
        const idx = anomalyHitIndex(c, ev.clientX, ev.clientY);
        if (idx < 0) {
          continue;
        }
        const url = anomalyURLFor(
          c.__chartWorkflow || "",
          c.__anomalySinceSecs && c.__anomalySinceSecs[idx],
          c.__anomalyUntilSecs && c.__anomalyUntilSecs[idx],
        );
        if (url) {
          window.location.href = url;
          ev.preventDefault();
        }
        return;
      }
    });
  }

  // anomalyHitIndex returns the index of the anomaly marker under the
  // given viewport coordinates, or -1 if none. Pulled out so both the
  // click handler and the tooltip use one source of truth.
  function anomalyHitIndex(canvas, clientX, clientY) {
    if (!canvas || !canvas.__uplot || !canvas.__anomalyXs) {
      return -1;
    }
    const rect = canvas.getBoundingClientRect();
    if (clientX < rect.left || clientX > rect.right ||
        clientY < rect.top || clientY > rect.bottom) {
      return -1;
    }
    const u = canvas.__uplot;
    const HIT = 10;
    const LIM = 1024;
    for (let k = 0; k < canvas.__anomalyXs.length && k < LIM; k++) {
      const px = u.valToPos(canvas.__anomalyXs[k], "x", true) + rect.left;
      const py = u.valToPos(canvas.__anomalyYs[k], "y", true) + rect.top;
      const dx = clientX - px;
      const dy = clientY - py;
      if (dx * dx + dy * dy <= HIT * HIT) {
        return k;
      }
    }
    return -1;
  }

  // installSSEHook listens for the tile patches the server emits.
  // Datastar replaces the tile DOM on each accepted ingest; we observe
  // the document and look at data-metric attributes to decide which
  // charts to refresh.
  function installSSEHook() {
    if (typeof MutationObserver !== "function") {
      return;
    }
    const observer = new MutationObserver(function (mutations) {
      const MAX = 128;
      for (let i = 0; i < mutations.length && i < MAX; i++) {
        const m = mutations[i];
        if (m.addedNodes && m.addedNodes.length > 0) {
          for (let j = 0; j < m.addedNodes.length && j < MAX; j++) {
            const node = m.addedNodes[j];
            if (node.nodeType !== 1) {
              continue;
            }
            if (typeof node.matches === "function" &&
                node.matches("[data-metric]")) {
              handleTilePatch(node);
            }
          }
        }
        if (m.target && typeof m.target.matches === "function" &&
            m.target.matches("[data-metric]")) {
          handleTilePatch(m.target);
        }
      }
    });
    observer.observe(document.body, {
      childList: true,
      subtree: true,
      attributes: true,
      attributeFilter: ["data-metric"],
    });
  }

  // installAnomalyTooltip surfaces the anomaly reason on hover. Uses
  // a single global tooltip element so we don't leak DOM per chart.
  function installAnomalyTooltip() {
    const tooltip = document.createElement("div");
    tooltip.className = "console-chart-tooltip";
    tooltip.style.display = "none";
    document.body.appendChild(tooltip);
    document.addEventListener("mousemove", function (ev) {
      const canvases = document.querySelectorAll(
        ".console-chart-canvas[data-chart-id]",
      );
      const MAX = 8;
      let foundReason = "";
      let hoveringMarker = false;
      for (let i = 0; i < canvases.length && i < MAX; i++) {
        const c = canvases[i];
        const idx = anomalyHitIndex(c, ev.clientX, ev.clientY);
        if (idx >= 0) {
          foundReason = c.__anomalyReasons[idx] || "anomaly";
          hoveringMarker = true;
          c.style.cursor = "pointer";
          break;
        } else if (c.style && c.style.cursor === "pointer") {
          c.style.cursor = "";
        }
      }
      if (foundReason) {
        tooltip.textContent = foundReason;
        tooltip.style.left = (ev.clientX + 12) + "px";
        tooltip.style.top = (ev.clientY + 12) + "px";
        tooltip.style.display = "block";
      } else {
        tooltip.style.display = "none";
      }
      if (!hoveringMarker) {
        // Clear cursors on canvases the mouse moved away from.
        for (let i = 0; i < canvases.length && i < MAX; i++) {
          if (canvases[i].style && canvases[i].style.cursor === "pointer") {
            canvases[i].style.cursor = "";
          }
        }
      }
    });
  }

  function handleTilePatch(node) {
    const metric = node.getAttribute("data-metric");
    if (!metric) {
      return;
    }
    const chartID = METRIC_TO_CHART[metric];
    if (!chartID) {
      return;
    }
    scheduleSetData(chartID);
  }

  function scheduleSetData(chartID) {
    const canvas = document.querySelector(
      ".console-chart-canvas[data-chart-id='" + chartID + "']",
    );
    if (!canvas || !canvas.__uplot) {
      return;
    }
    const now = Date.now();
    if (canvas.__lastFetch && now - canvas.__lastFetch < SETDATA_INTERVAL_MS) {
      return;
    }
    canvas.__lastFetch = now;
    fetchAndApply(canvas, chartID);
  }

  function fetchAndApply(canvas, chartID) {
    fetch("/console/api/metrics/chart/" + encodeURIComponent(chartID), {
      headers: { "Accept": "application/json" },
      credentials: "same-origin",
    }).then(function (resp) {
      if (!resp.ok) {
        return null;
      }
      return resp.json();
    }).then(function (payload) {
      if (!payload || !payload.x || !payload.series) {
        return;
      }
      applySetData(canvas, payload);
    }).catch(function (err) {
      console.warn("metrics.js: chart refresh failed", err);
    });
  }

  function applySetData(canvas, payload) {
    if (!canvas.__uplot) {
      return;
    }
    const data = [payload.x];
    const MAX = 16;
    for (let i = 0; i < payload.series.length && i < MAX; i++) {
      data.push(payload.series[i].values || []);
    }
    canvas.__anomalyXs = [];
    canvas.__anomalyYs = [];
    canvas.__anomalyReasons = [];
    canvas.__anomalySinceSecs = [];
    canvas.__anomalyUntilSecs = [];
    if (Array.isArray(payload.anomalies)) {
      const MAX_A = 256;
      for (let i = 0; i < payload.anomalies.length && i < MAX_A; i++) {
        const a = payload.anomalies[i];
        canvas.__anomalyXs.push(a.TimestampSecs);
        canvas.__anomalyYs.push(a.ValueMs);
        canvas.__anomalyReasons.push(a.Reason || "anomaly");
        canvas.__anomalySinceSecs.push(a.WindowStartSecs || 0);
        canvas.__anomalyUntilSecs.push(a.WindowEndSecs || 0);
      }
    }
    canvas.__lastSetData = Date.now();
    const xmin = Number(payload.xmin);
    const xmax = Number(payload.xmax);
    if (Number.isFinite(xmin) && Number.isFinite(xmax) && xmax > xmin) {
      canvas.__chartXMin = xmin;
      canvas.__chartXMax = xmax;
    }
    try {
      // Only re-pin the x-domain to the server window when the operator
      // has NOT manually drag-zoomed. Forcing setScale every 4Hz tick
      // would snap an active zoom back to the full window, making the
      // drag-zoom affordance dead on a live chart.
      if (!canvas.__userZoomed &&
          typeof canvas.__uplot.setScale === "function" &&
          Number.isFinite(canvas.__chartXMin) &&
          Number.isFinite(canvas.__chartXMax) &&
          canvas.__chartXMax > canvas.__chartXMin) {
        canvas.__uplot.setScale("x", {
          min: canvas.__chartXMin,
          max: canvas.__chartXMax,
        });
      }
      // resetScales=false while zoomed keeps the operator's drag-zoom
      // window across the data swap; otherwise let uPlot honor the
      // pinned init range.
      canvas.__uplot.setData(data, !canvas.__userZoomed);
    } catch (err) {
      console.warn("metrics.js: setData failed", err);
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bootAll, { once: true });
  } else {
    bootAll();
  }
})();
