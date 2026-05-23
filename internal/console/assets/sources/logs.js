// logs.js — /console/logs page enhancements:
//   - Server-Sent Events to /console/sse/logs prepend rows to the table.
//   - Pause / resume toggle pauses the EventSource (and resumes it).
//   - Severity / search / trace-ID controls submit the form via GET so
//     the URL captures the filter state — refresh / share works.
//
// Defensive: the boot function is a no-op when called on the wrong
// page (page-logs missing). All event listeners use { passive: true }
// where the spec allows it. No external dependencies.

(function () {
  if (typeof window === "undefined") return;
  if (window.consoleLogs && window.consoleLogs.__initialised) return;

  const ns = {};
  ns.__initialised = true;
  ns.__source = null;
  ns.__paused = false;

  ns.boot = function (root) {
    if (!root || root.getAttribute("data-page") !== "logs") return;
    ns.__root = root;
    ns.__tbody = root.querySelector("#logs-tbody");
    ns.__status = root.querySelector("#logs-stream-status");
    ns.__pauseBtn = root.querySelector("#logs-pause-resume");
    ns.__count = root.querySelector("#logs-count");
    ns.__filtersForm = root.querySelector("#logs-filters");

    if (ns.__pauseBtn) {
      ns.__pauseBtn.addEventListener("click", function (e) {
        e.preventDefault();
        if (ns.__paused) {
          ns.resume();
        } else {
          ns.pause();
        }
      });
    }
    ns.start();
  };

  ns.queryString = function () {
    if (!ns.__filtersForm) return "";
    const fd = new FormData(ns.__filtersForm);
    const params = new URLSearchParams();
    for (const [k, v] of fd.entries()) {
      if (typeof v === "string" && v.trim() !== "") {
        params.set(k, v.trim());
      }
    }
    const s = params.toString();
    return s ? "?" + s : "";
  };

  ns.start = function () {
    if (ns.__paused) return;
    ns.stop();
    if (!("EventSource" in window)) {
      ns.setStatus("offline", "SSE not supported");
      return;
    }
    const url = "/console/sse/logs" + ns.queryString();
    let src;
    try {
      src = new EventSource(url);
    } catch (err) {
      ns.setStatus("offline", "stream error");
      return;
    }
    ns.__source = src;
    ns.setStatus("connecting", "connecting...");
    src.addEventListener("open", function () {
      ns.setStatus("live", "live");
    });
    src.addEventListener("error", function () {
      ns.setStatus("offline", "disconnected");
    });
    // Parse the Datastar PatchElements wire format ourselves so we
    // can prepend rows + drive the connection indicator without the
    // full Datastar runtime hooked to this EventSource. The wire
    // shape is fixed: a sequence of `data: <key> <value>` lines
    // grouped into one event with name "datastar-patch-elements".
    src.addEventListener("datastar-patch-elements", function (e) {
      const data = (e && e.data) || "";
      const lines = data.split("\n");
      let selector = "";
      let mode = "outer";
      let html = "";
      for (const line of lines) {
        const idx = line.indexOf(" ");
        if (idx < 0) continue;
        const k = line.substring(0, idx);
        const v = line.substring(idx + 1);
        if (k === "selector") selector = v.trim();
        else if (k === "mode") mode = v.trim();
        else if (k === "elements") html += v;
      }
      if (!selector || !html) return;
      const target = document.querySelector(selector);
      if (!target) return;
      if (mode === "prepend") {
        target.insertAdjacentHTML("afterbegin", html);
        ns.trimRows(target);
      } else if (mode === "append") {
        target.insertAdjacentHTML("beforeend", html);
      } else if (mode === "outer") {
        target.outerHTML = html;
      } else if (mode === "inner") {
        target.innerHTML = html;
      } else if (mode === "remove") {
        target.remove();
      }
      ns.bumpRendered();
    });
  };

  // trimRows caps the rendered table at a sane upper bound so a busy
  // tail doesn't grow the DOM without limit. Matches the server's
  // logsListMax (500) — keeps initial-paint and tail-paint symmetric.
  ns.trimRows = function (tbody) {
    const max = 500;
    let rows = tbody.querySelectorAll("tr.logs-row");
    while (rows.length > max) {
      rows[rows.length - 1].remove();
      rows = tbody.querySelectorAll("tr.logs-row");
    }
  };

  ns.stop = function () {
    if (ns.__source) {
      try { ns.__source.close(); } catch (_) { /* ignore */ }
      ns.__source = null;
    }
  };

  ns.pause = function () {
    ns.__paused = true;
    ns.stop();
    if (ns.__pauseBtn) {
      ns.__pauseBtn.setAttribute("data-paused", "true");
      ns.__pauseBtn.setAttribute("aria-pressed", "true");
      const label = ns.__pauseBtn.querySelector("[data-pause-label]");
      if (label) label.textContent = "Resume";
    }
    ns.setStatus("paused", "paused");
  };

  ns.resume = function () {
    ns.__paused = false;
    if (ns.__pauseBtn) {
      ns.__pauseBtn.setAttribute("data-paused", "false");
      ns.__pauseBtn.setAttribute("aria-pressed", "false");
      const label = ns.__pauseBtn.querySelector("[data-pause-label]");
      if (label) label.textContent = "Pause";
    }
    ns.start();
  };

  ns.setStatus = function (state, label) {
    if (!ns.__status) return;
    ns.__status.setAttribute("data-connection", state);
    const lbl = ns.__status.querySelector(".logs-stream-label");
    if (lbl) lbl.textContent = label;
  };

  ns.bumpRendered = function () {
    if (!ns.__count || !ns.__tbody) return;
    const rendered = ns.__tbody.querySelectorAll("tr.logs-row").length;
    // Format: "<rendered> / <total>" — bump total too since a new row
    // arrived; the snapshot count was a point-in-time read.
    const txt = ns.__count.textContent || "";
    const parts = txt.split("/");
    let total = parts.length === 2 ? parseInt(parts[1].trim(), 10) : rendered;
    if (Number.isNaN(total)) total = rendered;
    total += 1;
    ns.__count.textContent = rendered + " / " + total;
  };

  window.consoleLogs = ns;
})();
