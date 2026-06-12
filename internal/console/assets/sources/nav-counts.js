/*
 * dagnats console — nav badge count filler.
 *
 * The console nav renders on every page, so we do NOT scan ten lists
 * per page render server-side. Instead each A-item nav link carries an
 * empty placeholder `<span class="console-nav-badge" data-nav-count=
 * "<key>" hidden>`, and this script fetches the single JSON endpoint
 * `/console/api/nav-counts` once after load and fills the matching
 * badges.
 *
 * Honesty: the endpoint OMITS a key whose source was unavailable. A
 * badge whose key is absent (or whose value is not a finite number)
 * stays hidden — we never paint a fabricated 0. Services + Traces have
 * no placeholder at all (no data/route yet).
 */
(function () {
  function fill(counts) {
    if (!counts || typeof counts !== "object") return;
    const badges = document.querySelectorAll("[data-nav-count]");
    badges.forEach(function (el) {
      const key = el.getAttribute("data-nav-count");
      if (!key) return;
      const n = counts[key];
      if (typeof n !== "number" || !isFinite(n)) {
        // Source unavailable for this key — leave the badge hidden
        // rather than showing a count the server couldn't confirm.
        return;
      }
      el.textContent = String(n);
      el.removeAttribute("hidden");
    });
  }

  function load() {
    fetch("/console/api/nav-counts", {
      headers: { Accept: "application/json" },
    })
      .then(function (resp) {
        if (!resp.ok) return null;
        return resp.json();
      })
      .then(fill)
      .catch(function () {
        // Network/parse failure: badges simply stay hidden. The nav
        // is fully usable without counts.
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", load);
  } else {
    load();
  }
})();
