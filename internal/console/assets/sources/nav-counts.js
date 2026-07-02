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
  // Every nav link is a full page load, so each render starts with the
  // badges hidden and this script re-fetches. Painting only after the
  // fetch resolves made the counts blink empty for ~100ms on EVERY
  // navigation. We cache the last-known counts in sessionStorage and
  // paint them synchronously first, then refresh from the network — so
  // navigations are flash-free (only the very first visit has no cache).
  const CACHE_KEY = "dn-nav-counts";

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

  function paintCached() {
    try {
      const raw = sessionStorage.getItem(CACHE_KEY);
      if (raw) fill(JSON.parse(raw));
    } catch (e) {
      // Private-mode / quota / parse failure — just skip the cache and
      // let the fetch fill the badges. Never throw from the paint path.
    }
  }

  function cache(counts) {
    try {
      sessionStorage.setItem(CACHE_KEY, JSON.stringify(counts));
    } catch (e) {
      // Storage unavailable — the live fetch still filled the badges.
    }
  }

  function load() {
    paintCached();
    fetch("/console/api/nav-counts", {
      headers: { Accept: "application/json" },
    })
      .then(function (resp) {
        if (!resp.ok) return null;
        return resp.json();
      })
      .then(function (counts) {
        if (!counts) return;
        fill(counts);
        cache(counts);
      })
      .catch(function () {
        // Network/parse failure: cached badges (if any) stay; the nav
        // is fully usable without counts.
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", load);
  } else {
    load();
  }
})();
