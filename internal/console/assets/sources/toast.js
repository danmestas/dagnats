/*
 * dagnats console — toast feedback bus.
 *
 * Listens for the `console:toast` CustomEvent dispatched by action
 * handlers, surfaces a small slide-in card in the top-right corner,
 * and auto-dismisses after 3 seconds. Manual dismiss via the close
 * button. `prefers-reduced-motion` collapses the slide animation into
 * a fade.
 *
 * Event shape:
 *   document.dispatchEvent(new CustomEvent("console:toast", {
 *     detail: {
 *       level: "info" | "error",
 *       message: "Retry queued — run abc-123 created",
 *       linkHref: "/console/runs/abc-123",   // optional
 *       linkLabel: "View run",                // optional (default "Open")
 *       undoToken: "abc",                     // optional, 5-second undo
 *       undoHref: "/console/dlq/501/undo",    // optional, undo POST target
 *     },
 *   }));
 *
 * The toast lives in a dedicated container appended to <body> on first
 * call. Multiple toasts stack vertically; older toasts dismiss first.
 * Lookalike to the connection-state.js scaffold so contributors can
 * read both without re-orienting.
 */

(function () {
  const CONTAINER_ID = "console-toasts";
  const VISIBLE_MS = 3000;
  const UNDO_VISIBLE_MS = 5000;
  const MAX_TOASTS = 5;

  let toastSeq = 0;
  const prefersReducedMotion = (function () {
    try {
      return window.matchMedia(
        "(prefers-reduced-motion: reduce)").matches;
    } catch (e) {
      return false;
    }
  })();

  function getContainer() {
    let el = document.getElementById(CONTAINER_ID);
    if (el) return el;
    el = document.createElement("div");
    el.id = CONTAINER_ID;
    el.className = "console-toasts";
    el.setAttribute("role", "status");
    el.setAttribute("aria-live", "polite");
    el.setAttribute("aria-atomic", "false");
    document.body.appendChild(el);
    return el;
  }

  function trim() {
    const container = getContainer();
    while (container.children.length > MAX_TOASTS) {
      container.removeChild(container.firstElementChild);
    }
  }

  function render(detail) {
    const container = getContainer();
    const id = "console-toast-" + (++toastSeq);
    const toast = document.createElement("article");
    toast.id = id;
    const level = (detail.level === "error") ? "error" : "info";
    toast.className = "console-toast console-toast-" + level;
    if (prefersReducedMotion) {
      toast.classList.add("console-toast-reduced");
    }
    toast.setAttribute("data-toast-level", level);
    toast.innerHTML = buildBody(detail);

    container.appendChild(toast);
    trim();

    requestAnimationFrame(function () {
      toast.classList.add("is-visible");
    });

    const ttl = detail.undoToken ? UNDO_VISIBLE_MS : VISIBLE_MS;
    const closeBtn = toast.querySelector(".console-toast-close");
    if (closeBtn) {
      closeBtn.addEventListener("click", function () { remove(toast); });
    }
    setTimeout(function () { remove(toast); }, ttl);
    return id;
  }

  function buildBody(detail) {
    const msg = escapeText(detail.message || "");
    const linkHref = detail.linkHref ? escapeAttr(detail.linkHref) : "";
    const linkLabel = escapeText(detail.linkLabel || "Open");
    const undoHref = detail.undoHref ? escapeAttr(detail.undoHref) : "";
    let link = "";
    if (linkHref) {
      link = '<a class="console-toast-link" href="' +
        linkHref + '">' + linkLabel + '</a>';
    }
    let undo = "";
    if (detail.undoToken && undoHref) {
      undo = '<button type="button" class="console-toast-undo" ' +
        'data-undo-token="' + escapeAttr(detail.undoToken) + '" ' +
        'data-undo-href="' + undoHref + '">Undo</button>';
    }
    return '<div class="console-toast-body">' +
      '<p class="console-toast-message">' + msg + '</p>' +
      link + undo +
      '</div>' +
      '<button type="button" class="console-toast-close" ' +
      'aria-label="Dismiss notification">&times;</button>';
  }

  function remove(toast) {
    if (!toast || !toast.parentNode) return;
    toast.classList.remove("is-visible");
    toast.classList.add("is-leaving");
    setTimeout(function () {
      if (toast.parentNode) toast.parentNode.removeChild(toast);
    }, prefersReducedMotion ? 0 : 200);
  }

  function escapeText(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      switch (c) {
        case "&": return "&amp;";
        case "<": return "&lt;";
        case ">": return "&gt;";
        case '"': return "&quot;";
        case "'": return "&#39;";
        default: return c;
      }
    });
  }
  function escapeAttr(s) { return escapeText(s); }

  function onToast(evt) {
    if (!evt || !evt.detail) return;
    if (typeof evt.detail.message !== "string") return;
    render(evt.detail);
  }

  function onUndoClick(evt) {
    const target = evt && evt.target;
    if (!target || !target.classList) return;
    if (!target.classList.contains("console-toast-undo")) return;
    const href = target.getAttribute("data-undo-href") || "";
    const token = target.getAttribute("data-undo-token") || "";
    if (!href || !token) return;
    target.disabled = true;
    fetch(href, {
      method: "POST",
      headers: { "X-Undo-Token": token, "Accept": "application/json" },
      credentials: "same-origin",
    }).then(function (resp) {
      const toast = target.closest(".console-toast");
      if (resp.ok) {
        document.dispatchEvent(new CustomEvent("console:toast", {
          detail: { level: "info", message: "Undone." },
        }));
        if (toast) remove(toast);
      } else {
        target.disabled = false;
        document.dispatchEvent(new CustomEvent("console:toast", {
          detail: { level: "error", message: "Undo failed." },
        }));
      }
    }).catch(function () {
      target.disabled = false;
      document.dispatchEvent(new CustomEvent("console:toast", {
        detail: { level: "error", message: "Undo failed." },
      }));
    });
  }

  function init() {
    document.addEventListener("console:toast", onToast);
    document.addEventListener("click", onUndoClick);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }

  // Exposed for tests + browser smoke harness.
  window.__dagnatsToast = {
    show: function (detail) {
      onToast(new CustomEvent("console:toast", { detail: detail }));
    },
    count: function () {
      const c = document.getElementById(CONTAINER_ID);
      return c ? c.children.length : 0;
    },
  };
})();
