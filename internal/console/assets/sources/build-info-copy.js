/*
 * dagnats console — build-info footer click-to-copy.
 *
 * Wires every `.build-info-host[data-copy]` element to copy its
 * data-copy payload to the clipboard on click or Enter/Space. The
 * footer is identity context; the most common operator action on a
 * host URL is "grab it for a CLI command", so a one-click affordance
 * pays for the tiny script.
 *
 * Norman's Feedback principle: a copy without confirmation is a copy
 * the operator can't trust. The element flips to `.is-copied` for
 * one second post-copy so the visual ack is unmistakable.
 */
(function () {
  function copy(el) {
    var text = el.getAttribute("data-copy") || el.textContent;
    if (!text) return;
    var done = function () {
      el.classList.add("is-copied");
      setTimeout(function () {
        el.classList.remove("is-copied");
      }, 1000);
    };
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(done, function () {});
    } else {
      // Legacy fallback for non-secure contexts: a transient textarea
      // + document.execCommand is the only path before the Clipboard
      // API. Cleaned up immediately.
      var ta = document.createElement("textarea");
      ta.value = text;
      ta.setAttribute("readonly", "");
      ta.style.position = "absolute";
      ta.style.left = "-9999px";
      document.body.appendChild(ta);
      ta.select();
      try {
        document.execCommand("copy");
        done();
      } catch (_) {}
      document.body.removeChild(ta);
    }
  }

  function init() {
    var hosts = document.querySelectorAll(
      ".build-info-host[data-copy]"
    );
    for (var i = 0; i < hosts.length; i++) {
      (function (el) {
        el.addEventListener("click", function () {
          copy(el);
        });
        el.addEventListener("keydown", function (ev) {
          if (ev.key === "Enter" || ev.key === " ") {
            ev.preventDefault();
            copy(el);
          }
        });
      })(hosts[i]);
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
