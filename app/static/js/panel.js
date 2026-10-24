/* Fusion Phase D panel: named SSE `state` stream.
   The console log keeps its own anonymous `data:` stream in console.js
   (source.onmessage). Named events never reach onmessage, so the two streams
   cannot pollute each other. This file only listens for `event: state`. */
(function () {
  "use strict";

  var badge = document.getElementById("panel-sse-state");
  var source = null;
  var retry = 1000;

  function text(value) {
    return value === null || value === undefined || value === "" ? "—" : String(value);
  }

  function render(payload) {
    var st = payload && payload.state;
    if (!st) return;
    var map = {
      "host-sandbox-state": st.sandbox_state,
      "host-mc-state": st.mc_state,
      "host-link-status": st.status,
      "agent-connected": st.connected ? "yes" : "no"
    };
    Object.keys(map).forEach(function (id) {
      var el = document.getElementById(id);
      if (el) el.textContent = text(map[id]);
    });
  }

  function connect() {
    if (!window.EventSource) {
      if (badge) badge.textContent = "SSE unsupported";
      return;
    }
    if (source) source.close();
    source = new EventSource("/events/state");
    source.addEventListener("state", function (ev) {
      retry = 1000;
      if (badge) badge.textContent = "live";
      try {
        render(JSON.parse(ev.data));
      } catch (err) {
        /* a malformed frame must not kill the stream */
      }
    });
    source.onerror = function () {
      if (badge) badge.textContent = "reconnecting…";
      source.close();
      setTimeout(connect, retry);
      retry = Math.min(retry * 2, 15000);
    };
  }

  connect();
})();
