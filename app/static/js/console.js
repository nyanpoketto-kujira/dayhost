/* DayHost console: SSE log stream + command sender.
   Fase 2: commands are really executed over RCON when RCON_PASSWORD is set.
   The backend enforces a deny-by-default allow-list (403) and a 1 cmd/s rate
   limit (429); see app/routers/console.py. */
(function () {
  "use strict";

  const logEl = document.getElementById("console-log");
  const stateEl = document.getElementById("sse-state");
  const form = document.getElementById("console-form");

  const MAX_LINES = 500;
  function append(text) {
    if (!logEl) return;
    const atBottom = logEl.scrollTop + logEl.clientHeight >= logEl.scrollHeight - 8;
    logEl.textContent += text + "\n";
    const lines = logEl.textContent.split("\n");
    if (lines.length > MAX_LINES) {
      logEl.textContent = lines.slice(-MAX_LINES).join("\n");
    }
    if (atBottom) logEl.scrollTop = logEl.scrollHeight;
  }

  let source = null;
  let retry = 1000;

  function connect() {
    if (source) source.close();
    source = new EventSource("/events");

    source.onopen = function () {
      retry = 1000;
      if (stateEl) stateEl.textContent = "live";
    };
    source.onmessage = function (ev) {
      append(ev.data);
    };
    source.onerror = function () {
      if (stateEl) stateEl.textContent = "reconnecting…";
      source.close();
      // Exponential backoff, capped at 15s.
      setTimeout(connect, retry);
      retry = Math.min(retry * 2, 15000);
    };
  }

  if (window.EventSource) {
    connect();
  } else if (stateEl) {
    stateEl.textContent = "SSE unsupported";
  }

  if (form) {
    form.addEventListener("submit", async function (e) {
      e.preventDefault();
      const input = form.querySelector('input[name="command"]');
      const cmd = (input && input.value || "").trim();
      if (!cmd) return;
      input.value = "";
      const body = new URLSearchParams({ command: cmd });
      try {
        const res = await fetch("/api/console/send", {
          method: "POST",
          headers: { "Content-Type": "application/x-www-form-urlencoded" },
          body: body.toString(),
        });
        const data = await res.json();
        if (!data.ok) {
          append("[error] " + (data.error || "command failed"));
        } else if (data.executed === false) {
          append("[stub] " + (data.note || "not executed"));
        }
      } catch (err) {
        append("[error] " + err);
      }
    });
  }
})();
