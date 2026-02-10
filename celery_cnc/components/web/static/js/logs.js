// Live-refresh the log tail and keep the view anchored to the bottom.
(function () {
  const REFRESH_INTERVAL_MS = 2000;

  function setVisible(element, visible) {
    if (!element) {
      return;
    }
    element.classList.toggle("is-hidden", !visible);
  }

  function scrollToBottom(element) {
    if (!element) {
      return;
    }
    element.scrollTop = element.scrollHeight;
  }

  function escapeHtml(value) {
    return value
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function stripAnsi(value) {
    return value.replace(/\u001b\[[0-9;]*[a-zA-Z]/g, "");
  }

  function classifyLine(line) {
    if (/\bCRITICAL\b/.test(line)) {
      return "critical";
    }
    if (/\bERROR\b/.test(line)) {
      return "error";
    }
    if (/\bWARN(?:ING)?\b/.test(line)) {
      return "warning";
    }
    if (/\bINFO\b/.test(line)) {
      return "info";
    }
    if (/\bDEBUG\b/.test(line)) {
      return "debug";
    }
    if (/\bTRACE\b/.test(line)) {
      return "trace";
    }
    return "default";
  }

  function formatLogContent(content) {
    const lines = content.split(/\r?\n/);
    return lines
      .map((line) => {
        const clean = stripAnsi(line);
        const escaped = escapeHtml(clean);
        const level = classifyLine(clean);
        const levelClass = level === "default" ? "log-line" : `log-line log-level-${level}`;
        return `<span class="${levelClass}">${escaped}</span>`;
      })
      .join("\n");
  }

  function buildUrl() {
    const url = new URL(window.location.href);
    url.searchParams.set("format", "json");
    return url.toString();
  }

  function applyPayload(payload, elements) {
    const error = typeof payload.error === "string" && payload.error.length > 0 ? payload.error : "";
    const selectedFile =
      typeof payload.selected_file === "string" && payload.selected_file.length > 0 ? payload.selected_file : "";
    const content = typeof payload.content === "string" ? payload.content : "";

    if (elements.error) {
      elements.error.textContent = error;
      setVisible(elements.error, Boolean(error));
    }

    if (elements.file) {
      elements.file.textContent = selectedFile ? `Reading ${selectedFile}` : "";
      setVisible(elements.file, Boolean(selectedFile));
    }

    if (elements.output) {
      elements.output.innerHTML = formatLogContent(content);
      setVisible(elements.output, content.length > 0);
      if (content.length > 0) {
        requestAnimationFrame(() => scrollToBottom(elements.output));
      }
    }

    if (elements.empty) {
      setVisible(elements.empty, !error && content.length === 0);
    }
  }

  function init() {
    const panel = document.querySelector("[data-log-panel='true']");
    if (!panel) {
      return;
    }

    const elements = {
      error: panel.querySelector("[data-log-error]"),
      file: panel.querySelector("[data-log-file]"),
      output: panel.querySelector("[data-log-output]"),
      empty: panel.querySelector("[data-log-empty]"),
    };

    if (elements.output && !elements.output.classList.contains("is-hidden")) {
      const existing = elements.output.textContent || "";
      if (existing.trim().length > 0) {
        elements.output.innerHTML = formatLogContent(existing);
      }
      scrollToBottom(elements.output);
    }

    let inFlight = false;

    async function refreshLogs() {
      if (inFlight) {
        return;
      }
      inFlight = true;
      try {
        const response = await fetch(buildUrl(), {
          cache: "no-store",
          headers: { Accept: "application/json" },
        });
        if (!response.ok) {
          return;
        }
        const payload = await response.json();
        if (!payload || typeof payload !== "object") {
          return;
        }
        applyPayload(payload, elements);
      } catch {
        return;
      } finally {
        inFlight = false;
      }
    }

    refreshLogs();
    const timerId = window.setInterval(refreshLogs, REFRESH_INTERVAL_MS);
    window.CeleryLogs = {
      stop() {
        window.clearInterval(timerId);
      },
    };
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
