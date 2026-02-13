// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

// Live-refresh the workers list.
(function () {
  let activeStop = null;

  function formatLastSeen(seconds, status) {
    if (seconds === null || seconds === undefined) {
      return "never";
    }
    if (seconds <= 2) {
      return status === "offline" ? "last seen just now" : "just now";
    }
    if (seconds < 60) {
      const label = `${Math.max(seconds, 0)}s ago`;
      return status === "offline" ? `last seen ${label}` : label;
    }
    const minutes = Math.floor(seconds / 60);
    const label = `${minutes}m ago`;
    return status === "offline" ? `last seen ${label}` : label;
  }

  function renderRow(worker) {
    const queues = Array.isArray(worker.queues) ? worker.queues.join(", ") : "";
    const poolSize = worker.pool_size ?? "—";
    const active = worker.active ?? "—";
    return `
      <tr>
        <td>
          <div class="task-info">
            <strong><a href="/workers/${encodeURIComponent(worker.hostname)}/">${worker.hostname}</a></strong>
          </div>
        </td>
        <td><span class="badge ${worker.badge}">${worker.status}</span></td>
        <td>${poolSize}</td>
        <td>${active}</td>
        <td>${queues}</td>
        <td><small>${formatLastSeen(worker.last_seen_seconds, worker.status)}</small></td>
      </tr>
    `;
  }

  function renderTable(table, workers) {
    const tbody = table.querySelector("tbody");
    if (!tbody) {
      return;
    }
    const rows = workers.map((worker) => renderRow(worker)).join("");
    tbody.innerHTML = rows || `<tr><td colspan="6">No workers reported.</td></tr>`;
  }

  function init(root) {
    const scope = root instanceof HTMLElement ? root : document;
    const wrapper = scope.querySelector("[data-workers-table]");
    const table = wrapper?.querySelector("table");
    if (!wrapper || !table) {
      return;
    }
    if (wrapper.dataset.polling === "true") {
      return;
    }

    wrapper.dataset.polling = "true";

    async function fetchLoop() {
      try {
        const response = await fetch("/api/workers/", { cache: "no-store" });
        if (!response.ok) {
          return;
        }
        const payload = await response.json();
        if (!payload || !Array.isArray(payload.workers)) {
          return;
        }
        renderTable(table, payload.workers);
      } catch {
        return;
      }
    }

    fetchLoop();
    const timerId = window.setInterval(fetchLoop, 2000);
    activeStop = () => {
      window.clearInterval(timerId);
      wrapper.dataset.polling = "false";
    };
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", () => init(document));
  } else {
    init(document);
  }

  window.CeleryWorkersList = {
    init,
    stop() {
      if (activeStop) {
        activeStop();
        activeStop = null;
      }
    },
  };
})();
