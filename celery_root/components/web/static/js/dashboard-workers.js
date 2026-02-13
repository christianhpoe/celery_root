// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

// Live-refresh the worker table on the dashboard.
(function () {
  let activeStop = null;

  function parseColumns(root) {
    const script = root.querySelector("#worker-state-cols");
    if (!script) {
      return [];
    }
    try {
      return JSON.parse(script.textContent || "[]");
    } catch {
      return [];
    }
  }

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

  function renderRow(worker, columns) {
    const cells = columns
      .map((state) => {
        const match = Array.isArray(worker.state_cells)
          ? worker.state_cells.find((cell) => cell.state === state)
          : null;
        const count = match ? match.count : 0;
        return `<td><a class="table-link" href="/tasks/?worker=${encodeURIComponent(
          worker.hostname,
        )}&state=${state}">${count}</a></td>`;
      })
      .join("");

    return `
      <tr>
        <td>
          <div class="task-info">
            <strong><a href="/workers/${encodeURIComponent(worker.hostname)}/">${worker.hostname}</a></strong>
          </div>
        </td>
        <td><span class="badge ${worker.badge}">${worker.status}</span></td>
        ${cells}
        <td><small>${formatLastSeen(worker.last_seen_seconds, worker.status)}</small></td>
      </tr>
    `;
  }

  function renderTable(table, workers, columns) {
    const tbody = table.querySelector("tbody");
    if (!tbody) {
      return;
    }
    const rows = workers.map((worker) => renderRow(worker, columns)).join("");
    tbody.innerHTML = rows || `<tr><td colspan="${columns.length + 3}">No workers reported.</td></tr>`;
  }

  function init(root) {
    const scope = root instanceof HTMLElement ? root : document;
    const wrapper = scope.querySelector("[data-worker-table]");
    const table = wrapper?.querySelector("table");
    if (!wrapper || !table) {
      return;
    }
    if (wrapper.dataset.polling === "true") {
      return;
    }

    const columns = parseColumns(scope);
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
        renderTable(table, payload.workers, columns);
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

  window.CeleryDashboardWorkers = {
    init,
    stop() {
      if (activeStop) {
        activeStop();
        activeStop = null;
      }
    },
  };

  function attachStopOnWorkerClick() {
    document.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof Element)) {
        return;
      }
      const link = target.closest("[data-worker-link]");
      if (!link) {
        return;
      }
      if (window.CeleryDashboardWorkers?.stop) {
        window.CeleryDashboardWorkers.stop();
      }
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", attachStopOnWorkerClick);
  } else {
    attachStopOnWorkerClick();
  }
})();
