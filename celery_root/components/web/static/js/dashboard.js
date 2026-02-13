// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

// Load dashboard data lazily to avoid blocking initial render.
(function () {
  function init() {
    const container = document.querySelector("[data-dashboard-fragment]");
    if (!container) {
      return;
    }
    const url = container.dataset.dashboardFragment;
    if (!url) {
      return;
    }
    const loading = document.querySelector("[data-dashboard-loading]");

    fetch(url, {
      headers: { "X-Requested-With": "XMLHttpRequest" },
    })
      .then((response) => {
        if (!response.ok) {
          throw new Error("Failed to load dashboard data");
        }
        return response.text();
      })
      .then((html) => {
        if (loading) {
          loading.remove();
        }
        container.innerHTML = html;
        window.CeleryDashboardWorkers?.init(container);
      })
      .catch(() => {
        if (loading) {
          loading.remove();
        }
        container.innerHTML =
          "<section class=\"detail-card\"><h3>Dashboard</h3><p>Unable to load dashboard data.</p></section>";
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
