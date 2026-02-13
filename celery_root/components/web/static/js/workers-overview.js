// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

// Load the broker/worker overview lazily.
(function () {
  function init() {
    const container = document.querySelector("[data-workers-fragment]");
    if (!container) {
      return;
    }
    const url = container.dataset.workersFragment;
    if (!url) {
      return;
    }
    const loading = document.querySelector("[data-workers-loading]");

    fetch(url, {
      headers: { "X-Requested-With": "XMLHttpRequest" },
    })
      .then((response) => {
        if (!response.ok) {
          throw new Error("Failed to load worker overview");
        }
        return response.text();
      })
      .then((html) => {
        if (loading) {
          loading.remove();
        }
        container.innerHTML = html;
      })
      .catch(() => {
        if (loading) {
          loading.remove();
        }
        container.innerHTML =
          "<section class=\"detail-card\"><h3>Workers overview</h3><p>Unable to load broker and worker data.</p></section>";
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
