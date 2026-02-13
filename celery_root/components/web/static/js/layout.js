// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

// Sidebar toggle without polling helpers.
(function () {
  const layout = document.getElementById("layout-root");
  const overlay = document.getElementById("layout-overlay");
  const toggle = document.querySelector(".sidebar-toggle");

  function attachToggle() {
    if (!toggle || !layout) {
      return;
    }

    const handle = () => layout.classList.toggle("sidebar-open");
    toggle.addEventListener("click", handle);
    overlay?.addEventListener("click", () => layout.classList.remove("sidebar-open"));
    window.addEventListener("resize", () => {
      if (window.innerWidth > 1024) {
        layout.classList.remove("sidebar-open");
      }
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", attachToggle);
  } else {
    attachToggle();
  }
})();
