// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

// Load broker data lazily to avoid blocking the initial page render.
(function () {
  function init() {
    const container = document.querySelector("[data-broker-fragment]");
    if (!container) {
      return;
    }
    const url = container.dataset.brokerFragment;
    if (!url) {
      return;
    }
    const loading = document.querySelector("[data-broker-loading]");

    function initTabsFallback(root) {
      const tabs = Array.from(root.querySelectorAll("[data-tabs]"));
      if (!tabs.length) {
        return;
      }
      tabs.forEach((tabRoot) => {
        if (tabRoot.dataset.tabsInitialized === "true") {
          return;
        }
        tabRoot.dataset.tabsInitialized = "true";
        const buttons = Array.from(tabRoot.querySelectorAll("[data-tab-target]"));
        if (!buttons.length) {
          return;
        }
        const panels = new Map();
        buttons.forEach((button) => {
          const targetId = button.dataset.tabTarget || "";
          if (!targetId) {
            return;
          }
          const panel = document.getElementById(targetId);
          if (panel) {
            panels.set(targetId, panel);
          }
        });
        if (!panels.size) {
          return;
        }
        function setActive(targetId) {
          buttons.forEach((button) => {
            const isActive = button.dataset.tabTarget === targetId;
            button.classList.toggle("is-active", isActive);
            button.setAttribute("aria-selected", isActive ? "true" : "false");
            button.tabIndex = isActive ? 0 : -1;
          });
          panels.forEach((panel, panelId) => {
            const isActive = panelId === targetId;
            panel.classList.toggle("is-active", isActive);
            panel.hidden = !isActive;
          });
        }
        const defaultTarget = tabRoot.dataset.defaultTab || buttons[0]?.dataset.tabTarget || "";
        if (defaultTarget) {
          setActive(defaultTarget);
        }
        buttons.forEach((button) => {
          button.addEventListener("click", () => {
            const targetId = button.dataset.tabTarget || "";
            if (targetId) {
              setActive(targetId);
            }
          });
        });
      });
    }

    fetch(url, {
      headers: { "X-Requested-With": "XMLHttpRequest" },
    })
      .then((response) => {
        if (!response.ok) {
          throw new Error("Failed to load broker data");
        }
        return response.text();
      })
      .then((html) => {
        if (loading) {
          loading.remove();
        }
        container.innerHTML = html;
        if (window.CeleryTabs?.initAll) {
          window.CeleryTabs.initAll(container);
        } else {
          initTabsFallback(container);
        }
      })
      .catch(() => {
        if (loading) {
          loading.remove();
        }
        container.innerHTML =
          "<section class=\"detail-card\"><h3>Broker activity</h3><p>Unable to load broker data.</p></section>";
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
