// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

// Simple accessible tabs controller.
(function () {
  function initTabs(root) {
    if (root.dataset.tabsInitialized === "true") {
      return;
    }
    root.dataset.tabsInitialized = "true";
    const tabButtons = Array.from(root.querySelectorAll("[data-tab-target]"));
    if (tabButtons.length === 0) {
      return;
    }

    const panels = new Map();
    tabButtons.forEach((button) => {
      const targetId = button.dataset.tabTarget;
      if (!targetId) {
        return;
      }
      const panel = document.getElementById(targetId);
      if (panel) {
        panels.set(targetId, panel);
      }
    });

    if (panels.size === 0) {
      return;
    }

    function setActive(targetId, focus) {
      tabButtons.forEach((button) => {
        const isActive = button.dataset.tabTarget === targetId;
        button.classList.toggle("is-active", isActive);
        button.setAttribute("aria-selected", isActive ? "true" : "false");
        button.tabIndex = isActive ? 0 : -1;
        if (isActive && focus) {
          button.focus();
        }
      });

      panels.forEach((panel, panelId) => {
        const isActive = panelId === targetId;
        panel.classList.toggle("is-active", isActive);
        panel.hidden = !isActive;
      });
    }

    const defaultTarget = root.dataset.defaultTab || tabButtons[0]?.dataset.tabTarget || "";
    const hashTarget = window.location.hash.replace("#", "");
    if (hashTarget && panels.has(hashTarget)) {
      setActive(hashTarget, false);
    } else if (defaultTarget) {
      setActive(defaultTarget, false);
    }

    tabButtons.forEach((button) => {
      button.addEventListener("click", () => {
        const targetId = button.dataset.tabTarget || "";
        const tabUrl = button.dataset.tabUrl || "";
        if (tabUrl) {
          const targetUrl = new URL(tabUrl, window.location.href).href;
          if (targetUrl !== window.location.href) {
            window.location.assign(targetUrl);
            return;
          }
        }
        if (targetId) {
          setActive(targetId, true);
        }
      });
    });
  }

  function initAll(scope) {
    const container = scope instanceof HTMLElement ? scope : document;
    const roots = Array.from(container.querySelectorAll("[data-tabs]"));
    if (!roots.length) {
      return;
    }
    roots.forEach((root) => initTabs(root));
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", () => initAll());
  } else {
    initAll();
  }

  window.CeleryTabs = {
    initTabs,
    initAll,
  };
})();
