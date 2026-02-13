// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

(function () {
  function escapeRegExp(value) {
    return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }

  function applyHighlight(term, containers) {
    if (!term || !containers.length) {
      return;
    }

    const safeTerm = escapeRegExp(term);
    const regex = new RegExp(`(${safeTerm})`, "gi");

    containers.forEach((element) => {
      const text = element.dataset.originalText ?? element.textContent ?? "";
      element.dataset.originalText = text;
      element.innerHTML = text.replace(regex, `<span class="highlight">$1</span>`);
    });
  }

  function initHighlights() {
    const container = document.querySelector("[data-search-term]");
    const term = container?.dataset?.searchTerm?.trim();
    const targets = container ? Array.from(container.querySelectorAll("[data-highlightable]")) : [];
    if (!term || targets.length === 0) {
      return;
    }
    applyHighlight(term, targets);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initHighlights);
  } else {
    initHighlights();
  }

  window.CelerySearch = {
    highlight: initHighlights,
  };
})();
