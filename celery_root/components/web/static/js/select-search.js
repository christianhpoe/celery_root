// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

// Adds a search filter above select elements.
(function () {
  function initSearchableSelects() {
    const inputs = Array.from(document.querySelectorAll("[data-select-search]"));
    if (inputs.length === 0) {
      return;
    }

    inputs.forEach((input) => {
      const targetId = input.dataset.selectTarget;
      if (!targetId) {
        return;
      }
      const select = document.getElementById(targetId);
      if (!select) {
        return;
      }

      let originalOptions = [];

      const captureOptions = () => {
        originalOptions = Array.from(select.options).map((option) => ({
          value: option.value,
          label: option.textContent ?? "",
          disabled: option.disabled,
          isPlaceholder: option.value === "",
        }));
      };

      captureOptions();

      const filterOptions = () => {
        const term = input.value.trim().toLowerCase();
        const selectedValue = select.value;
        const hasTerm = term.length > 0;
        const matches = originalOptions.filter((option) => {
          if (option.isPlaceholder) {
            return !hasTerm;
          }
          if (!hasTerm) {
            return true;
          }
          if (option.value === selectedValue) {
            return true;
          }
          return (
            option.label.toLowerCase().includes(term) ||
            option.value.toLowerCase().includes(term)
          );
        });

        select.innerHTML = "";

        if (matches.length === 0) {
          const emptyOption = document.createElement("option");
          emptyOption.textContent = "No matches";
          emptyOption.disabled = true;
          emptyOption.value = "";
          select.appendChild(emptyOption);
          return;
        }

        matches.forEach((option) => {
          const next = document.createElement("option");
          next.value = option.value;
          next.textContent = option.label;
          next.disabled = option.disabled;
          if (option.value === selectedValue) {
            next.selected = true;
          }
          select.appendChild(next);
        });
      };

      input.addEventListener("input", filterOptions);
      input.addEventListener("search", filterOptions);
      select.addEventListener("select:options", () => {
        captureOptions();
        filterOptions();
      });
      filterOptions();
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initSearchableSelects);
  } else {
    initSearchableSelects();
  }
})();
