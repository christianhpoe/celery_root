// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

// Quick schedule presets for beat form.
(function () {
  function init(form) {
    const inputId = form.dataset.scheduleInput || "";
    const scheduleInput = inputId ? document.getElementById(inputId) : null;
    if (!(scheduleInput instanceof HTMLInputElement)) {
      return;
    }

    const buttons = Array.from(form.querySelectorAll("[data-schedule-preset]"));
    if (!buttons.length) {
      return;
    }

    buttons.forEach((button) => {
      button.addEventListener("click", () => {
        const value = button.dataset.schedulePreset || "";
        if (!value) {
          return;
        }
        scheduleInput.value = value;
        scheduleInput.dispatchEvent(new Event("input", { bubbles: true }));
        scheduleInput.focus();
      });
    });
  }

  function initAll() {
    const forms = Array.from(document.querySelectorAll("form[data-schedule-presets='true']"));
    if (!forms.length) {
      return;
    }
    forms.forEach(init);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initAll);
  } else {
    initAll();
  }
})();
