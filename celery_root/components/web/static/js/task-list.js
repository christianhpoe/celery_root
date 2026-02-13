// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

// Quick time presets for the task list filters.
(function () {
  function formatUtc(date) {
    const pad = (value) => String(value).padStart(2, "0");
    return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())}T${pad(
      date.getUTCHours(),
    )}:${pad(date.getUTCMinutes())}`;
  }

  function applyPreset(form, minutes) {
    const startInput = form.querySelector("[data-time-start]");
    const endInput = form.querySelector("[data-time-end]");
    if (!(startInput instanceof HTMLInputElement) || !(endInput instanceof HTMLInputElement)) {
      return;
    }

    const rawNow = form.dataset.utcNow || "";
    const baseNow = rawNow ? new Date(rawNow) : new Date();
    const now = Number.isNaN(baseNow.getTime()) ? new Date() : baseNow;
    const end = new Date(now.getTime());
    const start = new Date(now.getTime() - minutes * 60 * 1000);
    startInput.value = formatUtc(start);
    endInput.value = formatUtc(end);

    if (typeof form.requestSubmit === "function") {
      form.requestSubmit();
    } else {
      form.submit();
    }
  }

  function init() {
    const form = document.querySelector("form[data-task-filter='true']");
    if (!form) {
      return;
    }
    const buttons = Array.from(form.querySelectorAll("[data-time-preset]"));
    if (!buttons.length) {
      return;
    }
    const clearButton = form.querySelector("[data-time-clear]");
    if (clearButton) {
      clearButton.addEventListener("click", (event) => {
        event.preventDefault();
        const startInput = form.querySelector("[data-time-start]");
        const endInput = form.querySelector("[data-time-end]");
        if (startInput instanceof HTMLInputElement) {
          startInput.value = "";
        }
        if (endInput instanceof HTMLInputElement) {
          endInput.value = "";
        }
        if (typeof form.requestSubmit === "function") {
          form.requestSubmit();
        } else {
          form.submit();
        }
      });
    }

    buttons.forEach((button) => {
      button.addEventListener("click", () => {
        const raw = button.dataset.timePreset || "";
        const minutes = Number.parseInt(raw, 10);
        if (Number.isNaN(minutes) || minutes <= 0) {
          return;
        }
        applyPreset(form, minutes);
      });
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
