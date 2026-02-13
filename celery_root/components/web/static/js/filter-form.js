// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

// Auto-submit filter forms with debounce.
(function () {
  const DEFAULT_DEBOUNCE_MS = 400;

  function serializeForm(form) {
    const data = new FormData(form);
    const params = new URLSearchParams();
    for (const [key, value] of data.entries()) {
      params.append(key, String(value));
    }
    return params.toString();
  }

  function isInteractiveField(field) {
    if (!(field instanceof HTMLInputElement || field instanceof HTMLSelectElement)) {
      return false;
    }
    if (field instanceof HTMLInputElement && field.type === "hidden") {
      return false;
    }
    return true;
  }

  function initAutoSubmit(form) {
    const debounceMs = Number(form.dataset.autoSubmitDebounce || DEFAULT_DEBOUNCE_MS);
    let timer = null;
    let lastValue = serializeForm(form);

    const scheduleSubmit = () => {
      if (timer) {
        window.clearTimeout(timer);
      }
      timer = window.setTimeout(() => {
        timer = null;
        const currentValue = serializeForm(form);
        if (currentValue === lastValue) {
          return;
        }
        lastValue = currentValue;
        form.requestSubmit();
      }, debounceMs);
    };

    const fields = Array.from(form.querySelectorAll("input, select")).filter(isInteractiveField);
    fields.forEach((field) => {
      field.addEventListener("input", scheduleSubmit);
      field.addEventListener("change", scheduleSubmit);
      if (field instanceof HTMLInputElement && field.type === "search") {
        field.addEventListener("search", scheduleSubmit);
      }
    });
  }

  function init() {
    const forms = Array.from(document.querySelectorAll("form[data-auto-submit='true']"));
    if (!forms.length) {
      return;
    }
    forms.forEach(initAutoSubmit);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
