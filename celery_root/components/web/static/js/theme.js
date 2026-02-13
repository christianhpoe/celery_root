// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

// Theme toggle with persistence.
(function () {
  const storageKey = "celery_root_theme";
  const themes = {
    monokai: "Monokai",
    darkula: "Darkula",
    generic: "Generic",
    dark: "Dark",
    white: "White",
    solaris: "Solaris",
  };
  const root = document.documentElement;
  let activeTheme = "white";

  function normalizeTheme(theme) {
    if (theme && Object.prototype.hasOwnProperty.call(themes, theme)) {
      return theme;
    }
    return null;
  }

  function applyTheme(theme) {
    const normalized = normalizeTheme(theme) || "white";
    activeTheme = normalized;
    root.setAttribute("data-theme", normalized);
  }

  function getPreferredTheme() {
    const stored = normalizeTheme(localStorage.getItem(storageKey));
    if (stored) {
      return stored;
    }
    return "white";
  }

  function setTheme(theme, persist) {
    applyTheme(theme);
    if (persist) {
      localStorage.setItem(storageKey, activeTheme);
    }
  }

  const initial = getPreferredTheme();
  applyTheme(initial);

  window.CeleryTheme = {
    getTheme() {
      return activeTheme;
    },
    setTheme(theme) {
      setTheme(theme, true);
    },
    themes,
  };
})();
