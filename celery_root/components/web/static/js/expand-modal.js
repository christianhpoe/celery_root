// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

// Expand long fields into a formatted modal view.
(function () {
  const MAX_PREVIEW = 160;
  const MAX_PREVIEW_JSON = 400;
  const JSON_INDENT = 2;

  function getOverlay() {
    return document.getElementById("modal-overlay");
  }

  function getTitleEl() {
    return document.getElementById("modal-title");
  }

  function getContentEl() {
    return document.getElementById("modal-content");
  }

  function setModalState(isOpen) {
    const overlay = getOverlay();
    if (!overlay) {
      return false;
    }
    if (isOpen) {
      overlay.classList.remove("is-hidden");
      overlay.removeAttribute("hidden");
      overlay.setAttribute("aria-hidden", "false");
      document.body.classList.add("modal-open");
    } else {
      overlay.classList.add("is-hidden");
      overlay.setAttribute("hidden", "");
      overlay.setAttribute("aria-hidden", "true");
      document.body.classList.remove("modal-open");
    }
    return true;
  }

  function escapeHtml(value) {
    return value
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  function normalizePythonJson(rawText) {
    let candidate = rawText.trim();
    if (!candidate) {
      return candidate;
    }
    candidate = candidate
      .replace(/\bNone\b/g, "null")
      .replace(/\bTrue\b/g, "true")
      .replace(/\bFalse\b/g, "false")
      .replace(/\(/g, "[")
      .replace(/\)/g, "]")
      .replace(/'([^'\\]*(?:\\.[^'\\]*)*)'/g, (_match, inner) => {
        const escaped = inner.replace(/"/g, '\\"');
        return `"${escaped}"`;
      });
    return candidate;
  }

  function parseJsonLike(rawText) {
    try {
      return { ok: true, value: JSON.parse(rawText) };
    } catch (error) {
      // Fall back to Python literal-ish strings.
    }
    const normalized = normalizePythonJson(rawText);
    if (!normalized || normalized === rawText) {
      try {
        return { ok: true, value: JSON.parse(normalized) };
      } catch (error) {
        return { ok: false, value: null };
      }
    }
    try {
      return { ok: true, value: JSON.parse(normalized) };
    } catch (error) {
      return { ok: false, value: null };
    }
  }

  function highlightJson(json) {
    const escaped = escapeHtml(json);
    return escaped.replace(
      /(\"(\\u[a-fA-F0-9]{4}|\\[^u]|[^\\\"])*\"(?=\s*:))|(\"(\\u[a-fA-F0-9]{4}|\\[^u]|[^\\\"])*\")|\b(true|false|null)\b|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g,
      (match) => {
        if (match.startsWith('"') && match.endsWith('"') && match.includes(':')) {
          return `<span class="json-key">${match}</span>`;
        }
        if (match.startsWith('"')) {
          return `<span class="json-string">${match}</span>`;
        }
        if (match === "true" || match === "false") {
          return `<span class="json-boolean">${match}</span>`;
        }
        if (match === "null") {
          return `<span class="json-null">${match}</span>`;
        }
        return `<span class="json-number">${match}</span>`;
      },
    );
  }

  function highlightTraceback(text) {
    const escaped = escapeHtml(text);
    return escaped
      .replace(/^(Traceback.*)$/gm, '<span class="traceback-head">$1</span>')
      .replace(/^(\s*File .*?)$/gm, '<span class="traceback-file">$1</span>')
      .replace(/^(.*\b(?:Error|Exception):.*)$/gm, '<span class="traceback-error">$1</span>');
  }

  function readSource(sourceId) {
    if (!sourceId) {
      return "";
    }
    const script = document.getElementById(sourceId);
    if (!script) {
      return "";
    }
    try {
      return JSON.parse(script.textContent || "");
    } catch (error) {
      return script.textContent || "";
    }
  }

  function formatJsonText(rawText) {
    const parsed = parseJsonLike(rawText);
    if (!parsed.ok) {
      return rawText;
    }
    return JSON.stringify(parsed.value, null, JSON_INDENT);
  }

  function buildPreviewText(rawText, format, previewMax) {
    if (!rawText) {
      return { text: "—", truncated: false };
    }
    const text = format === "json" ? formatJsonText(rawText) : rawText;
    if (text.length > previewMax) {
      return { text: `${text.slice(0, previewMax).trimEnd()}…`, truncated: true };
    }
    return { text, truncated: false };
  }

  function copyToClipboard(text) {
    if (navigator.clipboard && window.isSecureContext) {
      return navigator.clipboard.writeText(text);
    }
    return new Promise((resolve, reject) => {
      const textarea = document.createElement("textarea");
      textarea.value = text;
      textarea.setAttribute("readonly", "");
      textarea.style.position = "fixed";
      textarea.style.opacity = "0";
      document.body.appendChild(textarea);
      textarea.select();
      try {
        const ok = document.execCommand("copy");
        document.body.removeChild(textarea);
        if (ok) {
          resolve();
        } else {
          reject(new Error("copy-failed"));
        }
      } catch (error) {
        document.body.removeChild(textarea);
        reject(error);
      }
    });
  }

  function getCopyText(sourceId, format) {
    if (!sourceId) {
      return "";
    }
    const script = document.getElementById(sourceId);
    const rawText = script ? script.textContent || "" : "";
    if (!rawText) {
      return "";
    }
    try {
      const parsed = JSON.parse(rawText);
      if (typeof parsed === "string") {
        return parsed;
      }
      if (format === "json") {
        return JSON.stringify(parsed, null, JSON_INDENT);
      }
      return JSON.stringify(parsed);
    } catch (error) {
      return rawText;
    }
  }

  function initCopyButton(button) {
    const sourceId = button.dataset.copySource || "";
    if (!sourceId) {
      return;
    }
    const format = button.dataset.copyFormat || "text";
    const defaultLabel = button.textContent || "Copy";
    let resetTimer = null;

    button.addEventListener("click", async () => {
      const text = getCopyText(sourceId, format);
      try {
        await copyToClipboard(text);
        button.textContent = "Copied";
      } catch (error) {
        button.textContent = "Copy failed";
      }
      if (resetTimer) {
        clearTimeout(resetTimer);
      }
      resetTimer = window.setTimeout(() => {
        button.textContent = defaultLabel;
      }, 2000);
    });
  }

  function openModal(title, raw, format) {
    const overlay = getOverlay();
    const titleEl = getTitleEl();
    const contentEl = getContentEl();
    if (!overlay || !contentEl || !titleEl) {
      return;
    }
    const content = raw == null ? "" : String(raw);
    titleEl.textContent = title || "Details";
    contentEl.classList.remove("modal-json", "modal-traceback");

    if (format === "json") {
      const parsed = parseJsonLike(content);
      if (parsed.ok) {
        const pretty = JSON.stringify(parsed.value, null, JSON_INDENT);
        contentEl.innerHTML = highlightJson(pretty);
        contentEl.classList.add("modal-json");
      } else {
        contentEl.textContent = content || "—";
      }
    } else if (format === "traceback") {
      contentEl.innerHTML = highlightTraceback(content || "—");
      contentEl.classList.add("modal-traceback");
    } else {
      contentEl.textContent = content || "—";
    }

    setModalState(true);
  }

  function closeModal() {
    setModalState(false);
  }

  function initExpandable(block) {
    const sourceId = block.dataset.expandSource || "";
    const format = block.dataset.expandFormat || "text";
    const title = block.dataset.expandTitle || "Details";
    const previewMax = Number(
      block.dataset.previewMax || (format === "json" ? MAX_PREVIEW_JSON : MAX_PREVIEW),
    );
    const raw = readSource(sourceId);
    const rawText = raw == null ? "" : typeof raw === "string" ? raw : JSON.stringify(raw);

    const previewEl = block.querySelector(".expandable-preview");
    const button = block.querySelector(".expandable-btn");

    const preview = buildPreviewText(rawText, format, previewMax);
    if (previewEl) {
      previewEl.textContent = preview.text;
    }

    if (!button) {
      return;
    }

    if (!rawText || !preview.truncated) {
      button.classList.add("is-hidden");
      return;
    }

    button.addEventListener("click", () => openModal(title, rawText, format));
  }

  function init() {
    const overlay = getOverlay();
    if (!overlay) {
      return;
    }
    setModalState(false);
    const blocks = Array.from(document.querySelectorAll("[data-expand-source]"));
    blocks.forEach(initExpandable);
    const copyButtons = Array.from(document.querySelectorAll("[data-copy-source]"));
    copyButtons.forEach(initCopyButton);

    document.addEventListener("click", (event) => {
      const target = event.target;
      if (target instanceof Element) {
        if (target.closest("[data-modal-close], #modal-close")) {
          event.preventDefault();
          closeModal();
          return;
        }
      }
      if (event.target === getOverlay()) {
        closeModal();
      }
    });

    window.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        closeModal();
      }
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
