// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

// Builds typed task forms from Celery task signatures.
(function () {
  const JSON_INDENT = 2;

  function parseSchema(schemaEl) {
    try {
      return JSON.parse(schemaEl.textContent || "{}");
    } catch (error) {
      console.warn("Failed to parse task schema data", error);
      return null;
    }
  }

  function byId(id) {
    return id ? document.getElementById(id) : null;
  }

  function initForm(form) {
    const schemaId = form.dataset.schemaId || "task-schema-data";
    const schemaEl = document.getElementById(schemaId);
    if (!schemaEl) {
      return;
    }

    const schemaData = parseSchema(schemaEl);
    if (!schemaData) {
      return;
    }

    const taskSelect = byId(form.dataset.taskSelect || "");
    const appSelect = byId(form.dataset.appSelect || "");
    const paramContainer = byId(form.dataset.paramContainer || "");
    const paramHint = byId(form.dataset.paramHint || "");
    const argsInput = byId(form.dataset.argsInput || "");
    const kwargsInput = byId(form.dataset.kwargsInput || "");
    const queueInput = byId(form.dataset.queueInput || "");
    const jsonToggle = byId(form.dataset.jsonToggle || "");
    const jsonFields = byId(form.dataset.jsonFields || "");
    const paramCard = byId(form.dataset.paramCard || "");

    let untypedWarning = null;

    if (!taskSelect || !paramContainer || !argsInput || !kwargsInput) {
      return;
    }

    const schemaNested = form.dataset.schemaNested === "true";
    const taskOptionsMode = form.dataset.taskOptions || "static";

    let currentParams = [];

    function setHint(text) {
      if (paramHint) {
        paramHint.textContent = text;
      }
    }

    function ensureUntypedWarning() {
      if (untypedWarning) {
        return untypedWarning;
      }
      const warning = document.createElement("p");
      warning.className = "detail-runtime task-param-warning is-hidden";
      warning.textContent =
        "Untyped inputs detected. These fields will be sent as strings unless you use raw JSON.";
      warning.setAttribute("role", "status");
      if (paramHint && paramHint.parentNode) {
        paramHint.insertAdjacentElement("afterend", warning);
      } else if (paramContainer && paramContainer.parentNode) {
        paramContainer.insertAdjacentElement("beforebegin", warning);
      } else if (paramCard) {
        paramCard.appendChild(warning);
      }
      untypedWarning = warning;
      return warning;
    }

    function updateUntypedWarning(params) {
      const warning = ensureUntypedWarning();
      const hasUntyped = Array.isArray(params)
        && params.some((param) => {
          const annotation = String(param.annotation || "").toLowerCase();
          return annotation === "unknown" || annotation === "any";
        });
      warning.classList.toggle("is-hidden", !hasUntyped);
    }

    function toggleMode() {
      if (!jsonToggle) {
        return;
      }
      const useJson = Boolean(jsonToggle.checked);
      if (paramCard) {
        paramCard.classList.toggle("is-hidden", useJson);
      }
      if (jsonFields) {
        jsonFields.classList.toggle("is-hidden", !useJson);
      }
      if (useJson) {
        formatJsonField(argsInput, true);
        formatJsonField(kwargsInput, false);
      }
    }

    function applyDefault(input, param) {
      if (!param.default) {
        return;
      }
      if (param.input === "bool") {
        const truthy = String(param.default).toLowerCase();
        input.checked = ["true", "1", "yes", "y"].includes(truthy);
        return;
      }
      if (param.input === "select") {
        const options = Array.from(input.options);
        const match = options.find((option) => option.textContent === String(param.default));
        if (match) {
          match.selected = true;
        }
      }
    }

    function formatJsonField(field, expectArray) {
      const raw = String(field.value || "").trim();
      if (!raw) {
        return false;
      }
      try {
        const parsed = parseJsonValue(raw, expectArray);
        field.value = JSON.stringify(parsed, null, JSON_INDENT);
        return true;
      } catch (error) {
        return false;
      }
    }

    function registerJsonField(field, expectArray) {
      if (!field) {
        return;
      }
      field.addEventListener("blur", () => {
        formatJsonField(field, expectArray);
      });
    }

    function expectArrayFor(field) {
      const expect = field.dataset.jsonExpect || "";
      if (expect === "list") {
        return true;
      }
      if (expect === "dict") {
        return false;
      }
      const id = (field.id || "").toLowerCase();
      const name = (field.name || "").toLowerCase();
      if (id.includes("args") || name.includes("args")) {
        return true;
      }
      if (id.includes("kwargs") || name.includes("kwargs")) {
        return false;
      }
      return false;
    }

    function initJsonEditor() {
      const statusEls = Array.from(form.querySelectorAll("[data-json-status]"));
      const statusMap = new Map(
        statusEls
          .map((el) => [el.dataset.jsonStatus || "", el])
          .filter(([key]) => key),
      );

      function setStatus(targetId, message, isError) {
        const statusEl = statusMap.get(targetId);
        if (!statusEl) {
          return;
        }
        statusEl.textContent = message;
        statusEl.classList.toggle("is-error", Boolean(isError));
        statusEl.classList.toggle("is-ok", !isError && Boolean(message));
      }

      function clearStatus(targetId) {
        setStatus(targetId, "", false);
      }

      const formatButtons = Array.from(form.querySelectorAll("[data-json-format]"));
      formatButtons.forEach((button) => {
        const targetId = button.dataset.jsonFormat || "";
        const field = byId(targetId);
        if (!field) {
          return;
        }
        button.addEventListener("click", () => {
          const raw = String(field.value || "").trim();
          if (!raw) {
            clearStatus(targetId);
            return;
          }
          try {
            const parsed = parseJsonValue(raw, expectArrayFor(field));
            field.value = JSON.stringify(parsed, null, JSON_INDENT);
            setStatus(targetId, "Formatted", false);
          } catch (error) {
            setStatus(targetId, error instanceof Error ? error.message : "Invalid JSON.", true);
          }
        });
      });

      const validateButtons = Array.from(form.querySelectorAll("[data-json-validate]"));
      validateButtons.forEach((button) => {
        const targetId = button.dataset.jsonValidate || "";
        const field = byId(targetId);
        if (!field) {
          return;
        }
        button.addEventListener("click", () => {
          const raw = String(field.value || "").trim();
          if (!raw) {
            clearStatus(targetId);
            return;
          }
          try {
            parseJsonValue(raw, expectArrayFor(field));
            setStatus(targetId, "Valid JSON", false);
          } catch (error) {
            setStatus(targetId, error instanceof Error ? error.message : "Invalid JSON.", true);
          }
        });
      });
    }

    function buildField(param, index) {
      const wrapper = document.createElement("div");
      wrapper.className = "task-param-field";

      const label = document.createElement("label");
      const id = `${taskSelect.id}-param-${index}`;
      label.setAttribute("for", id);

      let displayName = param.name;
      if (param.kind === "var_positional") {
        displayName = `*${param.name}`;
      } else if (param.kind === "var_keyword") {
        displayName = `**${param.name}`;
      }

      label.textContent = `${displayName} (${param.annotation})`;
      wrapper.appendChild(label);

      let input;
      switch (param.input) {
        case "bool": {
          input = document.createElement("input");
          input.type = "checkbox";
          break;
        }
        case "int": {
          input = document.createElement("input");
          input.type = "number";
          input.step = "1";
          break;
        }
        case "float": {
          input = document.createElement("input");
          input.type = "number";
          input.step = "any";
          break;
        }
        case "select": {
          input = document.createElement("select");
          if (!param.required) {
            const option = document.createElement("option");
            option.value = "";
            option.textContent = "(optional)";
            input.appendChild(option);
          }
          param.options.forEach((optionValue) => {
            const option = document.createElement("option");
            option.value = JSON.stringify(optionValue);
            option.textContent = String(optionValue);
            input.appendChild(option);
          });
          break;
        }
        case "json-list": {
          input = document.createElement("textarea");
          input.placeholder = "[]";
          input.classList.add("json-input");
          break;
        }
        case "json-dict": {
          input = document.createElement("textarea");
          input.placeholder = "{}";
          input.classList.add("json-input");
          break;
        }
        default: {
          input = document.createElement("input");
          input.type = "text";
          break;
        }
      }

      input.id = id;
      input.dataset.paramName = param.name;
      input.dataset.paramKind = param.kind;
      input.dataset.paramInput = param.input;

      if (param.required && param.input !== "bool" && param.kind !== "var_positional" && param.kind !== "var_keyword") {
        input.required = true;
      }

      applyDefault(input, param);
      if (param.input === "json-list" || param.input === "json-dict") {
        registerJsonField(input, param.input === "json-list");
        formatJsonField(input, param.input === "json-list");
      }
      wrapper.appendChild(input);

      if (param.default) {
        const hint = document.createElement("small");
        hint.textContent = `default: ${param.default}`;
        wrapper.appendChild(hint);
      }

      return wrapper;
    }

    function getAppSchemas() {
      if (schemaNested && appSelect) {
        return schemaData[appSelect.value] || {};
      }
      return schemaData;
    }

    function renderParams() {
      paramContainer.innerHTML = "";
      currentParams = [];
      updateUntypedWarning([]);
      const selected = taskSelect.value;
      if (!selected) {
        setHint("Select a task to see its inputs.");
        if (queueInput && queueInput.dataset.autoQueue !== "false") {
          queueInput.value = "";
          queueInput.dataset.autoQueue = "true";
        }
        return;
      }

      const schema = getAppSchemas()[selected];
      if (!schema) {
        setHint("No signature metadata available for this task.");
        if (queueInput && queueInput.dataset.autoQueue !== "false") {
          queueInput.value = "";
          queueInput.dataset.autoQueue = "true";
        }
        return;
      }

      if (queueInput) {
        const auto = queueInput.dataset.autoQueue !== "false" || queueInput.value.trim() === "";
        if (auto) {
          queueInput.value = schema.queue || "";
          queueInput.dataset.autoQueue = "true";
        }
      }

      if (schema.doc) {
        setHint(schema.doc);
      } else {
        setHint("Provide inputs for the selected task.");
      }

      const params = Array.isArray(schema.params) ? schema.params : [];
      currentParams = params;
      updateUntypedWarning(params);

      if (params.length === 0) {
        setHint("No typed inputs were discovered for this task.");
        return;
      }

      params.forEach((param, index) => {
        paramContainer.appendChild(buildField(param, index));
      });
    }

    function updateTaskOptions() {
      if (taskOptionsMode !== "schema") {
        return;
      }
      const tasks = Object.keys(getAppSchemas()).sort();
      const selectedTask = taskSelect.dataset.selectedTask || "";
      const currentValue = taskSelect.value;

      taskSelect.innerHTML = "";
      const placeholder = document.createElement("option");
      placeholder.value = "";
      placeholder.textContent = "Select a task";
      taskSelect.appendChild(placeholder);

      const preferred = selectedTask || currentValue;
      if (preferred && !tasks.includes(preferred)) {
        const option = document.createElement("option");
        option.value = preferred;
        option.textContent = `${preferred} (current)`;
        option.selected = true;
        taskSelect.appendChild(option);
      }

      tasks.forEach((name) => {
        const option = document.createElement("option");
        option.value = name;
        option.textContent = name;
        if (name === preferred) {
          option.selected = true;
        }
        taskSelect.appendChild(option);
      });

      if (preferred && tasks.includes(preferred)) {
        taskSelect.value = preferred;
      }

      if (selectedTask) {
        taskSelect.dataset.selectedTask = "";
      }
      taskSelect.dispatchEvent(new CustomEvent("select:options"));
    }

    function parseJsonValue(raw, expectArray) {
      const parsed = JSON.parse(raw);
      if (expectArray && !Array.isArray(parsed)) {
        throw new Error("Expected a JSON list.");
      }
      if (!expectArray && (parsed === null || Array.isArray(parsed) || typeof parsed !== "object")) {
        throw new Error("Expected a JSON object.");
      }
      return parsed;
    }

    function valueFromField(field, param) {
      if (param.input === "bool") {
        return field.checked;
      }

      const raw = String(field.value || "").trim();
      if (!raw) {
        return undefined;
      }

      if (param.input === "int") {
        const value = Number.parseInt(raw, 10);
        if (Number.isNaN(value)) {
          throw new Error(`${param.name} must be an integer.`);
        }
        return value;
      }
      if (param.input === "float") {
        const value = Number.parseFloat(raw);
        if (Number.isNaN(value)) {
          throw new Error(`${param.name} must be a number.`);
        }
        return value;
      }
      if (param.input === "json-list") {
        return parseJsonValue(raw, true);
      }
      if (param.input === "json-dict") {
        return parseJsonValue(raw, false);
      }
      if (param.input === "select") {
        if (!raw) {
          return undefined;
        }
        try {
          return JSON.parse(raw);
        } catch (error) {
          return raw;
        }
      }

      return raw;
    }

    function buildPayload() {
      if (!Array.isArray(currentParams) || currentParams.length === 0) {
        return null;
      }

      const args = [];
      const kwargs = {};

      for (const param of currentParams) {
        const field = paramContainer.querySelector(`[data-param-name="${param.name}"]`);
        if (!field) {
          continue;
        }

        let value;
        try {
          value = valueFromField(field, param);
        } catch (error) {
          alert(error instanceof Error ? error.message : "Invalid input.");
          field.focus();
          return null;
        }

        if (value === undefined) {
          continue;
        }

        if (param.kind === "var_positional") {
          if (Array.isArray(value)) {
            args.push(...value);
          }
          continue;
        }

        if (param.kind === "var_keyword") {
          if (value && typeof value === "object" && !Array.isArray(value)) {
            Object.assign(kwargs, value);
          }
          continue;
        }

        if (param.kind === "positional_only") {
          args.push(value);
        } else {
          kwargs[param.name] = value;
        }
      }

      return { args, kwargs };
    }

    taskSelect.addEventListener("change", renderParams);
    if (appSelect) {
      appSelect.addEventListener("change", () => {
        updateTaskOptions();
        renderParams();
      });
    }
    if (jsonToggle) {
      jsonToggle.addEventListener("change", toggleMode);
    }
    if (queueInput) {
      queueInput.addEventListener("input", () => {
        queueInput.dataset.autoQueue = queueInput.value.trim() ? "false" : "true";
      });
    }

    form.addEventListener("submit", () => {
      if (jsonToggle && jsonToggle.checked) {
        return;
      }
      const payload = buildPayload();
      if (!payload) {
        return;
      }
      argsInput.value = JSON.stringify(payload.args, null, JSON_INDENT);
      kwargsInput.value = JSON.stringify(payload.kwargs, null, JSON_INDENT);
    });

    registerJsonField(argsInput, true);
    registerJsonField(kwargsInput, false);
    formatJsonField(argsInput, true);
    formatJsonField(kwargsInput, false);

    updateTaskOptions();
    renderParams();
    toggleMode();
    initJsonEditor();
  }

  function init() {
    const forms = Array.from(document.querySelectorAll("form[data-task-form]"));
    if (forms.length === 0) {
      return;
    }
    forms.forEach((form) => initForm(form));
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
