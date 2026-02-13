// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

(function () {
  const STATE_COLORS = {
    SUCCESS: "#22c55e",
    FAILURE: "#ef4444",
    STARTED: "#f59e0b",
    RETRY: "#a78bfa",
    PENDING: "#38bdf8",
    REVOKED: "#94a3b8",
  };
  const KIND_COLORS = {
    group: "#6366f1",
    chord: "#ec4899",
    assumed: "#a3a3a3",
    unknown: "#64748b",
  };
  const TOOLTIP_OFFSET = 12;

  function buildNodeMap(nodes) {
    const map = new Map();
    if (!Array.isArray(nodes)) {
      return map;
    }
    nodes.forEach((node) => {
      if (node && node.id) {
        map.set(node.id, node);
      }
    });
    return map;
  }

  function nodeLabel(node, fallback) {
    if (node && node.label) {
      return node.label;
    }
    return fallback;
  }

  function renderPlaceholder(container, edges, nodes) {
    container.innerHTML = "";
    container.classList.add("dag-placeholder");
    const list = document.createElement("ol");
    list.className = "dag-edge-list";
    const nodeMap = buildNodeMap(nodes);
    edges.forEach((edge) => {
      const item = document.createElement("li");
      const parent = edge.parent || "root";
      const parentLabel = nodeLabel(nodeMap.get(parent), parent);
      const childLabel = nodeLabel(nodeMap.get(edge.child), edge.child);
      item.textContent = `${parentLabel} → ${childLabel}`;
      list.appendChild(item);
    });
    container.appendChild(list);
  }

  function buildNodeElements(edges, nodes) {
    const nodeMap = buildNodeMap(nodes);
    const nodeIds = new Set();
    edges.forEach((edge) => {
      if (edge.parent) {
        nodeIds.add(edge.parent);
      }
      nodeIds.add(edge.child);
    });
    return Array.from(nodeIds).map((id) => {
      const node = nodeMap.get(id);
      const isTask = node?.state;
      const kind = node?.kind || (isTask ? "task" : "unknown");
      const color = isTask ? STATE_COLORS[node.state] || "#38bdf8" : KIND_COLORS[kind] || "#64748b";
      const shape = kind === "task" ? "ellipse" : "round-rectangle";
      return {
        data: {
          id,
          label: nodeLabel(node, id),
          url: node?.url || "",
          state: node?.state || "",
          kind,
          color,
          shape,
        },
      };
    });
  }

  function buildEdgeElements(edges) {
    return edges.map((edge, idx) => ({
      data: {
        id: `edge-${idx}`,
        source: edge.parent || edge.child,
        target: edge.child,
        label: edge.relation || "",
      },
    }));
  }

  function renderCytoscape(containerId, edges, nodes) {
    const container = document.getElementById(containerId);
    if (!container) {
      return;
    }

    if (window.cytoscape) {
      container.innerHTML = "";
      const tooltip = createTooltip(container);
      const elements = [
        ...buildNodeElements(edges, nodes),
        ...buildEdgeElements(edges),
      ];
      const cy = cytoscape({
        container,
        elements,
        style: [
          {
            selector: "node",
            style: {
              "background-color": "data(color)",
              shape: "data(shape)",
              label: "data(label)",
              color: "#fff",
              "font-size": "10px",
              "text-wrap": "wrap",
              "text-max-width": "120px",
              "text-valign": "center",
              "text-halign": "center",
              width: "label",
              height: "label",
              padding: "8px",
              "min-width": "52px",
              "min-height": "36px",
            },
          },
          {
            selector: "edge",
            style: {
              width: 2,
              "line-color": "#93c5fd",
              "target-arrow-color": "#93c5fd",
              "target-arrow-shape": "triangle",
              label: "data(label)",
              "font-size": "8px",
              color: "#e2e8f0",
              "curve-style": "bezier",
            },
          },
        ],
        layout: {
          name: "breadthfirst",
        },
      });
      cy.on("tap", "node", (event) => {
        const data = event.target.data();
        showTooltip(tooltip, container, data, event.renderedPosition);
      });
      cy.on("tap", (event) => {
        if (event.target === cy) {
          hideTooltip(tooltip);
        }
      });
      return;
    }

    renderPlaceholder(container, edges, nodes);
  }

  function normalizePayload(payload) {
    if (Array.isArray(payload)) {
      return { edges: payload, nodes: [] };
    }
    return {
      edges: Array.isArray(payload?.edges) ? payload.edges : [],
      nodes: Array.isArray(payload?.nodes) ? payload.nodes : [],
    };
  }

  function render(containerId, payload) {
    const container = document.getElementById(containerId);
    if (!container) {
      return;
    }
    const { edges, nodes } = normalizePayload(payload);
    if (window.cytoscape && typeof window.cytoscape === "function") {
      renderCytoscape(containerId, edges, nodes);
    } else {
      renderPlaceholder(container, edges, nodes);
    }
  }

  function createTooltip(container) {
    let tooltip = container.querySelector(".dag-tooltip");
    if (tooltip) {
      return tooltip;
    }
    tooltip = document.createElement("div");
    tooltip.className = "dag-tooltip is-hidden";
    const title = document.createElement("div");
    title.className = "dag-tooltip-title";
    const meta = document.createElement("dl");
    meta.className = "dag-tooltip-meta";
    const actions = document.createElement("div");
    actions.className = "dag-tooltip-actions";
    const link = document.createElement("a");
    link.className = "btn ghost btn-sm dag-tooltip-link";
    link.textContent = "Open task";
    actions.appendChild(link);
    tooltip.appendChild(title);
    tooltip.appendChild(meta);
    tooltip.appendChild(actions);
    container.appendChild(tooltip);
    return tooltip;
  }

  function fillTooltip(tooltip, data) {
    const title = tooltip.querySelector(".dag-tooltip-title");
    const meta = tooltip.querySelector(".dag-tooltip-meta");
    const link = tooltip.querySelector(".dag-tooltip-link");
    const label = typeof data.label === "string" ? data.label.split("\n")[0] : data.id;
    if (title) {
      title.textContent = label || data.id || "Node";
    }
    if (meta) {
      meta.innerHTML = "";
      const rows = [
        ["ID", data.id || "—"],
        ["State", data.state || "—"],
        ["Kind", data.kind || "—"],
      ];
      rows.forEach(([labelText, valueText]) => {
        const dt = document.createElement("dt");
        dt.textContent = labelText;
        const dd = document.createElement("dd");
        dd.textContent = valueText;
        meta.appendChild(dt);
        meta.appendChild(dd);
      });
    }
    if (link) {
      if (data.url) {
        link.setAttribute("href", data.url);
        link.classList.remove("is-hidden");
      } else {
        link.removeAttribute("href");
        link.classList.add("is-hidden");
      }
    }
  }

  function positionTooltip(tooltip, container, position) {
    const containerWidth = container.clientWidth;
    const containerHeight = container.clientHeight;
    tooltip.style.left = "0px";
    tooltip.style.top = "0px";
    tooltip.classList.remove("is-hidden");
    tooltip.style.visibility = "hidden";
    const width = tooltip.offsetWidth;
    const height = tooltip.offsetHeight;
    let left = position.x + TOOLTIP_OFFSET;
    let top = position.y + TOOLTIP_OFFSET;
    if (left + width > containerWidth - TOOLTIP_OFFSET) {
      left = Math.max(TOOLTIP_OFFSET, containerWidth - width - TOOLTIP_OFFSET);
    }
    if (top + height > containerHeight - TOOLTIP_OFFSET) {
      top = Math.max(TOOLTIP_OFFSET, containerHeight - height - TOOLTIP_OFFSET);
    }
    tooltip.style.left = `${left}px`;
    tooltip.style.top = `${top}px`;
    tooltip.style.visibility = "visible";
  }

  function showTooltip(tooltip, container, data, position) {
    if (!tooltip) {
      return;
    }
    fillTooltip(tooltip, data);
    positionTooltip(tooltip, container, position);
  }

  function hideTooltip(tooltip) {
    if (!tooltip) {
      return;
    }
    tooltip.classList.add("is-hidden");
  }

  document.addEventListener("cytoscape-ready", () => {
    const script = document.getElementById("dag-edges");
    const nodesScript = document.getElementById("dag-nodes");
    const edges = script ? JSON.parse(script.textContent || "[]") : [];
    const nodes = nodesScript ? JSON.parse(nodesScript.textContent || "[]") : [];
    const container = document.getElementById("dag-container");
    if (container) {
      render("dag-container", { nodes, edges });
    }
  });

  window.CeleryDag = {
    render: render,
  };
})();
