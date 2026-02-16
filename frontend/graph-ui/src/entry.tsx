// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

import { createRoot, type Root } from "react-dom/client";

import GraphApp from "./GraphApp";
import type { GraphOptions, GraphPayload } from "./graph/types";

import "./styles/index.css";

type ContainerLike = HTMLElement | string;

const roots = new Map<HTMLElement, Root>();
let renderSeq = 0;

function resolveContainer(target: ContainerLike): HTMLElement | null {
  if (typeof target === "string") {
    return document.getElementById(target);
  }
  return target;
}

function renderGraph(container: HTMLElement, payload: GraphPayload, options?: GraphOptions): void {
  const existing = roots.get(container);
  if (existing) {
    existing.render(<GraphApp payload={payload} options={options} />);
    return;
  }
  const root = createRoot(container);
  root.render(<GraphApp payload={payload} options={options} />);
  roots.set(container, root);
}

function render(container: ContainerLike, payload: GraphPayload, options?: GraphOptions): void {
  const el = resolveContainer(container);
  if (!el) {
    return;
  }
  const seq = (renderSeq += 1);
  const snapshotUrl = options?.snapshotUrl;
  if (snapshotUrl) {
    fetch(snapshotUrl, { headers: { Accept: "application/json" }, cache: "no-store" })
      .then((response) => (response.ok ? response.json() : null))
      .then((snapshot) => {
        if (seq !== renderSeq) {
          return;
        }
        if (snapshot) {
          renderGraph(el, snapshot as GraphPayload, options);
        } else {
          renderGraph(el, payload, options);
        }
      })
      .catch(() => {
        if (seq !== renderSeq) {
          return;
        }
        renderGraph(el, payload, options);
      });
    return;
  }
  renderGraph(el, payload, options);
}

function destroy(container: ContainerLike): void {
  const el = resolveContainer(container);
  if (!el) {
    return;
  }
  const root = roots.get(el);
  if (root) {
    root.unmount();
    roots.delete(el);
  }
}

declare global {
  interface Window {
    CeleryDag?: {
      render: (container: ContainerLike, payload: GraphPayload, options?: GraphOptions) => void;
      destroy: (container: ContainerLike) => void;
    };
  }
}

window.CeleryDag = {
  render,
  destroy,
};
