// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

import ELK from "elkjs/lib/elk.bundled";

import type { Node } from "@xyflow/react";

import { NODE_HEIGHT, NODE_WIDTH } from "./transforms";

export type LayoutDirection = "RIGHT" | "DOWN";

const elk = new ELK();

export async function layoutNodes<T>(
  nodes: Node<T>[],
  edges: { source: string; target: string }[],
  direction: LayoutDirection,
): Promise<Node<T>[]> {
  const elkGraph = {
    id: "root",
    layoutOptions: {
      "elk.algorithm": "layered",
      "elk.direction": direction,
      "elk.layered.spacing.nodeNodeBetweenLayers": "80",
      "elk.spacing.nodeNode": "32",
      "elk.layered.nodePlacement.bk.fixedAlignment": "BALANCED",
    },
    children: nodes.map((node) => ({
      id: node.id,
      width: NODE_WIDTH,
      height: NODE_HEIGHT,
    })),
    edges: edges.map((edge, index) => ({
      id: `edge-${index}`,
      sources: [edge.source],
      targets: [edge.target],
    })),
  };

  const layout = await elk.layout(elkGraph);
  const positions = new Map<string, { x: number; y: number }>();
  layout.children?.forEach((child) => {
    if (typeof child.x === "number" && typeof child.y === "number") {
      positions.set(child.id, { x: child.x, y: child.y });
    }
  });

  return nodes.map((node) => {
    const pos = positions.get(node.id) ?? { x: node.position.x, y: node.position.y };
    return {
      ...node,
      position: pos,
    };
  });
}
