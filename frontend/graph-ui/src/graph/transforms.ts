// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

import type { Edge, Node } from "@xyflow/react";
import { Position } from "@xyflow/react";

import type {
  GraphEdgePayload,
  GraphMeta,
  GraphMetaCounts,
  GraphNodePayload,
  GraphPayload,
  TaskState,
} from "./types";

export const NODE_HEIGHT = 96;
export const NODE_WIDTH = 220;

export interface GraphModel {
  meta: GraphMeta;
  nodes: Map<string, GraphNodePayload>;
  edges: GraphEdgePayload[];
  folded?: FoldedInfo;
}

export interface TaskNodeData {
  id: string;
  title: string;
  taskName: string | null;
  state: TaskState | null;
  startedAt: string | null;
  finishedAt: string | null;
  durationMs: number | null;
  retries: number | null;
  queue: string | null;
  worker: string | null;
  argsPreview: string | null;
  kwargsPreview: string | null;
  resultPreview: string | null;
  tracebackPreview: string | null;
  stamps: string[];
  parentId: string | null;
  rootId: string | null;
  kind: string | null;
  isVirtual: boolean;
  showMeta: boolean;
  handleDirection: "RIGHT" | "DOWN";
  foldedLatestId: string | null;
}

export interface GroupNodeData {
  id: string;
  label: string;
  kind: string | null;
  count: number;
}

export interface FoldedInfo {
  childToRoot: Map<string, string>;
  latestChildByRoot: Map<string, string>;
}

export type GraphFlowNode = Node<TaskNodeData> | Node<GroupNodeData>;

export interface FilterState {
  query: string;
  allowedStates: Set<TaskState>;
  onlyFailedPath: boolean;
}

export const DEFAULT_COUNTS: GraphMetaCounts = {
  total: 0,
  pending: 0,
  running: 0,
  retry: 0,
  success: 0,
  failure: 0,
  revoked: 0,
};

export function normalizePayload(payload: GraphPayload): GraphPayload {
  return {
    meta: payload.meta ?? { root_id: null, generated_at: null, counts: DEFAULT_COUNTS },
    nodes: Array.isArray(payload.nodes) ? payload.nodes : [],
    edges: Array.isArray(payload.edges) ? payload.edges : [],
  };
}

export function buildGraphModel(payload: GraphPayload): GraphModel {
  const normalized = normalizePayload(payload);
  const nodes = new Map<string, GraphNodePayload>();
  normalized.nodes.forEach((node) => nodes.set(node.id, node));
  return foldSelfScheduling({
    meta: normalized.meta,
    nodes,
    edges: normalized.edges,
  });
}

export function computeCounts(nodes: Iterable<GraphNodePayload>): GraphMetaCounts {
  const counts: GraphMetaCounts = { ...DEFAULT_COUNTS };
  for (const node of nodes) {
    if (!node.state) {
      continue;
    }
    counts.total += 1;
    switch (node.state) {
      case "PENDING":
        counts.pending += 1;
        break;
      case "RECEIVED":
        counts.pending += 1;
        break;
      case "STARTED":
        counts.running += 1;
        break;
      case "RETRY":
        counts.retry += 1;
        break;
      case "SUCCESS":
        counts.success += 1;
        break;
      case "FAILURE":
        counts.failure += 1;
        break;
      case "REVOKED":
        counts.revoked += 1;
        break;
      default:
        break;
    }
  }
  return counts;
}

const SELF_SCHEDULE_EXCLUDED_KINDS = new Set(["group", "chord", "map", "starmap", "chunks"]);

function isFoldableTask(node: GraphNodePayload | undefined): node is GraphNodePayload {
  if (!node) {
    return false;
  }
  if (!node.task_name) {
    return false;
  }
  if (!node.state && node.kind) {
    return false;
  }
  return true;
}

function buildSignature(node: GraphNodePayload): string {
  return `${node.task_name ?? ""}::${node.args_preview ?? ""}::${node.kwargs_preview ?? ""}`;
}

function nodeTimestamp(node: GraphNodePayload): number {
  const value = node.finished_at ?? node.started_at ?? "";
  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? Number.NEGATIVE_INFINITY : parsed;
}

function foldSelfScheduling(model: GraphModel): GraphModel {
  const { nodes, edges } = model;
  const matchingChildren = new Map<string, Set<string>>();
  const matchingParents = new Map<string, Set<string>>();
  const matchingEdgeKeys = new Set<string>();

  for (const edge of edges) {
    const kind = edge.kind ?? "chain";
    if (SELF_SCHEDULE_EXCLUDED_KINDS.has(kind)) {
      continue;
    }
    if (edge.source === edge.target) {
      continue;
    }
    const parent = nodes.get(edge.source);
    const child = nodes.get(edge.target);
    if (!isFoldableTask(parent) || !isFoldableTask(child)) {
      continue;
    }
    if (buildSignature(parent) !== buildSignature(child)) {
      continue;
    }
    matchingEdgeKeys.add(`${edge.source}::${edge.target}`);
    const children = matchingChildren.get(edge.source);
    if (children) {
      children.add(edge.target);
    } else {
      matchingChildren.set(edge.source, new Set([edge.target]));
    }
    const parents = matchingParents.get(edge.target);
    if (parents) {
      parents.add(edge.source);
    } else {
      matchingParents.set(edge.target, new Set([edge.source]));
    }
  }

  if (matchingEdgeKeys.size === 0) {
    return model;
  }

  const parentIds = new Set<string>(matchingChildren.keys());
  const childIds = new Set<string>(matchingParents.keys());
  const roots: string[] = [];
  for (const parentId of parentIds) {
    if (!childIds.has(parentId)) {
      roots.push(parentId);
    }
  }

  const childToRoot = new Map<string, string>();
  const latestChildByRoot = new Map<string, string>();
  const childrenByRoot = new Map<string, Set<string>>();

  const collect = (rootId: string) => {
    const queue: string[] = [rootId];
    const seen = new Set<string>([rootId]);
    const children = new Set<string>();
    while (queue.length > 0) {
      const current = queue.shift();
      if (!current) {
        continue;
      }
      const next = matchingChildren.get(current);
      if (!next) {
        continue;
      }
      for (const child of next) {
        if (child === rootId || seen.has(child)) {
          continue;
        }
        seen.add(child);
        children.add(child);
        childToRoot.set(child, rootId);
        queue.push(child);
      }
    }
    if (children.size === 0) {
      return;
    }
    childrenByRoot.set(rootId, children);
    let latestId: string | null = null;
    let latestTime = Number.NEGATIVE_INFINITY;
    for (const childId of children) {
      const node = nodes.get(childId);
      if (!node) {
        continue;
      }
      const ts = nodeTimestamp(node);
      if (ts >= latestTime) {
        latestTime = ts;
        latestId = childId;
      }
    }
    if (latestId) {
      latestChildByRoot.set(rootId, latestId);
    }
  };

  roots.forEach(collect);
  for (const parentId of parentIds) {
    if (!childToRoot.has(parentId) && !childrenByRoot.has(parentId)) {
      collect(parentId);
    }
  }

  if (childToRoot.size === 0) {
    return model;
  }

  const newNodes = new Map(nodes);
  for (const childId of childToRoot.keys()) {
    newNodes.delete(childId);
  }
  for (const [rootId, latestChildId] of latestChildByRoot.entries()) {
    const rootNode = newNodes.get(rootId);
    const latestNode = nodes.get(latestChildId);
    if (!rootNode || !latestNode) {
      continue;
    }
    newNodes.set(rootId, {
      ...rootNode,
      ...latestNode,
      id: rootNode.id,
      parent_id: rootNode.parent_id,
      root_id: rootNode.root_id,
      folded_latest_id: latestChildId,
    });
  }

  const newEdges: GraphEdgePayload[] = [];
  for (const edge of edges) {
    if (matchingEdgeKeys.has(`${edge.source}::${edge.target}`)) {
      continue;
    }
    const source = childToRoot.get(edge.source) ?? edge.source;
    const target = childToRoot.get(edge.target) ?? edge.target;
    if (source === target) {
      continue;
    }
    if (!newNodes.has(source) || !newNodes.has(target)) {
      continue;
    }
    const kind = edge.kind ?? "chain";
    newEdges.push({
      ...edge,
      id: `${source}->${target}:${kind}`,
      source,
      target,
    });
  }

  for (const [rootId, children] of childrenByRoot.entries()) {
    if (!newNodes.has(rootId) || children.size === 0) {
      continue;
    }
    newEdges.push({
      id: `${rootId}->${rootId}:self`,
      source: rootId,
      target: rootId,
      kind: "self",
      label: String(children.size),
    });
  }

  return {
    meta: model.meta,
    nodes: newNodes,
    edges: newEdges,
    folded: {
      childToRoot,
      latestChildByRoot,
    },
  };
}

export function buildTaskNodeData(
  node: GraphNodePayload,
  showMeta: boolean,
  handleDirection: "RIGHT" | "DOWN",
): TaskNodeData {
  const title = node.task_name ?? node.kind ?? node.id;
  const isVirtual = !node.state && Boolean(node.kind);
  return {
    id: node.id,
    title,
    taskName: node.task_name,
    state: node.state,
    startedAt: node.started_at,
    finishedAt: node.finished_at,
    durationMs: node.duration_ms,
    retries: node.retries,
    queue: node.queue,
    worker: node.worker,
    argsPreview: node.args_preview,
    kwargsPreview: node.kwargs_preview,
    resultPreview: node.result_preview,
    tracebackPreview: node.traceback_preview,
    stamps: parseStamps(node.stamps_preview ?? null),
    parentId: node.parent_id,
    rootId: node.root_id,
    kind: node.kind ?? null,
    isVirtual,
    showMeta,
    handleDirection,
    foldedLatestId: node.folded_latest_id ?? null,
  };
}

export function buildReactFlowNodes(
  nodes: Iterable<GraphNodePayload>,
  showMeta: boolean,
  highlight: Set<string>,
  queryMatches: Set<string>,
  handleDirection: "RIGHT" | "DOWN",
): Node<TaskNodeData>[] {
  const result: Node<TaskNodeData>[] = [];
  const sourcePosition = handleDirection === "DOWN" ? Position.Bottom : Position.Right;
  const targetPosition = handleDirection === "DOWN" ? Position.Top : Position.Left;
  for (const node of nodes) {
    if (node.kind === "group" && !node.state) {
      continue;
    }
    const type = node.kind === "chord" && !node.state ? "chordNode" : "taskNode";
    const isHighlighted = highlight.has(node.id);
    const isMatch = queryMatches.has(node.id);
    result.push({
      id: node.id,
      type,
      data: buildTaskNodeData(node, showMeta, handleDirection),
      position: { x: 0, y: 0 },
      sourcePosition,
      targetPosition,
      className: [
        isHighlighted ? "is-highlighted" : "",
        isMatch ? "is-search-match" : "",
      ]
        .filter(Boolean)
        .join(" "),
      style: {
        width: NODE_WIDTH,
        height: NODE_HEIGHT,
      },
    });
  }
  return result;
}

const GROUP_PADDING = 24;
const GROUP_HEADER = 28;
const GROUP_LABELS: Record<string, string> = {
  group: "Group",
  chord: "Chord Header",
  map: "Map",
  starmap: "Starmap",
  chunks: "Chunks",
};

export function applyGroupLayout(
  nodes: Node<TaskNodeData>[],
  edges: GraphEdgePayload[],
): Array<Node<TaskNodeData> | Node<GroupNodeData>> {
  const nodeMap = new Map(nodes.map((node) => [node.id, node]));
  const groupMembers = new Map<string, Node<TaskNodeData>[]>();

  edges.forEach((edge) => {
    if (edge.kind !== "group") {
      return;
    }
    const child = nodeMap.get(edge.target);
    if (!child || child.parentId) {
      return;
    }
    const members = groupMembers.get(edge.source);
    if (members) {
      members.push(child);
    } else {
      groupMembers.set(edge.source, [child]);
    }
  });

  const groupNodes: Array<Node<GroupNodeData>> = [];
  for (const [groupId, children] of groupMembers.entries()) {
    if (children.length < 2) {
      continue;
    }
    let minX = Number.POSITIVE_INFINITY;
    let minY = Number.POSITIVE_INFINITY;
    let maxX = Number.NEGATIVE_INFINITY;
    let maxY = Number.NEGATIVE_INFINITY;
    for (const child of children) {
      minX = Math.min(minX, child.position.x);
      minY = Math.min(minY, child.position.y);
      maxX = Math.max(maxX, child.position.x + NODE_WIDTH);
      maxY = Math.max(maxY, child.position.y + NODE_HEIGHT);
    }

    const groupNodeId = groupId;
    const groupNode = nodeMap.get(groupId);
    if (groupNode) {
      continue;
    }
    const groupKind = groupNode?.data.kind ?? "group";
    const label = GROUP_LABELS[groupKind] ?? "Group";
    const groupX = minX - GROUP_PADDING;
    const groupY = minY - GROUP_PADDING - GROUP_HEADER;
    const width = maxX - minX + GROUP_PADDING * 2;
    const height = maxY - minY + GROUP_PADDING * 2 + GROUP_HEADER;

    children.forEach((child) => {
      child.parentId = groupNodeId;
      child.extent = "parent";
      child.position = {
        x: child.position.x - groupX,
        y: child.position.y - groupY,
      };
    });

    groupNodes.push({
      id: groupNodeId,
      type: "groupNode",
      position: { x: groupX, y: groupY },
      data: {
        id: groupNodeId,
        label,
        kind: groupKind,
        count: children.length,
      },
      selectable: false,
      draggable: false,
      style: {
        width,
        height,
      },
      className: "dag-group-node",
    });
  }

  return [...groupNodes, ...nodes];
}

function parseStamps(value: string | null): string[] {
  if (!value) {
    return [];
  }
  try {
    const parsed = JSON.parse(value) as unknown;
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return Object.entries(parsed).map(([key, entry]) => `${key}: ${String(entry)}`);
    }
    if (Array.isArray(parsed)) {
      return parsed.map((entry) => String(entry));
    }
  } catch {
    return [value];
  }
  return [value];
}

export function buildReactFlowEdges(
  edges: GraphEdgePayload[],
  highlightEdges: Set<string>,
  runningEdges: Set<string>,
  disableAnimations: boolean,
): Edge[] {
  const seen = new Map<string, number>();
  return edges.map((edge) => {
    const id = edge.id || `${edge.source}->${edge.target}`;
    const count = seen.get(id) ?? 0;
    seen.set(id, count + 1);
    const uniqueId = count > 0 ? `${id}#${count}` : id;
    const kind = edge.kind ?? "chain";
    const isRunning = runningEdges.has(id);
    const isSelfLoop = kind === "self";
    return {
      id: uniqueId,
      source: edge.source,
      target: edge.target,
      type: isSelfLoop ? "selfLoop" : "smoothstep",
      animated: isRunning && !disableAnimations,
      className: [
        `edge-${kind}`,
        highlightEdges.has(id) ? "is-highlighted" : "",
      ]
        .filter(Boolean)
        .join(" "),
      label: isSelfLoop ? (edge.label ?? "1") : undefined,
    };
  });
}

export function buildQueryMatches(nodes: Iterable<GraphNodePayload>, query: string): Set<string> {
  const matches = new Set<string>();
  const lowered = query.trim().toLowerCase();
  if (!lowered) {
    return matches;
  }
  for (const node of nodes) {
    const idMatch = node.id.toLowerCase().includes(lowered);
    const nameMatch = (node.task_name ?? "").toLowerCase().includes(lowered);
    if (idMatch || nameMatch) {
      matches.add(node.id);
    }
  }
  return matches;
}

export function filterGraph(model: GraphModel, filter: FilterState): GraphModel {
  const query = filter.query.trim().toLowerCase();
  const allowedStates = filter.allowedStates;
  const matched = new Set<string>();
  const nodes = new Map<string, GraphNodePayload>();

  for (const node of model.nodes.values()) {
    const queryMatch =
      !query ||
      node.id.toLowerCase().includes(query) ||
      (node.task_name ?? "").toLowerCase().includes(query);
    const stateMatch = !node.state || allowedStates.has(node.state);
    if (queryMatch && stateMatch) {
      matched.add(node.id);
      nodes.set(node.id, node);
    }
  }

  if (filter.onlyFailedPath) {
    const failedIds = new Set(
      Array.from(model.nodes.values())
        .filter((node) => node.state === "FAILURE" || node.state === "RETRY")
        .map((node) => node.id),
    );
    const path = buildPathClosure(model.edges, failedIds);
    for (const nodeId of path) {
      const node = model.nodes.get(nodeId);
      if (node) {
        nodes.set(nodeId, node);
      }
    }
  }

  const edges = model.edges.filter(
    (edge) => nodes.has(edge.source) && nodes.has(edge.target),
  );

  return {
    meta: model.meta,
    nodes,
    edges,
  };
}

export function buildPathClosure(edges: GraphEdgePayload[], focusIds: Set<string>): Set<string> {
  const incoming = new Map<string, string[]>();
  const outgoing = new Map<string, string[]>();
  for (const edge of edges) {
    if (!incoming.has(edge.target)) {
      incoming.set(edge.target, []);
    }
    if (!outgoing.has(edge.source)) {
      outgoing.set(edge.source, []);
    }
    incoming.get(edge.target)?.push(edge.source);
    outgoing.get(edge.source)?.push(edge.target);
  }

  const visited = new Set<string>(focusIds);
  const queue: string[] = [...focusIds];

  while (queue.length > 0) {
    const current = queue.shift();
    if (!current) {
      continue;
    }
    const parents = incoming.get(current) ?? [];
    const children = outgoing.get(current) ?? [];
    for (const neighbor of [...parents, ...children]) {
      if (!visited.has(neighbor)) {
        visited.add(neighbor);
        queue.push(neighbor);
      }
    }
  }

  return visited;
}

export function buildHighlight(
  edges: GraphEdgePayload[],
  hoveredId: string | null,
): { nodes: Set<string>; edges: Set<string> } {
  if (!hoveredId) {
    return { nodes: new Set(), edges: new Set() };
  }
  const nodes = new Set<string>([hoveredId]);
  const edgeSet = new Set<string>();
  for (const edge of edges) {
    if (edge.source === hoveredId || edge.target === hoveredId) {
      edgeSet.add(edge.id || `${edge.source}->${edge.target}`);
    }
  }
  return { nodes, edges: edgeSet };
}

export function buildRunningEdges(edges: GraphEdgePayload[], runningIds: Set<string>): Set<string> {
  const runningEdges = new Set<string>();
  for (const edge of edges) {
    if (runningIds.has(edge.source) || runningIds.has(edge.target)) {
      runningEdges.add(edge.id || `${edge.source}->${edge.target}`);
    }
  }
  return runningEdges;
}
