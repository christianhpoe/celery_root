// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Background,
  Controls,
  ReactFlow,
  type ReactFlowInstance,
  ReactFlowProvider,
  applyNodeChanges,
} from "@xyflow/react";

import "@xyflow/react/dist/style.css";

import DetailsPanel from "./components/DetailsPanel";
import Legend from "./components/Legend";
import TaskNode from "./components/TaskNode";
import GroupNode from "./components/GroupNode";
import Toolbar from "./components/Toolbar";
import ChordJoinNode from "./components/ChordJoinNode";
import SelfLoopEdge from "./components/SelfLoopEdge";
import { layoutNodes, type LayoutDirection } from "./graph/layout";
import {
  applyGroupLayout,
  buildGraphModel,
  buildHighlight,
  buildPathClosure,
  buildQueryMatches,
  buildReactFlowEdges,
  buildReactFlowNodes,
  buildRunningEdges,
  computeCounts,
  filterGraph,
  NODE_HEIGHT,
  NODE_WIDTH,
  type FilterState,
  type GraphFlowNode,
  type GraphModel,
} from "./graph/transforms";
import type {
  GraphEdgePayload,
  GraphMetaCounts,
  GraphOptions,
  GraphPayload,
  GraphUpdatePayload,
  TaskState,
} from "./graph/types";

interface GraphAppProps {
  payload: GraphPayload;
  options?: GraphOptions;
}

const ACTIVE_STATES: TaskState[] = ["STARTED", "RETRY", "PENDING", "RECEIVED"];
const TASK_TYPE_PALETTE: string[] = [
  "#2563eb",
  "#0ea5e9",
  "#14b8a6",
  "#10b981",
  "#22c55e",
  "#84cc16",
  "#eab308",
  "#f59e0b",
  "#f97316",
  "#ef4444",
  "#f43f5e",
  "#ec4899",
  "#d946ef",
  "#a855f7",
  "#8b5cf6",
  "#6366f1",
  "#0f766e",
  "#0891b2",
  "#0284c7",
  "#64748b",
  "#7c3aed",
  "#be123c",
  "#b45309",
  "#047857",
];

function choosePaletteStep(length: number): number {
  if (length <= 1) {
    return 1;
  }
  const target = Math.floor(length / 2) + 1;
  const gcd = (a: number, b: number): number => (b === 0 ? a : gcd(b, a % b));
  for (let step = target; step < target + length; step += 1) {
    if (gcd(step, length) === 1) {
      return step;
    }
  }
  return 1;
}

function buildInitialFilter(totalNodes: number): FilterState {
  if (totalNodes > 2000) {
    return {
      query: "",
      allowedStates: new Set<TaskState>([...ACTIVE_STATES, "FAILURE"]),
      onlyFailedPath: false,
    };
  }
  return {
    query: "",
    allowedStates: new Set<TaskState>([
      "PENDING",
      "RECEIVED",
      "STARTED",
      "RETRY",
      "SUCCESS",
      "FAILURE",
      "REVOKED",
    ]),
    onlyFailedPath: false,
  };
}

function deriveSnapshotUrl(options?: GraphOptions): string | undefined {
  if (options?.snapshotUrl) {
    return options.snapshotUrl;
  }
  const refreshUrl = options?.refreshUrl;
  if (!refreshUrl) {
    return undefined;
  }
  if (refreshUrl.endsWith("/updates")) {
    return refreshUrl.replace(/\/updates\/?$/, "");
  }
  return refreshUrl;
}

function totalFromCounts(counts: GraphMetaCounts | null | undefined): number {
  if (!counts) {
    return 0;
  }
  return (
    counts.total +
    counts.pending +
    counts.running +
    counts.retry +
    counts.success +
    counts.failure +
    counts.revoked
  );
}

function snapshotHasMissingEdges(snapshot: GraphPayload): boolean {
  return snapshot.nodes.length > 1 && snapshot.edges.length === 0;
}

function snapshotKey(snapshotUrl: string): string {
  return `celery-root:graph:force-snapshot:${snapshotUrl}`;
}

function markForceSnapshot(snapshotUrl: string | undefined): void {
  if (!snapshotUrl) {
    return;
  }
  try {
    sessionStorage.setItem(snapshotKey(snapshotUrl), "1");
  } catch {
    // Ignore storage failures (private mode, etc.).
  }
}

function takeForceSnapshot(snapshotUrl: string | undefined): boolean {
  if (!snapshotUrl) {
    return false;
  }
  try {
    const key = snapshotKey(snapshotUrl);
    const value = sessionStorage.getItem(key);
    if (value !== "1") {
      return false;
    }
    sessionStorage.removeItem(key);
    return true;
  } catch {
    return false;
  }
}

function shouldSkipSnapshot(
  snapshot: GraphPayload,
  currentModel: GraphModel,
  hasSeenEdges: boolean,
): boolean {
  if (currentModel.nodes.size > 0 && snapshot.nodes.length === 0) {
    return true;
  }
  if (snapshotHasMissingEdges(snapshot) && (currentModel.nodes.size > 1 || hasSeenEdges)) {
    return true;
  }
  return false;
}

function GraphCanvas({ payload, options }: GraphAppProps) {
  const [graphModel, setGraphModel] = useState<GraphModel>(() => buildGraphModel(payload));
  const [filter, setFilter] = useState<FilterState>(() =>
    buildInitialFilter(payload.nodes.length),
  );
  const [direction, setDirection] = useState<LayoutDirection>("DOWN");
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [hoveredId, setHoveredId] = useState<string | null>(null);
  const [zoom, setZoom] = useState<number>(1);
  const [isLayouting, setIsLayouting] = useState<boolean>(false);
  const [rfNodes, setRfNodes] = useState<GraphFlowNode[]>([]);
  const [rfEdges, setRfEdges] = useState<ReturnType<typeof buildReactFlowEdges>>([]);
  const instanceRef = useRef<ReactFlowInstance | null>(null);
  const baseNodesRef = useRef<ReturnType<typeof buildReactFlowNodes>>([]);
  const layoutNodesRef = useRef<GraphModel["nodes"]>(new Map());
  const baseEdgesRef = useRef<ReturnType<typeof buildReactFlowEdges>>([]);
  const lastStableEdgesRef = useRef<GraphEdgePayload[]>([]);
  const graphModelRef = useRef<GraphModel>(graphModel);
  const snapshotAttemptRef = useRef<string | null>(null);
  const missingEdgesAttemptRef = useRef<Map<string, number>>(new Map());
  const missingEdgesTimerRef = useRef<number | null>(null);
  const hasSeenEdgesRef = useRef<boolean>(graphModel.edges.length > 0);
  const manualPositionsRef = useRef<Map<string, { x: number; y: number }>>(new Map());
  const lastLayoutKeyRef = useRef<string | null>(null);
  const hasFitRef = useRef(false);
  const lastUpdateRef = useRef<string | null>(payload.meta?.generated_at ?? null);
  const completedAtRef = useRef<number | null>(null);
  const [layoutPositions, setLayoutPositions] = useState<Map<string, { x: number; y: number }> | null>(null);
  const edgeRenderKeyRef = useRef<string | null>(null);
  const edgeRemountAttemptsRef = useRef<Map<string, number>>(new Map());
  const [flowKey, setFlowKey] = useState<number>(0);

  useEffect(() => {
    graphModelRef.current = graphModel;
    if (graphModel.edges.length > 0) {
      hasSeenEdgesRef.current = true;
      lastStableEdgesRef.current = graphModel.edges;
    }
  }, [graphModel]);

  const totalNodes = graphModel.nodes.size;
  const counts = useMemo(() => {
    const metaCounts = graphModel.meta?.counts;
    if (metaCounts) {
      const total =
        metaCounts.total +
        metaCounts.pending +
        metaCounts.running +
        metaCounts.retry +
        metaCounts.success +
        metaCounts.failure +
        metaCounts.revoked;
      if (total > 0) {
        return metaCounts;
      }
    }
    return computeCounts(graphModel.nodes.values());
  }, [graphModel.meta, graphModel.nodes]);
  const countTotal = totalFromCounts(counts);
  const hasActive = counts.pending > 0 || counts.running > 0 || counts.retry > 0;

  useEffect(() => {
    if (hasActive) {
      completedAtRef.current = null;
      return;
    }
    if (completedAtRef.current === null) {
      completedAtRef.current = Date.now();
    }
  }, [hasActive]);

  const disableAnimations = totalNodes > 500;
  const showMeta = totalNodes <= 1000 || zoom >= 1.1;

  const filteredModel = useMemo(() => filterGraph(graphModel, filter), [graphModel, filter]);
  const displayEdges = useMemo(() => {
    if (filteredModel.edges.length > 0 || filteredModel.nodes.size <= 1) {
      if (filteredModel.edges.length > 0) {
        lastStableEdgesRef.current = filteredModel.edges;
      }
      return filteredModel.edges;
    }
    return lastStableEdgesRef.current.length > 0 ? lastStableEdgesRef.current : filteredModel.edges;
  }, [filteredModel.edges, filteredModel.nodes]);
  const queryMatches = useMemo(
    () => buildQueryMatches(graphModel.nodes.values(), filter.query),
    [graphModel.nodes, filter.query],
  );
  const highlight = useMemo(() => {
    if (selectedId) {
      const pathNodes = buildPathClosure(displayEdges, new Set([selectedId]));
      const edgeSet = new Set<string>();
      displayEdges.forEach((edge) => {
        if (pathNodes.has(edge.source) && pathNodes.has(edge.target)) {
          edgeSet.add(edge.id || `${edge.source}->${edge.target}`);
        }
      });
      return { nodes: pathNodes, edges: edgeSet };
    }
    return buildHighlight(displayEdges, hoveredId);
  }, [displayEdges, hoveredId, selectedId]);
  const taskTypeColors = useMemo(() => {
    const names = new Set<string>();
    graphModel.nodes.forEach((node) => {
      if (node.task_name) {
        names.add(node.task_name);
      }
    });
    const sortedNames = Array.from(names).sort((left, right) => left.localeCompare(right));
    const colors = new Map<string, string>();
    const paletteSize = TASK_TYPE_PALETTE.length;
    const step = choosePaletteStep(paletteSize);
    sortedNames.forEach((name, index) => {
      const paletteIndex = (index * step) % paletteSize;
      colors.set(name, TASK_TYPE_PALETTE[paletteIndex]);
    });
    return colors;
  }, [graphModel.nodes]);
  const runningIds = useMemo(() => {
    const running = new Set<string>();
    graphModel.nodes.forEach((node) => {
      if (node.state === "STARTED") {
        running.add(node.id);
      }
    });
    return running;
  }, [graphModel.nodes]);
  const runningEdges = useMemo(
    () => buildRunningEdges(displayEdges, runningIds),
    [displayEdges, runningIds],
  );

  const baseNodes = useMemo(
    () =>
      buildReactFlowNodes(
        filteredModel.nodes.values(),
        showMeta,
        highlight.nodes,
        queryMatches,
        direction,
        taskTypeColors,
      ),
    [filteredModel.nodes, showMeta, highlight.nodes, queryMatches, direction, taskTypeColors],
  );
  baseNodesRef.current = baseNodes;
  layoutNodesRef.current = filteredModel.nodes;
  const baseEdges = useMemo(
    () => buildReactFlowEdges(displayEdges, highlight.edges, runningEdges, disableAnimations),
    [displayEdges, highlight.edges, runningEdges, disableAnimations],
  );
  baseEdgesRef.current = baseEdges;
  const topologyKey = useMemo(() => {
    const nodeIds = Array.from(filteredModel.nodes.keys()).sort().join("|");
    const edgeIds = displayEdges
      .map((edge) => edge.id || `${edge.source}->${edge.target}:${edge.kind ?? "chain"}`)
      .sort()
      .join("|");
    return `${nodeIds}::${edgeIds}`;
  }, [filteredModel.nodes, displayEdges]);
  useEffect(() => {
    let active = true;
    const layoutKey = `${topologyKey}:${direction}`;
    const shouldRelayout = lastLayoutKeyRef.current !== layoutKey;
    if (!shouldRelayout && layoutPositions) {
      return () => {
        active = false;
      };
    }
    if (shouldRelayout) {
      setIsLayouting(true);
      lastLayoutKeyRef.current = layoutKey;
    }
    const layoutInput = buildReactFlowNodes(
      layoutNodesRef.current.values(),
      false,
      new Set(),
      new Set(),
      direction,
      taskTypeColors,
    );
    const layoutNodeIds = new Set(layoutInput.map((node) => node.id));
    const layoutEdges = displayEdges
      .filter(
        (edge) =>
          layoutNodeIds.has(edge.source) &&
          layoutNodeIds.has(edge.target) &&
          edge.source !== edge.target,
      )
      .map((edge) => ({ source: edge.source, target: edge.target }));
    if (displayEdges.length === 0 && layoutInput.length > 0) {
      const ordered = [...layoutInput].sort((left, right) => left.id.localeCompare(right.id));
      const columns = Math.max(1, Math.ceil(Math.sqrt(ordered.length)));
      const gapX = NODE_WIDTH + 80;
      const gapY = NODE_HEIGHT + 80;
      const positions = new Map<string, { x: number; y: number }>();
      ordered.forEach((node, index) => {
        const col = index % columns;
        const row = Math.floor(index / columns);
        positions.set(node.id, { x: col * gapX, y: row * gapY });
      });
      setLayoutPositions(positions);
      const displayNodes = baseNodesRef.current.map((node) => ({
        ...node,
        position:
          manualPositionsRef.current.get(node.id) ??
          positions.get(node.id) ??
          node.position,
      }));
      const grouped = applyGroupLayout(displayNodes, baseEdgesRef.current);
      setRfNodes(grouped);
      setRfEdges(baseEdgesRef.current);
      if (active && shouldRelayout) {
        setIsLayouting(false);
      }
      return () => {
        active = false;
      };
    }
    layoutNodes(layoutInput, layoutEdges, direction)
      .then((layouted) => {
        if (!active) {
          return;
        }
        const positions = new Map<string, { x: number; y: number }>();
        layouted.forEach((node) => {
          positions.set(node.id, node.position);
        });
        setLayoutPositions(positions);
        const displayNodes = baseNodesRef.current.map((node) => ({
          ...node,
          position:
            manualPositionsRef.current.get(node.id) ??
            positions.get(node.id) ??
            node.position,
        }));
        const grouped = applyGroupLayout(displayNodes, baseEdgesRef.current);
        setRfNodes(grouped);
        setRfEdges(baseEdgesRef.current);
      })
      .finally(() => {
        if (active && shouldRelayout) {
          setIsLayouting(false);
        }
      });
    return () => {
      active = false;
    };
  }, [direction, layoutPositions, topologyKey, displayEdges]);

  useEffect(() => {
    if (!layoutPositions) {
      return;
    }
    setRfNodes((prev) => {
      const prevPositions = new Map(prev.map((node) => [node.id, node.position]));
      const displayNodes = baseNodes.map((node) => ({
        ...node,
        position:
          manualPositionsRef.current.get(node.id) ??
          prevPositions.get(node.id) ??
          layoutPositions.get(node.id) ??
          node.position,
      }));
      return applyGroupLayout(displayNodes, baseEdges);
    });
  }, [baseNodes, baseEdges, layoutPositions]);

  useEffect(() => {
    if (baseEdges.length > 0 || filteredModel.nodes.size <= 1) {
      setRfEdges(baseEdges);
    }
  }, [baseEdges, filteredModel.nodes.size]);
  useEffect(() => {
    if (rfNodes.length === 0 || rfEdges.length === 0) {
      return;
    }
    if (edgeRenderKeyRef.current === topologyKey) {
      return;
    }
    edgeRenderKeyRef.current = topologyKey;
    setRfEdges((edges) =>
      edges.map((edge) => ({
        ...edge,
        data: edge.data ? { ...edge.data } : edge.data,
      })),
    );
  }, [rfEdges.length, rfNodes.length, topologyKey]);
  useEffect(() => {
    if (rfNodes.length === 0 || rfEdges.length === 0) {
      return;
    }
    const attemptCount = edgeRemountAttemptsRef.current.get(topologyKey) ?? 0;
    if (attemptCount >= 2) {
      return;
    }
    let active = true;
    const checkEdges = () => {
      if (!active) {
        return;
      }
      const edgePaths = document.querySelectorAll(".react-flow__edge-path");
      const edgesSvg = document.querySelector(".react-flow__edges svg");
      if (!edgesSvg || edgePaths.length === 0) {
        edgeRemountAttemptsRef.current.set(topologyKey, attemptCount + 1);
        hasFitRef.current = false;
        setFlowKey((value) => value + 1);
      }
    };
    const rafId = requestAnimationFrame(() => {
      checkEdges();
      window.setTimeout(checkEdges, 150);
    });
    return () => {
      active = false;
      cancelAnimationFrame(rafId);
    };
  }, [rfEdges.length, rfNodes.length, topologyKey]);
  useEffect(() => {
    hasFitRef.current = false;
  }, [flowKey]);

  useEffect(() => {
    if (!hasFitRef.current && instanceRef.current && rfNodes.length > 0) {
      instanceRef.current.fitView({ padding: 0.2 });
      hasFitRef.current = true;
    }
  }, [rfNodes]);

  useEffect(() => {
    if (queryMatches.size === 1 && instanceRef.current) {
      const matchId = Array.from(queryMatches)[0];
      const node = rfNodes.find((item) => item.id === matchId);
      if (node) {
        instanceRef.current.setCenter(node.position.x, node.position.y, { zoom: 1.2 });
      }
    }
  }, [queryMatches, rfNodes]);

  useEffect(() => {
    if (graphModel.nodes.size > 0 || countTotal <= 0) {
      return;
    }
    const snapshotUrl = deriveSnapshotUrl(options);
    if (!snapshotUrl) {
      return;
    }
    const attemptKey = `${snapshotUrl}:${graphModel.meta?.generated_at ?? "unknown"}`;
    if (snapshotAttemptRef.current === attemptKey) {
      return;
    }
    snapshotAttemptRef.current = attemptKey;
    fetch(snapshotUrl, { headers: { Accept: "application/json" } })
      .then((response) => (response.ok ? response.json() : null))
      .then((snapshot) => {
        if (!snapshot) {
          return;
        }
        const payload = snapshot as GraphPayload;
        if (payload.nodes.length === 0) {
          return;
        }
        setGraphModel(buildGraphModel(payload));
      })
      .catch(() => undefined);
  }, [countTotal, graphModel.meta, graphModel.nodes.size, options]);

  useEffect(() => {
    if (graphModel.nodes.size <= 1 || graphModel.edges.length > 0) {
      return;
    }
    markForceSnapshot(deriveSnapshotUrl(options));
  }, [graphModel.edges.length, graphModel.nodes.size, options]);

  useEffect(() => {
    const snapshotUrl = deriveSnapshotUrl(options);
    if (!takeForceSnapshot(snapshotUrl)) {
      return;
    }
    let active = true;
    fetch(snapshotUrl, { headers: { Accept: "application/json" }, cache: "no-store" })
      .then((response) => (response.ok ? response.json() : null))
      .then((snapshot) => {
        if (!active || !snapshot) {
          return;
        }
        const payload = snapshot as GraphPayload;
        if (snapshotHasMissingEdges(payload)) {
          markForceSnapshot(snapshotUrl);
          return;
        }
        if (payload.nodes.length === 0 && graphModelRef.current.nodes.size > 0) {
          markForceSnapshot(snapshotUrl);
          return;
        }
        setGraphModel(buildGraphModel(payload));
        completedAtRef.current = null;
      })
      .catch(() => {
        markForceSnapshot(snapshotUrl);
      });
    return () => {
      active = false;
    };
  }, [options]);

  useEffect(() => {
    if (graphModel.edges.length > 0 || graphModel.nodes.size <= 1) {
      if (missingEdgesTimerRef.current) {
        window.clearTimeout(missingEdgesTimerRef.current);
        missingEdgesTimerRef.current = null;
      }
      return;
    }
    const snapshotUrl = deriveSnapshotUrl(options);
    if (!snapshotUrl) {
      return;
    }
    const attemptKey = `${snapshotUrl}:${graphModel.meta?.generated_at ?? "unknown"}:${graphModel.nodes.size}`;
    let active = true;
    const scheduleAttempt = () => {
      if (!active) {
        return;
      }
      const attempts = missingEdgesAttemptRef.current.get(attemptKey) ?? 0;
      if (attempts >= 5) {
        return;
      }
      const delay =
        attempts === 0
          ? 150
          : attempts === 1
            ? 400
            : attempts === 2
              ? 800
              : attempts === 3
                ? 1200
                : 2000;
      missingEdgesAttemptRef.current.set(attemptKey, attempts + 1);
      const timer = window.setTimeout(() => {
        fetch(snapshotUrl, { headers: { Accept: "application/json" } })
          .then((response) => (response.ok ? response.json() : null))
          .then((snapshot) => {
            if (!snapshot) {
              scheduleAttempt();
              return;
            }
            const payload = snapshot as GraphPayload;
            if (
              shouldSkipSnapshot(payload, graphModelRef.current, hasSeenEdgesRef.current) ||
              snapshotHasMissingEdges(payload)
            ) {
              if (snapshotHasMissingEdges(payload)) {
                markForceSnapshot(snapshotUrl);
              }
              scheduleAttempt();
              return;
            }
            setGraphModel(buildGraphModel(payload));
            completedAtRef.current = null;
          })
          .catch(() => {
            scheduleAttempt();
          });
      }, delay);
      missingEdgesTimerRef.current = timer;
    };
    scheduleAttempt();
    return () => {
      active = false;
      if (missingEdgesTimerRef.current) {
        window.clearTimeout(missingEdgesTimerRef.current);
        missingEdgesTimerRef.current = null;
      }
    };
  }, [graphModel.edges.length, graphModel.meta, graphModel.nodes.size, options]);

  useEffect(() => {
    if (!options?.refreshUrl || !hasActive) {
      return undefined;
    }
    let timer: number | undefined;
    let active = true;

    const poll = async () => {
      if (!active || document.hidden) {
        return;
      }
      if (completedAtRef.current && Date.now() - completedAtRef.current > 20000) {
        active = false;
        return;
      }
      const currentModel = graphModelRef.current;
      const since = lastUpdateRef.current;
      const url = new URL(options.refreshUrl, window.location.origin);
      if (since) {
        url.searchParams.set("since", since);
      }
      const response = await fetch(url.toString(), { headers: { Accept: "application/json" } });
      if (!response.ok) {
        return;
      }
      const payload = (await response.json()) as GraphUpdatePayload;
      lastUpdateRef.current = payload.generated_at;
      if (payload.topology_changed) {
        const snapshotUrl = deriveSnapshotUrl(options);
        if (snapshotUrl) {
          const snapshotResponse = await fetch(snapshotUrl, { headers: { Accept: "application/json" } });
          if (snapshotResponse.ok) {
            const snapshot = (await snapshotResponse.json()) as GraphPayload;
          if (shouldSkipSnapshot(snapshot, currentModel, hasSeenEdgesRef.current)) {
            if (snapshotHasMissingEdges(snapshot)) {
              markForceSnapshot(snapshotUrl);
            }
            return;
          }
            setGraphModel(buildGraphModel(snapshot));
            completedAtRef.current = null;
          }
        }
        return;
      }
      if (payload.node_updates.length === 0) {
        return;
      }
      if (
        payload.node_count !== currentModel.nodes.size ||
        payload.edge_count !== currentModel.edges.length
      ) {
        const snapshotUrl = deriveSnapshotUrl(options);
        if (snapshotUrl) {
          const snapshotResponse = await fetch(snapshotUrl, { headers: { Accept: "application/json" } });
          if (snapshotResponse.ok) {
            const snapshot = (await snapshotResponse.json()) as GraphPayload;
          if (shouldSkipSnapshot(snapshot, currentModel, hasSeenEdgesRef.current)) {
            if (snapshotHasMissingEdges(snapshot)) {
              markForceSnapshot(snapshotUrl);
            }
            return;
          }
            setGraphModel(buildGraphModel(snapshot));
            completedAtRef.current = null;
          }
        }
        return;
      }
      setGraphModel((prev) => {
        const updatedNodes = new Map(prev.nodes);
        const foldInfo = prev.folded;
        payload.node_updates.forEach((update) => {
          const { id, ...rest } = update;
          let targetId = id;
          let current = updatedNodes.get(targetId);
          if (!current && foldInfo) {
            const rootId = foldInfo.childToRoot.get(id);
            if (!rootId) {
              return;
            }
            const latestId = foldInfo.latestChildByRoot.get(rootId);
            if (latestId && latestId !== id) {
              return;
            }
            targetId = rootId;
            current = updatedNodes.get(targetId);
          }
          if (!current) {
            return;
          }
          updatedNodes.set(targetId, {
            ...current,
            ...rest,
            id: current.id,
            parent_id: current.parent_id,
            root_id: current.root_id,
          });
        });
        return { ...prev, nodes: updatedNodes, meta: { ...prev.meta, counts: payload.meta_counts } };
      });
    };

    const schedule = () => {
      timer = window.setTimeout(async () => {
        await poll();
        if (active) {
          schedule();
        }
      }, document.hidden ? 10000 : 2000);
    };

    schedule();

    return () => {
      active = false;
      if (timer) {
        window.clearTimeout(timer);
      }
    };
  }, [options, hasActive]);

  const onQueryChange = useCallback((value: string) => {
    setFilter((prev) => ({ ...prev, query: value }));
  }, []);

  const onToggleState = useCallback((state: TaskState) => {
    setFilter((prev) => {
      const next = new Set(prev.allowedStates);
      if (next.has(state)) {
        next.delete(state);
      } else {
        next.add(state);
      }
      return { ...prev, allowedStates: next };
    });
  }, []);

  const onToggleFailedPath = useCallback(() => {
    setFilter((prev) => ({ ...prev, onlyFailedPath: !prev.onlyFailedPath }));
  }, []);

  const handleFitView = useCallback(() => {
    instanceRef.current?.fitView({ padding: 0.2 });
  }, []);

  const handleRelayout = useCallback(() => {
    layoutNodes(rfNodes, rfEdges, direction).then((layouted) => {
      setRfNodes(layouted);
    });
  }, [rfNodes, rfEdges, direction]);

  const handleCenterRunning = useCallback(() => {
    if (!instanceRef.current) {
      return;
    }
    const runningNode = rfNodes.find((node) => runningIds.has(node.id));
    if (!runningNode) {
      return;
    }
    instanceRef.current.setCenter(runningNode.position.x, runningNode.position.y, { zoom: 1.2 });
  }, [rfNodes, runningIds]);

  const handleToggleDirection = useCallback(() => {
    setDirection((prev) => (prev === "RIGHT" ? "DOWN" : "RIGHT"));
  }, []);

  const selectedNode = selectedId ? graphModel.nodes.get(selectedId) : null;
  const selectedNodeData = selectedNode
    ? buildReactFlowNodes(
        [selectedNode].values(),
        true,
        new Set(),
        new Set(),
        direction,
        taskTypeColors,
      )[0].data
    : null;

  const childrenIds = useMemo(() => {
    if (!selectedId) {
      return [];
    }
    const children = new Set<string>();
    graphModel.edges.forEach((edge) => {
      if (edge.source === selectedId && edge.target !== selectedId) {
        children.add(edge.target);
      }
    });
    return Array.from(children);
  }, [graphModel.edges, selectedId]);

  const handleNodeClick = useCallback((_event: unknown, node: { id: string }) => {
    setSelectedId((prev) => (prev === node.id ? null : node.id));
  }, []);

  const handleMoveEnd = useCallback((_event: unknown, viewport: { zoom: number }) => {
    setZoom(viewport.zoom);
  }, []);

  const onNodeMouseEnter = useCallback((_event: unknown, node: { id: string }) => {
    setHoveredId(node.id);
  }, []);

  const onNodeMouseLeave = useCallback(() => {
    setHoveredId(null);
  }, []);
  const onNodesChange = useCallback((changes: Parameters<typeof applyNodeChanges>[0]) => {
    setRfNodes((nodes) => applyNodeChanges(changes, nodes));
    changes.forEach((change) => {
      if (change.type === "position" && change.position && !change.dragging) {
        manualPositionsRef.current.set(change.id, change.position);
      }
    });
  }, []);

  return (
    <div className="dag-shell">
      <Toolbar
        query={filter.query}
        matchCount={queryMatches.size}
        counts={counts}
        allowedStates={filter.allowedStates}
        onlyFailedPath={filter.onlyFailedPath}
        direction={direction}
        onQueryChange={onQueryChange}
        onToggleState={onToggleState}
        onToggleFailedPath={onToggleFailedPath}
        onRelayout={handleRelayout}
        onFitView={handleFitView}
        onCenterRunning={handleCenterRunning}
        onToggleDirection={handleToggleDirection}
      />
      <div className="dag-body">
        <div className="dag-graph">
          <ReactFlow
            key={flowKey}
            nodes={rfNodes}
            edges={rfEdges}
            nodeTypes={{ taskNode: TaskNode, chordNode: ChordJoinNode, groupNode: GroupNode }}
            edgeTypes={{ selfLoop: SelfLoopEdge }}
            nodesDraggable
            nodesConnectable={false}
            panOnScroll
            zoomOnScroll
            onInit={(instance) => {
              instanceRef.current = instance;
            }}
            onNodeClick={handleNodeClick}
            onMoveEnd={handleMoveEnd}
            onNodeMouseEnter={onNodeMouseEnter}
            onNodeMouseLeave={onNodeMouseLeave}
            onNodesChange={onNodesChange}
          >
            <Controls />
            <Background gap={24} size={1} />
          </ReactFlow>
          {filteredModel.nodes.size === 0 ? (
            <div className="dag-empty">
              {graphModel.nodes.size === 0
                ? "No graph data available."
                : "No nodes match the current filters."}
            </div>
          ) : null}
          {isLayouting ? <div className="dag-layout-banner">Relayouting…</div> : null}
        </div>
        <DetailsPanel
          node={selectedNodeData}
          childrenIds={childrenIds}
          onClose={() => setSelectedId(null)}
          taskDetailUrlTemplate={options?.taskDetailUrlTemplate}
        />
      </div>
      <Legend />
    </div>
  );
}

export default function GraphApp(props: GraphAppProps) {
  return (
    <ReactFlowProvider>
      <GraphCanvas {...props} />
    </ReactFlowProvider>
  );
}
