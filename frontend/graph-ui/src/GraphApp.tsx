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
  buildQueryMatches,
  buildReactFlowEdges,
  buildReactFlowNodes,
  buildRunningEdges,
  computeCounts,
  filterGraph,
  type FilterState,
  type GraphFlowNode,
  type GraphModel,
} from "./graph/transforms";
import type { GraphOptions, GraphPayload, GraphUpdatePayload, TaskState } from "./graph/types";

interface GraphAppProps {
  payload: GraphPayload;
  options?: GraphOptions;
}

const ACTIVE_STATES: TaskState[] = ["STARTED", "RETRY", "PENDING", "RECEIVED"];

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
  const manualPositionsRef = useRef<Map<string, { x: number; y: number }>>(new Map());
  const lastLayoutKeyRef = useRef<string | null>(null);
  const hasFitRef = useRef(false);
  const lastUpdateRef = useRef<string | null>(payload.meta?.generated_at ?? null);
  const completedAtRef = useRef<number | null>(null);
  const [layoutPositions, setLayoutPositions] = useState<Map<string, { x: number; y: number }> | null>(null);

  const totalNodes = graphModel.nodes.size;
  const counts = useMemo(() => computeCounts(graphModel.nodes.values()), [graphModel.nodes]);
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
  const queryMatches = useMemo(
    () => buildQueryMatches(graphModel.nodes.values(), filter.query),
    [graphModel.nodes, filter.query],
  );
  const highlight = useMemo(
    () => buildHighlight(filteredModel.edges, hoveredId),
    [filteredModel.edges, hoveredId],
  );
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
    () => buildRunningEdges(filteredModel.edges, runningIds),
    [filteredModel.edges, runningIds],
  );

  const baseNodes = useMemo(
    () =>
      buildReactFlowNodes(filteredModel.nodes.values(), showMeta, highlight.nodes, queryMatches, direction),
    [filteredModel.nodes, showMeta, highlight.nodes, queryMatches, direction],
  );
  baseNodesRef.current = baseNodes;
  layoutNodesRef.current = filteredModel.nodes;
  const baseEdges = useMemo(
    () => buildReactFlowEdges(filteredModel.edges, highlight.edges, runningEdges, disableAnimations),
    [filteredModel.edges, highlight.edges, runningEdges, disableAnimations],
  );
  baseEdgesRef.current = baseEdges;
  const topologyKey = useMemo(() => {
    const nodeIds = Array.from(filteredModel.nodes.keys()).sort().join("|");
    const edgeIds = filteredModel.edges
      .map((edge) => edge.id || `${edge.source}->${edge.target}:${edge.kind ?? "chain"}`)
      .sort()
      .join("|");
    return `${nodeIds}::${edgeIds}`;
  }, [filteredModel.nodes, filteredModel.edges]);
  useEffect(() => {
    let active = true;
    const shouldRelayout = lastLayoutKeyRef.current !== `${topologyKey}:${direction}`;
    if (shouldRelayout) {
      setIsLayouting(true);
      lastLayoutKeyRef.current = `${topologyKey}:${direction}`;
    }
    const layoutInput = buildReactFlowNodes(
      layoutNodesRef.current.values(),
      false,
      new Set(),
      new Set(),
      direction,
    );
    const layoutNodeIds = new Set(layoutInput.map((node) => node.id));
    const layoutEdges = filteredModel.edges
      .filter(
        (edge) =>
          layoutNodeIds.has(edge.source) &&
          layoutNodeIds.has(edge.target) &&
          edge.source !== edge.target,
      )
      .map((edge) => ({ source: edge.source, target: edge.target }));
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
  }, [direction, topologyKey, filteredModel.edges]);

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
    if (!options?.refreshUrl) {
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
            setGraphModel(buildGraphModel(snapshot));
            completedAtRef.current = null;
            requestAnimationFrame(() => {
              instanceRef.current?.fitView({ padding: 0.2 });
            });
          }
        }
        return;
      }
      if (payload.node_updates.length === 0) {
        return;
      }
      if (
        payload.node_count !== graphModel.nodes.size ||
        payload.edge_count !== graphModel.edges.length
      ) {
        const snapshotUrl = deriveSnapshotUrl(options);
        if (snapshotUrl) {
          const snapshotResponse = await fetch(snapshotUrl, { headers: { Accept: "application/json" } });
          if (snapshotResponse.ok) {
            const snapshot = (await snapshotResponse.json()) as GraphPayload;
            setGraphModel(buildGraphModel(snapshot));
            completedAtRef.current = null;
            requestAnimationFrame(() => {
              instanceRef.current?.fitView({ padding: 0.2 });
            });
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
  }, [options]);

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
    ? buildReactFlowNodes([selectedNode].values(), true, new Set(), new Set(), direction)[0].data
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
    setSelectedId(node.id);
  }, []);

  const handleMove = useCallback((_event: unknown, viewport: { zoom: number }) => {
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
            nodes={rfNodes}
            edges={rfEdges}
            nodeTypes={{ taskNode: TaskNode, chordNode: ChordJoinNode, groupNode: GroupNode }}
            edgeTypes={{ selfLoop: SelfLoopEdge }}
            fitView
            nodesDraggable
            nodesConnectable={false}
            panOnScroll
            zoomOnScroll
            onInit={(instance) => {
              instanceRef.current = instance;
            }}
            onNodeClick={handleNodeClick}
            onMove={handleMove}
            onNodeMouseEnter={onNodeMouseEnter}
            onNodeMouseLeave={onNodeMouseLeave}
            onNodesChange={onNodesChange}
          >
            <Controls />
            <Background gap={24} size={1} />
          </ReactFlow>
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
