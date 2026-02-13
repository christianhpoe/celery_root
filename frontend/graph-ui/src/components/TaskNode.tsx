// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

import { memo } from "react";
import { Handle, Position, type NodeProps } from "@xyflow/react";
import clsx from "clsx";

import type { TaskNodeData } from "../graph/transforms";
import { STATE_LABELS } from "../graph/stateColors";

function formatDuration(durationMs: number | null): string {
  if (!durationMs || durationMs <= 0) {
    return "—";
  }
  if (durationMs < 1000) {
    return `${Math.round(durationMs)}ms`;
  }
  const seconds = durationMs / 1000;
  if (seconds < 60) {
    return `${seconds.toFixed(1)}s`;
  }
  const minutes = Math.floor(seconds / 60);
  const remainder = seconds % 60;
  return `${minutes}m ${Math.round(remainder)}s`;
}

function shortId(value: string): string {
  if (!value) {
    return "—";
  }
  return value.length > 8 ? value.slice(0, 8) : value;
}

function TaskNode({ data, selected }: NodeProps<TaskNodeData>) {
  const state = data.state ?? "PENDING";
  const stateLabel = data.state ? STATE_LABELS[state] : data.kind ?? "Pending";
  const retryCount = data.retries ?? 0;
  const showMeta = data.showMeta;
  const kindLabel = data.kind ? data.kind.toUpperCase() : null;
  const stampCount = data.stamps.length;
  const displayId = data.foldedLatestId ?? data.id;

  const handlePosition = data.handleDirection === "DOWN" ? Position.Top : Position.Left;
  const handleSourcePosition = data.handleDirection === "DOWN" ? Position.Bottom : Position.Right;

  return (
    <div
      className={clsx("dag-node", `state-${state.toLowerCase()}`, {
        "is-selected": selected,
        "is-virtual": data.isVirtual,
      })}
    >
      <Handle type="target" position={handlePosition} className="dag-handle" />
      <Handle type="source" position={handleSourcePosition} className="dag-handle" />
      <div className="dag-node-header">
        <div className="dag-node-title" title={data.title}>
          {data.title}
        </div>
        <div className={clsx("dag-node-pill", `pill-${state.toLowerCase()}`)}>
          {stateLabel}
        </div>
      </div>
      {kindLabel || stampCount > 0 ? (
        <div className="dag-node-tags">
          {kindLabel ? <div className="dag-node-pill pill-kind">{kindLabel}</div> : null}
          {stampCount > 0 ? (
            <div className="dag-node-pill pill-stamp">{`STAMPED ${stampCount}`}</div>
          ) : null}
        </div>
      ) : null}
      <div className="dag-node-meta">
        <div className="dag-node-meta-item">
          <span className="dag-node-meta-label">Duration</span>
          <span>{formatDuration(data.durationMs)}</span>
        </div>
        {retryCount > 0 ? (
          <div className="dag-node-meta-item">
            <span className="dag-node-meta-label">Retries</span>
            <span>{retryCount}</span>
          </div>
        ) : null}
      </div>
      {showMeta ? (
        <div className="dag-node-foot">
          <span>{shortId(displayId)}</span>
          <span>{data.worker ?? "—"}</span>
        </div>
      ) : (
        <div className="dag-node-foot is-muted">Zoom in to see more</div>
      )}
    </div>
  );
}

export default memo(TaskNode);
