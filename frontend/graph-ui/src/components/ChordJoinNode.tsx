// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

import { memo } from "react";
import { Handle, Position, type NodeProps } from "@xyflow/react";
import clsx from "clsx";

import type { TaskNodeData } from "../graph/transforms";

function ChordJoinNode({ data, selected }: NodeProps<TaskNodeData>) {
  const handlePosition = data.handleDirection === "DOWN" ? Position.Top : Position.Left;
  const handleSourcePosition = data.handleDirection === "DOWN" ? Position.Bottom : Position.Right;

  return (
    <div
      className={clsx("dag-node", "dag-node-chord", {
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
        <div className="dag-node-pill pill-chord">Chord</div>
      </div>
      <div className="dag-node-meta">
        <div className="dag-node-meta-item">
          <span className="dag-node-meta-label">Join</span>
          <span>Waiting</span>
        </div>
        <div className="dag-node-meta-item">
          <span className="dag-node-meta-label">Callback</span>
          <span>{data.taskName ?? "—"}</span>
        </div>
      </div>
      <div className="dag-node-foot is-muted">Partial chord linkage</div>
    </div>
  );
}

export default memo(ChordJoinNode);
