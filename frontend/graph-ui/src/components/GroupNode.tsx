// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

import { memo } from "react";
import type { NodeProps } from "@xyflow/react";
import clsx from "clsx";

import type { GroupNodeData } from "../graph/transforms";

function GroupNode({ data, selected }: NodeProps<GroupNodeData>) {
  return (
    <div className={clsx("dag-group", { "is-selected": selected })}>
      <div className="dag-group-header">
        <div className="dag-group-title">{data.label}</div>
        <div className="dag-group-count">{data.count} tasks</div>
      </div>
    </div>
  );
}

export default memo(GroupNode);
