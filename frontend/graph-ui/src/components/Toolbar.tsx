// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

import clsx from "clsx";

import type { GraphMetaCounts, TaskState } from "../graph/types";
import { DEFAULT_STATE_ORDER, STATE_LABELS } from "../graph/stateColors";

interface ToolbarProps {
  query: string;
  matchCount: number;
  counts: GraphMetaCounts;
  allowedStates: Set<TaskState>;
  onlyFailedPath: boolean;
  direction: "RIGHT" | "DOWN";
  onQueryChange: (value: string) => void;
  onToggleState: (state: TaskState) => void;
  onToggleFailedPath: () => void;
  onRelayout: () => void;
  onFitView: () => void;
  onCenterRunning: () => void;
  onToggleDirection: () => void;
}

const STATE_ORDER = DEFAULT_STATE_ORDER;

export default function Toolbar(props: ToolbarProps) {
  return (
    <div className="dag-toolbar">
      <div className="dag-toolbar-left">
        <label className="dag-search">
          <span className="dag-search-label">Search</span>
          <input
            type="search"
            placeholder="Task id or name"
            value={props.query}
            onChange={(event) => props.onQueryChange(event.target.value)}
          />
          <span className="dag-search-count">
            {props.query ? `${props.matchCount} match` : ""}
          </span>
        </label>
        <div className="dag-filter-chips">
          {STATE_ORDER.map((state) => (
            <button
              key={state}
              type="button"
              className={clsx("dag-chip", props.allowedStates.has(state) && "is-active")}
              onClick={() => props.onToggleState(state)}
            >
              <span>{STATE_LABELS[state]}</span>
            </button>
          ))}
        </div>
      </div>
      <div className="dag-toolbar-right">
        <div className="dag-counts">
          <span>{props.counts.total} total</span>
          <span>{props.counts.running} running</span>
          <span>{props.counts.failure} failed</span>
        </div>
        <button
          type="button"
          className={clsx("dag-chip", props.onlyFailedPath && "is-active")}
          onClick={props.onToggleFailedPath}
        >
          Only failed path
        </button>
        <button type="button" className="dag-chip" onClick={props.onCenterRunning}>
          Center on running
        </button>
        <button type="button" className="dag-chip" onClick={props.onFitView}>
          Fit view
        </button>
        <button type="button" className="dag-chip" onClick={props.onRelayout}>
          Relayout
        </button>
        <button type="button" className="dag-chip" onClick={props.onToggleDirection}>
          {props.direction === "RIGHT" ? "Left -> Right" : "Top -> Bottom"}
        </button>
      </div>
    </div>
  );
}
