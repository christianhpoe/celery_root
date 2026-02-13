// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

import clsx from "clsx";

import type { TaskNodeData } from "../graph/transforms";

interface DetailsPanelProps {
  node: TaskNodeData | null;
  childrenIds: string[];
  onClose: () => void;
  taskDetailUrlTemplate?: string;
}

function buildTaskUrl(template: string | undefined, id: string): string | null {
  if (!template) {
    return null;
  }
  const encoded = encodeURIComponent(id);
  return template
    .replace("TASK_ID_PLACEHOLDER", encoded)
    .replace("{task_id}", encoded);
}

function PreviewBlock({ label, value }: { label: string; value: string | null }) {
  return (
    <div className="dag-panel-block">
      <div className="dag-panel-label">{label}</div>
      <pre className="dag-panel-pre">{value ?? "—"}</pre>
    </div>
  );
}

export default function DetailsPanel({ node, childrenIds, onClose, taskDetailUrlTemplate }: DetailsPanelProps) {
  if (!node) {
    return (
      <aside className="dag-panel is-empty">
        <div className="dag-panel-header">
          <h3>Task details</h3>
        </div>
        <div className="dag-panel-body">Select a task to see details.</div>
      </aside>
    );
  }

  const effectiveId = node.foldedLatestId ?? node.id;
  const showRootId = node.foldedLatestId !== null && node.foldedLatestId !== node.id;
  const url = buildTaskUrl(taskDetailUrlTemplate, effectiveId);

  return (
    <aside className={clsx("dag-panel", node.state ? `state-${node.state.toLowerCase()}` : "")}>
      <div className="dag-panel-header">
        <div>
          <p className="dag-panel-kicker">Task</p>
          <h3>{node.title}</h3>
        </div>
        <button type="button" className="dag-panel-close" onClick={onClose}>
          Close
        </button>
      </div>
      <div className="dag-panel-body">
        <div className="dag-panel-row">
          <span className="dag-panel-label">ID</span>
          <span className="dag-panel-value">{effectiveId}</span>
        </div>
        {showRootId ? (
          <div className="dag-panel-row">
            <span className="dag-panel-label">Root ID</span>
            <span className="dag-panel-value">{node.id}</span>
          </div>
        ) : null}
        <div className="dag-panel-row">
          <span className="dag-panel-label">State</span>
          <span className="dag-panel-value">{node.state ?? "—"}</span>
        </div>
        <div className="dag-panel-row">
          <span className="dag-panel-label">Kind</span>
          <span className="dag-panel-value">{node.kind ?? "—"}</span>
        </div>
        <div className="dag-panel-row">
          <span className="dag-panel-label">Started</span>
          <span className="dag-panel-value">{node.startedAt ?? "—"}</span>
        </div>
        <div className="dag-panel-row">
          <span className="dag-panel-label">Finished</span>
          <span className="dag-panel-value">{node.finishedAt ?? "—"}</span>
        </div>
        <div className="dag-panel-row">
          <span className="dag-panel-label">Duration</span>
          <span className="dag-panel-value">{node.durationMs ? `${node.durationMs}ms` : "—"}</span>
        </div>
        <div className="dag-panel-row">
          <span className="dag-panel-label">Retries</span>
          <span className="dag-panel-value">{node.retries ?? 0}</span>
        </div>
        <div className="dag-panel-row">
          <span className="dag-panel-label">Parent</span>
          <span className="dag-panel-value">{node.parentId ?? "—"}</span>
        </div>
        <div className="dag-panel-row">
          <span className="dag-panel-label">Children</span>
          <span className="dag-panel-value">
            {childrenIds.length > 0 ? childrenIds.join(", ") : "—"}
          </span>
        </div>
        {url ? (
          <a className="dag-panel-link" href={url}>
            Open task detail
          </a>
        ) : null}
        <PreviewBlock label="Args" value={node.argsPreview} />
        <PreviewBlock label="Kwargs" value={node.kwargsPreview} />
        {node.stamps.length > 0 ? (
          <PreviewBlock label="Stamps" value={node.stamps.join("\n")} />
        ) : null}
        <PreviewBlock label="Result" value={node.resultPreview} />
        <PreviewBlock label="Traceback" value={node.tracebackPreview} />
      </div>
    </aside>
  );
}
