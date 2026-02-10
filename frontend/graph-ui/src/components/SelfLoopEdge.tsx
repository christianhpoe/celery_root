import type { EdgeProps } from "@xyflow/react";
import { BaseEdge, EdgeLabelRenderer, Position } from "@xyflow/react";

import { NODE_HEIGHT, NODE_WIDTH } from "../graph/transforms";

const LOOP_WIDTH = 44;
const LOOP_HEIGHT = 18;
const ANCHOR_OFFSET = 10;
const LABEL_OFFSET_X = 10;
const LABEL_OFFSET_Y = -4;

function anchorForLoop(
  sourceX: number,
  sourceY: number,
  sourcePosition: Position | undefined,
): { x: number; y: number } {
  if (sourcePosition === Position.Bottom) {
    return {
      x: sourceX + NODE_WIDTH / 2 + ANCHOR_OFFSET,
      y: sourceY - NODE_HEIGHT - ANCHOR_OFFSET,
    };
  }
  return {
    x: sourceX + ANCHOR_OFFSET,
    y: sourceY - NODE_HEIGHT / 2 - ANCHOR_OFFSET,
  };
}

export default function SelfLoopEdge({
  id,
  sourceX,
  sourceY,
  sourcePosition,
  label,
  markerEnd,
  style,
}: EdgeProps) {
  const anchor = anchorForLoop(sourceX, sourceY, sourcePosition);
  const controlX = anchor.x + LOOP_WIDTH;
  const controlTop = anchor.y - LOOP_HEIGHT;
  const controlBottom = anchor.y + LOOP_HEIGHT * 0.4;
  const path = `M ${anchor.x} ${anchor.y} C ${controlX} ${controlTop}, ${controlX} ${controlBottom}, ${anchor.x} ${anchor.y}`;
  const labelX = controlX + LABEL_OFFSET_X;
  const labelY = controlTop + LABEL_OFFSET_Y;

  return (
    <>
      <BaseEdge id={id} path={path} markerEnd={markerEnd} style={style} />
      {label ? (
        <EdgeLabelRenderer>
          <div
            className="dag-edge-loop-label nodrag nopan"
            style={{
              position: "absolute",
              top: 0,
              left: 0,
              transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`,
            }}
          >
            {String(label)}
          </div>
        </EdgeLabelRenderer>
      ) : null}
    </>
  );
}
