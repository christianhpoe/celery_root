export type TaskState =
  | "PENDING"
  | "RECEIVED"
  | "STARTED"
  | "RETRY"
  | "SUCCESS"
  | "FAILURE"
  | "REVOKED";

export interface GraphMetaCounts {
  total: number;
  pending: number;
  running: number;
  retry: number;
  success: number;
  failure: number;
  revoked: number;
}

export interface GraphMeta {
  root_id: string | null;
  generated_at: string | null;
  counts: GraphMetaCounts;
}

export interface GraphNodePayload {
  id: string;
  task_name: string | null;
  state: TaskState | null;
  started_at: string | null;
  finished_at: string | null;
  duration_ms: number | null;
  retries: number | null;
  queue: string | null;
  worker: string | null;
  args_preview: string | null;
  kwargs_preview: string | null;
  result_preview: string | null;
  traceback_preview: string | null;
  stamps_preview?: string | null;
  parent_id: string | null;
  root_id: string | null;
  kind?: string | null;
  folded_latest_id?: string | null;
}

export interface GraphEdgePayload {
  id: string;
  source: string;
  target: string;
  kind?: "chain" | "group" | "chord" | "map" | "starmap" | "chunks" | "link_error" | "retry" | string;
  label?: string;
}

export interface GraphPayload {
  meta: GraphMeta;
  nodes: GraphNodePayload[];
  edges: GraphEdgePayload[];
}

export interface GraphOptions {
  refreshUrl?: string;
  snapshotUrl?: string;
  taskDetailUrlTemplate?: string;
  theme?: "light" | "dark";
}

export interface GraphUpdatePayload {
  generated_at: string | null;
  node_updates: Array<Partial<GraphNodePayload> & { id: string }>;
  meta_counts: GraphMetaCounts;
  topology_changed: boolean;
  node_count: number;
  edge_count: number;
}
