# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Task graph views."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, TypedDict

from django.http import Http404, JsonResponse
from django.shortcuts import render
from django.urls import reverse
from django.utils import timezone

from celery_root.components.web.services import open_db
from celery_root.core.db.models import TaskFilter

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime

    from django.http import HttpRequest, HttpResponse

    from celery_root.core.db import DbClient
    from celery_root.core.db.models import Task, TaskRelation

_TASK_NOT_FOUND = "Task not found"
_PREVIEW_LIMIT = 256
_CANVAS_TASK_KIND: dict[str, str] = {
    "celery.chain": "chain",
    "celery.group": "group",
    "celery.chord": "chord",
    "celery.chord_unlock": "chord",
    "celery.map": "map",
    "celery.starmap": "starmap",
    "celery.chunks": "chunks",
}


type BucketKey = Literal[
    "total",
    "pending",
    "running",
    "retry",
    "success",
    "failure",
    "revoked",
]
type StateBucket = Literal[
    "pending",
    "running",
    "retry",
    "success",
    "failure",
    "revoked",
]
type GraphCounts = dict[BucketKey, int]


class GraphMeta(TypedDict):
    """Metadata for a task graph payload."""

    root_id: str
    generated_at: str
    counts: GraphCounts


class GraphNode(TypedDict):
    """Serialized node payload for a task graph."""

    id: str
    task_name: str | None
    state: str | None
    started_at: str | None
    finished_at: str | None
    duration_ms: int | None
    retries: int | None
    queue: None
    worker: str | None
    args_preview: str | None
    kwargs_preview: str | None
    result_preview: str | None
    traceback_preview: str | None
    stamps_preview: str | None
    parent_id: str | None
    root_id: str
    kind: str | None


class GraphEdge(TypedDict):
    """Serialized edge payload for a task graph."""

    id: str
    source: str
    target: str
    kind: str


class GraphPayload(TypedDict):
    """Top-level task graph payload."""

    meta: GraphMeta
    nodes: list[GraphNode]
    edges: list[GraphEdge]


def _build_edges(relations: Sequence[TaskRelation]) -> list[dict[str, str | None]]:
    if not relations:
        return []
    edges: list[dict[str, str | None]] = []
    seen: set[tuple[str | None, str | None, str]] = set()
    for relation in relations:
        parent = relation.parent_id or relation.root_id
        relation_kind = relation.relation
        if relation_kind == "parent":
            relation_kind = "chain"
        key = (parent, relation.child_id, relation_kind)
        if key in seen:
            continue
        seen.add(key)
        edges.append(
            {
                "parent": parent,
                "child": relation.child_id,
                "relation": relation_kind,
            },
        )
    return edges


def _node_kind_map(edges: Sequence[dict[str, str | None]]) -> dict[str, str]:
    kinds: dict[str, str] = {}
    for edge in edges:
        parent = edge.get("parent")
        relation = edge.get("relation")
        if parent and relation in {"group", "chord", "map", "starmap", "chunks"}:
            kinds[parent] = relation
    return kinds


def _format_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _truncate(value: str | None, limit: int = _PREVIEW_LIMIT) -> str | None:
    if value is None:
        return None
    if len(value) <= limit:
        return value
    return f"{value[:limit]}..."


def _duration_ms(task: Task | None) -> int | None:
    if task is None:
        return None
    if task.runtime is not None:
        return round(task.runtime * 1000)
    if task.started and task.finished:
        return int((task.finished - task.started).total_seconds() * 1000)
    return None


def _serialize_node(task: Task | None, node_id: str, kind: str | None, root_id: str) -> GraphNode:
    return {
        "id": node_id,
        "task_name": task.name if task is not None else None,
        "state": task.state if task is not None else None,
        "started_at": _format_timestamp(task.started) if task is not None else None,
        "finished_at": _format_timestamp(task.finished) if task is not None else None,
        "duration_ms": _duration_ms(task),
        "retries": task.retries if task is not None else None,
        "queue": None,
        "worker": task.worker if task is not None else None,
        "args_preview": _truncate(task.args) if task is not None else None,
        "kwargs_preview": _truncate(task.kwargs_) if task is not None else None,
        "result_preview": _truncate(task.result) if task is not None else None,
        "traceback_preview": _truncate(task.traceback) if task is not None else None,
        "stamps_preview": _truncate(task.stamps) if task is not None else None,
        "parent_id": task.parent_id if task is not None else None,
        "root_id": (task.root_id or root_id) if task is not None else root_id,
        "kind": kind,
    }


def _serialize_edge(edge: dict[str, str | None]) -> GraphEdge | None:
    source = edge.get("parent")
    target = edge.get("child")
    if not source or not target:
        return None
    kind = edge.get("relation") or "chain"
    if kind not in {"chain", "group", "chord", "link_error", "retry", "map", "starmap", "chunks"}:
        kind = "chain"
    return {
        "id": f"{source}->{target}:{kind}",
        "source": source,
        "target": target,
        "kind": kind,
    }


def _canvas_kind(task: Task | None) -> str | None:
    if task is None or not task.name:
        return None
    return _CANVAS_TASK_KIND.get(task.name)


def _append_edge(
    edges: list[dict[str, str | None]],
    edge_keys: set[tuple[str, str, str]],
    *,
    parent: str | None,
    child: str | None,
    relation: str,
) -> None:
    if not parent or not child:
        return
    key = (parent, child, relation)
    if key in edge_keys:
        return
    edge_keys.add(key)
    edges.append({"parent": parent, "child": child, "relation": relation})


def _collect_nodes_and_edges(
    root_id: str,
    relations: Sequence[TaskRelation],
    db: DbClient,
) -> tuple[set[str], list[dict[str, str | None]]]:
    node_ids: set[str] = {root_id}
    edges = _build_edges(relations)
    edge_keys: set[tuple[str, str, str]] = set()
    for edge in edges:
        parent = edge.get("parent")
        child = edge.get("child")
        relation = edge.get("relation") or "chain"
        if parent and child:
            edge_keys.add((parent, child, relation))
        if parent:
            node_ids.add(parent)
        if child:
            node_ids.add(child)

    tasks = list(db.get_tasks(TaskFilter(root_id=root_id)))
    for item in tasks:
        node_ids.add(item.task_id)
        if item.parent_id:
            _append_edge(edges, edge_keys, parent=item.parent_id, child=item.task_id, relation="chain")
            node_ids.add(item.parent_id)
        if item.group_id:
            _append_edge(edges, edge_keys, parent=item.group_id, child=item.task_id, relation="group")
            node_ids.add(item.group_id)
        if item.chord_id:
            _append_edge(edges, edge_keys, parent=item.chord_id, child=item.task_id, relation="chord")
            node_ids.add(item.chord_id)
    return node_ids, edges


def _build_state_counts(nodes_payload: Sequence[GraphNode]) -> GraphCounts:
    counts: GraphCounts = {
        "total": 0,
        "pending": 0,
        "running": 0,
        "retry": 0,
        "success": 0,
        "failure": 0,
        "revoked": 0,
    }
    state_to_bucket: dict[str, StateBucket] = {
        "PENDING": "pending",
        "RECEIVED": "pending",
        "STARTED": "running",
        "RETRY": "retry",
        "SUCCESS": "success",
        "FAILURE": "failure",
        "REVOKED": "revoked",
    }
    for node in nodes_payload:
        state = node.get("state")
        if not state:
            continue
        counts["total"] += 1
        bucket = state_to_bucket.get(state)
        if bucket:
            counts[bucket] += 1
    return counts


def _build_graph_payload(task_id: str, db: DbClient) -> GraphPayload:
    task = db.get_task(task_id)
    if task is None:
        raise Http404(_TASK_NOT_FOUND)
    root_id = task.root_id or task.task_id
    relations = db.get_task_relations(root_id)
    node_ids, edges = _collect_nodes_and_edges(root_id, relations, db)
    kind_map = _node_kind_map(edges)
    nodes_payload: list[GraphNode] = []
    for node_id in sorted(node_ids):
        node_task = db.get_task(node_id)
        node_kind = _canvas_kind(node_task) or kind_map.get(node_id)
        nodes_payload.append(_serialize_node(node_task, node_id, node_kind, root_id))
    edges_payload = [edge for edge in (_serialize_edge(edge) for edge in edges) if edge is not None]
    meta: GraphMeta = {
        "root_id": root_id,
        "generated_at": timezone.now().isoformat(),
        "counts": _build_state_counts(nodes_payload),
    }
    return {"meta": meta, "nodes": nodes_payload, "edges": edges_payload}


def task_graph(request: HttpRequest, task_id: str) -> HttpResponse:
    """Render a task relation graph based on stored relations."""
    try:
        with open_db() as db:
            payload = _build_graph_payload(task_id, db)
    except Http404:
        return render(
            request,
            "404.html",
            {"title": "Task not found", "path": request.path},
            status=404,
        )
    meta = payload["meta"]
    return render(
        request,
        "tasks/graph.html",
        {
            "title": "Task graph",
            "nodes": payload["nodes"],
            "edges": payload["edges"],
            "meta": meta,
            "task_id": task_id,
            "last_updated": timezone.now(),
            "graph_snapshot_url": reverse("task-graph-api", args=[task_id]),
            "graph_updates_url": reverse("task-graph-updates", args=[task_id]),
            "task_detail_template": reverse("task-detail", args=["TASK_ID_PLACEHOLDER"]),
        },
    )


def task_graph_api(_request: HttpRequest, task_id: str) -> JsonResponse:
    """Return a JSON snapshot of a task graph."""
    with open_db() as db:
        payload = _build_graph_payload(task_id, db)
    return JsonResponse(payload)


def task_graph_updates_api(_request: HttpRequest, task_id: str) -> JsonResponse:
    """Return incremental task graph updates."""
    with open_db() as db:
        payload = _build_graph_payload(task_id, db)
    node_updates = []
    for node in payload["nodes"]:
        update = {
            "id": node["id"],
            "state": node.get("state"),
            "duration_ms": node.get("duration_ms"),
            "retries": node.get("retries"),
            "started_at": node.get("started_at"),
            "finished_at": node.get("finished_at"),
        }
        node_updates.append(update)
    return JsonResponse(
        {
            "generated_at": payload["meta"]["generated_at"],
            "node_updates": node_updates,
            "meta_counts": payload["meta"]["counts"],
            "topology_changed": False,
            "node_count": len(payload["nodes"]),
            "edge_count": len(payload["edges"]),
        },
    )
