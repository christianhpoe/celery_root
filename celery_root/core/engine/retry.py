"""Helpers for retrying failed task graphs."""

from __future__ import annotations

import json
from collections import defaultdict, deque
from typing import TYPE_CHECKING, Protocol

from . import tasks as root_tasks

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from datetime import datetime

    from celery_root.core.db.adapters.base import BaseDBController
    from celery_root.core.db.models import TaskRelation
    from celery_root.core.registry import WorkerRegistry

__all__ = ["smart_retry"]


class _TaskSender(Protocol):
    def __call__(  # noqa: PLR0913
        self,
        registry: WorkerRegistry,
        worker: str,
        name: str,
        *,
        args: Sequence[object] | None,
        kwargs: Mapping[str, object] | None,
        countdown: float | None,
        eta: datetime | None,
    ) -> object: ...


def smart_retry(
    registry: WorkerRegistry,
    db: BaseDBController,
    failed_task_id: str,
    *,
    sender: _TaskSender | None = None,
) -> list[object]:
    """Retry a failed task and its downstream relations.

    The DB controller is used to reconstruct the relation graph. All descendants of the
    failed task (inclusive) are re-sent in a breadth-first order so parents go before
    their children.
    """
    task = db.get_task(failed_task_id)
    if task is None:
        message = f"Task {failed_task_id} not found"
        raise ValueError(message)

    root_id = task.root_id or task.task_id
    relations = db.get_task_relations(root_id)
    adjacency = _build_adjacency(relations)

    order = _descendants_bfs(start=failed_task_id, adjacency=adjacency)
    results: list[object] = []
    send = sender or _send_via_celery

    for task_id in order:
        current = db.get_task(task_id)
        if current is None or current.name is None or current.worker is None:
            continue
        args, kwargs = _parse_args_kwargs(current.args, current.kwargs)
        result = send(
            registry,
            current.worker,
            current.name,
            args=args,
            kwargs=kwargs,
            countdown=None,
            eta=None,
        )
        results.append(result)
    return results


def _build_adjacency(relations: Sequence[TaskRelation]) -> dict[str, list[str]]:
    adjacency: dict[str, list[str]] = defaultdict(list)
    for relation in relations:
        if relation.parent_id is not None:
            adjacency[relation.parent_id].append(relation.child_id)
    return adjacency


def _descendants_bfs(start: str, adjacency: Mapping[str, list[str]]) -> list[str]:
    visited: set[str] = set()
    queue: deque[str] = deque([start])
    order: list[str] = []
    while queue:
        node = queue.popleft()
        if node in visited:
            continue
        visited.add(node)
        order.append(node)
        for child in adjacency.get(node, []):
            if child not in visited:
                queue.append(child)
    return order


def _parse_args_kwargs(args: str | None, kwargs: str | None) -> tuple[tuple[object, ...], dict[str, object]]:
    return _parse_args(args), _parse_kwargs(kwargs)


def _parse_args(value: str | None) -> tuple[object, ...]:
    if not value:
        return ()
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return (value,)
    if isinstance(parsed, list):
        return tuple(parsed)
    return (parsed,)


def _parse_kwargs(value: str | None) -> dict[str, object]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return {}
    if isinstance(parsed, dict):
        return dict(parsed)
    return {}


def _send_via_celery(  # noqa: PLR0913
    registry: WorkerRegistry,
    worker: str,
    name: str,
    *,
    args: Sequence[object] | None,
    kwargs: Mapping[str, object] | None,
    countdown: float | None,
    eta: datetime | None,
) -> object:
    return root_tasks.send_task(
        registry,
        worker,
        name,
        args=args or (),
        kwargs=kwargs or {},
        countdown=countdown,
        eta=eta,
    )
