# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Result backend utilities for Root operations."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from fnmatch import fnmatch
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from celery import Celery

    from celery_root.core.registry import WorkerRegistry

__all__ = ["StoredResult", "clear_results", "list_results"]


@dataclass(slots=True)
class StoredResult:
    """Representation of a task result stored in the backend."""

    task_id: str
    status: str | None
    result: object
    traceback: str | None
    name: str | None
    date_done: object | None


def _get_app(registry: WorkerRegistry, worker: str) -> Celery:
    return registry.get_app(worker)


def list_results(
    registry: WorkerRegistry,
    worker: str,
    *,
    key_pattern: str = "celery-task-meta-*",
    limit: int | None = 100,
) -> list[StoredResult]:
    """List stored backend results (best effort; Redis-focused)."""
    app = _get_app(registry, worker)
    backend = app.backend
    get_task_meta = getattr(backend, "get_task_meta", None)
    if not callable(get_task_meta):
        return []
    results: list[StoredResult] = []
    for idx, key in enumerate(_iter_keys(backend, key_pattern)):
        if limit is not None and idx >= limit:
            break
        task_id = _task_id_from_key(backend, key)
        meta = get_task_meta(task_id)
        results.append(
            StoredResult(
                task_id=task_id,
                status=str(meta.get("status")) if isinstance(meta, dict) else None,
                result=meta.get("result") if isinstance(meta, dict) else None,
                traceback=meta.get("traceback") if isinstance(meta, dict) else None,
                name=_extract_name(meta),
                date_done=meta.get("date_done") if isinstance(meta, dict) else None,
            ),
        )
    return results


def clear_results(
    registry: WorkerRegistry,
    worker: str,
    *,
    task_id: str | None = None,
    name_pattern: str | None = None,
    key_pattern: str = "celery-task-meta-*",
) -> int:
    """Clear backend results by task id, name pattern, or all."""
    app = _get_app(registry, worker)
    backend = app.backend
    get_task_meta = getattr(backend, "get_task_meta", None)
    cleared = 0
    if task_id is not None:
        get_key_for_task = getattr(backend, "get_key_for_task", None)
        if callable(get_key_for_task):
            key = str(get_key_for_task(task_id))
        else:
            prefix = str(getattr(backend, "meta_key_prefix", "celery-task-meta-"))
            key = f"{prefix}{task_id}"
        cleared += _delete_keys(backend, [key])
        backend.forget(task_id)
        return cleared

    keys_to_delete: list[str] = []
    for key in _iter_keys(backend, key_pattern):
        tid = _task_id_from_key(backend, key)
        if name_pattern is None:
            keys_to_delete.append(key)
            continue
        meta = get_task_meta(tid) if callable(get_task_meta) else {}
        name = _extract_name(meta)
        if name is not None and fnmatch(name, name_pattern):
            keys_to_delete.append(key)
    cleared += _delete_keys(backend, keys_to_delete)
    return cleared


def _iter_keys(backend: object, pattern: str) -> list[str]:
    client = getattr(backend, "client", None)
    keys: list[str] = []
    if client is not None:
        scanner = getattr(client, "scan_iter", None)
        if scanner is not None:
            keys = [str(k.decode() if isinstance(k, bytes) else k) for k in scanner(match=pattern)]
    return keys


def _delete_keys(backend: object, keys: list[str]) -> int:
    if not keys:
        return 0
    client = getattr(backend, "client", None)
    if client is None:
        return 0
    deleted = client.delete(*keys)
    try:
        return int(deleted)
    except (TypeError, ValueError):
        return 0


def _task_id_from_key(backend: object, key: str) -> str:
    prefix = "celery-task-meta-"
    if key.startswith(prefix):
        return key.removeprefix(prefix)
    if hasattr(backend, "meta_key_prefix"):
        prefix = str(backend.meta_key_prefix)
        if key.startswith(prefix):
            return key[len(prefix) :]
    return key


def _extract_name(meta: object) -> str | None:
    if not isinstance(meta, Mapping):
        return None
    name = meta.get("name") or meta.get("task") or meta.get("task_name")
    return str(name) if name is not None else None
