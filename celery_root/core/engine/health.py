# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Health checks for broker, backend, and workers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, cast

if TYPE_CHECKING:
    from celery import Celery

    from celery_root.core.registry import WorkerRegistry

__all__ = ["health_check"]


class _Connection(Protocol):
    def connect(self) -> None: ...
    def release(self) -> None: ...


def health_check(
    registry: WorkerRegistry,
    worker: str,
    *,
    connection: _Connection | None = None,
    timeout: float = 1.0,
) -> dict[str, object]:
    """Run lightweight broker/backend/worker pings for a single worker app."""
    app = registry.get_app(worker)
    broker_status = _check_broker(app, connection=connection)
    backend_status = _check_backend(app)
    worker_status = _check_workers(app, timeout=timeout)

    return {
        "broker": broker_status,
        "backend": backend_status,
        "workers": worker_status,
    }


def _check_broker(app: Celery, *, connection: _Connection | None) -> dict[str, object]:
    try:
        if connection is not None:
            connection.connect()
            connection.release()
        else:
            with app.connection_or_acquire() as conn:
                cast("_Connection", conn).connect()
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:  # pragma: no cover
        return {"ok": False, "error": str(exc)}
    else:
        return {"ok": True, "error": None}


def _check_backend(app: Celery) -> dict[str, object]:
    backend = app.backend
    get_task_meta = getattr(backend, "get_task_meta", None)
    if not callable(get_task_meta):
        return {"ok": False, "error": "Backend does not support get_task_meta"}
    try:
        get_task_meta("celery_root_healthcheck")
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:  # pragma: no cover
        return {"ok": False, "error": str(exc)}
    return {"ok": True, "error": None}


def _check_workers(app: Celery, *, timeout: float) -> dict[str, object]:
    try:
        responses = app.control.ping(timeout=timeout) or []
        responding = [next(iter(item)) for item in responses if isinstance(item, dict) and item]
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:  # pragma: no cover
        return {"ok": False, "responding": [], "error": str(exc)}
    else:
        return {"ok": bool(responding), "responding": responding, "error": None}
