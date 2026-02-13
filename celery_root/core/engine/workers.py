# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Worker control helpers for Root."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from celery import Celery

    from celery_root.core.registry import WorkerRegistry

__all__ = [
    "add_consumer",
    "autoscale",
    "get_stats",
    "pool_grow",
    "pool_shrink",
    "remove_consumer",
    "restart",
    "shutdown",
]


def _get_app(registry: WorkerRegistry, worker: str) -> Celery:
    return registry.get_app(worker)


def _pool_restart_enabled(app: Celery) -> bool:
    conf = getattr(app, "conf", None)
    if conf is None:
        return False
    getter = getattr(conf, "get", None)
    if callable(getter):
        value = getter("worker_pool_restarts")
        if value is not None:
            return bool(value)
    value = getattr(conf, "worker_pool_restarts", None)
    return bool(value)


def _safe_pool_restart(
    restart: Callable[..., list[object] | None],
    *,
    reload: bool,
    destination: Sequence[str] | None,
) -> list[object] | None:
    try:
        return restart(
            reload=reload,
            destination=list(destination) if destination is not None else None,
        )
    except ValueError:
        return None


def pool_grow(
    registry: WorkerRegistry,
    worker: str,
    amount: int = 1,
    *,
    destination: Sequence[str] | None = None,
) -> list[object] | None:
    """Increase worker pool size."""
    app = _get_app(registry, worker)
    return cast(
        "list[object] | None",
        app.control.pool_grow(n=amount, destination=list(destination) if destination is not None else None),
    )


def pool_shrink(
    registry: WorkerRegistry,
    worker: str,
    amount: int = 1,
    *,
    destination: Sequence[str] | None = None,
) -> list[object] | None:
    """Decrease worker pool size."""
    app = _get_app(registry, worker)
    return cast(
        "list[object] | None",
        app.control.pool_shrink(n=amount, destination=list(destination) if destination is not None else None),
    )


def autoscale(
    registry: WorkerRegistry,
    worker: str,
    max_concurrency: int,
    min_concurrency: int = 0,
    *,
    destination: Sequence[str] | None = None,
) -> list[object] | None:
    """Set autoscale limits."""
    app = _get_app(registry, worker)
    return cast(
        "list[object] | None",
        app.control.autoscale(
            max_concurrency,
            min_concurrency,
            destination=list(destination) if destination is not None else None,
        ),
    )


def shutdown(
    registry: WorkerRegistry,
    worker: str,
    *,
    destination: Sequence[str] | None = None,
) -> list[object] | None:
    """Shut down worker process."""
    app = _get_app(registry, worker)
    return cast(
        "list[object] | None",
        app.control.broadcast("shutdown", destination=list(destination) if destination is not None else None),
    )


def restart(
    registry: WorkerRegistry,
    worker: str,
    *,
    reload: bool = False,
    destination: Sequence[str] | None = None,
) -> list[object] | None:
    """Restart worker pool (soft reload optional)."""
    app = _get_app(registry, worker)
    control = app.control
    pool_restart = getattr(control, "pool_restart", None)
    if callable(pool_restart):
        if not _pool_restart_enabled(app):
            return None
        return _safe_pool_restart(
            pool_restart,
            reload=reload,
            destination=list(destination) if destination is not None else None,
        )
    return cast(
        "list[object] | None",
        control.broadcast(
            "restart",
            arguments={"reload": reload},
            destination=list(destination) if destination is not None else None,
        ),
    )


def add_consumer(  # noqa: PLR0913
    registry: WorkerRegistry,
    worker: str,
    queue: str,
    *,
    exchange: str | None = None,
    routing_key: str | None = None,
    destination: Sequence[str] | None = None,
) -> list[object] | None:
    """Add a queue consumer."""
    app = _get_app(registry, worker)
    return cast(
        "list[object] | None",
        app.control.add_consumer(
            queue,
            exchange=exchange,
            routing_key=routing_key,
            destination=list(destination) if destination is not None else None,
        ),
    )


def remove_consumer(
    registry: WorkerRegistry,
    worker: str,
    queue: str,
    *,
    destination: Sequence[str] | None = None,
) -> list[object] | None:
    """Remove a queue consumer."""
    app = _get_app(registry, worker)
    return cast(
        "list[object] | None",
        app.control.cancel_consumer(queue, destination=list(destination) if destination is not None else None),
    )


def get_stats(
    registry: WorkerRegistry,
    worker: str,
    *,
    destination: Sequence[str] | None = None,
) -> dict[str, object] | None:
    """Fetch worker stats via inspector."""
    app = _get_app(registry, worker)
    inspector = app.control.inspect(destination=list(destination) if destination else None)
    return cast("dict[str, object] | None", inspector.stats() if inspector is not None else None)
