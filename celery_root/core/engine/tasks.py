# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Task control helpers for Root."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from datetime import datetime

    from celery import Celery
    from celery.result import AsyncResult

    from celery_root.core.registry import WorkerRegistry

__all__ = ["rate_limit", "revoke", "send_task", "time_limit"]


def _get_app(registry: WorkerRegistry, worker: str) -> Celery:
    """Resolve a Celery app from the registry."""
    return registry.get_app(worker)


def send_task(  # noqa: PLR0913
    registry: WorkerRegistry,
    worker: str,
    name: str,
    *,
    args: Sequence[object] | None = None,
    kwargs: Mapping[str, object] | None = None,
    eta: datetime | None = None,
    countdown: float | None = None,
    expires: datetime | float | None = None,
    queue: str | None = None,
    routing_key: str | None = None,
    priority: int | None = None,
) -> AsyncResult[object]:
    """Send a task to a worker app."""
    app = _get_app(registry, worker)
    return app.send_task(
        name,
        args=tuple(args) if args is not None else (),
        kwargs=dict(kwargs) if kwargs is not None else {},
        eta=eta,
        countdown=countdown,
        expires=expires,
        queue=queue,
        routing_key=routing_key,
        priority=priority,
    )


def revoke(  # noqa: PLR0913
    registry: WorkerRegistry,
    worker: str,
    task_id: str,
    *,
    terminate: bool = False,
    signal: str | int | None = None,
    destination: Sequence[str] | None = None,
) -> list[object] | None:
    """Revoke a task; optionally terminate it with a signal."""
    app = _get_app(registry, worker)
    if signal is None:
        return cast(
            "list[object] | None",
            app.control.revoke(
                task_id,
                terminate=terminate,
                destination=list(destination) if destination is not None else None,
            ),
        )
    return cast(
        "list[object] | None",
        app.control.revoke(
            task_id,
            terminate=terminate,
            signal=str(signal),
            destination=list(destination) if destination is not None else None,
        ),
    )


def rate_limit(
    registry: WorkerRegistry,
    worker: str,
    task_name: str,
    rate: str,
    *,
    destination: Sequence[str] | None = None,
) -> list[object] | None:
    """Set a rate limit for a task on the worker."""
    app = _get_app(registry, worker)
    return cast(
        "list[object] | None",
        app.control.rate_limit(
            task_name,
            rate,
            destination=list(destination) if destination is not None else None,
        ),
    )


def time_limit(  # noqa: PLR0913
    registry: WorkerRegistry,
    worker: str,
    task_name: str,
    *,
    soft: int | None = None,
    hard: int | None = None,
    destination: Sequence[str] | None = None,
) -> list[object] | None:
    """Update soft and hard time limits for a task."""
    app = _get_app(registry, worker)
    return cast(
        "list[object] | None",
        app.control.time_limit(
            task_name,
            soft=soft,
            hard=hard,
            destination=list(destination) if destination is not None else None,
        ),
    )
