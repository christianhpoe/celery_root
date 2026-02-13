# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""High-level helpers for beat schedule operations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from celery_root.components.beat import BeatController

if TYPE_CHECKING:
    from collections.abc import Sequence

    from celery_root.core.db.adapters.base import BaseDBController
    from celery_root.core.db.models import Schedule
    from celery_root.core.registry import WorkerRegistry

__all__ = ["delete_schedule", "detect_backend", "list_schedules", "save_schedule"]


def detect_backend(registry: WorkerRegistry, worker: str) -> str:
    """Best-effort detection of beat backend for a worker app."""
    app = registry.get_app(worker)
    scheduler = str(app.conf.get("beat_scheduler") or "")
    if "django_celery_beat" in scheduler:
        return "django_celery_beat"
    if scheduler:
        return scheduler
    return "file"


def list_schedules(
    db: BaseDBController,
    *,
    registry: WorkerRegistry | None = None,
    worker: str | None = None,
    sync_backend: bool = False,
) -> Sequence[Schedule]:
    """List schedules, optionally syncing from the backend."""
    if sync_backend and registry and worker:
        controller = BeatController(registry.get_app(worker), db)
        return controller.sync_to_db()
    return db.get_schedules()


def save_schedule(
    db: BaseDBController,
    schedule: Schedule,
    *,
    registry: WorkerRegistry | None = None,
    worker: str | None = None,
) -> None:
    """Persist a schedule to the DB and backend if available."""
    if registry and worker:
        controller = BeatController(registry.get_app(worker), db)
        controller.save_schedule(schedule)
        return
    db.store_schedule(schedule)


def delete_schedule(
    db: BaseDBController,
    schedule_id: str,
    *,
    registry: WorkerRegistry | None = None,
    worker: str | None = None,
) -> None:
    """Remove a schedule from the DB and backend if available."""
    if registry and worker:
        controller = BeatController(registry.get_app(worker), db)
        controller.delete_schedule(schedule_id)
        return
    db.delete_schedule(schedule_id)
