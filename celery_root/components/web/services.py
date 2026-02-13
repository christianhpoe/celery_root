# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Shared data-access helpers for the Django web app."""

from __future__ import annotations

import importlib
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from celery import Celery
from django.conf import settings

from celery_root.core.db.adapters.sqlite import SQLiteController
from celery_root.core.registry import WorkerRegistry

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from celery_root.core.db.adapters.base import BaseDBController


@dataclass(slots=True)
class WorkerOption:
    """Descriptor for a worker app that can be started locally."""

    label: str
    module: str
    app_name: str
    queue: str | None


_CLEANUP_INTERVAL_SECONDS = 60.0
_LAST_CLEANUP_AT: list[float | None] = [None]
_LOGGER = logging.getLogger(__name__)


def _should_cleanup(now: float) -> bool:
    last_cleanup = _LAST_CLEANUP_AT[0]
    if last_cleanup is None or (now - last_cleanup) >= _CLEANUP_INTERVAL_SECONDS:
        _LAST_CLEANUP_AT[0] = now
        return True
    return False


def _split_path(path: str) -> tuple[str, str]:
    if ":" in path:
        module_path, attr = path.split(":", 1)
        return module_path, attr
    if "." not in path:
        message = "Worker import path must be module:attr or module.attr"
        raise ValueError(message)
    module_path, attr = path.rsplit(".", 1)
    return module_path, attr


def _load_app(path: str) -> Celery:
    module_path, attr = _split_path(path)
    module = importlib.import_module(module_path)
    try:
        app = getattr(module, attr)
    except AttributeError as exc:
        message = f"{path} does not define {attr}"
        raise ImportError(message) from exc
    if not isinstance(app, Celery):
        message = f"{path} did not resolve to a Celery app"
        raise TypeError(message)
    return app


def app_name(app: Celery) -> str:
    """Resolve a stable name for the Celery app."""
    raw_main = getattr(app, "main", None)
    name = str(raw_main) if raw_main else ""
    if not name:
        conf_main = app.conf.get("main")
        name = str(conf_main) if conf_main else ""
    if not name:
        name = f"celery_app_{id(app)}"
    return name


def db_path() -> Path:
    """Return the configured database path for the Root store."""
    raw_path = getattr(settings, "CELERY_ROOT_DB_PATH", None)
    return Path(raw_path) if raw_path is not None else Path("celery_root.db")


def retention_days() -> int:
    """Return the configured retention window in days."""
    return int(getattr(settings, "CELERY_ROOT_RETENTION_DAYS", 7))


@contextmanager
def open_db() -> Iterator[BaseDBController]:
    """Open a DB controller for a request and close it afterwards."""
    controller = SQLiteController(db_path())
    controller.initialize()
    days = retention_days()
    if days > 0 and _should_cleanup(time.monotonic()):
        removed = controller.cleanup(days)
        if removed:
            _LOGGER.info("DB cleanup removed %d records older than %d days.", removed, days)
        else:
            _LOGGER.debug("DB cleanup ran; no records removed.")
    try:
        yield controller
    finally:
        controller.close()


def get_registry() -> WorkerRegistry:
    """Build a registry from configured worker import paths."""
    registry = WorkerRegistry()
    for path in getattr(settings, "CELERY_ROOT_WORKERS", []):
        if not path:
            continue
        app = _load_app(path)
        registry.register(app)
    return registry


def get_default_app() -> tuple[Celery, str] | None:
    """Return the first configured Celery app and its registry key."""
    registry = get_registry()
    apps = registry.get_apps()
    if not apps:
        return None
    app = apps[0]
    return app, app_name(app)


def list_worker_options() -> list[WorkerOption]:
    """Return worker app options from configured import paths."""
    options: list[WorkerOption] = []
    for path in getattr(settings, "CELERY_ROOT_WORKERS", []):
        if not path:
            continue
        module_path, _ = _split_path(path)
        app = _load_app(path)
        queue = app.conf.get("task_default_queue")
        options.append(
            WorkerOption(
                label=app_name(app),
                module=module_path,
                app_name=app_name(app),
                queue=str(queue) if queue else None,
            ),
        )
    return options


def list_task_names(apps: Sequence[Celery]) -> list[str]:
    """Collect task names from the configured Celery apps."""
    task_names: set[str] = set()
    for app in apps:
        for name in app.tasks:
            if name.startswith("celery."):
                continue
            task_names.add(name)
    return sorted(task_names)
