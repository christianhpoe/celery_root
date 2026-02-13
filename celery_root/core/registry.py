# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Registry for Celery apps and broker groupings."""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import TYPE_CHECKING

from celery import Celery

if TYPE_CHECKING:
    from collections.abc import Iterable


@dataclass(slots=True)
class BrokerGroup:
    """Group of Celery apps sharing a broker URL."""

    broker_url: str
    apps: list[Celery]


class WorkerRegistry:
    """Registry for Celery apps (by instance or import path)."""

    def __init__(self, workers: Iterable[Celery | str] | None = None) -> None:
        """Create a registry and optionally pre-register workers."""
        self._apps: dict[str, Celery] = {}
        if workers is not None:
            for worker in workers:
                self.register(worker)

    def register(self, worker: Celery | str) -> None:
        """Register a Celery app or import path."""
        app = worker if isinstance(worker, Celery) else self._load_app(worker)
        name = self._resolve_name(app)
        existing = self._apps.get(name)
        if existing is not None and existing is not app:
            message = f"Duplicate app name registered: {name}"
            raise ValueError(message)
        self._apps[name] = app

    def get_apps(self) -> tuple[Celery, ...]:
        """Return all registered apps."""
        return tuple(self._apps.values())

    def get_app(self, name: str) -> Celery:
        """Return a registered app by name."""
        try:
            return self._apps[name]
        except KeyError as exc:
            message = f"Unknown app: {name}"
            raise KeyError(message) from exc

    def get_brokers(self) -> dict[str, BrokerGroup]:
        """Group registered apps by broker URL."""
        brokers: dict[str, BrokerGroup] = {}
        for app in self._apps.values():
            broker_url = str(app.conf.broker_url or "")
            group = brokers.get(broker_url)
            if group is None:
                group = BrokerGroup(broker_url=broker_url, apps=[])
                brokers[broker_url] = group
            group.apps.append(app)
        return brokers

    def _load_app(self, path: str) -> Celery:
        module_path, attr = self._split_path(path)
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

    @staticmethod
    def _split_path(path: str) -> tuple[str, str]:
        if ":" in path:
            module_path, attr = path.split(":", 1)
            return module_path, attr
        if "." not in path:
            message = "Import path must be module:attr or module.attr"
            raise ImportError(message)
        module_path, attr = path.rsplit(".", 1)
        return module_path, attr

    @staticmethod
    def _resolve_name(app: Celery) -> str:
        raw_main = getattr(app, "main", None)
        name = str(raw_main) if raw_main else ""
        if not name:
            conf_main = app.conf.get("main")
            name = str(conf_main) if conf_main else ""
        if not name:
            name = f"celery_app_{id(app)}"
        return name
