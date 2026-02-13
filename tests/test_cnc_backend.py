# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from celery import Celery

from celery_root.core.engine import backend
from celery_root.core.registry import WorkerRegistry


class DummyBackendClient:
    def __init__(self, store: dict[str, dict[str, object]]) -> None:
        self.store = store

    def scan_iter(self, match: str) -> list[str]:
        prefix = "celery-task-meta-"
        if match and not match.startswith(prefix):
            return []
        return [f"{prefix}{task_id}" for task_id in self.store]

    def delete(self, *keys: str) -> int:
        prefix = "celery-task-meta-"
        deleted = 0
        for key in keys:
            task_id = key.removeprefix(prefix)
            if task_id in self.store:
                del self.store[task_id]
                deleted += 1
        return deleted


class DummyBackend:
    meta_key_prefix = "celery-task-meta-"

    def __init__(self) -> None:
        self.store: dict[str, dict[str, object]] = {
            "task-1": {"status": "SUCCESS", "result": 3, "name": "demo.add"},
            "task-2": {"status": "FAILURE", "result": None, "name": "demo.mul"},
        }
        self.client = DummyBackendClient(self.store)

    def get_task_meta(self, task_id: str) -> dict[str, object]:
        return self.store.get(task_id, {})

    def forget(self, task_id: str) -> None:
        self.store.pop(task_id, None)

    def get_key_for_task(self, task_id: str) -> str:
        return f"{self.meta_key_prefix}{task_id}"


class DummyApp:
    def __init__(self) -> None:
        self.backend = DummyBackend()
        self.control = SimpleNamespace()
        self.conf = SimpleNamespace(broker_url="memory://")
        self.main = "dummy"


def make_registry(app: DummyApp) -> WorkerRegistry:
    registry = WorkerRegistry()
    registry._apps["dummy"] = cast("Celery", app)  # noqa: SLF001
    return registry


def test_list_results_returns_metadata() -> None:
    app = DummyApp()
    registry = make_registry(app)

    results = backend.list_results(registry, "dummy")

    task_ids = {result.task_id for result in results}
    assert task_ids == {"task-1", "task-2"}


def test_clear_results_by_name_pattern() -> None:
    app = DummyApp()
    registry = make_registry(app)

    cleared = backend.clear_results(registry, "dummy", name_pattern="demo.*")

    assert cleared == 2
    assert backend.list_results(registry, "dummy") == []


def test_clear_results_by_task_id() -> None:
    app = DummyApp()
    registry = make_registry(app)

    cleared = backend.clear_results(registry, "dummy", task_id="task-1")

    assert cleared == 1
    remaining = backend.list_results(registry, "dummy")
    assert {res.task_id for res in remaining} == {"task-2"}
