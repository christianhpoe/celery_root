# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from datetime import datetime

    from celery import Celery

from celery_root.core.db.adapters.base import BaseDBController
from celery_root.core.db.models import (
    Schedule,
    Task,
    TaskEvent,
    TaskFilter,
    TaskRelation,
    TaskStats,
    ThroughputBucket,
    TimeRange,
    Worker,
    WorkerEvent,
)
from celery_root.core.engine import retry
from celery_root.core.registry import WorkerRegistry


class FakeDB(BaseDBController):
    def __init__(self, tasks: dict[str, Task], relations: list[TaskRelation]) -> None:
        self.tasks = tasks
        self.relations = relations

    def initialize(self) -> None: ...

    def get_schema_version(self) -> int:
        return 1

    def ensure_schema(self) -> None: ...

    def migrate(self, _from_version: int, _to_version: int) -> None: ...

    def store_task_event(self, _event: TaskEvent) -> None: ...

    def get_tasks(self, _filters: TaskFilter | None = None) -> Sequence[Task]:
        return list(self.tasks.values())

    def get_tasks_page(
        self,
        _filters: TaskFilter | None,
        *,
        sort_key: str | None,
        sort_dir: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[Task], int]:
        _ = (sort_key, sort_dir)
        tasks = list(self.tasks.values())
        total = len(tasks)
        return tasks[offset : offset + limit], total

    def list_task_names(self) -> list[str]:
        return sorted({task.name or "unknown" for task in self.tasks.values()})

    def get_task(self, task_id: str) -> Task | None:
        return self.tasks.get(task_id)

    def store_task_relation(self, relation: TaskRelation) -> None:
        self.relations.append(relation)

    def get_task_relations(self, root_id: str) -> Sequence[TaskRelation]:
        return [rel for rel in self.relations if rel.root_id == root_id]

    def store_worker_event(self, _event: WorkerEvent) -> None: ...

    def get_workers(self) -> Sequence[Worker]:
        return []

    def get_worker(self, _hostname: str) -> Worker | None:
        return None

    def get_task_stats(self, _task_name: str | None, _time_range: TimeRange | None) -> TaskStats:
        return TaskStats()

    def get_throughput(self, _time_range: TimeRange, _bucket_seconds: int) -> Sequence[ThroughputBucket]:
        return []

    def get_state_distribution(self) -> dict[str, int]:
        return {}

    def get_heatmap(self, _time_range: TimeRange | None) -> list[list[int]]:
        return []

    def get_schedules(self) -> Sequence[Schedule]:
        return []

    def store_schedule(self, schedule: Schedule) -> None: ...

    def delete_schedule(self, _schedule_id: str) -> None: ...

    def cleanup(self, _older_than_days: int) -> int:
        return 0

    def close(self) -> None: ...


class DummyApp:
    def __init__(self) -> None:
        self.sent: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self.main = "dummy"
        self.conf = type("Conf", (), {"broker_url": "memory://"})()


def make_registry(app: DummyApp) -> WorkerRegistry:
    registry = WorkerRegistry()
    registry._apps["dummy"] = cast("Celery", app)  # noqa: SLF001
    return registry


def test_smart_retry_resends_descendants_in_order() -> None:
    tasks_map = {
        "a": Task(
            task_id="a",
            name="t1",
            state="FAILURE",
            worker="dummy",
            args=json.dumps([1]),
            kwargs_="{}",
        ),
        "b": Task(
            task_id="b",
            name="t2",
            state="PENDING",
            worker="dummy",
            args=json.dumps([2]),
            kwargs_="{}",
        ),
        "c": Task(
            task_id="c",
            name="t3",
            state="PENDING",
            worker="dummy",
            args=json.dumps([3]),
            kwargs_="{}",
        ),
    }
    relations = [
        TaskRelation(root_id="a", parent_id="a", child_id="b", relation="chain"),
        TaskRelation(root_id="a", parent_id="b", child_id="c", relation="chain"),
    ]
    db = FakeDB(tasks_map, relations)
    app = DummyApp()
    registry = make_registry(app)

    called: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def sender(
        _reg: WorkerRegistry,
        _worker: str,
        name: str,
        *,
        args: Sequence[object] | None,
        kwargs: Mapping[str, object] | None,
        countdown: float | None,
        eta: datetime | None,
    ) -> object:
        _ = (countdown, eta)
        called.append((name, tuple(args or ()), dict(kwargs or {})))
        return name

    results = retry.smart_retry(registry, db, "a", sender=cast("retry._TaskSender", sender))  # noqa: SLF001

    assert results == ["t1", "t2", "t3"]
    assert called == [
        ("t1", (1,), {}),
        ("t2", (2,), {}),
        ("t3", (3,), {}),
    ]
