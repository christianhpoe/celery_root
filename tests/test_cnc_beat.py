# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from celery import Celery

from celery_root.core.db.adapters.base import BaseDBController
from celery_root.core.db.models import (
    BrokerQueueEvent,
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
from celery_root.core.engine import beat
from celery_root.core.registry import WorkerRegistry


class FakeDB(BaseDBController):
    def __init__(self) -> None:
        self.stored: list[Schedule] = []

    def initialize(self) -> None: ...

    def get_schema_version(self) -> int:
        return 1

    def ensure_schema(self) -> None: ...

    def migrate(self, _from_version: int, _to_version: int) -> None: ...

    def store_task_event(self, _event: TaskEvent) -> None: ...

    def get_tasks(self, _filters: TaskFilter | None = None) -> list[Task]:
        return []

    def get_tasks_page(
        self,
        _filters: TaskFilter | None,
        *,
        sort_key: str | None,
        sort_dir: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[Task], int]:
        _ = (sort_key, sort_dir, limit, offset)
        return [], 0

    def list_task_names(self) -> list[str]:
        return []

    def get_task(self, _task_id: str) -> Task | None:
        return None

    def store_task_relation(self, _relation: TaskRelation) -> None: ...

    def get_task_relations(self, _root_id: str) -> list[TaskRelation]:
        return []

    def store_worker_event(self, _event: WorkerEvent) -> None: ...

    def store_broker_queue_event(self, _event: BrokerQueueEvent) -> None: ...

    def get_broker_queue_snapshot(self, _broker_url: str) -> list[BrokerQueueEvent]:
        return []

    def get_workers(self) -> list[Worker]:
        return []

    def get_worker(self, _hostname: str) -> Worker | None:
        return None

    def get_worker_event_snapshot(self, _hostname: str) -> WorkerEvent | None:
        return None

    def get_task_stats(self, _task_name: str | None, _time_range: TimeRange | None) -> TaskStats:
        return TaskStats()

    def get_throughput(self, _time_range: TimeRange, _bucket_seconds: int) -> list[ThroughputBucket]:
        return []

    def get_state_distribution(self) -> dict[str, int]:
        return {}

    def get_heatmap(self, _time_range: TimeRange | None) -> list[list[int]]:
        return []

    def get_schedules(self) -> list[Schedule]:
        return list(self.stored)

    def store_schedule(self, schedule: Schedule) -> None:
        self.stored.append(schedule)

    def delete_schedule(self, schedule_id: str) -> None:
        self.stored = [s for s in self.stored if s.schedule_id != schedule_id]

    def cleanup(self, _older_than_days: int) -> int:
        return 0

    def close(self) -> None: ...


class DummyApp:
    def __init__(self, scheduler: str | None = None) -> None:
        self.conf = {"beat_scheduler": scheduler} if scheduler else {}
        self.main = "dummy"
        self.conf["broker_url"] = "memory://"


def make_registry(app: DummyApp) -> WorkerRegistry:
    registry = WorkerRegistry()
    registry._apps["dummy"] = cast("Celery", app)
    return registry


def test_detect_backend_prefers_django_celery() -> None:
    app = DummyApp("django_celery_beat.schedulers:DatabaseScheduler")
    registry = make_registry(app)
    assert beat.detect_backend(registry, "dummy") == "django_celery_beat"


def test_schedule_crud_via_db_controller() -> None:
    db = FakeDB()
    schedule = Schedule(
        schedule_id="1",
        name="nightly",
        task="tasks.cleanup",
        schedule="0 2 * * *",
        args=None,
        kwargs_=None,
        enabled=True,
        last_run_at=datetime.now(UTC),
        total_run_count=5,
    )

    beat.save_schedule(db, schedule)
    assert beat.list_schedules(db) == [schedule]

    beat.delete_schedule(db, "1")
    assert beat.list_schedules(db) == []
