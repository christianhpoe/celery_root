# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from celery_root.components.metrics import stats
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


class FakeDB(BaseDBController):
    def __init__(self) -> None:
        self.task_stats = TaskStats(
            count=3,
            min_runtime=1.0,
            max_runtime=4.0,
            avg_runtime=2.5,
            p95=3.5,
            p99=4.0,
        )
        self.throughput = [
            ThroughputBucket(bucket_start=datetime.now(UTC), count=5),
        ]
        self.state_dist = {"SUCCESS": 2, "FAILURE": 1}
        self.heatmap = [[1, 0], [0, 2]]

    def initialize(self) -> None: ...

    def get_schema_version(self) -> int:
        return 1

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

    def get_workers(self) -> list[Worker]:
        return []

    def get_worker(self, _hostname: str) -> Worker | None:
        return None

    def get_task_stats(self, _task_name: str | None, _time_range: TimeRange | None) -> TaskStats:
        return self.task_stats

    def get_throughput(self, _time_range: TimeRange, _bucket_seconds: int) -> list[ThroughputBucket]:
        return self.throughput

    def get_state_distribution(self) -> dict[str, int]:
        return self.state_dist

    def get_heatmap(self, _time_range: TimeRange | None) -> list[list[int]]:
        return self.heatmap

    def get_schedules(self) -> list[Schedule]:
        return []

    def store_schedule(self, schedule: Schedule) -> None: ...

    def delete_schedule(self, _schedule_id: str) -> None: ...

    def cleanup(self, _older_than_days: int) -> int:
        return 0

    def close(self) -> None: ...


def test_stats_passthrough_methods() -> None:
    db = FakeDB()
    now = datetime.now(UTC)
    timerange = TimeRange(start=now - timedelta(hours=1), end=now)

    assert stats.task_runtime_stats(db, None, timerange) == db.task_stats
    assert stats.throughput(db, timerange, 60) == db.throughput
    assert stats.state_distribution(db) == db.state_dist
    assert stats.heatmap_data(db, None) == db.heatmap
