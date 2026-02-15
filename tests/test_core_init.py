# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from celery_root import CeleryRoot, _derive_worker_import_paths, _parse_worker_paths, _resolve_app_import_path
from celery_root.config import BeatConfig, CeleryRootConfig, DatabaseConfigSqlite
from celery_root.core.db.adapters.base import BaseDBController
from celery_root.core.db.adapters.sqlite import SQLiteController
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
from tests.fixtures import app_one

if TYPE_CHECKING:
    from collections.abc import Sequence


class _DummyController(BaseDBController):
    def initialize(self) -> None:
        return None

    def get_schema_version(self) -> int:
        return 1

    def ensure_schema(self) -> None:
        return None

    def migrate(self, from_version: int, to_version: int) -> None:
        _ = (from_version, to_version)

    def store_task_event(self, event: TaskEvent) -> None:
        _ = event

    def get_tasks(self, filters: TaskFilter | None = None) -> Sequence[Task]:
        _ = filters
        return []

    def get_tasks_page(
        self,
        filters: TaskFilter | None,
        *,
        sort_key: str | None,
        sort_dir: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[Task], int]:
        _ = (filters, sort_key, sort_dir, limit, offset)
        return [], 0

    def list_task_names(self) -> Sequence[str]:
        return []

    def get_task(self, task_id: str) -> Task | None:
        _ = task_id
        return None

    def store_task_relation(self, relation: TaskRelation) -> None:
        _ = relation

    def get_task_relations(self, root_id: str) -> Sequence[TaskRelation]:
        _ = root_id
        return []

    def store_worker_event(self, event: WorkerEvent) -> None:
        _ = event

    def store_broker_queue_event(self, event: BrokerQueueEvent) -> None:
        _ = event

    def get_broker_queue_snapshot(self, broker_url: str) -> Sequence[BrokerQueueEvent]:
        _ = broker_url
        return []

    def get_workers(self) -> Sequence[Worker]:
        return []

    def get_worker(self, hostname: str) -> Worker | None:
        _ = hostname
        return None

    def get_worker_event_snapshot(self, hostname: str) -> WorkerEvent | None:
        _ = hostname
        return None

    def get_task_stats(self, task_name: str | None, time_range: TimeRange | None) -> TaskStats:
        _ = (task_name, time_range)
        return TaskStats()

    def get_throughput(self, time_range: TimeRange, bucket_seconds: int) -> Sequence[ThroughputBucket]:
        _ = (time_range, bucket_seconds)
        return []

    def get_state_distribution(self) -> dict[str, int]:
        return {}

    def get_heatmap(self, time_range: TimeRange | None) -> list[list[int]]:
        _ = time_range
        return []

    def get_schedules(self) -> Sequence[Schedule]:
        return []

    def store_schedule(self, schedule: Schedule) -> None:
        _ = schedule

    def delete_schedule(self, schedule_id: str) -> None:
        _ = schedule_id

    def cleanup(self, older_than_days: int) -> int:
        _ = older_than_days
        return 0

    def close(self) -> None:
        return None


def test_worker_path_parsing() -> None:
    assert _parse_worker_paths(None) == []
    assert _parse_worker_paths("a,b, ,c") == ["a", "b", "c"]

    app = app_one.app
    path = _resolve_app_import_path(app)
    assert path is not None
    assert path.endswith(":app")

    derived = _derive_worker_import_paths((app, "tests.fixtures.app_one:app"))
    assert derived


def test_celery_root_config_updates(tmp_path: Path) -> None:
    config = CeleryRootConfig(database=DatabaseConfigSqlite(db_path=tmp_path / "db.sqlite"))
    root = CeleryRoot(config=config, retention_days=3)
    assert isinstance(root.config.database, DatabaseConfigSqlite)
    assert root.config.database.retention_days == 3

    with pytest.raises(ValueError, match="retention_days must be positive"):
        CeleryRoot(config=config, retention_days=-1)


def test_sqlite_path_resolution() -> None:
    db_rel = Path("data/db.sqlite")
    config = CeleryRootConfig(database=DatabaseConfigSqlite(db_path=db_rel))
    root = CeleryRoot(config=config)
    assert root.config.database.db_path is not None
    assert root.config.database.db_path.is_absolute()


def test_resolve_db_controller_factory(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "db.sqlite"
    sqlite = SQLiteController(sqlite_path)
    config = CeleryRootConfig(database=DatabaseConfigSqlite(db_path=sqlite_path))
    root = CeleryRoot(config=config, db_controller=sqlite)
    assert root._resolve_db_controller_factory() is None

    dummy = _DummyController()
    root2 = CeleryRoot(config=config, db_controller=dummy)
    factory = root2._resolve_db_controller_factory()
    assert factory is not None
    assert factory() is dummy


def test_purge_schedule_file(tmp_path: Path) -> None:
    schedule_path = tmp_path / "schedule.db"
    schedule_path.write_text("data")
    config = CeleryRootConfig(
        database=DatabaseConfigSqlite(db_path=tmp_path / "db.sqlite"),
        beat=BeatConfig(schedule_path=schedule_path),
    )
    root = CeleryRoot(config=config)
    root._purge_schedule_file()
    assert not schedule_path.exists()


def test_purge_existing_db(tmp_path: Path) -> None:
    db_path = tmp_path / "purge.db"
    db_path.write_text("data")
    config = CeleryRootConfig(database=DatabaseConfigSqlite(db_path=db_path, purge_db=True))
    CeleryRoot(config=config)
    assert not db_path.exists()


def test_ensure_worker_import_paths_env(monkeypatch: pytest.MonkeyPatch) -> None:
    config = CeleryRootConfig(database=DatabaseConfigSqlite(db_path=None))
    monkeypatch.setenv("CELERY_ROOT_WORKERS", "tests.fixtures.app_one:app")
    updated = CeleryRoot._ensure_worker_import_paths(config, ())
    assert updated.worker_import_paths
