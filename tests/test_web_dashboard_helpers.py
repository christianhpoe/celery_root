# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import django
import pytest

from celery_root.components.web.views import dashboard as dashboard_views
from celery_root.core.db.models import (
    BrokerQueueEvent,
    Task,
    TaskFilter,
    TaskStats,
    ThroughputBucket,
    Worker,
)

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture(scope="module", autouse=True)
def _django_setup() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "celery_root.components.web.settings")
    if not django.apps.apps.ready:
        django.setup()


class _DummyDb:
    def __init__(
        self,
        tasks: list[Task],
        workers: list[Worker],
        stats: TaskStats,
        snapshots: list[BrokerQueueEvent],
        buckets: list[ThroughputBucket],
    ) -> None:
        self._tasks = tasks
        self._workers = workers
        self._stats = stats
        self._snapshots = snapshots
        self._buckets = buckets

    def get_workers(self) -> list[Worker]:
        return list(self._workers)

    def get_tasks(self, _filters: TaskFilter | None = None) -> list[Task]:
        return list(self._tasks)

    def get_task_stats(self, _task_name: str | None, _time_range: object | None) -> TaskStats:
        return self._stats

    def get_broker_queue_snapshot(self, _broker_url: str) -> list[BrokerQueueEvent]:
        return list(self._snapshots)

    def get_state_distribution(self) -> dict[str, int]:
        return {"SUCCESS": 2, "FAILURE": 1, "STARTED": 1, "PENDING": 0}

    def get_throughput(self, _time_range: object, bucket_seconds: int) -> list[ThroughputBucket]:
        _ = bucket_seconds
        return list(self._buckets)


class _DummyRegistry:
    def get_brokers(self) -> dict[str, object]:
        return {"redis://": object()}


@contextmanager
def _open_db(db: _DummyDb) -> Iterator[_DummyDb]:
    yield db


def _make_tasks(now: datetime) -> list[Task]:
    return [
        Task(task_id="t1", name="demo", state="SUCCESS", worker="alpha", finished=now - timedelta(minutes=10)),
        Task(task_id="t2", name="demo", state="FAILURE", worker="beta", finished=now - timedelta(minutes=70)),
        Task(task_id="t3", name="demo", state="STARTED", worker="alpha", started=now - timedelta(minutes=5)),
    ]


def test_format_helpers() -> None:
    assert dashboard_views._format_delta(2, suffix="vs. last hour").startswith("+2")
    assert dashboard_views._format_percent(0.25).startswith("↗ 0.2%")
    avg, hint = dashboard_views._format_runtime(TaskStats(count=1, avg_runtime=2.0))
    assert avg == "2.0s"
    assert "p95" in hint


def test_compute_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    tasks = _make_tasks(now)
    workers = [Worker(hostname="alpha", status="ONLINE", last_heartbeat=now, active_tasks=1)]
    stats = TaskStats(count=3, avg_runtime=1.5)
    snapshots = [
        BrokerQueueEvent(broker_url="redis://", queue="celery", messages=3, consumers=1, timestamp=now),
    ]
    buckets = [ThroughputBucket(bucket_start=now, count=5)]
    db = _DummyDb(tasks, workers, stats, snapshots, buckets)

    @contextmanager
    def _open_db_for_test() -> Iterator[_DummyDb]:
        yield db

    def _get_registry_for_test() -> _DummyRegistry:
        return _DummyRegistry()

    monkeypatch.setattr(dashboard_views, "open_db", _open_db_for_test)
    monkeypatch.setattr(dashboard_views, "get_registry", _get_registry_for_test)

    metrics = dashboard_views._compute_metrics(now)
    assert metrics.tasks_today == len(tasks)

    cards = dashboard_views._summary_cards(metrics)
    assert cards

    state_cards = dashboard_views._state_cards(now)
    assert state_cards

    series = dashboard_views._throughput_series(now)
    assert series

    feed = dashboard_views._activity_feed(now)
    assert feed
