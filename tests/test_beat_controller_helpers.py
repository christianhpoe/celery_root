# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from celery.schedules import crontab
from celery.schedules import schedule as interval_schedule

from celery_root.components.beat import controller as beat_controller
from celery_root.components.beat.controller import BeatController
from celery_root.core.db.models import Schedule
from tests.fixtures import app_one

if TYPE_CHECKING:
    import pytest


class _Cron:
    minute = "0"
    hour = "1"
    day_of_month = "*"
    month_of_year = "*"
    day_of_week = "*"


class _DummyPeriodic:
    def __init__(self) -> None:
        self.crontab_id = 1
        self.interval_id = None
        self.crontab = _Cron()
        self.interval = None


class _DummyEntry:
    def __init__(self) -> None:
        self.schedule = crontab(minute="0", hour="1", day_of_month="*", month_of_year="*", day_of_week="*")
        self.task = "demo.add"
        self.args = ()
        self.kwargs: dict[str, object] = {}
        self.options = {"enabled": True}
        self.last_run_at = datetime.now(UTC)
        self.total_run_count = 1


class _DummyScheduler:
    def __init__(self) -> None:
        self.schedule: dict[str, _DummyEntry] = {"demo": _DummyEntry()}
        self.synced = False
        self.closed = False

    def setup_schedule(self) -> None:
        return None

    def sync(self) -> None:
        self.synced = True

    def close(self) -> None:
        self.closed = True


def test_parse_and_format_schedule() -> None:
    schedule_obj = beat_controller._parse_schedule("*/5 * * * *")
    assert isinstance(schedule_obj, crontab)
    formatted = beat_controller._format_schedule(schedule_obj)
    assert "*/5" in formatted

    interval = beat_controller._parse_schedule("interval:30")
    assert isinstance(interval, interval_schedule)
    formatted_interval = beat_controller._format_schedule(interval)
    assert formatted_interval.startswith("interval:")


def test_interval_helpers() -> None:
    interval_spec = beat_controller._parse_interval_seconds("every 5 seconds")
    assert interval_spec.every == 5.0
    interval_spec = beat_controller._parse_interval_seconds("bad")
    assert interval_spec.every == 60.0

    assert beat_controller._parse_args("[1, 2]") == (1, 2)
    assert beat_controller._parse_kwargs('{"x": 1}') == {"x": 1}


def test_format_django_schedule() -> None:
    task = _DummyPeriodic()
    formatted = beat_controller._format_django_schedule(cast("Any", task))
    assert formatted == "0 1 * * *"


def test_detect_backend() -> None:
    app = app_one.app
    controller = BeatController(app)
    backend = controller.detect_backend()
    assert backend.name in {"file", "django_celery_beat"}


def test_file_schedule_operations(monkeypatch: pytest.MonkeyPatch) -> None:
    app = app_one.app
    controller = BeatController(app)
    scheduler = _DummyScheduler()
    monkeypatch.setattr(BeatController, "_file_scheduler", lambda _self: scheduler)

    schedules = controller._list_file_schedules()
    assert schedules

    schedule = Schedule(
        schedule_id="demo",
        name="demo",
        task="demo.add",
        schedule="*/5 * * * *",
        args=None,
        kwargs_=None,
        enabled=True,
        last_run_at=datetime.now(UTC),
        total_run_count=1,
        app=None,
    )
    controller._save_file_schedule(schedule)
    assert scheduler.synced

    controller._delete_file_schedule(schedule.schedule_id)
    assert scheduler.closed


def test_django_schedule_operations(monkeypatch: pytest.MonkeyPatch) -> None:
    app = app_one.app
    controller = BeatController(app)
    saved: dict[str, int] = {"count": 0}
    deleted: dict[str, int] = {"count": 0}

    class _PeriodicTask:
        objects: _QuerySet

        def __init__(self, *, name: str, task: str) -> None:
            self.id = 1
            self.name = name
            self.task = task
            self.args = None
            self.kwargs = None
            self.enabled = True
            self.last_run_at = None
            self.total_run_count = None
            self.crontab_id = None
            self.interval_id = None
            self.crontab = None
            self.interval = None

        def save(self) -> None:
            saved["count"] += 1

    class _QuerySet:
        def __init__(self) -> None:
            self._task: _PeriodicTask | None = None

        def filter(self, **_kwargs: object) -> _QuerySet:
            return self

        def first(self) -> _PeriodicTask | None:
            return self._task

        def delete(self) -> tuple[int, dict[str, int]]:
            deleted["count"] += 1
            return (1, {})

    _PeriodicTask.objects = _QuerySet()

    class _ScheduleManager:
        def get_or_create(self, **_kwargs: object) -> tuple[object, bool]:
            return object(), True

    class _CrontabSchedule:
        objects = _ScheduleManager()

    class _IntervalSchedule:
        objects = _ScheduleManager()

    class _PeriodicTasks:
        @classmethod
        def changed(cls) -> None:
            return None

    class _Models:
        PeriodicTask = _PeriodicTask
        CrontabSchedule = _CrontabSchedule
        IntervalSchedule = _IntervalSchedule
        PeriodicTasks = _PeriodicTasks

    monkeypatch.setattr(BeatController, "_django_models", lambda _self: _Models)
    monkeypatch.setattr(BeatController, "_django_changed", lambda _self: None)

    schedule = Schedule(
        schedule_id="1",
        name="demo",
        task="demo.add",
        schedule="*/5 * * * *",
        args="[]",
        kwargs_="{}",
    )
    controller._save_django_schedule(schedule)
    assert saved["count"] == 1

    controller._delete_django_schedule("1")
    assert deleted["count"] == 1
