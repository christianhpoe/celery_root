# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import django
import pytest
from django.http import Http404, HttpResponse
from django.test import RequestFactory

from celery_root.components.web.views import beat as beat_views
from celery_root.config import (
    BeatConfig,
    CeleryRootConfig,
    DatabaseConfigSqlite,
    LoggingConfigFile,
    get_settings,
    set_settings,
)
from celery_root.core.db.models import Schedule

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


@pytest.fixture(scope="module", autouse=True)
def _django_setup() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "celery_root.components.web.settings")
    if not django.apps.apps.ready:
        django.setup()


class _DummyApp:
    def __init__(self, name: str) -> None:
        self.main = name


class _DummyRegistry:
    def __init__(self, apps: list[_DummyApp]) -> None:
        self._apps = apps

    def get_apps(self) -> tuple[_DummyApp, ...]:
        return tuple(self._apps)


class _DummyDb:
    def __init__(self, schedules: list[Schedule]) -> None:
        self._schedules = schedules
        self.saved: list[Schedule] = []

    def store_schedule(self, schedule: Schedule) -> None:
        self.saved.append(schedule)


class _DummyController:
    def __init__(self, schedules: list[Schedule]) -> None:
        self._schedules = schedules

    def list_schedules(self) -> list[Schedule]:
        return list(self._schedules)

    def save_schedule(self, schedule: Schedule) -> None:
        self._schedules.append(schedule)

    def delete_schedule(self, schedule_id: str) -> None:
        self._schedules = [item for item in self._schedules if item.schedule_id != schedule_id]

    def sync_to_db(self) -> list[Schedule]:
        return list(self._schedules)


@contextmanager
def _open_db(db: _DummyDb) -> Iterator[_DummyDb]:
    yield db


def _fake_render(_request: object, _template: str, context: dict[str, object]) -> HttpResponse:
    payload = json.dumps({"title": context.get("title")})
    return HttpResponse(payload, content_type="application/json")


def test_parse_form_validation() -> None:
    factory = RequestFactory()
    request = factory.post("/beat/add/", {"name": "", "task": ""})
    with pytest.raises(ValueError, match="name, worker, task, and schedule are required"):
        beat_views._parse_form(request, None)


def test_list_schedules_and_add(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    original = get_settings()
    config = CeleryRootConfig(
        logging=LoggingConfigFile(log_dir=tmp_path / "logs"),
        database=DatabaseConfigSqlite(db_path=tmp_path / "db.sqlite"),
        beat=BeatConfig(),
    )
    set_settings(config)

    now = datetime.now(UTC)
    schedules = [
        Schedule(schedule_id="s1", name="demo", task="demo.add", schedule="*/5 * * * *", last_run_at=now),
    ]
    db = _DummyDb(schedules)
    registry = _DummyRegistry([_DummyApp("demo")])

    monkeypatch.setattr(beat_views, "open_db", lambda: _open_db(db))
    monkeypatch.setattr(beat_views, "get_registry", lambda: registry)
    monkeypatch.setattr(beat_views, "BeatController", lambda _app, _db: _DummyController(schedules))
    monkeypatch.setattr(beat_views, "render", _fake_render)

    rows = beat_views.list_schedules()
    assert rows

    factory = RequestFactory()
    request = factory.post(
        "/beat/add/",
        {
            "name": "demo",
            "worker": "demo",
            "task": "demo.add",
            "schedule": "*/5 * * * *",
            "args": "[]",
            "kwargs": "{}",
        },
    )
    try:
        response = beat_views.beat_add(request)
        assert response.status_code == 302
    finally:
        set_settings(original)


def test_ensure_beat_enabled(tmp_path: Path) -> None:
    original = get_settings()
    config = CeleryRootConfig(
        logging=LoggingConfigFile(log_dir=tmp_path / "logs"),
        database=DatabaseConfigSqlite(db_path=tmp_path / "db.sqlite"),
        beat=None,
    )
    set_settings(config)
    try:
        with pytest.raises(Http404):
            beat_views._ensure_beat_enabled()
    finally:
        set_settings(original)


def test_beat_edit_delete_sync(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    original = get_settings()
    config = CeleryRootConfig(
        logging=LoggingConfigFile(log_dir=tmp_path / "logs2"),
        database=DatabaseConfigSqlite(db_path=tmp_path / "db2.sqlite"),
        beat=BeatConfig(),
    )
    set_settings(config)

    now = datetime.now(UTC)
    schedules = [
        Schedule(schedule_id="s1", name="demo", task="demo.add", schedule="*/5 * * * *", app="demo", last_run_at=now),
    ]
    db = _DummyDb(schedules)
    registry = _DummyRegistry([_DummyApp("demo")])

    controller = _DummyController(schedules)
    monkeypatch.setattr(beat_views, "open_db", lambda: _open_db(db))
    monkeypatch.setattr(beat_views, "get_registry", lambda: registry)
    monkeypatch.setattr(beat_views, "BeatController", lambda _app, _db: controller)
    monkeypatch.setattr(beat_views, "render", _fake_render)
    monkeypatch.setattr(beat_views, "list_task_names", lambda _apps: ["demo.add"])

    factory = RequestFactory()
    edit_request = factory.get("/beat/s1/edit/")
    try:
        response = beat_views.beat_edit(edit_request, "s1")
        assert response.status_code == 200

        edit_post = factory.post(
            "/beat/s1/edit/",
            {"name": "demo", "worker": "demo", "task": "demo.add", "schedule": "*/10 * * * *"},
        )
        response = beat_views.beat_edit(edit_post, "s1")
        assert response.status_code == 302

        delete_request = factory.post("/beat/s1/delete/", {"app": "demo"})
        response = beat_views.beat_delete(delete_request, "s1")
        assert response.status_code == 302

        sync_request = factory.post("/beat/sync/")
        response = beat_views.beat_sync(sync_request)
        assert response.status_code == 302
    finally:
        set_settings(original)
