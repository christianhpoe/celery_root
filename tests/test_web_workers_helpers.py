# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import django
import pytest
from django.http import HttpResponse
from django.test import RequestFactory

from celery_root.components.web.views import broker as broker_views
from celery_root.components.web.views import workers as worker_views
from celery_root.core.db.models import Task, TaskFilter, Worker, WorkerEvent
from celery_root.core.engine import workers as worker_control

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture(scope="module", autouse=True)
def _django_setup() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "celery_root.components.web.settings")
    if not django.apps.apps.ready:
        django.setup()


class _DummyDb:
    def __init__(self, workers: list[Worker], tasks: list[Task], snapshot: WorkerEvent | None) -> None:
        self._workers = workers
        self._tasks = tasks
        self._snapshot = snapshot

    def get_workers(self) -> list[Worker]:
        return list(self._workers)

    def get_tasks(self, _filters: TaskFilter | None = None) -> list[Task]:
        return list(self._tasks)

    def get_worker_event_snapshot(self, _hostname: str) -> WorkerEvent | None:
        return self._snapshot

    def get_worker(self, hostname: str) -> Worker | None:
        for worker in self._workers:
            if worker.hostname == hostname:
                return worker
        return None


@contextmanager
def _open_db(db: _DummyDb) -> Iterator[_DummyDb]:
    yield db


def _fake_render(_request: object, _template: str, context: dict[str, object]) -> HttpResponse:
    payload = json.dumps({"title": context.get("title")})
    return HttpResponse(payload, content_type="application/json")


def test_status_and_parsing_helpers() -> None:
    assert worker_views._resolve_status("OFFLINE", 0, None) == "offline"
    assert worker_views._resolve_status("ONLINE", 2, None) == "busy"
    assert worker_views._resolve_status("ONLINE", 0, timedelta(minutes=3)) == "offline"

    name, info = worker_views._parse_task_info("demo.add [rate_limit=1/s]")
    assert name == "demo.add"
    assert info["rate_limit"] == "1/s"

    assert worker_views._normalize_info("hello") == "hello"
    assert worker_views._normalize_info(None) is None

    rows = worker_views._parse_task_rows(["demo.add [rate_limit=1/s]", "demo.mul [rate_limit=2/s]"])
    assert rows[0]["name"] == "demo.add"

    assert worker_views._get_nested({"pool": {"max-concurrency": 4}}, "pool", "max-concurrency") == 4
    assert worker_views._format_seconds(3600.5) == "1h 0m"
    assert worker_views._stringify({"a": 1}) == "a=1"


def test_metadata_and_queue_helpers() -> None:
    stats = {"pool": {"max-concurrency": 4}, "broker": {"transport": "redis"}, "clock": 1}
    conf = {"task_default_queue": "celery"}
    rows = worker_views._build_metadata_rows(stats, conf)
    labels = {row["label"] for row in rows}
    assert "Pool max concurrency" in labels
    assert "Broker transport" in labels

    queue_rows = worker_views._parse_queue_rows(
        [
            {"name": "celery", "exchange": {"name": "celery"}, "routing_key": "celery"},
            {"name": "other", "exchange": None},
        ],
    )
    assert queue_rows[0]["name"] == "celery"

    assert worker_views._parse_active_count([1, 2]) == 2
    assert worker_views._parse_active_count(3) == 3
    assert worker_views._parse_active_count("nope") is None


def test_snapshot_and_detail_fragment(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    workers = [Worker(hostname="alpha", status="ONLINE", last_heartbeat=now, pool_size=4)]
    tasks = [Task(task_id="task-1", name="demo.add", state="STARTED", worker="alpha")]
    snapshot = WorkerEvent(
        hostname="alpha",
        event="worker-snapshot",
        timestamp=now,
        info={
            "stats": {"pool": {"max-concurrency": 4}},
            "queues": [{"name": "celery"}],
            "registered": ["demo.add"],
            "active": [],
            "app": "demo",
        },
    )
    db = _DummyDb(workers, tasks, snapshot)
    monkeypatch.setattr(worker_views, "open_db", lambda: _open_db(db))
    monkeypatch.setattr(worker_views, "render", _fake_render)
    monkeypatch.setattr(broker_views, "list_broker_groups", lambda **_kwargs: [])

    factory = RequestFactory()
    list_request = factory.get("/workers/")
    response = worker_views.worker_list_fragment(list_request)
    assert response.status_code == 200

    detail_request = factory.get("/workers/alpha/")
    detail_response = worker_views.worker_detail_fragment(detail_request, "alpha")
    assert detail_response.status_code == 200


def test_worker_actions(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    workers = [Worker(hostname="alpha", status="ONLINE", last_heartbeat=now, pool_size=4)]
    db = _DummyDb(workers, [], None)
    monkeypatch.setattr(worker_views, "open_db", lambda: _open_db(db))
    monkeypatch.setattr(worker_views, "render", _fake_render)

    class _Registry:
        def get_apps(self) -> list[object]:
            class _App:
                def __init__(self) -> None:
                    self.main = "demo"
                    self.conf: dict[str, object] = {"main": "demo"}

            return [_App()]

    monkeypatch.setattr(worker_views, "get_registry", _Registry)
    monkeypatch.setattr(worker_control, "restart", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(worker_control, "pool_grow", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(worker_control, "pool_shrink", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(worker_control, "autoscale", lambda *_args, **_kwargs: None)

    factory = RequestFactory()
    restart_request = factory.post("/workers/restart/")
    assert worker_views.workers_restart(restart_request).status_code == 302

    grow_request = factory.post("/workers/alpha/grow/")
    assert worker_views.worker_grow(grow_request, "alpha").status_code == 302

    shrink_request = factory.post("/workers/alpha/shrink/")
    assert worker_views.worker_shrink(shrink_request, "alpha").status_code == 302

    autoscale_request = factory.post("/workers/alpha/autoscale/", {"min": "1", "max": "2"})
    assert worker_views.worker_autoscale(autoscale_request, "alpha").status_code == 302


def test_overview_and_broker_detail(monkeypatch: pytest.MonkeyPatch) -> None:
    worker = worker_views._WorkerRow(
        hostname="alpha",
        status="online",
        pool_size=0,
        active=1,
        registered=2,
        queues=["celery"],
        last_seen=None,
        concurrency=4,
    )
    stats_map = {"pool": {"max-concurrency": 4}}
    pool_size = worker_views._resolve_pool_size(worker, stats_map)
    registered = worker_views._resolve_registered_count(worker, stats_map)
    overview = worker_views._build_overview(pool_size, worker.active, registered, worker.concurrency)
    assert overview

    meta_rows = worker_views._build_metadata_rows(stats_map, {"task_default_queue": "celery"})
    split = worker_views._split_metadata_rows(meta_rows)
    assert split

    class _Conf(dict[str, object]):
        def __getattr__(self, name: str) -> object | None:
            return self.get(name)

    class _App:
        def __init__(self) -> None:
            self.main = "demo"
            self.conf = _Conf({"broker_url": "redis://"})

    class _Registry:
        def get_app(self, _name: str) -> _App:
            return _App()

    monkeypatch.setattr(worker_views, "get_registry", _Registry)
    monkeypatch.setattr(broker_views, "queue_rows_for_broker_snapshot", lambda _url: ([], None))
    _broker_rows, broker_key, _broker_label, _broker_type = worker_views._resolve_broker_detail("demo")
    assert broker_key is not None
