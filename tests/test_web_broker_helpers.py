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
from django.http import HttpResponse
from django.test import RequestFactory

from celery_root.components.web.views import broker as broker_views
from celery_root.core.db.models import BrokerQueueEvent, Worker

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture(scope="module", autouse=True)
def _django_setup() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "celery_root.components.web.settings")
    if not django.apps.apps.ready:
        django.setup()


class _DummyConf(dict[str, object]):
    def __getattr__(self, name: str) -> object | None:
        return self.get(name)

    def __setattr__(self, name: str, value: object) -> None:
        self[name] = value


class _DummyApp:
    def __init__(self, name: str, broker_url: str, backend: str) -> None:
        self.main = name
        self.conf = _DummyConf({"broker_url": broker_url, "result_backend": backend})


class _DummyGroup:
    def __init__(self, broker_url: str, apps: list[_DummyApp]) -> None:
        self.broker_url = broker_url
        self.apps = apps


class _DummyRegistry:
    def __init__(self, group: _DummyGroup) -> None:
        self._group = group

    def get_brokers(self) -> dict[str, _DummyGroup]:
        return {self._group.broker_url: self._group}

    def get_apps(self) -> tuple[_DummyApp, ...]:
        return tuple(self._group.apps)

    def get_app(self, name: str) -> _DummyApp:
        for app in self._group.apps:
            if app.main == name:
                return app
        raise KeyError(name)


class _DummyDb:
    def __init__(self, workers: list[Worker], snapshots: list[BrokerQueueEvent]) -> None:
        self._workers = workers
        self._snapshots = snapshots

    def get_workers(self) -> list[Worker]:
        return list(self._workers)

    def get_broker_queue_snapshot(self, _broker_url: str) -> list[BrokerQueueEvent]:
        return list(self._snapshots)


@contextmanager
def _open_db(db: _DummyDb) -> Iterator[_DummyDb]:
    yield db


def _fake_render(_request: object, _template: str, context: dict[str, object]) -> HttpResponse:
    payload = json.dumps({"title": context.get("title")})
    return HttpResponse(payload, content_type="application/json")


def test_encode_decode_and_labels() -> None:
    encoded = broker_views.encode_broker_key("redis://localhost:6379/0")
    assert broker_views._decode_broker_key(encoded) == "redis://localhost:6379/0"
    assert broker_views.broker_type_label("redis://localhost:6379/0") == "Redis"


def test_encode_redacts_passwords() -> None:
    encoded = broker_views.encode_broker_key("amqp://user:secret@localhost:5672//")
    assert broker_views._decode_broker_key(encoded) == "amqp://user:***@localhost:5672//"


def test_list_broker_groups(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _DummyApp("app", "redis://", "redis://backend")
    group = _DummyGroup("redis://", [app])
    registry = _DummyRegistry(group)
    now = datetime.now(UTC)
    snapshots = [
        BrokerQueueEvent(broker_url="redis://", queue="celery", messages=3, consumers=1, timestamp=now),
    ]
    workers = [Worker(hostname="alpha", status="ONLINE", broker_url="redis://", queues=["celery"])]
    db = _DummyDb(workers, snapshots)

    monkeypatch.setattr(broker_views, "get_registry", lambda: registry)
    monkeypatch.setattr(broker_views, "open_db", lambda: _open_db(db))

    groups = broker_views.list_broker_groups()
    assert groups
    assert groups[0].broker_type == "redis"


def test_broker_detail_and_purge(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _DummyApp("app", "redis://", "redis://backend")
    group = _DummyGroup("redis://", [app])
    registry = _DummyRegistry(group)
    now = datetime.now(UTC)
    snapshots = [
        BrokerQueueEvent(broker_url="redis://", queue="celery", messages=0, consumers=0, timestamp=now),
    ]
    workers = [Worker(hostname="alpha", status="ONLINE", broker_url="redis://", queues=["celery"])]
    db = _DummyDb(workers, snapshots)

    monkeypatch.setattr(broker_views, "get_registry", lambda: registry)
    monkeypatch.setattr(broker_views, "open_db", lambda: _open_db(db))
    monkeypatch.setattr(broker_views, "render", _fake_render)
    monkeypatch.setattr(broker_views, "purge_queues", lambda *_args, **_kwargs: {"celery": 0})

    factory = RequestFactory()
    detail_request = factory.get("/broker/redis/")
    response = broker_views.broker_detail(detail_request, broker_views.encode_broker_key("redis://"))
    assert response.status_code == 200

    purge_request = factory.post("/broker/purge/", {"queue": "celery", "app": "app"})
    purge_response = broker_views.broker_purge(purge_request)
    assert purge_response.status_code == 302


def test_broker_purge_idle(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _DummyApp("app", "redis://", "redis://backend")
    group = _DummyGroup("redis://", [app])
    registry = _DummyRegistry(group)
    now = datetime.now(UTC)
    snapshots = [
        BrokerQueueEvent(broker_url="redis://", queue="celery", messages=0, consumers=0, timestamp=now),
    ]
    db = _DummyDb([], snapshots)

    called: list[str] = []
    monkeypatch.setattr(broker_views, "get_registry", lambda: registry)
    monkeypatch.setattr(broker_views, "open_db", lambda: _open_db(db))
    monkeypatch.setattr(
        broker_views,
        "purge_queues",
        lambda _registry, _worker, queue=None, **_kwargs: called.append(str(queue)),
    )

    factory = RequestFactory()
    request = factory.post("/broker/purge-idle/", {"app": "app"})
    response = broker_views.broker_purge_idle(request)
    assert response.status_code == 302
    assert "celery" in called
