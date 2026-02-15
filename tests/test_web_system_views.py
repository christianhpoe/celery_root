# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Any, Self

import django
import pytest
from django.test import RequestFactory

from celery_root.components.web.views import system as system_views
from celery_root.config import CeleryRootConfig, DatabaseConfigSqlite, LoggingConfigFile, PrometheusConfig

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture(scope="module", autouse=True)
def _django_setup() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "celery_root.components.web.settings")
    if not django.apps.apps.ready:
        django.setup()


class _DummyApp:
    def __init__(self, main: str) -> None:
        self.main = main


class _DummyRegistry:
    def __init__(self, apps: list[_DummyApp]) -> None:
        self._apps = apps

    def get_apps(self) -> tuple[_DummyApp, ...]:
        return tuple(self._apps)


class _DummyResponse:
    def __init__(self, body: bytes, content_type: str) -> None:
        self._body = body
        self.headers: dict[str, str] = {"Content-Type": content_type}

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> Self:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        return None


def test_healthcheck_no_workers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(system_views, "get_registry", lambda: _DummyRegistry([]))
    request = RequestFactory().get("/healthcheck")
    response = system_views.healthcheck(request)
    assert response.status_code == 503
    payload = json.loads(response.content)
    assert payload["ok"] is False
    assert payload["error"] == "No workers configured."


def test_healthcheck_unknown_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = _DummyRegistry([_DummyApp("alpha")])
    monkeypatch.setattr(system_views, "get_registry", lambda: registry)

    def _raise(*_args: object, **_kwargs: object) -> dict[str, Any]:
        message = "missing"
        raise KeyError(message)

    monkeypatch.setattr(system_views, "health_check", _raise)
    request = RequestFactory().get("/healthcheck", {"worker": "missing"})
    response = system_views.healthcheck(request)
    assert response.status_code == 404
    payload = json.loads(response.content)
    assert payload["ok"] is False
    assert payload["error"] == "Unknown worker."


def test_healthcheck_reports_failed_check(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = _DummyRegistry([_DummyApp("alpha")])
    monkeypatch.setattr(system_views, "get_registry", lambda: registry)
    monkeypatch.setattr(
        system_views,
        "health_check",
        lambda _registry, _worker: {"broker": {"ok": True}, "backend": {"ok": False}},
    )
    request = RequestFactory().get("/healthcheck")
    response = system_views.healthcheck(request)
    assert response.status_code == 200
    payload = json.loads(response.content)
    assert payload["ok"] is False
    assert payload["worker"] == "alpha"


def test_metrics_disabled_returns_404(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config = CeleryRootConfig(
        logging=LoggingConfigFile(log_dir=tmp_path / "logs"),
        database=DatabaseConfigSqlite(db_path=tmp_path / "db.sqlite"),
        prometheus=None,
    )
    monkeypatch.setattr(system_views, "get_settings", lambda: config)
    response = system_views.metrics(RequestFactory().get("/metrics"))
    assert response.status_code == 404
    assert b"Prometheus exporter is disabled." in response.content


def test_metrics_unavailable_returns_503(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config = CeleryRootConfig(
        logging=LoggingConfigFile(log_dir=tmp_path / "logs"),
        database=DatabaseConfigSqlite(db_path=tmp_path / "db.sqlite"),
        prometheus=PrometheusConfig(port=9001, prometheus_path="/metrics"),
    )
    monkeypatch.setattr(system_views, "get_settings", lambda: config)

    def _raise(_url: str, **_kwargs: object) -> _DummyResponse:
        message = "boom"
        raise OSError(message)

    monkeypatch.setattr("celery_root.components.web.views.system.urllib.request.urlopen", _raise)
    response = system_views.metrics(RequestFactory().get("/metrics"))
    assert response.status_code == 503
    assert b"Prometheus exporter unavailable." in response.content


def test_metrics_success(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config = CeleryRootConfig(
        logging=LoggingConfigFile(log_dir=tmp_path / "logs"),
        database=DatabaseConfigSqlite(db_path=tmp_path / "db.sqlite"),
        prometheus=PrometheusConfig(port=9100, prometheus_path="/metrics"),
    )
    monkeypatch.setattr(system_views, "get_settings", lambda: config)
    seen: list[str] = []

    def _fake_urlopen(url: str, **_kwargs: object) -> _DummyResponse:
        seen.append(url)
        return _DummyResponse(b"metrics", "text/plain")

    monkeypatch.setattr("celery_root.components.web.views.system.urllib.request.urlopen", _fake_urlopen)
    response = system_views.metrics(RequestFactory().get("/metrics"))
    assert response.status_code == 200
    assert response.content == b"metrics"
    assert response.headers["Content-Type"] == "text/plain"
    assert seen == ["http://127.0.0.1:9100/metrics"]
