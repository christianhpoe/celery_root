# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import json
import os

import django
import pytest
from django.http import Http404, HttpResponse
from django.test import RequestFactory

from celery_root.components.web.components import ComponentInfo
from celery_root.components.web.views import metrics as metrics_views


@pytest.fixture(scope="module", autouse=True)
def _django_setup() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "celery_root.components.web.settings")
    if not django.apps.apps.ready:
        django.setup()


def _fake_render(_request: object, _template: str, context: dict[str, object]) -> HttpResponse:
    payload = json.dumps({"title": context.get("title")})
    return HttpResponse(payload, content_type="application/json")


def test_prometheus_disabled_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(metrics_views, "component_snapshot", dict)
    request = RequestFactory().get("/prometheus/")
    with pytest.raises(Http404, match="Prometheus exporter is disabled"):
        metrics_views.prometheus(request)


def test_prometheus_enabled_renders(monkeypatch: pytest.MonkeyPatch) -> None:
    info = ComponentInfo(
        key="prometheus",
        display_name="Prometheus",
        enabled=True,
        status="up",
        pid=123,
        url="http://127.0.0.1:9001/metrics",
        config={"port": 9001, "path": "/metrics"},
    )
    monkeypatch.setattr(metrics_views, "component_snapshot", lambda: {"prometheus": info})
    monkeypatch.setattr(metrics_views, "render", _fake_render)
    request = RequestFactory().get("/prometheus/")
    response = metrics_views.prometheus(request)
    assert response.status_code == 200
    payload = json.loads(response.content)
    assert payload["title"] == "Prometheus"


def test_opentelemetry_disabled_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(metrics_views, "component_snapshot", dict)
    request = RequestFactory().get("/opentelemetry/")
    with pytest.raises(Http404, match="OpenTelemetry exporter is disabled"):
        metrics_views.opentelemetry(request)


def test_opentelemetry_enabled_renders(monkeypatch: pytest.MonkeyPatch) -> None:
    info = ComponentInfo(
        key="open_telemetry",
        display_name="OpenTelemetry",
        enabled=True,
        status="up",
        pid=456,
        config={"endpoint": "http://localhost:4317"},
    )
    monkeypatch.setattr(metrics_views, "component_snapshot", lambda: {"open_telemetry": info})
    monkeypatch.setattr(metrics_views, "render", _fake_render)
    request = RequestFactory().get("/opentelemetry/")
    response = metrics_views.opentelemetry(request)
    assert response.status_code == 200
    payload = json.loads(response.content)
    assert payload["title"] == "OpenTelemetry"
