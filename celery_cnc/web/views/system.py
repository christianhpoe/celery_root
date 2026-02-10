"""System endpoints for the Celery CnC web app."""

from __future__ import annotations

import urllib.request
from typing import Any

from django.http import HttpRequest, HttpResponse, JsonResponse

from celery_cnc.cnc.health import health_check
from celery_cnc.config import get_settings
from celery_cnc.web.services import app_name, get_registry


def healthcheck(request: HttpRequest) -> JsonResponse:
    """Return broker/backend/worker health checks."""
    registry = get_registry()
    apps = registry.get_apps()
    if not apps:
        return JsonResponse({"ok": False, "error": "No workers configured."}, status=503)
    worker = request.GET.get("worker") or app_name(apps[0])
    try:
        checks = health_check(registry, worker)
    except KeyError:
        return JsonResponse({"ok": False, "error": "Unknown worker."}, status=404)
    ok = _all_ok(checks)
    return JsonResponse({"ok": ok, "worker": worker, "checks": checks})


def metrics(_: HttpRequest) -> HttpResponse:
    """Proxy Prometheus metrics when enabled."""
    config = get_settings()
    if not config.prometheus:
        return HttpResponse("Prometheus exporter is disabled.", status=404)
    url = f"http://127.0.0.1:{config.prometheus_port}{config.prometheus_path}"
    try:
        with urllib.request.urlopen(url, timeout=5) as response:  # noqa: S310 - local proxy
            body = response.read()
            content_type = response.headers.get("Content-Type", "text/plain; version=0.0.4")
    except OSError:
        return HttpResponse("Prometheus exporter unavailable.", status=503)
    return HttpResponse(body, content_type=content_type)


def _all_ok(checks: dict[str, Any]) -> bool:
    for value in checks.values():
        if isinstance(value, dict):
            ok = value.get("ok")
            if ok is False:
                return False
    return True
