# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Metric component pages."""

from __future__ import annotations

from django.http import Http404, HttpRequest, HttpResponse
from django.shortcuts import render

from celery_root.components.web.components import component_snapshot

_PROMETHEUS_DISABLED = "Prometheus exporter is disabled."
_OTEL_DISABLED = "OpenTelemetry exporter is disabled."


def prometheus(request: HttpRequest) -> HttpResponse:
    """Prometheus status page."""
    snapshot = component_snapshot()
    info = snapshot.get("prometheus")
    if info is None or not info.enabled:
        raise Http404(_PROMETHEUS_DISABLED)
    return render(
        request,
        "prometheus.html",
        {
            "title": "Prometheus",
            "component": info,
        },
    )


def opentelemetry(request: HttpRequest) -> HttpResponse:
    """OpenTelemetry status page."""
    snapshot = component_snapshot()
    info = snapshot.get("open_telemetry")
    if info is None or not info.enabled:
        raise Http404(_OTEL_DISABLED)
    return render(
        request,
        "opentelemetry.html",
        {
            "title": "OpenTelemetry",
            "component": info,
        },
    )
